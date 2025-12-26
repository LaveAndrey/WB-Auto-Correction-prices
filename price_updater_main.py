import asyncio
import logging
import logging.handlers
import sys
import os
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum
import traceback
import signal
import atexit
import json
import statistics

import pytz
import aiomysql
import aiohttp
from aiohttp import ClientTimeout, ClientSession
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from dotenv import load_dotenv

load_dotenv()

LOAD_PRICE_TO_WB = False


class Config:
    DB_HOST = os.getenv('DB_HOST')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_NAME = os.getenv('DB_NAME')
    DB_PORT = int(os.getenv('DB_PORT'))

    WB_SALES_TOKEN = os.getenv('WB_SALES_TOKEN')
    WB_PRICES_TOKEN = os.getenv('WB_PRICES_TOKEN')
    WB_CONTENT_TOKEN = os.getenv('WB_CONTENT_TOKEN')

    BATCH_SIZE = int(os.getenv('BATCH_SIZE'))
    WORKERS_COUNT = int(os.getenv('WORKERS_COUNT'))
    MAX_QUEUE_SIZE = int(os.getenv('MAX_QUEUE_SIZE'))

    BANK_COMMISSION = float(os.getenv('BANK_COMMISSION'))
    MIN_MARGIN_FACTOR = float(os.getenv('MIN_MARGIN_FACTOR'))
    MIN_PRICE_CHANGE = float(os.getenv('MIN_PRICE_CHANGE'))
    SALES_HOURS_FILTER = int(os.getenv('SALES_HOURS_FILTER'))
    CYCLE_INTERVAL = int(os.getenv('CYCLE_INTERVAL'))

    MIN_SALES_FOR_CALC = int(os.getenv('MIN_SALES_FOR_CALC'))
    MAX_PRICE_CHANGE_PERCENT = float(os.getenv('MAX_PRICE_CHANGE_PERCENT'))

    ANALYTICS_TABLE = os.getenv('ANALYTICS_TABLE', 'product_price_analytics')
    PRICE_HISTORY_TABLE = os.getenv('PRICE_HISTORY_TABLE', 'oc_product_price_history')


class ProcessingStatus(Enum):
    SUCCESS = "success"
    SKIPPED_NO_DATA = "skipped_no_data"
    SKIPPED_MIN_PRICE = "skipped_min_price"
    SKIPPED_MIN_CHANGE = "skipped_min_change"
    SKIPPED_INVALID = "skipped_invalid"
    ERROR = "error"


@dataclass
class SaleData:
    nm_id: int
    vendor_code: str
    finished_price: float
    price_with_desc: float
    for_pay: float
    spp_percent: float
    discount_percent: float
    total_price: float
    date: str
    quantity: int = 1

    @classmethod
    def from_api_dict(cls, data: Dict) -> Optional['SaleData']:
        try:
            price_with_desc = data.get('priceWithDisc') or data.get('price_with_desc') or 0
            discount_percent = data.get('discountPercent') or data.get('discount') or 0
            total_price = data.get('totalPrice') or 0

            return cls(
                nm_id=data.get('nmId', 0),
                vendor_code=str(data.get('supplierArticle', '')).strip(),
                finished_price=float(data.get('finishedPrice', 0)),
                price_with_desc=float(price_with_desc),
                for_pay=float(data.get('forPay', 0)),
                spp_percent=float(data.get('spp', 0)),
                discount_percent=float(discount_percent),
                total_price=float(total_price),
                date=data.get('lastChangeDate', ''),
                quantity=data.get('quantity', 1)
            )
        except (ValueError, TypeError):
            return None


class DatabaseLogger:
    def __init__(self, db_pool):
        self.db_pool = db_pool
        self.cycle_id = 0

    async def set_cycle_id(self, cycle_id: int):
        self.cycle_id = cycle_id

    async def log(self, level: str, message: str, vendor_code: str = None, details: dict = None):
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        INSERT INTO oc_price_updater_logs 
                        (level, vendor_code, message, details, cycle_id, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (level, vendor_code, message,
                          json.dumps(details) if details else None,
                          self.cycle_id,
                          datetime.now(pytz.timezone('Europe/Moscow'))))
        except Exception as e:
            logging.getLogger('price_updater').error(f"Ошибка записи в лог БД: {e}")


@dataclass
class ProductData:
    vendor_code: str
    purchase_price: float
    target_profit: float
    current_price_wb: float
    current_real_price: float
    sku_wb: int = 0
    status: int = 1

    @classmethod
    def from_db_row(cls, row: Dict) -> Optional['ProductData']:
        try:
            return cls(
                vendor_code=str(row['model']),
                purchase_price=float(row['purchase_price']),
                target_profit=float(row['target_profit_rub']),
                current_price_wb=float(row.get('price_wb', 0) or 0),
                current_real_price=float(row.get('wb_real_price', 0) or 0),
                sku_wb=int(row.get('sku_wb', 0) or 0),
                status=int(row.get('status', 1))
            )
        except (ValueError, TypeError, KeyError):
            return None


@dataclass
class PriceUpdate:
    vendor_code: str
    new_price_wb: float
    new_real_price: float
    old_price_wb: float
    profit_correction: float
    finished_price: float
    status: ProcessingStatus
    error_msg: str = ""
    discount: Optional[float] = None
    sku_wb: int = 0
    logistics_cost: float = 0
    current_profit: float = 0
    target_profit: float = 0
    sales_count: int = 0
    spp_used: float = 0
    purchase_price: float = 0
    old_real_price: float = 0

    @property
    def reason(self) -> str:
        if self.error_msg:
            return self.error_msg

        if self.status == ProcessingStatus.SUCCESS:
            direction = "↑" if self.new_price_wb > self.old_price_wb else "↓"
            profit_diff = self.target_profit - self.current_profit
            return f"Цена {direction} на {abs(self.profit_correction):.0f} ₽ (прибыль: {self.current_profit:.0f} → {self.target_profit:.0f} ₽)"

        status_reasons = {
            ProcessingStatus.SKIPPED_NO_DATA: "Недостаточно данных о продажах",
            ProcessingStatus.SKIPPED_MIN_PRICE: "Цена ниже минимальной",
            ProcessingStatus.SKIPPED_MIN_CHANGE: "Изменение меньше порога",
            ProcessingStatus.SKIPPED_INVALID: "Некорректное значение",
            ProcessingStatus.ERROR: "Ошибка обработки"
        }
        return status_reasons.get(self.status, str(self.status.value))


class PriceUpdater:
    def __init__(self):
        self.logger = self._setup_logging()
        self.db_pool = None
        self.session = None
        self.db_logger = None
        self.is_running = False
        self.queue = None
        self.stats = defaultdict(int)
        self.successful_updates = []
        self.current_cycle = 0

        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)
        atexit.register(self.cleanup)

    def _setup_logging(self) -> logging.Logger:
        logger = logging.getLogger('price_updater')
        logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        file_handler = logging.handlers.RotatingFileHandler(
            'price_updater.log',
            maxBytes=10 * 1024 * 1024,
            backupCount=10,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger

    async def initialize(self):
        self.logger.info("Инициализация системы ценообразования...")

        try:
            self.db_pool = await aiomysql.create_pool(
                host=Config.DB_HOST,
                user=Config.DB_USER,
                password=Config.DB_PASSWORD,
                db=Config.DB_NAME,
                port=Config.DB_PORT,
                autocommit=True,
                charset='utf8mb4',
                minsize=3,
                maxsize=Config.WORKERS_COUNT * 2,
                pool_recycle=3600
            )
            self.logger.info("✓ БД подключена")
            self.db_logger = DatabaseLogger(self.db_pool)

        except Exception as e:
            self.logger.error(f"✗ БД недоступна: {e}")
            raise

        timeout = ClientTimeout(total=60)
        self.session = ClientSession(timeout=timeout)
        self.queue = asyncio.Queue(maxsize=Config.MAX_QUEUE_SIZE)
        self.logger.info("Инициализация завершена")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def fetch_wb_sales(self) -> List[SaleData]:
        moscow_tz = pytz.timezone('Europe/Moscow')
        period_start = datetime.now(moscow_tz) - timedelta(hours=Config.SALES_HOURS_FILTER)
        date_from = period_start.strftime("%Y-%m-%dT%H:%M:%S")

        url = "https://statistics-api.wildberries.ru/api/v1/supplier/sales"
        headers = {
            "Authorization": Config.WB_SALES_TOKEN,
            "Accept": "application/json"
        }
        params = {
            "dateFrom": date_from,
        }

        try:
            async with self.session.get(url, headers=headers, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.logger.info(f"Получено {len(data)} записей от WB API с {date_from}")

                    sales = []
                    for item in data:
                        if item.get("isRealization"):
                            sale = SaleData.from_api_dict(item)
                            if sale and sale.finished_price > 0:
                                sales.append(sale)

                    self.logger.info(f"Актуальных продаж: {len(sales)}")
                    return sales
                else:
                    text = await resp.text()
                    self.logger.error(f"Ошибка API WB: {resp.status} - {text[:200]}")
                    return []

        except Exception as e:
            self.logger.error(f"Ошибка при запросе к WB API: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def get_logistics_by_sa_name(self, sa_names: List[str]) -> Dict[str, float]:
        if not sa_names:
            return {}

        moscow_tz = pytz.timezone('Europe/Moscow')
        date_from = (datetime.now(moscow_tz) - timedelta(days=90)).strftime("%Y-%m-%d")
        date_to = datetime.now(moscow_tz).strftime("%Y-%m-%d")

        url = "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"
        headers = {"Authorization": Config.WB_SALES_TOKEN}

        all_records = []
        next_rrdid = 0
        page = 1

        try:
            while True:
                params = {
                    "dateFrom": date_from,
                    "dateTo": date_to,
                    "rrdid": next_rrdid,
                    "limit": 100000
                }

                async with self.session.get(url, headers=headers, params=params) as resp:
                    if resp.status == 204:
                        break

                    if resp.status != 200:
                        break

                    data = await resp.json()
                    all_records.extend(data)

                    if not data:
                        break

                    last_record = data[-1]
                    next_rrdid = last_record.get('rrd_id') or last_record.get('rrdId') or 0
                    if next_rrdid == 0:
                        break

                    page += 1
                    await asyncio.sleep(0.2)

        except Exception as e:
            self.logger.error(f"Ошибка загрузки: {e}")
            return {}

        sorted_records = sorted(all_records,
                                key=lambda x: x.get('rr_dt', '') or x.get('create_dt', ''),
                                reverse=True)

        last_logistics = {}
        processed_sa_names = set()

        for record in sorted_records:
            sa_name = record.get('sa_name')

            if not sa_name or sa_name not in sa_names or sa_name in processed_sa_names:
                continue

            delivery_amount = record.get('delivery_amount', 0)
            return_amount = record.get('return_amount', 0)
            delivery_rub = record.get('delivery_rub', 0)

            is_main_logistics = (
                    delivery_amount > 0 and
                    delivery_rub > 0 and
                    return_amount == 0
            )

            if is_main_logistics:
                last_logistics[sa_name] = delivery_rub
                processed_sa_names.add(sa_name)

                if len(last_logistics) >= len(sa_names):
                    break

        return last_logistics

    def _get_last_non_zero_spp(self, sales: List[SaleData]) -> float:
        try:
            sorted_sales = sorted(sales, key=lambda x: x.date, reverse=True)

            for sale in sorted_sales:
                if sale.spp_percent > 0:
                    return sale.spp_percent

            return 0.0
        except Exception:
            return 0.0

    async def fetch_products_batch(self, vendor_codes: List[str]) -> Dict[str, ProductData]:
        if not vendor_codes:
            return {}

        product_map = {}

        for i in range(0, len(vendor_codes), Config.BATCH_SIZE):
            batch = vendor_codes[i:i + Config.BATCH_SIZE]
            placeholders = ', '.join(['%s'] * len(batch))

            try:
                async with self.db_pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cursor:
                        await cursor.execute(f"""
                            SELECT model, purchase_price, target_profit_rub, 
                                   price_wb, wb_real_price, sku_wb, status
                            FROM oc_product
                            WHERE model IN ({placeholders})
                              AND purchase_price > 0
                              AND target_profit_rub > 0
                              AND status = 1
                        """, batch)

                        rows = await cursor.fetchall()
                        self.logger.info(f"Загружено {len(rows)} товаров из БД")

                        for row in rows:
                            product = ProductData.from_db_row(row)
                            if product:
                                product_map[product.vendor_code] = product

            except Exception as e:
                self.logger.error(f"Ошибка загрузки батча: {e}")
                continue

        return product_map

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def fetch_nm_id(self, vendor_code: str) -> int:
        url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
        headers = {
            "Authorization": Config.WB_CONTENT_TOKEN,
            "Content-Type": "application/json"
        }

        body = {
            "settings": {
                "cursor": {"limit": 100},
                "filter": {
                    "withPhoto": -1,
                    "textSearch": str(vendor_code).strip()
                }
            }
        }

        try:
            async with self.session.post(url, headers=headers, json=body) as resp:
                response_text = await resp.text()

                if resp.status == 200:
                    try:
                        data = json.loads(response_text)
                    except json.JSONDecodeError:
                        self.logger.error(f"API вернул невалидный JSON: {response_text[:200]}")
                        return 0

                    cards = data.get("cards", [])
                    if not cards:
                        return 0

                    for card in cards:
                        card_vendor_code = str(card.get("vendorCode", "")).strip()
                        if card_vendor_code == str(vendor_code).strip():
                            nm_id = card.get("nmID", 0)
                            if nm_id:
                                return nm_id

                    return 0

                elif resp.status == 401:
                    self.logger.error("Ошибка авторизации: неверный токен Content API")
                    return 0
                else:
                    self.logger.error(f"Ошибка API: {resp.status}. Ответ: {response_text[:500]}")
                    return 0

        except asyncio.TimeoutError:
            self.logger.error(f"Таймаут при поиске '{vendor_code}'")
            return 0
        except Exception as e:
            self.logger.error(f"Ошибка при поиске '{vendor_code}': {str(e)}")
            return 0

    async def process_product_new_logic(self, vendor_code: str,
                                        sales: List[SaleData],
                                        product: ProductData,
                                        logistics_cost: float) -> PriceUpdate:
        try:
            valid_sales = []
            for sale in sales:
                if (sale.price_with_desc > 0 and
                        sale.discount_percent > 0 and
                        sale.for_pay > 0):
                    valid_sales.append(sale)

            if len(valid_sales) < Config.MIN_SALES_FOR_CALC:
                await self.db_logger.log(
                    level="WARNING",
                    message=f"Недостаточно данных для расчета",
                    vendor_code=vendor_code,
                    details={"valid_sales": len(valid_sales), "required": Config.MIN_SALES_FOR_CALC}
                )
                return PriceUpdate(
                    vendor_code=vendor_code,
                    new_price_wb=0,
                    new_real_price=0,
                    old_price_wb=product.current_price_wb,
                    profit_correction=0,
                    status=ProcessingStatus.SKIPPED_NO_DATA,
                    error_msg=f"Недостаточно данных: {len(valid_sales)}",
                    sku_wb=product.sku_wb,
                    logistics_cost=logistics_cost,
                    finished_price=0
                )

            if product.sku_wb == 0:
                new_nm_id = await self.fetch_nm_id(vendor_code)
                if new_nm_id == 0:
                    await self.db_logger.log(
                        level="ERROR",
                        message=f"Не удалось получить nmID",
                        vendor_code=vendor_code
                    )
                    return PriceUpdate(
                        vendor_code=vendor_code,
                        new_price_wb=0,
                        new_real_price=0,
                        old_price_wb=product.current_price_wb,
                        profit_correction=0,
                        status=ProcessingStatus.ERROR,
                        error_msg="Не удалось получить nmID",
                        sku_wb=0,
                        logistics_cost=logistics_cost,
                        finished_price=0
                    )
                await self.save_nm_id_to_db(vendor_code, new_nm_id)
                product.sku_wb = new_nm_id

            price_wd_list = []
            net_forpay_list = []
            discount_list = []

            last_spp = self._get_last_non_zero_spp(valid_sales)

            for sale in valid_sales:
                net_forpay = sale.for_pay - logistics_cost
                price_wd_list.append(sale.price_with_desc)
                net_forpay_list.append(net_forpay)
                discount_list.append(sale.discount_percent)

            avg_price_wd = statistics.mean(price_wd_list)
            avg_net_forpay = statistics.mean(net_forpay_list)
            avg_discount = statistics.median(discount_list)

            bank_commission = avg_net_forpay * Config.BANK_COMMISSION
            current_profit = avg_net_forpay - bank_commission - product.purchase_price
            profit_diff = product.target_profit - current_profit

            new_price_wd = avg_price_wd + profit_diff

            if last_spp > 0:
                finished_price = new_price_wd * (1 - (last_spp / 100))
            else:
                finished_price = new_price_wd

            min_price = product.purchase_price * Config.MIN_MARGIN_FACTOR
            if new_price_wd < min_price:
                self.logger.warning(f"Новая цена ниже минимальной: {new_price_wd:.0f} < {min_price:.0f}")
                await self.db_logger.log(
                    level="WARNING",
                    message=f"Цена ниже минимальной",
                    vendor_code=vendor_code,
                    details={
                        "new_price": new_price_wd,
                        "min_price": min_price,
                        "adjustment": "приведено к минимальной"
                    }
                )
                new_price_wd = min_price
                profit_diff = new_price_wd - avg_price_wd

            if avg_discount >= 100:
                avg_discount = 99.9
            if avg_discount <= 0:
                avg_discount = 0.1

            new_total_price = new_price_wd / (1 - avg_discount / 100)
            new_total_price_rounded = round(new_total_price, 0)

            validation = self._validate_price_update(
                vendor_code, product, new_price_wd, new_total_price_rounded, profit_diff
            )
            if validation:
                validation.logistics_cost = logistics_cost
                await self.db_logger.log(
                    level="WARNING",
                    message=f"Валидация не пройдена: {validation.status.value}",
                    vendor_code=vendor_code,
                    details={"status": validation.status.value, "reason": validation.reason}
                )
                return validation

            update = PriceUpdate(
                vendor_code=vendor_code,
                new_price_wb=new_total_price_rounded,
                new_real_price=round(new_price_wd, 2),
                old_price_wb=product.current_price_wb,
                old_real_price=product.current_real_price,
                profit_correction=abs(profit_diff),
                status=ProcessingStatus.SUCCESS,
                error_msg=f"Корректировка прибыли: {profit_diff:+.0f} ₽",
                discount=avg_discount,
                sku_wb=product.sku_wb,
                logistics_cost=logistics_cost,
                finished_price=finished_price,
                current_profit=current_profit,
                target_profit=product.target_profit,
                sales_count=len(valid_sales),
                spp_used=last_spp,
                purchase_price=product.purchase_price
            )

            await self.db_logger.log(
                level="SUCCESS",
                message=f"Цена успешно рассчитана",
                vendor_code=vendor_code,
                details={
                    "old_price": product.current_price_wb,
                    "new_price": new_total_price_rounded,
                    "price_change": new_total_price_rounded - product.current_price_wb,
                    "profit_correction": profit_diff,
                    "finished_price": finished_price,
                    "discount": avg_discount
                }
            )

            return update

        except Exception as e:
            error_msg = f"Ошибка в логике для {vendor_code}: {e}"
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())

            await self.db_logger.log(
                level="ERROR",
                message=f"Ошибка обработки",
                vendor_code=vendor_code,
                details={"error": str(e), "traceback": traceback.format_exc()[-500:]}
            )

            return PriceUpdate(
                vendor_code=vendor_code,
                new_price_wb=0,
                new_real_price=0,
                old_price_wb=product.current_price_wb,
                profit_correction=0,
                status=ProcessingStatus.ERROR,
                error_msg=str(e),
                sku_wb=product.sku_wb,
                logistics_cost=logistics_cost,
                finished_price=0
            )

    def _validate_price_update(self, vendor_code: str,
                               product: ProductData,
                               new_price_wd: float,
                               new_total_price: float,
                               profit_diff: float) -> Optional[PriceUpdate]:
        min_price = product.purchase_price * Config.MIN_MARGIN_FACTOR
        if new_price_wd < min_price:
            return PriceUpdate(
                vendor_code=vendor_code,
                new_price_wb=0,
                new_real_price=new_price_wd,
                old_price_wb=product.current_price_wb,
                old_real_price=product.current_real_price,
                profit_correction=profit_diff,
                status=ProcessingStatus.SKIPPED_MIN_PRICE,
                error_msg=f"Цена ниже минимальной: {new_price_wd:.0f} < {min_price:.0f}",
                sku_wb=product.sku_wb,
                finished_price=0,
                current_profit=0,
                target_profit=product.target_profit,
                sales_count=0,
                spp_used=0,
                purchase_price=product.purchase_price
            )

        price_change = abs(new_total_price - product.current_price_wb)
        if price_change < Config.MIN_PRICE_CHANGE:
            return PriceUpdate(
                vendor_code=vendor_code,
                new_price_wb=0,
                new_real_price=new_price_wd,
                old_price_wb=product.current_price_wb,
                old_real_price=product.current_real_price,
                profit_correction=profit_diff,
                status=ProcessingStatus.SKIPPED_MIN_CHANGE,
                error_msg=f"Изменение меньше порога: {price_change:.0f} < {Config.MIN_PRICE_CHANGE}",
                sku_wb=product.sku_wb,
                finished_price=0,
                current_profit=0,
                target_profit=product.target_profit,
                sales_count=0,
                spp_used=0,
                purchase_price=product.purchase_price
            )

        if product.current_price_wb > 0:
            price_change_percent = abs((new_total_price - product.current_price_wb) / product.current_price_wb) * 100
            if price_change_percent > Config.MAX_PRICE_CHANGE_PERCENT:
                return PriceUpdate(
                    vendor_code=vendor_code,
                    new_price_wb=0,
                    new_real_price=new_price_wd,
                    old_price_wb=product.current_price_wb,
                    old_real_price=product.current_real_price,
                    profit_correction=profit_diff,
                    status=ProcessingStatus.SKIPPED_INVALID,
                    error_msg=f"Изменение превышает лимит: {price_change_percent:.1f}% > {Config.MAX_PRICE_CHANGE_PERCENT}%",
                    sku_wb=product.sku_wb,
                    finished_price=0,
                    current_profit=0,
                    target_profit=product.target_profit,
                    sales_count=0,
                    spp_used=0,
                    purchase_price=product.purchase_price
                )

        return None

    async def save_nm_id_to_db(self, vendor_code: str, nm_id: int) -> bool:
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        UPDATE oc_product 
                        SET sku_wb = %s
                        WHERE model = %s
                    """, (nm_id, vendor_code))

                    self.logger.info(f"nmID {nm_id} сохранен в БД для {vendor_code}")
                    return True

        except Exception as e:
            self.logger.error(f"Ошибка сохранения nmID для {vendor_code}: {e}")
            return False

    async def save_price_update(self, update: PriceUpdate):
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    current_time = datetime.now(pytz.timezone('Europe/Moscow'))

                    await cur.execute("""
                        UPDATE oc_product 
                        SET price_wb = %s, 
                            wb_real_price = %s,
                            sku_wb = %s,
                            last_price_update = %s
                        WHERE model = %s
                    """, (
                        update.new_price_wb,
                        update.new_real_price,
                        update.sku_wb,
                        current_time,
                        update.vendor_code
                    ))

                    await cur.execute("""
                        INSERT INTO oc_product_price_history 
                        (product_id, vendor_code, old_price_wb, new_price_wb,
                         old_real_price, new_real_price, profit_correction, avg_finished_price,
                         discount, change_reason, status, created_at,
                         current_margin, target_margin, sales_count, spp_used,
                         purchase_price, logistics_cost)
                        SELECT 
                            p.product_id, 
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                        FROM oc_product p
                        WHERE p.model = %s
                    """, (
                        update.vendor_code,
                        update.old_price_wb,
                        update.new_price_wb,
                        update.old_real_price,
                        update.new_real_price,
                        update.profit_correction,
                        update.finished_price,
                        update.discount or 0,
                        update.reason,
                        update.status.value,
                        current_time,
                        update.current_profit,
                        update.target_profit,
                        update.sales_count,
                        update.spp_used,
                        update.purchase_price,
                        update.logistics_cost,
                        update.vendor_code,
                    ))

                    self.stats['prices_updated'] += 1

                    profit_diff = update.target_profit - update.current_profit
                    await self.db_logger.log(
                        level="SUCCESS",
                        message=f"Цена сохранена с деталями прибыли",
                        vendor_code=update.vendor_code,
                        details={
                            "old_price": update.old_price_wb,
                            "new_price": update.new_price_wb,
                            "price_change": update.new_price_wb - update.old_price_wb,
                            "current_profit": f"{update.current_profit:.0f} ₽",
                            "target_profit": f"{update.target_profit:.0f} ₽",
                            "profit_diff": f"{profit_diff:+.0f} ₽",
                            "sales_used": update.sales_count,
                            "spp_used": f"{update.spp_used:.1f}%",
                            "logistics": f"{update.logistics_cost:.0f} ₽",
                            "purchase_price": f"{update.purchase_price:.0f} ₽"
                        }
                    )

                    self.logger.info(f"Цена и детальная история обновлены: {update.vendor_code}")

        except Exception as e:
            error_msg = f"Ошибка сохранения цены для {update.vendor_code}: {e}"
            self.logger.error(error_msg)

            await self.db_logger.log(
                level="ERROR",
                message=error_msg,
                vendor_code=update.vendor_code,
                details={
                    "error": str(e),
                    "traceback": traceback.format_exc()[-500:],
                    "cycle_id": self.current_cycle
                }
            )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def upload_prices_to_wb(self, updates: List[PriceUpdate]):
        data = []
        for update in updates:
            if update.sku_wb > 0 and update.discount is not None:
                data.append({
                    "nmID": update.sku_wb,
                    "price": int(update.new_price_wb),
                    "discount": int(update.discount)
                })

        if not data:
            self.logger.info("Нет данных для отправки на WB")
            return

        url = "https://discounts-prices-api.wildberries.ru/api/v2/upload/task"
        headers = {
            "Authorization": Config.WB_PRICES_TOKEN,
            "Content-Type": "application/json"
        }

        self.logger.info(f"Отправка {len(data)} цен на WB API...")

        try:
            async with self.session.post(url, headers=headers, json={"data": data}) as resp:
                if resp.status == 200:
                    res = await resp.json()
                    task_id = res.get('data', {}).get('id')
                    self.logger.info(f"Цены отправлены на WB: ID задачи={task_id}")
                    self.stats['prices_uploaded_to_wb'] = len(data)

                    await self.db_logger.log(
                        level="SUCCESS",
                        message=f"Цены отправлены на WB",
                        details={
                            "count": len(data),
                            "task_id": task_id,
                            "cycle_id": self.current_cycle
                        }
                    )
                else:
                    text = await resp.text()
                    error_msg = f"Ошибка отправки цен на WB: {resp.status} - {text[:200]}"
                    self.logger.error(error_msg)

                    await self.db_logger.log(
                        level="ERROR",
                        message=error_msg,
                        details={
                            "status_code": resp.status,
                            "response": text[:500],
                            "cycle_id": self.current_cycle
                        }
                    )
        except Exception as e:
            error_msg = f"Ошибка при отправке цен на WB: {e}"
            self.logger.error(error_msg)

            await self.db_logger.log(
                level="ERROR",
                message=error_msg,
                details={
                    "error": str(e),
                    "cycle_id": self.current_cycle
                }
            )

    async def worker(self, worker_id: int):
        try:
            while self.is_running:
                try:
                    vendor_code, sales, product, logistics_cost = await asyncio.wait_for(
                        self.queue.get(),
                        timeout=1.0
                    )

                    update = await self.process_product_new_logic(vendor_code, sales, product, logistics_cost)

                    if update.status == ProcessingStatus.SUCCESS:
                        await self.save_price_update(update)
                        self.successful_updates.append(update)

                    self.stats[update.status.value] += 1
                    self.queue.task_done()

                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    self.logger.error(f"Ошибка в воркере {worker_id}: {e}")
                    await self.db_logger.log(
                        level="ERROR",
                        message=f"Ошибка в воркере",
                        details={
                            "worker_id": worker_id,
                            "error": str(e),
                            "cycle_id": self.current_cycle
                        }
                    )

        except asyncio.CancelledError:
            self.logger.info(f"Воркер {worker_id} остановлен")

    async def run_cycle(self):
        cycle_start = datetime.now()
        self.current_cycle += 1

        await self.db_logger.set_cycle_id(self.current_cycle)

        await self.db_logger.log(
            level="INFO",
            message=f"Начало цикла #{self.current_cycle}",
            details={"cycle_id": self.current_cycle}
        )

        self.stats = defaultdict(int)
        self.successful_updates = []

        try:
            self.logger.info("Получение данных о продажах...")
            sales_data = await self.fetch_wb_sales()

            if not sales_data:
                self.logger.warning("Нет данных о продажах")
                await self.db_logger.log(
                    level="WARNING",
                    message="Нет данных о продажах",
                    details={"cycle_id": self.current_cycle}
                )
                return

            sa_names_from_sales = []
            for sale in sales_data:
                if sale.vendor_code and sale.vendor_code not in sa_names_from_sales:
                    sa_names_from_sales.append(sale.vendor_code)

            self.logger.info(f"Найдено {len(sa_names_from_sales)} уникальных sa_name в продажах")

            logistics_by_sa_name = await self.get_logistics_by_sa_name(sa_names_from_sales)

            sales_by_vendor = defaultdict(list)
            for sale in sales_data:
                if sale.vendor_code:
                    sales_by_vendor[sale.vendor_code].append(sale)

            vendor_codes = list(sales_by_vendor.keys())
            self.logger.info(f"Найдено {len(vendor_codes)} артикулов с продажами")

            product_map = await self.fetch_products_batch(vendor_codes)
            self.logger.info(f"Загружено {len(product_map)} товаров из БД")

            if not product_map:
                self.logger.warning("Нет товаров для обработки")
                await self.db_logger.log(
                    level="WARNING",
                    message="Нет товаров для обработки в БД",
                    details={"cycle_id": self.current_cycle}
                )
                return

            queue_tasks = 0

            for vendor_code in vendor_codes:
                if vendor_code in product_map:
                    product = product_map[vendor_code]
                    logistics_cost = logistics_by_sa_name.get(vendor_code, 0)

                    await self.queue.put((vendor_code,
                                          sales_by_vendor[vendor_code],
                                          product,
                                          logistics_cost))
                    queue_tasks += 1

            self.logger.info(f"В очередь добавлено {queue_tasks} задач")

            workers = []
            for i in range(Config.WORKERS_COUNT):
                worker_task = asyncio.create_task(self.worker(i))
                workers.append(worker_task)

            await self.queue.join()

            for worker_task in workers:
                worker_task.cancel()

            await asyncio.gather(*workers, return_exceptions=True)

            if LOAD_PRICE_TO_WB and self.successful_updates:
                self.logger.info("Отправка обновлений цен на WB...")
                await self.upload_prices_to_wb(self.successful_updates)
            else:
                self.logger.info("Отправка цен на WB отключена (LOAD_PRICE_TO_WB=False)")

            cycle_end = datetime.now()
            duration = (cycle_end - cycle_start).total_seconds()

            self.logger.info("=" * 80)
            self.logger.info("СТАТИСТИКА ЦИКЛА:")
            self.logger.info(f"   Всего артикулов: {len(vendor_codes)}")
            self.logger.info(f"   Товаров в БД: {len(product_map)}")
            self.logger.info(f"   Успешно обработано: {self.stats.get('success', 0)}")
            self.logger.info(f"   Обновлено цен: {self.stats.get('prices_updated', 0)}")
            self.logger.info(f"   Отправлено на WB: {self.stats.get('prices_uploaded_to_wb', 0)}")
            self.logger.info(f"   Пропущено (мало данных): {self.stats.get('skipped_no_data', 0)}")
            self.logger.info(f"   Пропущено (низкая цена): {self.stats.get('skipped_min_price', 0)}")
            self.logger.info(f"   Пропущено (мало изменений): {self.stats.get('skipped_min_change', 0)}")
            self.logger.info(f"   Ошибок: {self.stats.get('error', 0)}")
            self.logger.info(f"   Время выполнения: {duration:.2f} сек")
            self.logger.info("=" * 80)

            await self.db_logger.log(
                level="INFO",
                message=f"Цикл #{self.current_cycle} завершен",
                details={
                    "duration_sec": duration,
                    "vendor_codes": len(vendor_codes),
                    "products_in_db": len(product_map),
                    "success": self.stats.get('success', 0),
                    "prices_updated": self.stats.get('prices_updated', 0),
                    "prices_uploaded_to_wb": self.stats.get('prices_uploaded_to_wb', 0),
                    "skipped_no_data": self.stats.get('skipped_no_data', 0),
                    "skipped_min_price": self.stats.get('skipped_min_price', 0),
                    "skipped_min_change": self.stats.get('skipped_min_change', 0),
                    "errors": self.stats.get('error', 0)
                }
            )

        except Exception as e:
            error_msg = f"Критическая ошибка в цикле: {e}"
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())

            await self.db_logger.log(
                level="ERROR",
                message=error_msg,
                details={
                    "error": str(e),
                    "traceback": traceback.format_exc()[-500:],
                    "cycle_id": self.current_cycle
                }
            )

    async def run(self):
        self.is_running = True
        cycle_count = 0

        while self.is_running:
            cycle_count += 1
            self.logger.info(f"\n{'#' * 80}")
            self.logger.info(f"ЦИКЛ #{cycle_count}")
            self.logger.info(f"{'#' * 80}")

            try:
                await self.run_cycle()

                hours = Config.CYCLE_INTERVAL // 3600
                minutes = (Config.CYCLE_INTERVAL % 3600) // 60
                self.logger.info(f"Ожидание следующего цикла ({hours}ч {minutes}мин)...")
                await asyncio.sleep(Config.CYCLE_INTERVAL)

            except KeyboardInterrupt:
                self.logger.info("Остановка по запросу пользователя")
                await self.db_logger.log(
                    level="INFO",
                    message="Остановка по запросу пользователя",
                    details={"cycle_id": self.current_cycle}
                )
                break
            except Exception as e:
                error_msg = f"Фатальная ошибка: {e}"
                self.logger.error(error_msg)
                self.logger.error(traceback.format_exc())

                await self.db_logger.log(
                    level="ERROR",
                    message=error_msg,
                    details={
                        "error": str(e),
                        "traceback": traceback.format_exc()[-500:],
                        "cycle_id": self.current_cycle
                    }
                )

                wait_time = min(300 * (2 ** (cycle_count % 5)), 3600)
                self.logger.info(f"Пауза {wait_time} сек перед повторной попыткой...")
                await asyncio.sleep(wait_time)

    def _handle_shutdown(self, signum, frame):
        signal_name = {signal.SIGTERM: 'SIGTERM', signal.SIGINT: 'SIGINT'}.get(signum, str(signum))
        self.logger.info(f"Получен сигнал {signal_name}, завершение...")
        self.is_running = False

    async def cleanup(self):
        self.logger.info("Очистка ресурсов...")

        await self.db_logger.log(
            level="INFO",
            message="Завершение работы системы",
            details={"cycle_id": self.current_cycle}
        )

        if self.session and not self.session.closed:
            await self.session.close()
            self.logger.info("HTTP сессия закрыта")

        if self.db_pool:
            self.db_pool.close()
            await self.db_pool.wait_closed()
            self.logger.info("Пул БД закрыт")

        self.logger.info("Очистка завершена")


async def main():
    updater = PriceUpdater()

    try:
        await updater.initialize()
        await updater.run()
    except Exception as e:
        updater.logger.critical(f"Фатальная ошибка: {e}")
        updater.logger.critical(traceback.format_exc())
        sys.exit(1)
    finally:
        await updater.cleanup()


if __name__ == "__main__":
    missing = []
    if not Config.WB_SALES_TOKEN:
        missing.append("WB_SALES_TOKEN")
    if not Config.WB_PRICES_TOKEN:
        missing.append("WB_PRICES_TOKEN")
    if not Config.WB_CONTENT_TOKEN:
        missing.append("WB_CONTENT_TOKEN")
    if missing:
        print(f"ОШИБКА: Не установлены переменные: {', '.join(missing)}")
        sys.exit(1)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nПрограмма остановлена пользователем")
        sys.exit(0)