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

# Загрузка конфигурации
load_dotenv = None
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    print("Предупреждение: python-dotenv не установлен. Используются переменные окружения.")

LOAD_PRICE_TO_WB = True


# === КОНФИГУРАЦИЯ ===
class Config:
    # Database
    DB_HOST = os.getenv('DB_HOST')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_NAME = os.getenv('DB_NAME')
    DB_PORT = int(os.getenv('DB_PORT'))

    # WB API
    WB_SALES_TOKEN = os.getenv('WB_SALES_TOKEN')
    WB_PRICES_TOKEN = os.getenv('WB_PRICES_TOKEN')
    WB_CONTENT_TOKEN = os.getenv('WB_CONTENT_TOKEN')

    # Processing
    BATCH_SIZE = int(os.getenv('BATCH_SIZE'))
    WORKERS_COUNT = int(os.getenv('WORKERS_COUNT'))
    MAX_QUEUE_SIZE = int(os.getenv('MAX_QUEUE_SIZE'))

    # Business logic
    BANK_COMMISSION = float(os.getenv('BANK_COMMISSION'))
    MIN_MARGIN_FACTOR = float(os.getenv('MIN_MARGIN_FACTOR'))
    MIN_PRICE_CHANGE = float(os.getenv('MIN_PRICE_CHANGE'))
    SALES_HOURS_FILTER = int(os.getenv('SALES_HOURS_FILTER'))
    CYCLE_INTERVAL = int(os.getenv('CYCLE_INTERVAL'))

    # Validation
    MIN_SALES_FOR_CALC = int(os.getenv('MIN_SALES_FOR_CALC'))
    MAX_PRICE_CHANGE_PERCENT = float(os.getenv('MAX_PRICE_CHANGE_PERCENT'))

    # Аналитика
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
    """Структурированные данные о продаже"""
    nm_id: int
    vendor_code: str
    finished_price: float  # finishedPrice - что заплатил покупатель
    price_with_desc: float  # priceWithDisc - цена на витрине (ВАЖНО!)
    for_pay: float  # forPay - что получили мы
    spp_percent: float  # spp - СПП (не важно для расчета)
    discount_percent: float  # discountPercent - скидка покупателя (ВАЖНО!)
    total_price: float  # totalPrice - базовая цена (ВАЖНО!)
    date: str
    quantity: int = 1

    @classmethod
    def from_api_dict(cls, data: Dict) -> Optional['SaleData']:
        """Создание из сырых данных API"""
        try:
            # ВАЖНО: проверяем наличие нужных полей в API
            price_with_desc = data.get('priceWithDisc') or data.get('price_with_desc') or 0
            discount_percent = data.get('discountPercent') or data.get('discount') or 0
            total_price = data.get('totalPrice') or 0

            return cls(
                nm_id=data.get('nmId', 0),
                vendor_code=str(data.get('supplierArticle', '')).strip(),
                finished_price=float(data.get('finishedPrice', 0)),
                price_with_desc=float(price_with_desc),  # цена на витрине
                for_pay=float(data.get('forPay', 0)),
                spp_percent=float(data.get('spp', 0)),
                discount_percent=float(discount_percent),  # скидка покупателя
                total_price=float(total_price),  # базовая цена
                date=data.get('lastChangeDate', ''),
                quantity=data.get('quantity', 1)
            )
        except (ValueError, TypeError) as e:
            return None


@dataclass
class ProductData:
    """Структурированные данные о товаре"""
    vendor_code: str
    purchase_price: float
    target_profit: float
    current_price_wb: float  # базовая цена в БД (totalPrice)
    current_real_price: float  # цена на витрине в БД (priceWithDisc)
    sku_wb: int = 0
    status: int = 1

    @classmethod
    def from_db_row(cls, row: Dict) -> Optional['ProductData']:
        """Создание из строки БД"""
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
    new_price_wb: float  # Новая базовая цена (totalPrice)
    new_real_price: float  # Новая цена на витрине (priceWithDesc)
    old_price_wb: float
    profit_correction: float
    status: ProcessingStatus
    error_msg: str = ""
    discount: Optional[float] = None  # discountPercent для отправки на WB
    sku_wb: int = 0

    @property
    def reason(self) -> str:
        """Причина изменения цены"""
        if self.error_msg:
            return self.error_msg

        if self.status == ProcessingStatus.SUCCESS:
            if self.new_price_wb > self.old_price_wb:
                return f"Цена ↑ на {abs(self.profit_correction):.0f} ₽"
            elif self.new_price_wb < self.old_price_wb:
                return f"Цена ↓ на {abs(self.profit_correction):.0f} ₽"
            else:
                return f"Цена без изменений"

        status_reasons = {
            ProcessingStatus.SKIPPED_NO_DATA: "Недостаточно данных о продажах",
            ProcessingStatus.SKIPPED_MIN_PRICE: "Цена ниже минимальной",
            ProcessingStatus.SKIPPED_MIN_CHANGE: "Изменение меньше порога",
            ProcessingStatus.SKIPPED_INVALID: "Некорректное значение",
            ProcessingStatus.ERROR: "Ошибка обработки"
        }
        return status_reasons.get(self.status, str(self.status.value))


class PriceUpdater:
    """Основной класс для управления ценообразованием"""

    def __init__(self):
        self.logger = self._setup_logging()
        self.db_pool = None
        self.session = None
        self.is_running = False
        self.queue = None
        self.stats = defaultdict(int)
        self.successful_updates = []

        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)
        atexit.register(self.cleanup)

    def _setup_logging(self) -> logging.Logger:
        """Настройка логирования"""
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
        """Инициализация компонентов"""
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
        """Получение продаж с WB API за последние N часов"""
        # ВАЖНО: убираем +1 час и форматируем с временем
        moscow_tz = pytz.timezone('Europe/Moscow')
        period_start = datetime.now(moscow_tz) - timedelta(hours=Config.SALES_HOURS_FILTER)

        # Форматируем правильно для API
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
                    print(json.dumps(data, indent=2))
                    self.logger.info(f"Получено {len(data)} записей от WB API с {date_from}")

                    # ДЕБАГ: выводим времена первых записей
                    if data:
                        self.logger.info("=== Первые 5 записей по времени ===")
                        for i, item in enumerate(data[:5]):
                            self.logger.info(f"[{i}] {item.get('lastChangeDate')} | "
                                             f"finishedPrice: {item.get('finishedPrice')}")

                    sales = []
                    old_sales_count = 0

                    self._analyze_sales_timing(data, sales)

                    for item in data:
                        if item.get("isRealization"):
                            sale = SaleData.from_api_dict(item)
                            if sale and sale.finished_price > 0:
                                # Проверяем, попадает ли в наш период
                                if self._is_recent_sale(sale):
                                    sales.append(sale)
                                else:
                                    old_sales_count += 1
                                    self.logger.debug(f"Пропущена старая продажа: {sale.date}")

                    self.logger.info(f"Старых продаж (не в период): {old_sales_count}")
                    self.logger.info(f"Актуальных продаж: {len(sales)}")
                    return sales
                else:
                    text = await resp.text()
                    self.logger.error(f"Ошибка API WB: {resp.status} - {text[:200]}")
                    return []

        except Exception as e:
            self.logger.error(f"Ошибка при запросе к WB API: {e}")
            raise

    def _parse_date(self, date_str: str) -> datetime:
        """Парсинг даты из строки"""
        if 'Z' in date_str:
            date_str = date_str.replace('Z', '+00:00')
        dt = datetime.fromisoformat(date_str)
        if dt.tzinfo is None:
            dt = pytz.utc.localize(dt)
        return dt

    def _is_recent_sale(self, sale: SaleData) -> bool:
        """Проверка актуальности продажи"""
        try:
            if not sale.date:
                return False

            sale_dt = self._parse_date(sale.date)
            now = datetime.now(pytz.utc)
            return (now - sale_dt) <= timedelta(hours=Config.SALES_HOURS_FILTER)

        except Exception:
            return False

    def _analyze_sales_timing(self, data: List[Dict], sales: List[SaleData]):
        """Анализ временных меток полученных данных"""
        if not data:
            return

        # Собираем все временные метки
        all_timestamps = []
        recent_timestamps = []

        for item in data:
            ts = item.get('lastChangeDate')
            if ts:
                all_timestamps.append(ts)

        for sale in sales:
            if sale.date:
                recent_timestamps.append(sale.date)

        # Находим min/max
        if all_timestamps:
            self.logger.info(f"Первая запись в ответе: {min(all_timestamps)}")
            self.logger.info(f"Последняя запись в ответе: {max(all_timestamps)}")

        if recent_timestamps:
            self.logger.info(f"Первая актуальная продажа: {min(recent_timestamps)}")
            self.logger.info(f"Последняя актуальная продажа: {max(recent_timestamps)}")

        # Считаем по часам
        hour_counts = {}
        for ts in all_timestamps:
            try:
                hour = ts.split('T')[1][:2] + ':00'
                hour_counts[hour] = hour_counts.get(hour, 0) + 1
            except:
                continue

        self.logger.info("Распределение записей по часам:")
        for hour, count in sorted(hour_counts.items(), reverse=True):
            self.logger.info(f"  {hour}: {count} записей")

    async def fetch_products_batch(self, vendor_codes: List[str]) -> Dict[str, ProductData]:
        """Загрузка товаров из БД батчами"""
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
                        self.logger.info(
                            f"📦 Загружено {len(rows)} товаров из БД для батча {i // Config.BATCH_SIZE + 1}")

                        for row in rows:
                            product = ProductData.from_db_row(row)
                            if product:
                                product_map[product.vendor_code] = product

            except Exception as e:
                self.logger.error(f"Ошибка загрузки батча {i}: {e}")
                continue

        return product_map

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def fetch_nm_id(self, vendor_code: str) -> int:
        """Получение nmID по vendor_code из WB API"""
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

        self.logger.info(f"🔍 Поиск nmID для артикула: '{vendor_code}'")

        try:
            async with self.session.post(url, headers=headers, json=body) as resp:
                response_text = await resp.text()

                if resp.status == 200:
                    try:
                        data = json.loads(response_text)
                    except json.JSONDecodeError:
                        self.logger.error(f"❌ API вернул невалидный JSON: {response_text[:200]}")
                        return 0

                    cards = data.get("cards", [])
                    if not cards:
                        self.logger.warning(f"📭 Карточек по запросу '{vendor_code}' не найдено")
                        return 0

                    for card in cards:
                        card_vendor_code = str(card.get("vendorCode", "")).strip()
                        if card_vendor_code == str(vendor_code).strip():
                            nm_id = card.get("nmID", 0)
                            if nm_id:
                                self.logger.info(f"✅ Найден nmID: {nm_id} для '{vendor_code}'")
                                return nm_id

                    return 0

                elif resp.status == 401:
                    self.logger.error("❌ Ошибка авторизации: неверный токен Content API")
                    return 0
                else:
                    self.logger.error(f"❌ Ошибка API: {resp.status}. Ответ: {response_text[:500]}")
                    return 0

        except asyncio.TimeoutError:
            self.logger.error(f"⏱️ Таймаут при поиске '{vendor_code}'")
            return 0
        except Exception as e:
            self.logger.error(f"💥 Ошибка при поиске '{vendor_code}': {str(e)}")
            return 0

    async def process_product_new_logic(self, vendor_code: str,
                                        sales: List[SaleData],
                                        product: ProductData) -> PriceUpdate:
        """НОВАЯ ПРАВИЛЬНАЯ ЛОГИКА РАСЧЕТА"""
        try:
            self.logger.info(f"🔍 НОВАЯ ЛОГИКА: обработка {vendor_code}")

            # 1. Проверка наличия необходимых данных
            valid_sales = []
            for sale in sales:
                if (sale.price_with_desc > 0 and
                        sale.discount_percent > 0 and
                        sale.for_pay > 0):
                    valid_sales.append(sale)

            if len(valid_sales) < Config.MIN_SALES_FOR_CALC:
                return PriceUpdate(
                    vendor_code=vendor_code,
                    new_price_wb=0,
                    new_real_price=0,
                    old_price_wb=product.current_price_wb,
                    profit_correction=0,
                    status=ProcessingStatus.SKIPPED_NO_DATA,
                    error_msg=f"Недостаточно данных с price_with_desc: {len(valid_sales)}",
                    sku_wb=product.sku_wb
                )

            # 2. Получение nmID если нужно
            if product.sku_wb == 0:
                new_nm_id = await self.fetch_nm_id(vendor_code)
                if new_nm_id == 0:
                    return PriceUpdate(
                        vendor_code=vendor_code,
                        new_price_wb=0,
                        new_real_price=0,
                        old_price_wb=product.current_price_wb,
                        profit_correction=0,
                        status=ProcessingStatus.ERROR,
                        error_msg="Не удалось получить nmID",
                        sku_wb=0
                    )
                await self.save_nm_id_to_db(vendor_code, new_nm_id)
                product.sku_wb = new_nm_id

            # 3. Сбор данных для расчета
            price_wd_list = []  # price_with_desc - цена на витрине
            forpay_list = []  # forPay - что получаем
            discount_list = []  # discount_percent - скидка покупателя
            costs_list = []

            self.logger.info(f"📊 Анализ {len(valid_sales)} продаж:")

            for sale in valid_sales:
                price_wd_list.append(sale.price_with_desc)
                forpay_list.append(sale.for_pay)
                discount_list.append(sale.discount_percent)

                costs = sale.price_with_desc - sale.for_pay
                costs_list.append(costs)

                # Логируем для отладки
                self.logger.debug(f"   Продажа: price_wd={sale.price_with_desc:.0f}₽, "
                                  f"forpay={sale.for_pay:.0f}₽, "
                                  f"расходы={costs:.0f}₽ ({costs / sale.price_with_desc * 100:.1f}%), "
                                  f"discount={sale.discount_percent:.1f}%, "
                                  f"СПП%={sale.spp_percent}")

            # 4. Расчет средних значений (медиана для discount)
            avg_price_wd = statistics.mean(price_wd_list)  # средняя цена на витрине
            avg_forpay = statistics.mean(forpay_list)  # средние деньги на счету
            avg_discount = statistics.median(discount_list)  # медианная скидка
            avg_costs = statistics.mean(costs_list)

            self.logger.info(f"💰 АНАЛИЗ РАСХОДОВ для {vendor_code}:")
            self.logger.info(f"   Средняя цена на витрине: {avg_price_wd:.0f} ₽")
            self.logger.info(f"   Средний forpay: {avg_forpay:.0f} ₽")
            self.logger.info(f"   Средние расходы: {avg_costs:.0f} ₽")
            self.logger.info(f"   Расходы в % от цены: {avg_costs / avg_price_wd * 100:.1f}%")

            # 5. Расчет текущей прибыли
            bank_commission = avg_forpay * Config.BANK_COMMISSION
            current_profit = avg_forpay - bank_commission - product.purchase_price

            # 6. Сравнение с целевой прибылью
            profit_diff = product.target_profit - current_profit

            # 7. Логирование ситуации
            if profit_diff > 0:
                self.logger.info(f"📈 Нужно ПОВЫСИТЬ прибыль на {profit_diff:.0f} руб")
            elif profit_diff < 0:
                self.logger.info(f"📉 Можно ПОНИЗИТЬ цену на {abs(profit_diff):.0f} руб")
            else:
                self.logger.info(f"⚖️ Прибыль соответствует цели")

            # 8. Расчет новой цены на витрине
            new_price_wd = avg_price_wd + profit_diff

            # 9. Проверка минимальной цены
            min_price = product.purchase_price * Config.MIN_MARGIN_FACTOR
            if new_price_wd < min_price:
                self.logger.warning(f"⚠️ Новая цена ниже минимальной: {new_price_wd:.0f} < {min_price:.0f}")
                new_price_wd = min_price
                profit_diff = new_price_wd - avg_price_wd

            # 10. Расчет новой базовой цены (с сохранением скидки)
            if avg_discount >= 100:
                avg_discount = 99.9  # защита от 100% скидки
            if avg_discount <= 0:
                avg_discount = 0.1  # минимальная скидка

            new_total_price = new_price_wd / (1 - avg_discount / 100)
            new_total_price_rounded = round(new_total_price, 0)

            # 11. ДЕТАЛЬНОЕ ЛОГИРОВАНИЕ
            self.logger.info(f"🎯 ФИНАЛЬНЫЙ РАСЧЕТ для {vendor_code}:")
            self.logger.info(f"   Продаж использовано: {len(valid_sales)} шт")
            self.logger.info(f"   Средняя цена на витрине: {avg_price_wd:.0f} ₽")
            self.logger.info(f"   Средний forpay: {avg_forpay:.0f} ₽")
            self.logger.info(f"   Медианная скидка: {avg_discount:.1f}%")
            self.logger.info(f"   Закупка: {product.purchase_price:.0f} ₽")
            self.logger.info(f"   Комиссия банка ({Config.BANK_COMMISSION * 100:.1f}%): {bank_commission:.1f} ₽")
            self.logger.info(f"   Текущая прибыль: {current_profit:.0f} ₽")
            self.logger.info(f"   Целевая прибыль: {product.target_profit:.0f} ₽")
            self.logger.info(f"   Разница прибыли: {profit_diff:+.0f} ₽")
            self.logger.info(f"   Новая цена на витрине: {new_price_wd:.0f} ₽")
            self.logger.info(f"   Новая базовая цена: {new_total_price_rounded:.0f} ₽")
            self.logger.info(f"   Старая базовая цена в БД: {product.current_price_wb:.0f} ₽")

            # 12. ПРОВЕРОЧНЫЙ РАСЧЕТ
            check_price_wd = new_total_price_rounded * (1 - avg_discount / 100)
            self.logger.info(
                f"   Проверка расчета: {new_total_price_rounded:.0f} × (1 - {avg_discount / 100:.2f}) = {check_price_wd:.0f} ₽")

            # 13. ВАЛИДАЦИЯ
            validation = self._validate_price_update(
                vendor_code, product, new_price_wd, new_total_price_rounded, profit_diff
            )
            if validation:
                return validation

            # 14. Создание объекта обновления
            return PriceUpdate(
                vendor_code=vendor_code,
                new_price_wb=new_total_price_rounded,  # базовая цена для WB API
                new_real_price=round(new_price_wd, 2),  # цена на витрине
                old_price_wb=product.current_price_wb,
                profit_correction=abs(profit_diff),
                status=ProcessingStatus.SUCCESS,
                error_msg=f"Корректировка прибыли: {profit_diff:+.0f} ₽",
                discount=avg_discount,
                sku_wb=product.sku_wb
            )

        except Exception as e:
            self.logger.error(f"❌ Ошибка в новой логике для {vendor_code}: {e}")
            self.logger.error(traceback.format_exc())

            return PriceUpdate(
                vendor_code=vendor_code,
                new_price_wb=0,
                new_real_price=0,
                old_price_wb=product.current_price_wb,
                profit_correction=0,
                status=ProcessingStatus.ERROR,
                error_msg=str(e),
                sku_wb=product.sku_wb
            )

    def _validate_price_update(self, vendor_code: str, product: ProductData,
                               new_price_wd: float, new_total_price: float,
                               profit_diff: float) -> Optional[PriceUpdate]:
        """Валидация обновления цены"""

        # 1. Минимальная цена на витрине
        min_price = product.purchase_price * Config.MIN_MARGIN_FACTOR
        if new_price_wd < min_price:
            return PriceUpdate(
                vendor_code=vendor_code,
                new_price_wb=0,
                new_real_price=new_price_wd,
                old_price_wb=product.current_price_wb,
                profit_correction=profit_diff,
                status=ProcessingStatus.SKIPPED_MIN_PRICE,
                error_msg=f"Цена ниже минимальной: {new_price_wd:.0f} < {min_price:.0f}",
                sku_wb=product.sku_wb
            )

        # 2. Минимальное изменение цены
        price_change = abs(new_total_price - product.current_price_wb)
        if price_change < Config.MIN_PRICE_CHANGE:
            return PriceUpdate(
                vendor_code=vendor_code,
                new_price_wb=0,
                new_real_price=new_price_wd,
                old_price_wb=product.current_price_wb,
                profit_correction=profit_diff,
                status=ProcessingStatus.SKIPPED_MIN_CHANGE,
                error_msg=f"Изменение меньше порога: {price_change:.0f} < {Config.MIN_PRICE_CHANGE}",
                sku_wb=product.sku_wb
            )

        # 3. Максимальное процентное изменение
        if product.current_price_wb > 0:
            price_change_percent = abs((new_total_price - product.current_price_wb) / product.current_price_wb) * 100
            if price_change_percent > Config.MAX_PRICE_CHANGE_PERCENT:
                return PriceUpdate(
                    vendor_code=vendor_code,
                    new_price_wb=0,
                    new_real_price=new_price_wd,
                    old_price_wb=product.current_price_wb,
                    profit_correction=profit_diff,
                    status=ProcessingStatus.SKIPPED_INVALID,
                    error_msg=f"Изменение превышает лимит: {price_change_percent:.1f}% > {Config.MAX_PRICE_CHANGE_PERCENT}%",
                    sku_wb=product.sku_wb
                )

        return None

    async def save_nm_id_to_db(self, vendor_code: str, nm_id: int) -> bool:
        """Сохранение nmID в БД"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        UPDATE oc_product 
                        SET sku_wb = %s
                        WHERE model = %s
                    """, (nm_id, vendor_code))

                    self.logger.info(f"💾 nmID {nm_id} сохранен в БД для {vendor_code}")
                    return True

        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения nmID для {vendor_code}: {e}")
            return False

    async def save_price_update(self, update: PriceUpdate):
        """Сохранение обновления цены в БД"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Обновляем цену в основной таблице
                    await cur.execute("""
                        UPDATE oc_product 
                        SET price_wb = %s, 
                            wb_real_price = %s,
                            sku_wb = %s,
                            last_price_update = NOW()
                        WHERE model = %s
                    """, (update.new_price_wb, update.new_real_price, update.sku_wb, update.vendor_code))

                    # Записываем историю
                    await cur.execute("""
                        INSERT INTO oc_product_price_history 
                        (product_id, vendor_code, old_price_wb, new_price_wb,
                         old_real_price, new_real_price, profit_correction,
                         discount, change_reason, status, created_at, sale_count)
                        SELECT product_id, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        FROM oc_product WHERE model = %s
                    """, (
                        update.vendor_code,
                        update.old_price_wb,
                        update.new_price_wb,
                        0,  # old_real_price
                        update.new_real_price,
                        update.profit_correction,
                        update.discount or 0,
                        update.reason,
                        update.status.value,
                        datetime.now(pytz.timezone('Europe/Moscow')),
                    ))

                    self.stats['prices_updated'] += 1
                    self.logger.info(f"✅ Цена обновлена в БД: {update.vendor_code}")

        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения цены для {update.vendor_code}: {e}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def upload_prices_to_wb(self, updates: List[PriceUpdate]):
        """Отправка обновлений цен и скидок на WB"""
        data = []
        for update in updates:
            if update.sku_wb > 0 and update.discount is not None:
                data.append({
                    "nmID": update.sku_wb,
                    "price": int(update.new_price_wb),
                    "discount": int(update.discount)
                })
                self.logger.info(f"📤 Подготовка отправки на WB: nmID={update.sku_wb}, "
                                 f"цена={update.new_price_wb:.0f}, скидка={update.discount:.0f}%")

        if not data:
            self.logger.info("ℹ️ Нет данных для отправки на WB")
            return

        url = "https://discounts-prices-api.wildberries.ru/api/v2/upload/task"
        headers = {
            "Authorization": Config.WB_PRICES_TOKEN,
            "Content-Type": "application/json"
        }

        self.logger.info(f"🚀 Отправка {len(data)} цен на WB API...")

        try:
            async with self.session.post(url, headers=headers, json={"data": data}) as resp:
                if resp.status == 200:
                    res = await resp.json()
                    task_id = res.get('data', {}).get('id')
                    self.logger.info(f"✅ Цены отправлены на WB: ID задачи={task_id}")
                    self.stats['prices_uploaded_to_wb'] = len(data)
                else:
                    text = await resp.text()
                    self.logger.error(f"❌ Ошибка отправки цен на WB: {resp.status} - {text[:200]}")
        except Exception as e:
            self.logger.error(f"❌ Ошибка при отправке цен на WB: {e}")

    async def worker(self, worker_id: int):
        """Воркер для обработки товаров"""
        try:
            while self.is_running:
                try:
                    vendor_code, sales, product = await asyncio.wait_for(
                        self.queue.get(),
                        timeout=1.0
                    )

                    self.logger.debug(f"👷 Воркер {worker_id} обрабатывает {vendor_code}")

                    # ИСПОЛЬЗУЕМ НОВУЮ ЛОГИКУ!
                    update = await self.process_product_new_logic(vendor_code, sales, product)

                    if update.status == ProcessingStatus.SUCCESS:
                        await self.save_price_update(update)
                        self.successful_updates.append(update)

                    self.stats[update.status.value] += 1
                    self.queue.task_done()

                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    self.logger.error(f"❌ Ошибка в воркере {worker_id}: {e}")

        except asyncio.CancelledError:
            self.logger.info(f"🛑 Воркер {worker_id} остановлен")

    async def run_cycle(self):
        """Запуск одного цикла обработки"""
        cycle_start = datetime.now()
        self.logger.info("=" * 80)
        self.logger.info(f"🚀 Запуск цикла обработки: {cycle_start.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("=" * 80)

        self.stats.clear()
        self.successful_updates = []

        try:
            # 1. Получение продаж
            self.logger.info("📥 Получение данных о продажах с WB API...")
            sales_data = await self.fetch_wb_sales()

            if not sales_data:
                self.logger.warning("⚠️ Нет данных о продажах для обработки")
                return

            # 2. Группировка по артикулам
            sales_by_vendor = defaultdict(list)
            for sale in sales_data:
                if sale.vendor_code:
                    sales_by_vendor[sale.vendor_code].append(sale)

            vendor_codes = list(sales_by_vendor.keys())
            self.logger.info(f"✅ Найдено {len(vendor_codes)} артикулов с продажами")

            # 3. Загрузка товаров из БД
            self.logger.info("🛒 Загрузка данных о товарах из БД...")
            product_map = await self.fetch_products_batch(vendor_codes)
            self.logger.info(f"✅ Загружено {len(product_map)} товаров из БД")

            if not product_map:
                self.logger.warning("⚠️ Нет товаров для обработки")
                return

            # 4. Постановка задач в очередь
            self.logger.info("⏳ Постановка задач в очередь...")
            queue_tasks = 0
            for vendor_code in vendor_codes:
                if vendor_code in product_map:
                    await self.queue.put((vendor_code,
                                          sales_by_vendor[vendor_code],
                                          product_map[vendor_code]))
                    queue_tasks += 1

            self.logger.info(f"✅ В очередь добавлено {queue_tasks} задач")

            # 5. Запуск воркеров
            self.logger.info(f"👷 Запуск {Config.WORKERS_COUNT} воркеров...")
            workers = []
            for i in range(Config.WORKERS_COUNT):
                worker_task = asyncio.create_task(self.worker(i))
                workers.append(worker_task)

            # 6. Ожидание завершения
            self.logger.info("⏳ Ожидание завершения обработки...")
            await self.queue.join()

            # 7. Остановка воркеров
            self.logger.info("🛑 Остановка воркеров...")
            for worker_task in workers:
                worker_task.cancel()

            await asyncio.gather(*workers, return_exceptions=True)

            # 8. Отправка на WB
            if LOAD_PRICE_TO_WB and self.successful_updates:
                self.logger.info("🚀 Отправка обновлений цен на WB...")
                await self.upload_prices_to_wb(self.successful_updates)
            else:
                self.logger.info("ℹ️ Отправка цен на WB отключена (LOAD_PRICE_TO_WB=False)")

            # 9. Статистика
            cycle_end = datetime.now()
            duration = (cycle_end - cycle_start).total_seconds()

            self.logger.info("=" * 80)
            self.logger.info("📈 СТАТИСТИКА ЦИКЛА:")
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

        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка в цикле: {e}")
            self.logger.error(traceback.format_exc())

    async def run(self):
        """Основной цикл работы системы"""
        self.is_running = True
        cycle_count = 0

        while self.is_running:
            cycle_count += 1
            self.logger.info(f"\n{'#' * 80}")
            self.logger.info(f"🔄 ЦИКЛ #{cycle_count}")
            self.logger.info(f"{'#' * 80}")

            try:
                await self.run_cycle()

                # Пауза между циклами
                hours = Config.CYCLE_INTERVAL // 3600
                minutes = (Config.CYCLE_INTERVAL % 3600) // 60
                self.logger.info(f"⏸️ Ожидание следующего цикла ({hours}ч {minutes}мин)...")
                await asyncio.sleep(Config.CYCLE_INTERVAL)

            except KeyboardInterrupt:
                self.logger.info("🛑 Остановка по запросу пользователя")
                break
            except Exception as e:
                self.logger.error(f"❌ Фатальная ошибка: {e}")
                self.logger.error(traceback.format_exc())

                wait_time = min(300 * (2 ** (cycle_count % 5)), 3600)
                self.logger.info(f"⏸️ Пауза {wait_time} сек перед повторной попыткой...")
                await asyncio.sleep(wait_time)

    def _handle_shutdown(self, signum, frame):
        """Обработка сигналов завершения"""
        signal_name = {signal.SIGTERM: 'SIGTERM', signal.SIGINT: 'SIGINT'}.get(signum, str(signum))
        self.logger.info(f"🛑 Получен сигнал {signal_name}, завершение...")
        self.is_running = False

    async def cleanup(self):
        """Очистка ресурсов"""
        self.logger.info("🧹 Очистка ресурсов...")

        if self.session and not self.session.closed:
            await self.session.close()
            self.logger.info("✅ HTTP сессия закрыта")

        if self.db_pool:
            self.db_pool.close()
            await self.db_pool.wait_closed()
            self.logger.info("✅ Пул БД закрыт")

        self.logger.info("✅ Очистка завершена")


async def main():
    """Точка входа"""
    updater = PriceUpdater()

    try:
        await updater.initialize()
        await updater.run()
    except Exception as e:
        updater.logger.critical(f"💀 Фатальная ошибка: {e}")
        updater.logger.critical(traceback.format_exc())
        sys.exit(1)
    finally:
        await updater.cleanup()


if __name__ == "__main__":
    # Проверка обязательных переменных
    missing = []
    if not Config.WB_SALES_TOKEN:
        missing.append("WB_SALES_TOKEN")
    if not Config.WB_PRICES_TOKEN:
        missing.append("WB_PRICES_TOKEN")
    if not Config.WB_CONTENT_TOKEN:
        missing.append("WB_CONTENT_TOKEN")
    if missing:
        print(f"❌ ОШИБКА: Не установлены переменные: {', '.join(missing)}")
        sys.exit(1)

    # Запуск
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Программа остановлена пользователем")
        sys.exit(0)