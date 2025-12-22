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

LOAD_PRICE_TO_WB = False


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
    finished_price: float
    for_pay: float
    spp_percent: float
    date: str
    quantity: int = 1

    @classmethod
    def from_api_dict(cls, data: Dict) -> Optional['SaleData']:
        """Создание из сырых данных API"""
        try:
            return cls(
                nm_id=data.get('nmId', 0),
                vendor_code=str(data.get('supplierArticle', '')).strip(),
                finished_price=float(data.get('finishedPrice', 0)),
                for_pay=float(data.get('forPay', 0)),
                spp_percent=float(data.get('spp', 0)),
                date=data.get('lastChangeDate', ''),
                quantity=data.get('quantity', 1)
            )
        except (ValueError, TypeError):
            return None


@dataclass
class ProductData:
    """Структурированные данные о товаре"""
    vendor_code: str
    purchase_price: float
    target_profit: float
    current_price_wb: float
    current_real_price: float
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
class AnalyticsData:
    """Данные для аналитики по артикулу"""
    vendor_code: str
    date_period: str  # YYYY-MM-DD HH:00:00
    total_sales: int
    avg_finished_price: float
    avg_clean_forpay: float
    min_finished_price: float
    max_finished_price: float
    median_finished_price: float
    total_revenue: float
    avg_spp_percent: float
    purchase_price: float
    target_profit: float
    recommended_price: float
    current_price: float
    price_change_pct: float
    profit_deviation: float

    @classmethod
    def from_sales_data(cls,
                        vendor_code: str,
                        sales: List[SaleData],
                        product: ProductData,
                        recommended_price: float = 0) -> Optional['AnalyticsData']:
        """Создание агрегированных данных из продаж"""
        if not sales:
            return None

        finished_prices = []
        clean_forpays = []
        spp_percents = []
        total_quantity = 0
        total_revenue = 0

        for sale in sales:
            if sale.finished_price > 0 and sale.for_pay > 0:
                spp_amount = sale.finished_price * (sale.spp_percent / 100.0)
                clean_fpay = max(0.01, sale.for_pay - spp_amount)


                for _ in range(sale.quantity):
                    finished_prices.append(sale.finished_price)
                    clean_forpays.append(clean_fpay)
                    spp_percents.append(sale.spp_percent)
                    total_revenue += sale.finished_price
                    total_quantity += 1

        if not finished_prices:
            return None

        # Сортировка для медианы
        sorted_prices = sorted(finished_prices)
        n = len(sorted_prices)
        median = (sorted_prices[n // 2] if n % 2 != 0
                  else (sorted_prices[n // 2 - 1] + sorted_prices[n // 2]) / 2)

        # Расчет средних значений
        avg_finished = sum(finished_prices) / len(finished_prices)
        avg_clean_forpay = sum(clean_forpays) / len(clean_forpays)

        # Расчет отклонения прибыли
        actual_profit = avg_clean_forpay * (1 - Config.BANK_COMMISSION) - product.purchase_price
        profit_deviation = actual_profit - product.target_profit

        # Расчет изменения цены
        price_change_pct = 0
        if product.current_price_wb > 0 and recommended_price > 0:
            price_change_pct = ((recommended_price - product.current_price_wb) / product.current_price_wb) * 100

        return cls(
            vendor_code=vendor_code,
            date_period=datetime.now().strftime('%Y-%m-%d %H:00:00'),
            total_sales=total_quantity,
            avg_finished_price=avg_finished,
            avg_clean_forpay=avg_clean_forpay,
            min_finished_price=min(finished_prices),
            max_finished_price=max(finished_prices),
            median_finished_price=median,
            total_revenue=total_revenue,
            avg_spp_percent=sum(spp_percents) / len(spp_percents) if spp_percents else 0,
            purchase_price=product.purchase_price,
            target_profit=product.target_profit,
            recommended_price=recommended_price,
            current_price=product.current_price_wb,
            price_change_pct=price_change_pct,
            profit_deviation=profit_deviation
        )


@dataclass
class PriceUpdate:
    vendor_code: str
    new_price_wb: float
    new_real_price: float
    old_price_wb: float
    profit_correction: float
    status: ProcessingStatus
    error_msg: str = ""
    analytics_data: Optional[AnalyticsData] = None
    discount: Optional[float] = None
    sku_wb: int = 0

    @property
    def reason(self) -> str:
        """Правильная причина изменения цены"""
        if self.error_msg:
            return self.error_msg

        if self.status == ProcessingStatus.SUCCESS:
            if self.new_price_wb > self.old_price_wb:
                return f"Цена ↑ на {self.profit_correction:.0f} ₽"
            elif self.new_price_wb < self.old_price_wb:
                return f"Цена ↓ на {self.profit_correction:.0f} ₽"
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
        logger.setLevel(logging.INFO)

        # Форматтер
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Файловый хендлер с ротацией
        file_handler = logging.handlers.RotatingFileHandler(
            'price_updater.log',
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=10,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)

        # Консольный хендлер
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger

    async def initialize(self):
        """Инициализация компонентов"""
        self.logger.info("Инициализация системы ценообразования...")

        # Пул соединений с БД
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
        """Получение продаж с WB API"""
        date_from = (datetime.now(pytz.timezone('Europe/Moscow')) -
                     timedelta(hours=Config.SALES_HOURS_FILTER + 1)).strftime("%Y-%m-%d")

        url = "https://statistics-api.wildberries.ru/api/v1/supplier/sales"
        headers = {
            "Authorization": Config.WB_SALES_TOKEN,
            "Accept": "application/json"
        }
        params = {
            "dateFrom": date_from,
            "flag": 1
        }

        try:
            async with self.session.get(url, headers=headers, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.logger.info(f"Получено {len(data)} записей от WB API")


                    sales = []
                    for item in data:
                        if item.get("isRealization"):
                            sale = SaleData.from_api_dict(item)
                            if sale and self._is_recent_sale(sale):
                                sales.append(sale)

                    self.logger.info(f"Отфильтровано {len(sales)} актуальных продаж")
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
                                self.logger.debug(f"   - {product.vendor_code}: закупка={product.purchase_price:.2f}, "
                                                  f"цель={product.target_profit:.2f}, текущая цена={product.current_price_wb:.2f}")

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
        """Получение nmID по vendor_code из WB API с использованием textSearch"""
        url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
        headers = {
            "Authorization": Config.WB_CONTENT_TOKEN,
            "Content-Type": "application/json"
        }

        # Правильное тело запроса согласно официальной спецификации
        body = {
            "settings": {
                "cursor": {
                    "limit": 100
                },
                "filter": {
                    "withPhoto": -1,  # все карточки (с фото и без)
                    "textSearch": str(vendor_code).strip()  # ПРАВИЛЬНЫЙ параметр для поиска!
                }
            }
        }

        self.logger.info(f"🔍 Поиск nmID для артикула: '{vendor_code}' через textSearch")

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

                    # Ищем точное совпадение с vendor_code
                    for card in cards:
                        card_vendor_code = str(card.get("vendorCode", "")).strip()
                        if card_vendor_code == str(vendor_code).strip():
                            nm_id = card.get("nmID", 0)
                            if nm_id:
                                self.logger.info(f"✅ Точное совпадение! '{vendor_code}' -> nmID: {nm_id}")
                                return nm_id

                    # Если есть результаты, но нет точного совпадения
                    found_codes = [str(c.get("vendorCode", "N/A")).strip() for c in cards]
                    self.logger.warning(
                        f"⚠️ Точного совпадения для '{vendor_code}' нет. "
                        f"Найдены артикулы: {found_codes}"
                    )
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

    async def process_product(self, vendor_code: str,
                              sales: List[SaleData],
                              product: ProductData) -> PriceUpdate:
        """Обработка одного товара"""
        try:
            # Логирование начала обработки
            self.logger.info(f"🔍 Начало обработки артикула: {vendor_code}")
            self.logger.info(f"   Продаж: {len(sales)}, Товар: {product}")

            # Валидация входных данных
            if not sales or not product:
                self.logger.warning(f"Пропуск {vendor_code}: нет данных о продажах или товаре")
                return PriceUpdate(
                    vendor_code=vendor_code,
                    new_price_wb=0,
                    new_real_price=0,
                    old_price_wb=product.current_price_wb if product else 0,
                    profit_correction=0,
                    status=ProcessingStatus.SKIPPED_NO_DATA,
                    error_msg="Нет данных о продажах или товаре",
                    sku_wb=product.sku_wb if product else 0
                )

            if product.sku_wb == 0:
                self.logger.info(f"📡 Запрос nmID для артикула: {vendor_code}")
                new_nm_id = await self.fetch_nm_id(vendor_code)

                if new_nm_id == 0:
                    self.logger.error(f"Не удалось получить nmID для {vendor_code}")
                    return PriceUpdate(
                        vendor_code=vendor_code,
                        new_price_wb=0,
                        new_real_price=0,
                        old_price_wb=product.current_price_wb,
                        profit_correction=0,
                        status=ProcessingStatus.ERROR,
                        error_msg="Не удалось получить nmID",
                        sku_wb=0  # Явно указываем 0
                    )
                else:
                    # Сохраняем nmID в БД
                    await self.save_nm_id_to_db(vendor_code, new_nm_id)
                    # Обновляем объект product для дальнейшей обработки
                    product.sku_wb = new_nm_id
                    self.logger.info(f"✅ Получен и сохранен nmID={new_nm_id} для {vendor_code}")

            # Агрегация данных по продажам
            self.logger.info(f"📊 Агрегация данных по продажам для {vendor_code}")
            finished_prices = []
            clean_forpays = []
            spp_amounts = []

            for sale in sales:
                if sale.finished_price > 0 and sale.for_pay > 0:
                    # Исключаем СПП из расчета
                    spp_amount = sale.finished_price * (sale.spp_percent / 100.0)
                    clean_fpay = max(0.01, sale.for_pay - spp_amount)

                    # Учитываем количество (если продано несколько штук)
                    for _ in range(sale.quantity):
                        finished_prices.append(sale.finished_price)
                        clean_forpays.append(clean_fpay)
                        spp_amounts.append(spp_amount)

                    # Логирование деталей продажи
                    self.logger.debug(f"   Продажа: finished={sale.finished_price:.2f}, "
                                      f"for_pay={sale.for_pay:.2f}, spp={sale.spp_percent}%, "
                                      f"чистый={clean_fpay:.2f}, количество={sale.quantity}")

            # Проверка достаточности данных
            if len(finished_prices) < Config.MIN_SALES_FOR_CALC:
                self.logger.warning(f"Пропуск {vendor_code}: недостаточно продаж "
                                    f"({len(finished_prices)} < {Config.MIN_SALES_FOR_CALC})")
                return PriceUpdate(
                    vendor_code=vendor_code,
                    new_price_wb=0,
                    new_real_price=0,
                    old_price_wb=product.current_price_wb,
                    profit_correction=0,
                    status=ProcessingStatus.SKIPPED_NO_DATA,
                    error_msg=f"Недостаточно продаж: {len(finished_prices)}",
                    sku_wb=product.sku_wb
                )

            # Расчет средних значений
            avg_finished = sum(finished_prices) / len(finished_prices)
            avg_clean_fpay = sum(clean_forpays) / len(clean_forpays)
            avg_spp_amount = sum(spp_amounts) / len(spp_amounts)

            self.logger.info(f"📈 Средние значения для {vendor_code}:")
            self.logger.info(f"   Средняя finished цена: {avg_finished:.2f} ₽")
            self.logger.info(f"   Средний чистый for_pay: {avg_clean_fpay:.2f} ₽")
            self.logger.info(f"   Средняя сумма СПП: {avg_spp_amount:.2f} ₽")

            # Расчет фактической прибыли
            commission_amount = avg_clean_fpay * Config.BANK_COMMISSION
            actual_profit = avg_clean_fpay - commission_amount - product.purchase_price

            self.logger.info(f"💰 Расчет прибыли для {vendor_code}:")
            self.logger.info(f"   Чистый доход: {avg_clean_fpay:.2f} ₽")
            self.logger.info(f"   Комиссия банка ({Config.BANK_COMMISSION * 100:.1f}%): {commission_amount:.2f} ₽")
            self.logger.info(f"   Себестоимость: {product.purchase_price:.2f} ₽")
            self.logger.info(f"   Фактическая прибыль: {actual_profit:.2f} ₽")


            profit_correction = product.target_profit - actual_profit
            self.logger.info(f"🎯 Корректировка прибыли для {vendor_code}:")
            self.logger.info(f"   Целевая прибыль: {product.target_profit:.2f} ₽")
            self.logger.info(f"   Корректировка: {profit_correction:+.2f} ₽")

            if not sales:
                discount = None
            else:
                last_sale = max(sales, key=lambda s: self._parse_date(s.date))
                discount = last_sale.spp_percent
                self.logger.info(f"🎫 Скидка из последней продажи {vendor_code}: {discount}%")

            if discount is None or not 0 <= discount < 100:
                self.logger.warning(f"Некорректная скидка для {vendor_code}: {discount}")
                return PriceUpdate(
                    vendor_code=vendor_code,
                    new_price_wb=0,
                    new_real_price=0,
                    old_price_wb=product.current_price_wb,
                    profit_correction=profit_correction,
                    status=ProcessingStatus.SKIPPED_INVALID,
                    error_msg="Некорректная скидка из последней продажи",
                    sku_wb=product.sku_wb
                )

            # ПРАВИЛЬНЫЙ РАСЧЕТ НОВОЙ ЦЕНЫ:
            # 1. Нужная прибыль на руках (после всех вычетов)
            needed_profit = product.purchase_price + product.target_profit  # 160 + 200 = 360

            # 2. Нужный for_pay с учетом комиссии банка
            needed_for_pay = needed_profit / (1 - Config.BANK_COMMISSION)  # 360 / 0.98 ≈ 367.35

            # 3. Коэффициент конверсии finished → for_pay из текущих данных
            if avg_finished > 0 and avg_clean_fpay > 0:
                conversion_factor = avg_clean_fpay / avg_finished  # 298.55 / 395.28 ≈ 0.755
                self.logger.info(f"📊 Коэффициент конверсии finished→for_pay: {conversion_factor:.3f}")
            else:
                # Fallback: предполагаем соотношение через скидку и комиссию
                conversion_factor = (1 - discount / 100) * (1 - Config.BANK_COMMISSION)

            # 4. Новая finished цена
            new_finished = needed_for_pay / conversion_factor  # 367.35 / 0.755 ≈ 486.56
            self.logger.info(f"🔄 Новая finished цена для {vendor_code}:")
            self.logger.info(f"   Старая: {avg_finished:.2f} ₽")
            self.logger.info(f"   Новая: {new_finished:.2f} ₽")
            self.logger.info(f"   Изменение: {(new_finished - avg_finished):+.2f} ₽")

            # 5. Расчет полной цены (с учетом СПП)
            new_full_price = round(new_finished / (1 - discount / 100.0), 0)  # 486.56 / 0.62 ≈ 785
            self.logger.info(f"🔢 Расчет полной цены для {vendor_code}:")
            self.logger.info(f"   Finished цена: {new_finished:.2f} ₽")
            self.logger.info(f"   Скидка: {discount}%")
            self.logger.info(f"   Полная цена: {new_full_price:.2f} ₽")

            # Создание аналитических данных
            analytics_data = AnalyticsData.from_sales_data(
                vendor_code=vendor_code,
                sales=sales,
                product=product,
                recommended_price=new_finished
            )


            validation_result = self._validate_price_update(
                vendor_code, product, new_finished, new_full_price, profit_correction
            )

            if validation_result:
                validation_result.new_price_wb = new_full_price  # Добавляем полную цену
                validation_result.new_real_price = new_finished  # И реальную

                # Для валидационных ошибок: из большего вычитаем меньшее
                max_price = max(product.current_price_wb, new_full_price)
                min_price = min(product.current_price_wb, new_full_price)
                validation_result.profit_correction = max_price - min_price  # Всегда положительное

                validation_result.analytics_data = analytics_data
                validation_result.sku_wb = product.sku_wb
                validation_result.discount = discount
                self.logger.warning(f"Валидация не пройдена для {vendor_code}: {validation_result.error_msg}")
                return validation_result


            old_price = product.current_price_wb
            new_price = new_full_price


            max_price = max(old_price, new_price)
            min_price = min(old_price, new_price)


            price_difference = max_price - min_price


            if new_price > old_price:
                # Цена повысилась
                profit_correction_value = price_difference
                change_direction = "+"
            elif new_price < old_price:
                # Цена снизилась
                profit_correction_value = price_difference
                change_direction = "-"
            else:
                # Цена без изменений
                profit_correction_value = 0
                change_direction = "="

            self.logger.info(f"📊 Расчет разницы цен для {vendor_code}:")
            self.logger.info(f"   Старая цена: {old_price:.2f} ₽")
            self.logger.info(f"   Новая цена: {new_price:.2f} ₽")
            self.logger.info(f"   Разница: {price_difference:.2f} ₽ ({change_direction})")
            self.logger.info(f"   Profit Correction: {profit_correction_value:.2f} ₽")

            # Создание объекта обновления с аналитикой
            update = PriceUpdate(
                vendor_code=vendor_code,
                new_price_wb=new_full_price,
                new_real_price=round(new_finished, 2),
                old_price_wb=product.current_price_wb,
                profit_correction=profit_correction_value,
                status=ProcessingStatus.SUCCESS,
                analytics_data=analytics_data,
                error_msg=f"Изменение цены: {change_direction}{price_difference:.2f} руб. Скидка: {discount}%",
                discount=discount,
                sku_wb=product.sku_wb
            )

            self.logger.info(f"✅ Обработка завершена для {vendor_code}")
            return update

        except Exception as e:
            self.logger.error(f"❌ Ошибка обработки {vendor_code}: {e}")
            self.logger.error(traceback.format_exc())

            return PriceUpdate(
                vendor_code=vendor_code,
                new_price_wb=0,
                new_real_price=0,
                old_price_wb=product.current_price_wb if product else 0,
                profit_correction=0,
                status=ProcessingStatus.ERROR,
                error_msg=str(e),
                sku_wb=product.sku_wb if product else 0
            )

    def _validate_price_update(self, vendor_code: str, product: ProductData,
                               new_finished_price: float,
                               new_full_price: float,
                               profit_correction: float) -> Optional[PriceUpdate]:
        """Валидация обновления цены (на finished цене)"""
        self.logger.info(f"⚖️ Валидация цены для {vendor_code}:")
        self.logger.info(f"   Новая finished цена: {new_finished_price:.2f} ₽")
        self.logger.info(f"   Новая полная цена: {new_full_price:.2f} ₽")
        self.logger.info(f"   Текущая полная цена: {product.current_price_wb:.2f} ₽")
        self.logger.info(f"   Текущая реальная цена: {product.current_real_price:.2f} ₽")


        min_allowed_price = product.purchase_price * Config.MIN_MARGIN_FACTOR
        self.logger.info(f"   Минимальная допустимая finished цена: {min_allowed_price:.2f} ₽ "
                         f"(закупка {product.purchase_price:.2f} × фактор {Config.MIN_MARGIN_FACTOR})")

        if new_finished_price < min_allowed_price:
            self.logger.warning(
                f"   ❌ Finished цена ниже минимальной: {new_finished_price:.2f} < {min_allowed_price:.2f}")
            return PriceUpdate(
                vendor_code=vendor_code,
                new_price_wb=0,
                new_real_price=new_finished_price,
                old_price_wb=product.current_price_wb,
                profit_correction=profit_correction,
                status=ProcessingStatus.SKIPPED_MIN_PRICE,
                error_msg=f"Finished цена ниже минимума: {new_finished_price:.2f} < {min_allowed_price:.2f}"
            )


        price_change_abs = abs(new_full_price - product.current_price_wb)
        self.logger.info(f"   Абсолютное изменение полной цены: {price_change_abs:.2f} ₽")
        self.logger.info(f"   Порог изменения: {Config.MIN_PRICE_CHANGE} ₽")
        self.logger.info(f"   Изменение полной цены: {product.current_price_wb:.2f} → {new_full_price:.2f} ₽")

        if price_change_abs < Config.MIN_PRICE_CHANGE:
            self.logger.warning(
                f"   ❌ Изменение полной цены меньше порога: {price_change_abs:.2f} < {Config.MIN_PRICE_CHANGE}")
            return PriceUpdate(
                vendor_code=vendor_code,
                new_price_wb=0,
                new_real_price=new_finished_price,
                old_price_wb=product.current_price_wb,
                profit_correction=profit_correction,
                status=ProcessingStatus.SKIPPED_MIN_CHANGE,
                error_msg=f"Изменение полной цены меньше порога: {price_change_abs:.2f}"
            )


        if product.current_price_wb > 0:
            price_change_percent = abs((new_full_price - product.current_price_wb) / product.current_price_wb) * 100
            self.logger.info(f"   Процентное изменение полной цены: {price_change_percent:.1f}%")
            self.logger.info(f"   Максимальное изменение: {Config.MAX_PRICE_CHANGE_PERCENT}%")

            if price_change_percent > Config.MAX_PRICE_CHANGE_PERCENT:
                self.logger.warning(
                    f"   ❌ Изменение превышает лимит: {price_change_percent:.1f}% > {Config.MAX_PRICE_CHANGE_PERCENT}%")
                return PriceUpdate(
                    vendor_code=vendor_code,
                    new_price_wb=0,
                    new_real_price=new_finished_price,
                    old_price_wb=product.current_price_wb,
                    profit_correction=profit_correction,
                    status=ProcessingStatus.SKIPPED_INVALID,
                    error_msg=f"Изменение полной цены превышает {Config.MAX_PRICE_CHANGE_PERCENT}%: {price_change_percent:.1f}%"
                )


        if not (0.01 <= new_finished_price <= 1000000):  # Разумные пределы для finished цены
            self.logger.warning(f"   ❌ Finished цена вне диапазона: {new_finished_price:.2f}")
            return PriceUpdate(
                vendor_code=vendor_code,
                new_price_wb=0,
                new_real_price=new_finished_price,
                old_price_wb=product.current_price_wb,
                profit_correction=profit_correction,
                status=ProcessingStatus.SKIPPED_INVALID,
                error_msg=f"Finished цена вне диапазона: {new_finished_price:.2f}"
            )

        self.logger.info(f"   ✅ Валидация пройдена для {vendor_code}")
        return None

    async def save_analytics_data(self, analytics_data: AnalyticsData):
        """Сохранение агрегированных данных в таблицу аналитики"""
        if not analytics_data:
            return

        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(f"""
                        INSERT INTO {Config.ANALYTICS_TABLE} (
                            vendor_code, date_period, total_sales, 
                            avg_finished_price, avg_clean_forpay,
                            min_finished_price, max_finished_price, median_finished_price,
                            total_revenue, avg_spp_percent,
                            purchase_price, target_profit,
                            recommended_price, current_price,
                            price_change_pct, profit_deviation
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            total_sales = VALUES(total_sales),
                            avg_finished_price = VALUES(avg_finished_price),
                            avg_clean_forpay = VALUES(avg_clean_forpay),
                            min_finished_price = VALUES(min_finished_price),
                            max_finished_price = VALUES(max_finished_price),
                            median_finished_price = VALUES(median_finished_price),
                            total_revenue = VALUES(total_revenue),
                            avg_spp_percent = VALUES(avg_spp_percent),
                            purchase_price = VALUES(purchase_price),
                            target_profit = VALUES(target_profit),
                            recommended_price = VALUES(recommended_price),
                            current_price = VALUES(current_price),
                            price_change_pct = VALUES(price_change_pct),
                            profit_deviation = VALUES(profit_deviation),
                            updated_at = CURRENT_TIMESTAMP
                    """, (
                        analytics_data.vendor_code,
                        analytics_data.date_period,
                        analytics_data.total_sales,
                        round(analytics_data.avg_finished_price, 2),
                        round(analytics_data.avg_clean_forpay, 2),
                        round(analytics_data.min_finished_price, 2),
                        round(analytics_data.max_finished_price, 2),
                        round(analytics_data.median_finished_price, 2),
                        round(analytics_data.total_revenue, 2),
                        round(analytics_data.avg_spp_percent, 2),
                        round(analytics_data.purchase_price, 2),
                        round(analytics_data.target_profit, 2),
                        round(analytics_data.recommended_price, 2),
                        round(analytics_data.current_price, 2),
                        round(analytics_data.price_change_pct, 2),
                        round(analytics_data.profit_deviation, 2)
                    ))

                    self.stats['analytics_saved'] += 1
                    self.logger.info(f"💾 Аналитика сохранена для {analytics_data.vendor_code}: "
                                     f"продаж={analytics_data.total_sales}, "
                                     f"ср.цена={analytics_data.avg_finished_price:.2f}")

        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения аналитики для {analytics_data.vendor_code}: {e}")

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
        """Сохранение обновления цены в БД с расширенным логированием"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # 1. Получаем старые данные
                    await cur.execute("""
                        SELECT product_id, price_wb, wb_real_price, purchase_price, target_profit_rub
                        FROM oc_product 
                        WHERE model = %s
                    """, (update.vendor_code,))

                    product_data = await cur.fetchone()
                    if not product_data:
                        self.logger.error(f"❌ Товар {update.vendor_code} не найден в БД")
                        return

                    # Явное преобразование типов из Decimal в float
                    product_id = int(product_data[0]) if product_data[0] else 0
                    old_price_wb = float(product_data[1]) if product_data[1] else 0.0
                    old_real_price = float(product_data[2]) if product_data[2] else 0.0
                    purchase_price = float(product_data[3]) if product_data[3] else 0.0
                    target_profit = float(product_data[4]) if product_data[4] else 0.0

                    self.logger.info(f"📋 Данные товара {update.vendor_code} из БД:")
                    self.logger.info(f"   ID: {product_id}")
                    self.logger.info(f"   Старая полная цена: {old_price_wb:.2f} ₽")
                    self.logger.info(f"   Старая реальная цена: {old_real_price:.2f} ₽")
                    self.logger.info(f"   Закупочная цена: {purchase_price:.2f} ₽")
                    self.logger.info(f"   Целевая прибыль: {target_profit:.2f} ₽")

                    # 2. Подготовка данных для истории
                    sales_count = 0
                    avg_finished = 0.0
                    discount = 0.0

                    if update.analytics_data:
                        sales_count = update.analytics_data.total_sales
                        avg_finished = float(update.analytics_data.avg_finished_price)

                    if update.discount is not None:
                        discount = float(update.discount)

                    # 3. Обновляем цену в основной таблице
                    await cur.execute("""
                        UPDATE oc_product 
                        SET price_wb = %s, 
                            wb_real_price = %s,
                            sku_wb = %s,
                            last_price_update = NOW()
                        WHERE model = %s
                    """, (
                        float(update.new_price_wb),
                        float(update.new_real_price),
                        int(update.sku_wb),
                        update.vendor_code
                    ))

                    update_count = cur.rowcount
                    self.logger.info(f"🔄 Обновлено записей в oc_product: {update_count}")

                    # 4. Записываем РАСШИРЕННЫЙ лог изменения
                    await cur.execute("""
                        INSERT INTO oc_product_price_history 
                        (product_id, vendor_code, old_price_wb, new_price_wb,
                         old_real_price, new_real_price, profit_correction,
                         sales_count, avg_finished_price, discount,
                         change_reason, status, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        product_id,
                        update.vendor_code,
                        old_price_wb,  # уже float
                        float(update.new_price_wb),
                        old_real_price,  # уже float
                        float(update.new_real_price),
                        round(float(update.profit_correction), 2),  # Явное преобразование
                        sales_count,
                        round(float(avg_finished), 2),  # Явное преобразование
                        round(float(discount), 2),  # Явное преобразование
                        update.reason,
                        update.status.value,
                        datetime.now(pytz.timezone('Europe/Moscow'))
                    ))

                    history_id = cur.lastrowid
                    self.logger.info(f"📝 Запись истории создана с ID: {history_id}")

                    self.stats['prices_updated'] += 1
                    self.stats['logs_saved'] = self.stats.get('logs_saved', 0) + 1

                    # Логируем изменение
                    price_change = update.new_price_wb - old_price_wb
                    real_price_change = update.new_real_price - old_real_price
                    change_sign = "+" if price_change > 0 else ""
                    real_change_sign = "+" if real_price_change > 0 else ""

                    self.logger.info(f"✅ Цена обновлена в БД: {update.vendor_code}")
                    self.logger.info(f"   Полная цена: {old_price_wb:.2f} → {update.new_price_wb:.2f} "
                                     f"({change_sign}{price_change:.2f} ₽)")
                    self.logger.info(f"   Реальная цена: {old_real_price:.2f} → {update.new_real_price:.2f} "
                                     f"({real_change_sign}{real_price_change:.2f} ₽)")
                    self.logger.info(f"   Количество продаж: {sales_count}")
                    self.logger.info(f"   Средняя finished цена: {avg_finished:.2f} ₽")
                    self.logger.info(f"   Скидка: {discount}%")
                    self.logger.info(f"   Статус: {update.status.value}")
                    self.logger.info(f"   Причина: {update.reason}")

        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения цены для {update.vendor_code}: {e}")
            self.logger.error(traceback.format_exc())
            raise

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
                                 f"цена={update.new_price_wb:.0f}, скидка={update.discount}%")

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


                    for item in data:
                        self.logger.debug(f"   Отправлено: nmID={item['nmID']}, цена={item['price']}, "
                                          f"скидка={item['discount']}%")
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

                    self.logger.debug(f"👷 Воркер {worker_id} обрабатывает артикул: {vendor_code}")


                    update = await self.process_product(vendor_code, sales, product)


                    if update.analytics_data:
                        await self.save_analytics_data(update.analytics_data)


                    if update.status == ProcessingStatus.SUCCESS:
                        await self.save_price_update(update)
                        self.successful_updates.append(update)


                    self.stats[update.status.value] += 1

                    self.queue.task_done()
                    self.logger.debug(f"👷 Воркер {worker_id} завершил обработку: {vendor_code}")

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

            self.logger.info("📥 Получение данных о продажах с WB API...")
            sales_data = await self.fetch_wb_sales()

            if not sales_data:
                self.logger.warning("⚠️ Нет данных о продажах для обработки")
                return


            self.logger.info("📊 Группировка продаж по артикулам...")
            sales_by_vendor = defaultdict(list)
            for sale in sales_data:
                if sale.vendor_code:
                    sales_by_vendor[sale.vendor_code].append(sale)

            vendor_codes = list(sales_by_vendor.keys())
            self.logger.info(f"✅ Найдено {len(vendor_codes)} артикулов с продажами")


            top_vendors = sorted(sales_by_vendor.items(), key=lambda x: len(x[1]), reverse=True)[:5]
            for vendor, sales_list in top_vendors:
                self.logger.info(f"   Топ: {vendor} - {len(sales_list)} продаж")

            if not vendor_codes:
                return


            self.logger.info("🛒 Загрузка данных о товарах из БД...")
            product_map = await self.fetch_products_batch(vendor_codes)
            self.logger.info(f"✅ Загружено {len(product_map)} товаров из БД")

            if not product_map:
                self.logger.warning("⚠️ Нет товаров для обработки")
                return


            self.logger.info("⏳ Постановка задач в очередь...")
            queue_tasks = []
            for vendor_code in vendor_codes:
                if vendor_code in product_map:
                    task = (vendor_code,
                            sales_by_vendor[vendor_code],
                            product_map[vendor_code])
                    queue_tasks.append(task)


            for task in queue_tasks:
                await self.queue.put(task)

            self.logger.info(f"✅ В очередь добавлено {len(queue_tasks)} задач")


            self.logger.info(f"👷 Запуск {Config.WORKERS_COUNT} воркеров...")
            workers = []
            for i in range(Config.WORKERS_COUNT):
                worker_task = asyncio.create_task(self.worker(i))
                workers.append(worker_task)
                self.logger.info(f"   Воркер {i} запущен")


            self.logger.info("⏳ Ожидание завершения обработки...")
            await self.queue.join()


            self.logger.info("🛑 Остановка воркеров...")
            for worker_task in workers:
                worker_task.cancel()

            await asyncio.gather(*workers, return_exceptions=True)


            if LOAD_PRICE_TO_WB and self.successful_updates:
                self.logger.info("🚀 Отправка обновлений цен на WB...")
                await self.upload_prices_to_wb(self.successful_updates)
            else:
                self.logger.info("ℹ️ Отправка цен на WB отключена (LOAD_PRICE_TO_WB=False)")


            cycle_end = datetime.now()
            duration = (cycle_end - cycle_start).total_seconds()

            self.logger.info("=" * 80)
            self.logger.info("📈 СТАТИСТИКА ЦИКЛА:")
            self.logger.info(f"   Всего артикулов: {len(vendor_codes)}")
            self.logger.info(f"   Товаров в БД: {len(product_map)}")
            self.logger.info(f"   Успешно обработано: {self.stats.get('success', 0)}")
            self.logger.info(f"   Обновлено цен: {self.stats.get('prices_updated', 0)}")
            self.logger.info(f"   Отправлено на WB: {self.stats.get('prices_uploaded_to_wb', 0)}")
            self.logger.info(f"   Сохранено аналитики: {self.stats.get('analytics_saved', 0)}")
            self.logger.info(f"   Пропущено (мало данных): {self.stats.get('skipped_no_data', 0)}")
            self.logger.info(f"   Пропущено (низкая цена): {self.stats.get('skipped_min_price', 0)}")
            self.logger.info(f"   Пропущено (мало изменений): {self.stats.get('skipped_min_change', 0)}")
            self.logger.info(f"   Ошибок: {self.stats.get('error', 0)}")
            self.logger.info(f"   Время выполнения: {duration:.2f} сек")
            self.logger.info(f"   Скорость: {len(vendor_codes) / duration:.1f} арт/сек")
            self.logger.info(f"   Всего задач в очереди: {self.queue.qsize()}")
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


        self.logger.info("📊 ИТОГОВАЯ СТАТИСТИКА:")
        for key, value in self.stats.items():
            self.logger.info(f"   {key}: {value}")

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