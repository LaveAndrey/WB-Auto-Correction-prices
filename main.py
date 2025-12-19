#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Production-ready система ценообразования на основе finishedPrice без СПП.
С сохранением агрегированных данных для аналитики.
"""

import asyncio
import logging
import logging.handlers
import sys
import os
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum
import json
import traceback
import signal
import atexit

import pytz
import aiomysql
import aiohttp
from aiohttp import ClientTimeout, ClientSession
import redis.asyncio as redis
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Загрузка конфигурации
load_dotenv = None
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    print("Предупреждение: python-dotenv не установлен. Используются переменные окружения.")


# === КОНФИГУРАЦИЯ ===
class Config:
    # Database
    DB_HOST = os.getenv('DB_HOST')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_NAME = os.getenv('DB_NAME')
    DB_PORT = int(os.getenv('DB_PORT'))

    # WB API
    WB_TOKEN = os.getenv('WB_SALES_TOKEN')

    # Redis (опционально)
    REDIS_URL = os.getenv('REDIS_URL')
    REDIS_TTL = int(os.getenv('REDIS_TTL'))

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

                # Учет количества
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
    """Обновление цены для батча"""
    vendor_code: str
    new_price_wb: float
    new_real_price: float
    old_price_wb: float
    profit_diff: float
    status: ProcessingStatus
    error_msg: str = ""
    analytics_data: Optional[AnalyticsData] = None


class PriceUpdater:
    """Основной класс для управления ценообразованием"""

    def __init__(self):
        self.logger = self._setup_logging()
        self.redis_client = None
        self.db_pool = None
        self.session = None
        self.is_running = False
        self.queue = None
        self.stats = defaultdict(int)

        # Регистрация обработчиков
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

        # Redis для кеширования (опционально)
        if Config.REDIS_URL:
            try:
                self.redis_client = await redis.from_url(
                    Config.REDIS_URL,
                    decode_responses=True,
                    max_connections=10
                )
                await self.redis_client.ping()
                self.logger.info("✓ Redis подключен")
            except Exception as e:
                self.logger.warning(f"Redis недоступен: {e}. Работа без кеширования.")
                self.redis_client = None
        else:
            self.logger.info("Redis не настроен, работа без кеширования")

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

        # HTTP сессия для API
        timeout = ClientTimeout(total=60)
        self.session = ClientSession(timeout=timeout)

        # Очередь для обработки
        self.queue = asyncio.Queue(maxsize=Config.MAX_QUEUE_SIZE)

        self.logger.info("Инициализация завершена")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def fetch_wb_sales(self) -> List[SaleData]:
        """Получение продаж с WB API с кешированием"""
        cache_key = f"wb_sales:{datetime.now().strftime('%Y-%m-%d')}"

        # Проверка кеша Redis (если доступен)
        if self.redis_client:
            try:
                cached = await self.redis_client.get(cache_key)
                if cached:
                    self.logger.debug("Данные из кеша Redis")
                    return [SaleData(**item) for item in json.loads(cached)]
            except Exception as e:
                self.logger.warning(f"Ошибка чтения кеша: {e}")

        # Запрос к API
        date_from = (datetime.now(pytz.timezone('Europe/Moscow')) -
                     timedelta(hours=Config.SALES_HOURS_FILTER + 1)).strftime("%Y-%m-%d")

        url = "https://statistics-api.wildberries.ru/api/v1/supplier/sales"
        headers = {
            "Authorization": Config.WB_TOKEN,
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

                    # Преобразование и фильтрация
                    sales = []
                    for item in data:
                        if item.get("isRealization"):
                            sale = SaleData.from_api_dict(item)
                            if sale and self._is_recent_sale(sale):
                                sales.append(sale)

                    self.logger.info(f"Отфильтровано {len(sales)} актуальных продаж")

                    # Кеширование в Redis (если доступен)
                    if self.redis_client and sales:
                        try:
                            await self.redis_client.setex(
                                cache_key,
                                Config.REDIS_TTL,
                                json.dumps([asdict(s) for s in sales])
                            )
                        except Exception as e:
                            self.logger.warning(f"Ошибка записи в кеш: {e}")

                    return sales
                else:
                    text = await resp.text()
                    self.logger.error(f"Ошибка API WB: {resp.status} - {text[:200]}")
                    return []

        except Exception as e:
            self.logger.error(f"Ошибка при запросе к WB API: {e}")
            raise

    def _is_recent_sale(self, sale: SaleData) -> bool:
        """Проверка актуальности продажи"""
        try:
            if not sale.date:
                return False

            if 'Z' in sale.date:
                sale_dt = datetime.fromisoformat(sale.date.replace('Z', '+00:00'))
            else:
                sale_dt = datetime.fromisoformat(sale.date)

            if sale_dt.tzinfo is None:
                sale_dt = pytz.utc.localize(sale_dt)

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
                                   price_wb, wb_real_price, status
                            FROM oc_product
                            WHERE model IN ({placeholders})
                              AND purchase_price > 0
                              AND target_profit_rub > 0
                              AND status = 1
                        """, batch)

                        rows = await cursor.fetchall()

                        for row in rows:
                            product = ProductData.from_db_row(row)
                            if product:
                                product_map[product.vendor_code] = product

            except Exception as e:
                self.logger.error(f"Ошибка загрузки батча {i}: {e}")
                continue

        return product_map

    async def process_product(self, vendor_code: str,
                              sales: List[SaleData],
                              product: ProductData) -> PriceUpdate:
        """Обработка одного товара"""
        try:
            # Валидация входных данных
            if not sales or not product:
                return PriceUpdate(
                    vendor_code=vendor_code,
                    new_price_wb=0,
                    new_real_price=0,
                    old_price_wb=product.current_price_wb if product else 0,
                    profit_diff=0,
                    status=ProcessingStatus.SKIPPED_NO_DATA,
                    error_msg="Нет данных о продажах или товаре"
                )

            # Агрегация данных по продажам
            finished_prices = []
            clean_forpays = []

            for sale in sales:
                if sale.finished_price > 0 and sale.for_pay > 0:
                    # Исключаем СПП из расчета
                    spp_amount = sale.finished_price * (sale.spp_percent / 100.0)
                    clean_fpay = max(0.01, sale.for_pay - spp_amount)

                    # Учитываем количество (если продано несколько штук)
                    for _ in range(sale.quantity):
                        finished_prices.append(sale.finished_price)
                        clean_forpays.append(clean_fpay)

            # Проверка достаточности данных
            if len(finished_prices) < Config.MIN_SALES_FOR_CALC:
                return PriceUpdate(
                    vendor_code=vendor_code,
                    new_price_wb=0,
                    new_real_price=0,
                    old_price_wb=product.current_price_wb,
                    profit_diff=0,
                    status=ProcessingStatus.SKIPPED_NO_DATA,
                    error_msg=f"Недостаточно продаж: {len(finished_prices)}"
                )

            # Расчет средних значений
            avg_finished = sum(finished_prices) / len(finished_prices)
            avg_clean_fpay = sum(clean_forpays) / len(clean_forpays)

            # Расчет прибыли
            actual_profit = avg_clean_fpay * (1 - Config.BANK_COMMISSION) - product.purchase_price
            profit_diff = actual_profit - product.target_profit

            # Новая цена
            new_price_wb = avg_finished - profit_diff

            # Валидация и защитные проверки
            validation_result = self._validate_price_update(
                vendor_code, product, new_price_wb, avg_finished, profit_diff
            )

            # Создание аналитических данных ДО проверки валидации
            # чтобы сохранить данные даже если цена не изменилась
            analytics_data = AnalyticsData.from_sales_data(
                vendor_code=vendor_code,
                sales=sales,
                product=product,
                recommended_price=new_price_wb if new_price_wb > 0 else 0
            )

            if validation_result:
                validation_result.analytics_data = analytics_data
                return validation_result

            # Создание объекта обновления с аналитикой
            update = PriceUpdate(
                vendor_code=vendor_code,
                new_price_wb=round(new_price_wb, 2),
                new_real_price=round(avg_finished, 2),
                old_price_wb=product.current_price_wb,
                profit_diff=profit_diff,
                status=ProcessingStatus.SUCCESS,
                analytics_data=analytics_data
            )

            return update

        except Exception as e:
            self.logger.error(f"Ошибка обработки {vendor_code}: {e}")

            return PriceUpdate(
                vendor_code=vendor_code,
                new_price_wb=0,
                new_real_price=0,
                old_price_wb=product.current_price_wb if product else 0,
                profit_diff=0,
                status=ProcessingStatus.ERROR,
                error_msg=str(e)
            )

    def _validate_price_update(self, vendor_code: str, product: ProductData,
                               new_price: float, real_price: float, profit_diff: float) -> Optional[PriceUpdate]:
        """Валидация обновления цены"""
        # Минимальная цена
        min_allowed_price = product.purchase_price * Config.MIN_MARGIN_FACTOR
        if new_price < min_allowed_price:
            return PriceUpdate(
                vendor_code=vendor_code,
                new_price_wb=0,
                new_real_price=real_price,
                old_price_wb=product.current_price_wb,
                profit_diff=profit_diff,
                status=ProcessingStatus.SKIPPED_MIN_PRICE,
                error_msg=f"Цена ниже минимума: {new_price:.2f} < {min_allowed_price:.2f}"
            )

        # Минимальное изменение
        price_change_abs = abs(new_price - product.current_price_wb)
        if price_change_abs < Config.MIN_PRICE_CHANGE:
            return PriceUpdate(
                vendor_code=vendor_code,
                new_price_wb=0,
                new_real_price=real_price,
                old_price_wb=product.current_price_wb,
                profit_diff=profit_diff,
                status=ProcessingStatus.SKIPPED_MIN_CHANGE,
                error_msg=f"Изменение меньше порога: {price_change_abs:.2f}"
            )

        # Максимальное процентное изменение
        if product.current_price_wb > 0:
            price_change_percent = abs((new_price - product.current_price_wb) / product.current_price_wb) * 100
            if price_change_percent > Config.MAX_PRICE_CHANGE_PERCENT:
                return PriceUpdate(
                    vendor_code=vendor_code,
                    new_price_wb=0,
                    new_real_price=real_price,
                    old_price_wb=product.current_price_wb,
                    profit_diff=profit_diff,
                    status=ProcessingStatus.SKIPPED_INVALID,
                    error_msg=f"Изменение превышает {Config.MAX_PRICE_CHANGE_PERCENT}%: {price_change_percent:.1f}%"
                )

        # Проверка на NaN/Infinity
        if not (0.01 <= new_price <= 1000000):  # Разумные пределы
            return PriceUpdate(
                vendor_code=vendor_code,
                new_price_wb=0,
                new_real_price=real_price,
                old_price_wb=product.current_price_wb,
                profit_diff=profit_diff,
                status=ProcessingStatus.SKIPPED_INVALID,
                error_msg=f"Цена вне диапазона: {new_price:.2f}"
            )

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

        except Exception as e:
            self.logger.error(f"Ошибка сохранения аналитики для {analytics_data.vendor_code}: {e}")

    async def save_price_update(self, update: PriceUpdate):
        """Сохранение обновления цены в БД"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                        UPDATE oc_product 
                        SET price_wb = %s, 
                            wb_real_price = %s,
                            last_price_update = NOW()
                        WHERE model = %s
                    """, (update.new_price_wb, update.new_real_price, update.vendor_code))

                    self.stats['prices_updated'] += 1

        except Exception as e:
            self.logger.error(f"Ошибка сохранения цены для {update.vendor_code}: {e}")
            raise

    async def worker(self, worker_id: int):
        """Воркер для обработки товаров"""

        try:
            while self.is_running:
                try:
                    # Получение задачи из очереди
                    vendor_code, sales, product = await asyncio.wait_for(
                        self.queue.get(),
                        timeout=1.0
                    )

                    # Обработка
                    update = await self.process_product(vendor_code, sales, product)

                    # Сохранение аналитических данных (всегда, если есть)
                    if update.analytics_data:
                        await self.save_analytics_data(update.analytics_data)

                    # Сохранение обновления цены (только если успех)
                    if update.status == ProcessingStatus.SUCCESS:
                        await self.save_price_update(update)

                    # Логирование статистики
                    self.stats[update.status.value] += 1

                    self.queue.task_done()

                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    self.logger.error(f"Ошибка в воркере {worker_id}: {e}")

        except asyncio.CancelledError:
            pass

    async def run_cycle(self):
        """Запуск одного цикла обработки"""
        cycle_start = datetime.now()
        self.logger.info("=" * 60)
        self.logger.info(f"Запуск цикла обработки: {cycle_start.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("=" * 60)

        # Сброс статистики
        self.stats.clear()

        try:
            # Шаг 1: Получение продаж
            self.logger.info("📥 Получение данных о продажах...")
            sales_data = await self.fetch_wb_sales()

            if not sales_data:
                self.logger.warning("Нет данных о продажах для обработки")
                return

            # Шаг 2: Группировка продаж по артикулам
            self.logger.info("📊 Группировка продаж...")
            sales_by_vendor = defaultdict(list)
            for sale in sales_data:
                if sale.vendor_code:
                    sales_by_vendor[sale.vendor_code].append(sale)

            vendor_codes = list(sales_by_vendor.keys())
            self.logger.info(f"Найдено {len(vendor_codes)} артикулов с продажами")

            if not vendor_codes:
                return

            # Шаг 3: Загрузка данных о товарах
            self.logger.info("🛒 Загрузка данных о товарах...")
            product_map = await self.fetch_products_batch(vendor_codes)
            self.logger.info(f"Загружено {len(product_map)} товаров из БД")

            if not product_map:
                self.logger.warning("Нет товаров для обработки")
                return

            # Шаг 4: Постановка задач в очередь
            self.logger.info("⏳ Постановка задач в очередь...")
            queue_tasks = []
            for vendor_code in vendor_codes:
                if vendor_code in product_map:
                    task = (vendor_code,
                            sales_by_vendor[vendor_code],
                            product_map[vendor_code])
                    queue_tasks.append(task)

            # Добавление в очередь
            for task in queue_tasks:
                await self.queue.put(task)

            # Шаг 5: Запуск воркеров
            self.logger.info(f"👷 Запуск {Config.WORKERS_COUNT} воркеров...")
            workers = []
            for i in range(Config.WORKERS_COUNT):
                worker_task = asyncio.create_task(self.worker(i))
                workers.append(worker_task)

            # Ожидание завершения обработки
            await self.queue.join()

            # Отмена воркеров
            for worker_task in workers:
                worker_task.cancel()

            await asyncio.gather(*workers, return_exceptions=True)

            # Шаг 6: Статистика
            cycle_end = datetime.now()
            duration = (cycle_end - cycle_start).total_seconds()

            self.logger.info("=" * 60)
            self.logger.info("📈 СТАТИСТИКА ЦИКЛА:")
            self.logger.info(f"   Всего артикулов: {len(vendor_codes)}")
            self.logger.info(f"   Успешно обработано: {len(product_map)}")
            self.logger.info(f"   Обновлено цен: {self.stats.get('success', 0)}")
            self.logger.info(f"   Сохранено аналитики: {self.stats.get('analytics_saved', 0)}")
            self.logger.info(f"   Пропущено (мало данных): {self.stats.get('skipped_no_data', 0)}")
            self.logger.info(f"   Пропущено (низкая цена): {self.stats.get('skipped_min_price', 0)}")
            self.logger.info(f"   Пропущено (мало изменений): {self.stats.get('skipped_min_change', 0)}")
            self.logger.info(f"   Ошибок: {self.stats.get('error', 0)}")
            self.logger.info(f"   Время выполнения: {duration:.2f} сек")
            self.logger.info(f"   Скорость: {len(vendor_codes) / duration:.1f} арт/сек")
            self.logger.info("=" * 60)

        except Exception as e:
            self.logger.error(f"Критическая ошибка в цикле: {e}")
            self.logger.error(traceback.format_exc())

    async def run(self):
        """Основной цикл работы системы"""
        self.is_running = True

        cycle_count = 0

        while self.is_running:
            cycle_count += 1
            self.logger.info(f"\n{'#' * 60}")
            self.logger.info(f"ЦИКЛ #{cycle_count}")
            self.logger.info(f"{'#' * 60}")

            try:
                await self.run_cycle()

                # Пауза между циклами
                self.logger.info(f"Ожидание следующего цикла ({Config.CYCLE_INTERVAL // 3600} ч)...")
                await asyncio.sleep(Config.CYCLE_INTERVAL)

            except KeyboardInterrupt:
                self.logger.info("Остановка по запросу пользователя")
                break
            except Exception as e:
                self.logger.error(f"Фатальная ошибка: {e}")
                self.logger.error(traceback.format_exc())

                # Пауза при ошибках
                wait_time = min(300 * (2 ** (cycle_count % 5)), 3600)
                self.logger.info(f"Пауза {wait_time} сек перед повторной попыткой...")
                await asyncio.sleep(wait_time)

    def _handle_shutdown(self, signum, frame):
        """Обработка сигналов завершения"""
        self.logger.info(f"Получен сигнал {signum}, завершение...")
        self.is_running = False

    async def cleanup(self):
        """Очистка ресурсов"""
        self.logger.info("Очистка ресурсов...")

        if self.session and not self.session.closed:
            await self.session.close()
            self.logger.info("✓ HTTP сессия закрыта")

        if self.db_pool:
            self.db_pool.close()
            await self.db_pool.wait_closed()
            self.logger.info("✓ Пул БД закрыт")

        if self.redis_client:
            await self.redis_client.close()
            self.logger.info("✓ Redis клиент закрыт")

        self.logger.info("Очистка завершена")


async def main():
    """Точка входа"""
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
    # Проверка обязательных переменных
    if not Config.WB_TOKEN:
        print("ОШИБКА: WB_SALES_TOKEN не установлен!")
        print("Установите переменную окружения WB_SALES_TOKEN")
        sys.exit(1)

    # Запуск
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nПрограмма остановлена пользователем")
        sys.exit(0)