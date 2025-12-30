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

LOAD_PRICE_TO_WB = True

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
    target_forpay: float = 0

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

        class MoscowTimeFormatter(logging.Formatter):
            def __init__(self, fmt=None, datefmt=None):
                super().__init__(fmt, datefmt)
                self.moscow_tz = pytz.timezone('Europe/Moscow')

            def formatTime(self, record, datefmt=None):
                dt_utc = datetime.utcfromtimestamp(record.created).replace(tzinfo=pytz.utc)
                dt_moscow = dt_utc.astimezone(self.moscow_tz)

                if datefmt:
                    return dt_moscow.strftime(datefmt)
                else:
                    return dt_moscow.isoformat()

        formatter = MoscowTimeFormatter(
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

    def _log_separator(self, title: str = "", char: str = "=", length: int = 80):
        """Визуальный разделитель для логов"""
        if title:
            padding = (length - len(title) - 4) // 2
            self.logger.info(f"\n{char * padding} {title} {char * padding}")
        else:
            self.logger.info(f"\n{char * length}")

    def _log_table(self, title: str, data: dict):
        """Логирование данных в табличном формате"""
        self._log_separator(title, "-")
        max_key_len = max(len(str(k)) for k in data.keys())

        for key, value in data.items():
            if isinstance(value, float):
                value_str = f"{value:,.2f}" if value >= 1000 else f"{value:.2f}"
            else:
                value_str = str(value)

            self.logger.info(f"  {key:<{max_key_len}} : {value_str}")
        self._log_separator("-", "-")

    async def initialize(self):
        self._log_separator("НАЧАЛО РАБОТЫ СИСТЕМЫ ЦЕНООБРАЗОВАНИЯ", "=")
        self.logger.info("Инициализация системы...")

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
            self.logger.info("✅ Подключение к базе данных: УСПЕШНО")
            self.db_logger = DatabaseLogger(self.db_pool)

        except Exception as e:
            self.logger.error(f"❌ Подключение к базе данных: ОШИБКА - {e}")
            raise

        timeout = ClientTimeout(total=60)
        self.session = ClientSession(timeout=timeout)
        self.queue = asyncio.Queue(maxsize=Config.MAX_QUEUE_SIZE)

        self._log_table("КОНФИГУРАЦИЯ СИСТЕМЫ", {
            "WORKERS_COUNT": Config.WORKERS_COUNT,
            "BATCH_SIZE": Config.BATCH_SIZE,
            "SALES_HOURS_FILTER": f"{Config.SALES_HOURS_FILTER}ч",
            "CYCLE_INTERVAL": f"{Config.CYCLE_INTERVAL // 3600}ч {(Config.CYCLE_INTERVAL % 3600) // 60}м",
            "MIN_SALES_FOR_CALC": Config.MIN_SALES_FOR_CALC,
            "MIN_MARGIN_FACTOR": f"×{Config.MIN_MARGIN_FACTOR}",
            "BANK_COMMISSION": f"{Config.BANK_COMMISSION * 100}%"
        })

        self.logger.info("✅ Инициализация завершена успешно")
        self._log_separator()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def fetch_wb_sales(self) -> List[SaleData]:
        self._log_separator("ПОЛУЧЕНИЕ ПРОДАЖ С WB", "-")

        moscow_tz = pytz.timezone('Europe/Moscow')
        period_start = datetime.now(moscow_tz) - timedelta(hours=Config.SALES_HOURS_FILTER)
        date_from = period_start.strftime("%Y-%m-%dT%H:%M:%S")

        self.logger.info(f"📊 Запрашиваем продажи за последние {Config.SALES_HOURS_FILTER} часов")
        self.logger.info(f"📅 Период: с {date_from}")

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
                    self.logger.info(f"📥 Получено {len(data)} записей от WB API")

                    sales = []
                    invalid_sales = 0

                    for item in data:
                        if item.get("isRealization"):
                            sale = SaleData.from_api_dict(item)
                            if sale and sale.finished_price > 0:
                                sales.append(sale)
                            else:
                                invalid_sales += 1
                        else:
                            invalid_sales += 1

                    self.logger.info(f"📈 Актуальных продаж: {len(sales)}")
                    if invalid_sales > 0:
                        self.logger.warning(f"⚠️  Отфильтровано невалидных записей: {invalid_sales}")

                    # Группируем по vendor_code для лога
                    sales_by_vendor = defaultdict(int)
                    for sale in sales:
                        sales_by_vendor[sale.vendor_code] += 1

                    if sales_by_vendor:
                        self.logger.info("📋 Распределение продаж по артикулам:")
                        for vendor, count in sorted(sales_by_vendor.items(), key=lambda x: x[1], reverse=True):
                            self.logger.info(f"   • {vendor}: {count} продаж")
                    else:
                        self.logger.warning("⚠️  Нет продаж для обработки")

                    return sales
                else:
                    text = await resp.text()
                    self.logger.error(f"❌ Ошибка API WB: {resp.status}")
                    self.logger.error(f"   Ответ сервера: {text[:200]}")
                    return []

        except Exception as e:
            self.logger.error(f"❌ Ошибка при запросе к WB API: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def get_logistics_by_sa_name_improved(self, sa_names: List[str]) -> Dict[str, float]:
        """
        Получение стоимости логистики из последних доступных отчётов.
        Стратегия: берем данные за 7 дней, находим самую свежую дату с продажами,
        для каждого артикула используем МЕДИАНУ стоимости за эту дату (устойчива к выбросам).
        Для артикулов без данных за последний день используется РЕЗЕРВНАЯ СТРАТЕГИЯ
        (поиск по всему периоду).
        """
        if not sa_names:
            return {}

        self._log_separator("РАСЧЕТ ЛОГИСТИКИ: УЛУЧШЕННЫЙ МЕТОД", "-")
        self.logger.info(f"📦 Анализируем логистику для {len(sa_names)} артикулов")

        moscow_tz = pytz.timezone('Europe/Moscow')
        date_to = datetime.now(moscow_tz).strftime("%Y-%m-%d")
        date_from = (datetime.now(moscow_tz) - timedelta(days=7)).strftime("%Y-%m-%d")

        self.logger.info(f"📅 Запрашиваем период: с {date_from} по {date_to}")

        url = "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"
        headers = {"Authorization": Config.WB_SALES_TOKEN}
        params = {"dateFrom": date_from, "dateTo": date_to, "limit": 100000, "period": "daily"}

        all_filtered_records = []  # Все подходящие записи за период

        try:
            async with self.session.get(url, headers=headers, params=params) as resp:
                if resp.status == 204:
                    self.logger.warning("📭 Отчёт за период пуст (204 No Content)")
                    return {}
                if resp.status != 200:
                    self.logger.error(f"❌ Ошибка API: {resp.status}")
                    return {}
                data = await resp.json()
                self.logger.info(f"📥 Получено {len(data)} записей за период.")

                # 1. ФИЛЬТРАЦИЯ: обрабатываем все записи, собираем нужные данные
                for record in data:
                    sa_name = record.get('sa_name')
                    # Если артикул не в нашем списке, пропускаем
                    if not sa_name or sa_name not in sa_names:
                        continue

                    delivery_amount = record.get('delivery_amount', 0)
                    return_amount = record.get('return_amount', 0)
                    delivery_rub = record.get('delivery_rub', 0)
                    rr_dt = record.get('rr_dt', '')  # Дата операции

                    try:
                        # Преобразуем типы, защищаемся от некорректных данных
                        delivery_amount_int = int(delivery_amount)
                        delivery_rub_float = float(delivery_rub)
                        return_amount_int = int(return_amount) if return_amount else 0
                    except (ValueError, TypeError):
                        continue  # Пропускаем записи с неконвертируемыми данными

                    # Отбираем только основные доставки (не возвраты) с положительной стоимостью
                    if delivery_amount_int > 0 and delivery_rub_float > 0 and return_amount_int == 0:
                        cost_per_unit = delivery_rub_float / delivery_amount_int
                        date_only = rr_dt[:10] if rr_dt else ''

                        all_filtered_records.append({
                            'sa_name': sa_name,
                            'cost_per_unit': cost_per_unit,
                            'date_only': date_only,
                            'delivery_amount': delivery_amount_int,
                            'delivery_rub': delivery_rub_float,
                            'rr_dt': rr_dt
                        })

                self.logger.info(f"✅ После фильтрации осталось {len(all_filtered_records)} записей с логистикой.")

        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка при запросе отчёта: {e}")
            return {}

        # Если после фильтрации данных нет - выходим
        if not all_filtered_records:
            self.logger.error("❌ Нет подходящих записей о доставках для анализа.")
            return {}

        # 2. ОПРЕДЕЛЯЕМ ПОСЛЕДНЮЮ ДАТУ С ДАННЫМИ
        # Получаем уникальные даты из отфильтрованных записей
        unique_dates = sorted({rec['date_only'] for rec in all_filtered_records if rec['date_only']}, reverse=True)

        if not unique_dates:
            self.logger.error("❌ Не удалось определить даты из записей.")
            return {}

        latest_date = unique_dates[0]
        self.logger.info(f"🗓️  Самая последняя дата с данными о доставках: {latest_date}")

        # 3. РАСЧЁТ ПО ОСНОВНОЙ СТРАТЕГИИ (Медиана за последний день)
        logistics_by_article = {}
        processed_articles = set()

        # Группируем записи за последний день по артикулу
        records_for_latest_date = [r for r in all_filtered_records if r['date_only'] == latest_date]
        grouped_by_article_latest = defaultdict(list)
        for rec in records_for_latest_date:
            grouped_by_article_latest[rec['sa_name']].append(rec['cost_per_unit'])

        for sa_name, costs in grouped_by_article_latest.items():
            if costs:
                # Используем МЕДИАНУ - она устойчивее к единичным выбросам, чем максимум
                median_cost = statistics.median(costs)
                logistics_by_article[sa_name] = median_cost
                processed_articles.add(sa_name)
                # Детальное логирование для первых нескольких артикулов
                if len(processed_articles) <= 5:
                    self.logger.info(f"   📊 {sa_name}: {len(costs)} доставок, медиана = {median_cost:.2f}₽ "
                                     f"(диапазон: {min(costs):.2f} - {max(costs):.2f}₽)")

        # 4. РЕЗЕРВНАЯ СТРАТЕГИЯ (Fallback) для артикулов без данных за последний день
        missing_articles = set(sa_names) - processed_articles
        if missing_articles:
            self.logger.info(f"🔍 Для {len(missing_articles)} артикулов нет данных за {latest_date}. "
                             f"Применяем резервную стратегию.")
            # Ищем записи по этим артикулам за ВЕСЬ период (7 дней)
            backup_records = [r for r in all_filtered_records if r['sa_name'] in missing_articles]
            grouped_backup = defaultdict(list)
            for rec in backup_records:
                grouped_backup[rec['sa_name']].append(rec['cost_per_unit'])

            for sa_name in missing_articles:
                costs = grouped_backup.get(sa_name)
                if costs:
                    # Для резервной стратегии тоже используем медиану
                    median_cost = statistics.median(costs)
                    logistics_by_article[sa_name] = median_cost
                    # Находим дату самой последней доставки для этого артикула (для информации)
                    article_dates = sorted(
                        {r['date_only'] for r in backup_records if r['sa_name'] == sa_name and r['date_only']},
                        reverse=True)
                    last_date_for_article = article_dates[0] if article_dates else "нет данных"
                    self.logger.info(f"   ↪️  {sa_name}: использованы данные от {last_date_for_article}, "
                                     f"медиана = {median_cost:.2f}₽ (на основе {len(costs)} доставок)")
                else:
                    # Если даже за 7 дней данных нет - артикул остаётся без значения
                    self.logger.warning(f"   ⚠️  {sa_name}: данных о доставках не найдено за весь период.")

        # 5. ФИНАЛЬНЫЙ РЕЗУЛЬТАТ И СТАТИСТИКА
        self._log_separator("ИТОГИ РАСЧЁТА", "=")
        found_count = len(logistics_by_article)
        missing_final_count = len(sa_names) - found_count

        self.logger.info(f"📊 Результаты расчёта для отчётного периода {date_from} - {date_to}:")
        self.logger.info(f"   ✅ Найдена стоимость для: {found_count} артикулов")
        if missing_final_count > 0:
            self.logger.warning(f"   ⚠️  Не удалось определить стоимость для: {missing_final_count} артикулов")

        if logistics_by_article:
            costs = list(logistics_by_article.values())
            self.logger.info(f"\n📈 Статистика определённых стоимостей логистики (за ед.):")
            self.logger.info(f"   Мин.    : {min(costs):.2f}₽")
            self.logger.info(f"   Медиана : {statistics.median(costs):.2f}₽")
            self.logger.info(f"   Ср.     : {sum(costs) / len(costs):.2f}₽")
            self.logger.info(f"   Макс.   : {max(costs):.2f}₽")

            # Примеры (топ-5 самых "дорогих" по логистике)
            sorted_by_cost = sorted(logistics_by_article.items(), key=lambda x: x[1], reverse=True)[:5]
            self.logger.info(f"\n🏆 Топ-5 артикулов с самой высокой стоимостью логистики:")
            for idx, (art, cost) in enumerate(sorted_by_cost, 1):
                self.logger.info(f"   {idx}. {art}: {cost:.2f}₽")

        return logistics_by_article

    def _get_last_non_zero_spp(self, sales: List[SaleData]) -> float:
        try:
            spp_sales = [sale for sale in sales if sale.spp_percent > 0]

            if not spp_sales:
                self.logger.debug(f"📉 Нет продаж с положительным СПП, используем 0%")
                return 0.0

            avg_spp = sum(sale.spp_percent for sale in spp_sales) / len(spp_sales)
            self.logger.info(f"📊 Средний СПП: {avg_spp:.2f}% (на основе {len(spp_sales)} продаж)")
            return avg_spp

        except Exception as e:
            self.logger.warning(f"⚠️  Ошибка при расчете среднего СПП: {e}. Используем 0%")
            return 0.0

    async def fetch_products_batch(self, vendor_codes: List[str]) -> Dict[str, ProductData]:
        if not vendor_codes:
            return {}

        self._log_separator("ЗАГРУЗКА ТОВАРОВ ИЗ БАЗЫ ДАННЫХ", "-")
        self.logger.info(f"🛒 Загружаем информацию о {len(vendor_codes)} товарах")

        product_map = {}
        loaded_count = 0
        skipped_count = 0

        for i in range(0, len(vendor_codes), Config.BATCH_SIZE):
            batch = vendor_codes[i:i + Config.BATCH_SIZE]
            batch_num = i // Config.BATCH_SIZE + 1
            total_batches = (len(vendor_codes) + Config.BATCH_SIZE - 1) // Config.BATCH_SIZE

            self.logger.info(f"📦 Батч {batch_num}/{total_batches}: {len(batch)} товаров")

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

                        for row in rows:
                            product = ProductData.from_db_row(row)
                            if product:
                                product_map[product.vendor_code] = product
                                loaded_count += 1
                            else:
                                skipped_count += 1

                        self.logger.info(f"   ✓ Загружено: {len(rows)} записей")

            except Exception as e:
                self.logger.error(f"❌ Ошибка загрузки батча {batch_num}: {e}")
                continue

        # Логируем результаты загрузки
        self._log_separator("РЕЗУЛЬТАТЫ ЗАГРУЗКИ ТОВАРОВ", "-")
        self.logger.info(f"✅ Успешно загружено: {loaded_count} товаров")
        self.logger.info(f"⚠️  Пропущено (нет в БД/неактивны): {skipped_count}")

        if loaded_count > 0:
            self.logger.info("📋 Примеры загруженных товаров:")
            sample_items = list(product_map.items())[:3]  # Показываем первые 3
            for vendor_code, product in sample_items:
                self.logger.info(f"   • {vendor_code}: закуп={product.purchase_price:.0f}₽, "
                                 f"цель={product.target_profit:.0f}₽, цена={product.current_price_wb:.0f}₽")

        return product_map

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def fetch_nm_id(self, vendor_code: str) -> int:
        self.logger.info(f"🔍 Поиск nmID для артикула: {vendor_code}")

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
                        self.logger.error(f"❌ API вернул невалидный JSON")
                        return 0

                    cards = data.get("cards", [])
                    self.logger.info(f"📋 Найдено карточек: {len(cards)}")

                    if not cards:
                        self.logger.warning(f"⚠️  Не найдено карточек для {vendor_code}")
                        return 0

                    for card in cards:
                        card_vendor_code = str(card.get("vendorCode", "")).strip()
                        if card_vendor_code == str(vendor_code).strip():
                            nm_id = card.get("nmID", 0)
                            if nm_id:
                                self.logger.info(f"✅ Найден nmID: {nm_id} для {vendor_code}")
                                return nm_id

                    self.logger.warning(f"⚠️  Не найден nmID для артикула {vendor_code} в ответе API")
                    return 0

                elif resp.status == 401:
                    self.logger.error("❌ Ошибка авторизации: неверный токен Content API")
                    return 0
                else:
                    self.logger.error(f"❌ Ошибка API: {resp.status}")
                    return 0

        except asyncio.TimeoutError:
            self.logger.error(f"⏰ Таймаут при поиске nmID для '{vendor_code}'")
            return 0
        except Exception as e:
            self.logger.error(f"❌ Ошибка при поиске nmID для '{vendor_code}': {str(e)}")
            return 0

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def get_wb_discounts(self, nm_ids: List[int]) -> Dict[int, Dict[str, float]]:
        if not nm_ids:
            return {}

        self._log_separator("ПОЛУЧЕНИЕ АКТУАЛЬНЫХ СКИДОК С WB", "-")
        self.logger.info(f"🎯 Запрашиваем скидки для {len(nm_ids)} товаров")

        results = {}
        url = "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter"
        headers = {
            "Authorization": Config.WB_PRICES_TOKEN,
            "Content-Type": "application/json"
        }

        success_count = 0
        error_count = 0

        for i, nm_id in enumerate(nm_ids, 1):
            self.logger.info(f"📊 Товар {i}/{len(nm_ids)}: nmID {nm_id}")

            try:
                params = {
                    "limit": 1,
                    "offset": 0,
                    "filterNmID": nm_id
                }

                async with self.session.get(url, headers=headers, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()

                        if not data.get("error") and data.get("data", {}).get("listGoods"):
                            goods_info = data["data"]["listGoods"][0]

                            discount = float(goods_info.get("discount", 0))
                            club_discount = float(goods_info.get("clubDiscount", 0))
                            effective_discount = club_discount if club_discount > 0 else discount

                            results[nm_id] = {
                                "discount": discount,
                                "club_discount": club_discount,
                                "effective_discount": effective_discount
                            }

                            self.logger.info(f"   ✅ Скидки: обычная={discount}%, "
                                             f"клубная={club_discount}%, используем={effective_discount}%")
                            success_count += 1
                        else:
                            self.logger.warning(f"   ⚠️  Не удалось получить скидки: "
                                                f"{data.get('errorText', 'Unknown error')}")
                            results[nm_id] = {"effective_discount": 0}
                            error_count += 1

                    else:
                        text = await resp.text()
                        self.logger.warning(f"   ⚠️  Ошибка API: {resp.status}")
                        results[nm_id] = {"effective_discount": 0}
                        error_count += 1

                    await asyncio.sleep(0.15)

            except Exception as e:
                self.logger.error(f"   ❌ Ошибка: {e}")
                results[nm_id] = {"effective_discount": 0}
                error_count += 1
                await asyncio.sleep(0.5)

        self._log_separator("ИТОГИ ПОЛУЧЕНИЯ СКИДОК", "-")
        self.logger.info(f"✅ Успешно: {success_count}")
        self.logger.info(f"⚠️  С ошибками: {error_count}")

        return results

    async def process_product_new_logic(self, vendor_code: str,
                                        sales: List[SaleData],
                                        product: ProductData,
                                        logistics_cost: float) -> PriceUpdate:
        try:
            self._log_separator(f"АНАЛИЗ ТОВАРА: {vendor_code}", "=")

            # Получаем историю логистики из отчетов WB за последние 7 дней
            historical_logistics = await self.get_logistics_history_from_reports(vendor_code)

            # Умная стратегия буфера
            base_buffer_factor = 1.07  # 5% буфер по умолчанию
            final_buffer_factor = 1.0  # Итоговый коэффициент
            apply_buffer = True
            buffer_reason = "стандартный буфер 7%"

            if historical_logistics and len(historical_logistics) >= 2:
                current_logistics = logistics_cost

                # 1. Если есть данные за несколько дней, анализируем тренд
                avg_historical = statistics.mean(historical_logistics)
                max_historical = max(historical_logistics)
                min_historical = min(historical_logistics)

                self.logger.info(f"📊 ИСТОРИЯ ЛОГИСТИКИ {vendor_code}:")
                self.logger.info(f"   Текущая: {current_logistics:.0f}₽")
                self.logger.info(f"   Средняя за период: {avg_historical:.0f}₽")
                self.logger.info(f"   Максимум: {max_historical:.0f}₽")
                self.logger.info(f"   Минимум: {min_historical:.0f}₽")

                # Анализ тренда
                if len(historical_logistics) >= 3:
                    # Разделяем на две части: старые и новые данные
                    old_part = historical_logistics[:-2]
                    new_part = historical_logistics[-2:]

                    avg_old = statistics.mean(old_part)
                    avg_new = statistics.mean(new_part)

                    # Если тренд роста - увеличиваем буфер
                    if avg_new > avg_old * 1.10:  # Рост на 10%
                        base_buffer_factor = 1.08  # 8% буфер
                        buffer_reason = "тренд роста (8%)"
                        self.logger.info(f"   📈 Тренд: рост (+{(avg_new / avg_old * 100 - 100):.0f}%)")
                    # Если тренд снижения - уменьшаем буфер
                    elif avg_new < avg_old * 0.90:  # Снижение на 10%
                        base_buffer_factor = 1.02  # 2% буфер
                        buffer_reason = "тренд снижения (2%)"
                        self.logger.info(f"   📉 Тренд: снижение ({(avg_new / avg_old * 100 - 100):.0f}%)")

                # 2. Анализ волатильности
                if len(historical_logistics) >= 3:
                    stdev = statistics.stdev(historical_logistics) if len(historical_logistics) > 1 else 0
                    cv = (stdev / avg_historical * 100) if avg_historical > 0 else 0

                    self.logger.info(f"   Волатильность: {cv:.1f}%")

                    # Если высокая волатильность - увеличиваем буфер
                    if cv > 30:
                        base_buffer_factor = 1.08
                        buffer_reason = "высокая волатильность (8%)"

                # 3. Если текущая логистика уже выше средней - увеличиваем буфер
                if current_logistics > avg_historical * 1.15:
                    base_buffer_factor = 1.10  # 10% буфер
                    buffer_reason = "текущая выше средней на 15% (10%)"

                # 4. Если логистика сильно упала - уменьшаем буфер
                elif current_logistics < avg_historical * 0.85:
                    base_buffer_factor = 1.02  # 2% буфер
                    buffer_reason = "сильное снижение (2%)"

                # 5. Если логистика близка к максимуму за период - увеличиваем буфер
                if current_logistics > max_historical * 0.95:
                    base_buffer_factor = max(base_buffer_factor, 1.08)
                    buffer_reason = f"близко к максимуму ({buffer_reason})"

            else:
                self.logger.info(f"📊 ИСТОРИЯ ЛОГИСТИКИ {vendor_code}: недостаточно данных")
                buffer_reason = "недостаточно истории, стандартный буфер 5%"

            # Применяем буфер
            logistics_cost_buffered = logistics_cost * base_buffer_factor
            final_buffer_factor = base_buffer_factor

            self.logger.info(f"📊 ИСХОДНЫЕ ДАННЫЕ:")
            logistics_info = {
                "Артикул": vendor_code,
                "Закупочная цена": f"{product.purchase_price:.2f}₽",
                "Целевая прибыль": f"{product.target_profit:.2f}₽",
                "Текущая цена WB": f"{product.current_price_wb:.2f}₽",
                "Логистика (из отчета)": f"{logistics_cost:.2f}₽",
                "Логистика (с буфером)": f"{logistics_cost_buffered:.2f}₽",
                "Коэффициент буфера": f"{final_buffer_factor}",
                "Причина буфера": buffer_reason,
                "nmID": product.sku_wb if product.sku_wb else "не установлен"
            }

            if historical_logistics:
                days_count = len(historical_logistics)
                logistics_info[
                    f"История ({days_count} дней)"] = f"{min(historical_logistics):.0f}-{max(historical_logistics):.0f}₽"

            self._log_table("ПАРАМЕТРЫ ТОВАРА", logistics_info)

            self.logger.info(f"📈 ДАННЫЕ О ПРОДАЖАХ:")
            self.logger.info(f"   Всего продаж: {len(sales)}")
            self.logger.info(f"   Требуется для расчета: {Config.MIN_SALES_FOR_CALC}")

            # Логируем детали каждой продажи
            if len(sales) <= 10:  # Логируем подробно если продаж мало
                self.logger.info("   Детали продаж:")
                for i, sale in enumerate(sales, 1):
                    self.logger.info(f"   {i:2d}. totalPrice: {sale.total_price:7.0f}₽ | "
                                     f"discount: {sale.discount_percent:5.1f}% | "
                                     f"priceWithDisc: {sale.price_with_desc:7.0f}₽ | "
                                     f"forPay: {sale.for_pay:7.0f}₽")
            else:
                # Для большого количества продаж логируем статистику
                price_stats = {
                    "min": min(s.price_with_desc for s in sales),
                    "max": max(s.price_with_desc for s in sales),
                    "avg": statistics.mean(s.price_with_desc for s in sales)
                }
                self.logger.info(f"   Статистика priceWithDisc: "
                                 f"мин={price_stats['min']:.0f}₽, "
                                 f"макс={price_stats['max']:.0f}₽, "
                                 f"сред={price_stats['avg']:.0f}₽")

            valid_sales = []
            for sale in sales:
                if (sale.price_with_desc > 0 and
                        sale.discount_percent > 0 and
                        sale.for_pay > 0):
                    valid_sales.append(sale)

            self.logger.info(f"✅ Валидных продаж: {len(valid_sales)}")

            if len(valid_sales) < Config.MIN_SALES_FOR_CALC:
                self._log_separator("РЕШЕНИЕ: ПРОПУСК", "!")
                self.logger.warning(f"⚠️  НЕДОСТАТОЧНО ДАННЫХ")
                self.logger.warning(f"   Требуется: {Config.MIN_SALES_FOR_CALC}, имеется: {len(valid_sales)}")

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
                    logistics_cost=logistics_cost_buffered,
                    finished_price=0
                )

            # Получение или поиск nmID
            if product.sku_wb == 0:
                self.logger.info("🔍 Поиск nmID...")
                new_nm_id = await self.fetch_nm_id(vendor_code)
                if new_nm_id == 0:
                    self.logger.error("❌ Не удалось получить nmID")
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
                        logistics_cost=logistics_cost_buffered,
                        finished_price=0
                    )
                await self.save_nm_id_to_db(vendor_code, new_nm_id)
                product.sku_wb = new_nm_id
                self.logger.info(f"✅ nmID сохранен: {new_nm_id}")

            self.logger.info("🎯 РАСЧЕТ СКИДКИ ИЗ ПРОДАЖ:")

            # Собираем все скидки из валидных продаж
            discount_list = [sale.discount_percent for sale in valid_sales if sale.discount_percent > 0]

            if discount_list:
                # Используем медиану для устойчивости к выбросам
                median_discount = statistics.median(discount_list)

                # Логируем статистику скидок
                min_discount = min(discount_list)
                max_discount = max(discount_list)
                mean_discount = statistics.mean(discount_list)

                self._log_table("СТАТИСТИКА СКИДОК ИЗ ПРОДАЖ", {
                    "Всего значений": len(discount_list),
                    "Мин. скидка": f"{min_discount:.1f}%",
                    "Макс. скидка": f"{max_discount:.1f}%",
                    "Средняя скидка": f"{mean_discount:.1f}%",
                    "Медианная скидка": f"{median_discount:.1f}%"
                })

                # Используем медианную скидку
                api_discount = median_discount
                self.logger.info(f"✅ Используем медианную скидку: {api_discount:.1f}%")
            else:
                # Если нет данных о скидках в продажах
                self.logger.warning("⚠️  В продажах нет данных о скидках")

                # Попробуем получить из API как fallback
                discounts_info = await self.get_wb_discounts([product.sku_wb])
                api_discount = discounts_info.get(product.sku_wb, {}).get("effective_discount", 0)

                if api_discount > 0:
                    self.logger.info(f"✅ Используем скидку из API: {api_discount:.1f}%")
                else:
                    api_discount = 0.1  # Минимальная скидка
                    self.logger.info(f"⚠️  Устанавливаем минимальную скидку: {api_discount:.1f}%")

            # Ограничиваем максимальную скидку
            if api_discount >= 100:
                api_discount = 99.9
                self.logger.warning(f"⚠️  Скидка скорректирована до максимума: {api_discount:.1f}%")
            elif api_discount < 0:
                api_discount = 0.1
                self.logger.warning(f"⚠️  Скидка скорректирована до минимума: {api_discount:.1f}%")

            # Сбор статистики по продажам
            self.logger.info("📊 АНАЛИЗ ПРОДАЖ:")

            price_wd_list = []
            forpay_list = []
            forpay_to_price_ratios = []

            for sale in valid_sales:
                price_wd_list.append(sale.price_with_desc)
                forpay_list.append(sale.for_pay)
                if sale.price_with_desc > 0 and sale.for_pay > 0:
                    ratio = sale.for_pay / sale.price_with_desc
                    forpay_to_price_ratios.append(ratio)

            avg_price_wd = statistics.mean(price_wd_list)
            avg_forpay = statistics.mean(forpay_list)
            avg_ratio = statistics.mean(forpay_to_price_ratios) if forpay_to_price_ratios else 0.674

            self._log_table("СТАТИСТИКА ПРОДАЖ", {
                "Средний priceWithDisc": f"{avg_price_wd:.2f}₽",
                "Средний forPay": f"{avg_forpay:.2f}₽",
                "Соотношение forPay/priceWithDisc": f"{avg_ratio:.3f}",
                "Продаж использовано": len(valid_sales),
                "Скидка Продавца": f"{api_discount:.1f}%"
            })

            # Расчет текущей прибыли
            self.logger.info("🧮 РАСЧЕТ ТЕКУЩЕЙ ПРИБЫЛИ:")

            bank_commission_current = avg_forpay * Config.BANK_COMMISSION
            current_profit = avg_forpay - logistics_cost_buffered - bank_commission_current - product.purchase_price

            self._log_table("РАСЧЕТ ТЕКУЩЕЙ ПРИБЫЛИ", {
                "Средний forPay": f"{avg_forpay:.2f}₽",
                "Логистика": f"-{logistics_cost_buffered:.2f}₽",
                "Банковская комиссия": f"-{bank_commission_current:.2f}₽ ({Config.BANK_COMMISSION * 100}%)",
                "Закупочная цена": f"-{product.purchase_price:.2f}₽",
                "ИТОГО ПРИБЫЛЬ": f"{current_profit:.2f}₽"
            })

            # Расчет целевого forPay
            self.logger.info("🎯 РАСЧЕТ ЦЕЛЕВЫХ ПОКАЗАТЕЛЕЙ:")

            target_forpay = (product.target_profit + logistics_cost_buffered + product.purchase_price) / (
                    1 - Config.BANK_COMMISSION)

            bank_commission_target = target_forpay * Config.BANK_COMMISSION
            expected_profit = target_forpay - logistics_cost_buffered - bank_commission_target - product.purchase_price

            self._log_table("РАСЧЕТ ДЛЯ ЦЕЛЕВОЙ ПРИБЫЛИ", {
                "Целевая прибыль": f"{product.target_profit:.2f}₽",
                "+ Логистика": f"+{logistics_cost_buffered:.2f}₽",
                "+ Закупка": f"+{product.purchase_price:.2f}₽",
                "Сумма до комиссии": f"{(product.target_profit + logistics_cost_buffered + product.purchase_price):.2f}₽",
                "Банковская комиссия": f"{Config.BANK_COMMISSION * 100}%",
                "Требуемый forPay": f"{target_forpay:.2f}₽",
                "Ожидаемая прибыль": f"{expected_profit:.2f}₽"
            })

            # Расчет новой цены
            self.logger.info("💵 РАСЧЕТ НОВОЙ ЦЕНЫ:")

            required_price_wd = target_forpay / avg_ratio
            new_price_wd = required_price_wd
            price_wd_diff = new_price_wd - avg_price_wd

            self._log_table("РАСЧЕТ НОВОГО PRICEWITHDISC", {
                "Требуемый forPay": f"{target_forpay:.2f}₽",
                "Среднее соотношение": f"{avg_ratio:.3f}",
                "Нужный priceWithDisc": f"{required_price_wd:.2f}₽",
                "Текущий priceWithDisc": f"{avg_price_wd:.2f}₽",
                "Изменение": f"{price_wd_diff:+.2f}₽"
            })

            # Проверка минимальной цены
            min_price = product.purchase_price * Config.MIN_MARGIN_FACTOR
            self.logger.info(f"📏 ПРОВЕРКА МИНИМАЛЬНОЙ ЦЕНЫ:")
            self.logger.info(f"   Минимальная цена: {min_price:.2f}₽ (закупка × {Config.MIN_MARGIN_FACTOR})")
            self.logger.info(f"   Рассчитанная цена: {new_price_wd:.2f}₽")

            if new_price_wd < min_price:
                self.logger.warning(f"⚠️  ЦЕНА НИЖЕ МИНИМАЛЬНОЙ!")
                self.logger.warning(f"   {new_price_wd:.0f}₽ < {min_price:.0f}₽")
                self.logger.info(f"   Корректируем до минимальной: {min_price:.2f}₽")

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
                price_wd_diff = new_price_wd - avg_price_wd

            # Расчет СПП
            spp = self._get_last_non_zero_spp(sales)
            finished_price = new_price_wd * (1 - spp / 100)
            self.logger.info(f"📉 СПП (не учитывается): {spp:.1f}%")
            self.logger.info(f"   Цена со скидкой СПП: {finished_price:.2f}₽")

            # Расчет полной цены на WB
            if api_discount >= 100:
                api_discount = 99.9

            new_total_price = new_price_wd / (1 - api_discount / 100)
            new_total_price_rounded = round(new_total_price, 0)

            price_change_absolute = new_total_price_rounded - product.current_price_wb
            price_change_percent = (
                    price_change_absolute / product.current_price_wb * 100) if product.current_price_wb > 0 else 0

            self._log_table("РАСЧЕТ ЦЕНЫ НА WB", {
                "Новый priceWithDisc": f"{new_price_wd:.2f}₽",
                "Скидка WB": f"{api_discount:.1f}%",
                "Цена без скидки": f"{new_total_price:.2f}₽",
                "Округленная цена": f"{new_total_price_rounded:.0f}₽",
                "Текущая цена WB": f"{product.current_price_wb:.0f}₽",
                "Изменение цены": f"{price_change_absolute:+.0f}₽",
                "Процент изменения": f"{price_change_percent:+.1f}%"
            })

            # Валидация
            self.logger.info("✅ ПРОВЕРКА ВАЛИДАЦИИ:")
            validation = self._validate_price_update(
                vendor_code, product, new_price_wd, new_total_price_rounded, price_wd_diff
            )

            if validation:
                validation.discount = api_discount
                validation.logistics_cost = logistics_cost_buffered

                self._log_separator("РЕШЕНИЕ: НЕ ИЗМЕНЯТЬ ЦЕНУ", "!")
                self.logger.warning(f"❌ {validation.reason}")

                await self.db_logger.log(
                    level="WARNING",
                    message=f"Валидация не пройдена: {validation.status.value}",
                    vendor_code=vendor_code,
                    details={"status": validation.status.value, "reason": validation.reason}
                )
                return validation

            # Создание результата
            update = PriceUpdate(
                vendor_code=vendor_code,
                new_price_wb=new_total_price_rounded,
                new_real_price=round(new_price_wd, 2),
                old_price_wb=product.current_price_wb,
                old_real_price=product.current_real_price,
                profit_correction=abs(price_wd_diff),
                status=ProcessingStatus.SUCCESS,
                error_msg=f"Корректировка прибыли: {product.target_profit - current_profit:+.0f} ₽",
                discount=api_discount,
                sku_wb=product.sku_wb,
                logistics_cost=logistics_cost_buffered,
                finished_price=finished_price,
                current_profit=current_profit,
                target_profit=product.target_profit,
                sales_count=len(valid_sales),
                spp_used=spp,
                purchase_price=product.purchase_price,
                target_forpay=target_forpay
            )

            # Итоговый отчет
            self._log_separator("ИТОГОВОЕ РЕШЕНИЕ", "✅")
            self._log_table("РЕЗУЛЬТАТ РАСЧЕТА", {
                "Артикул": vendor_code,
                "Старая цена": f"{product.current_price_wb:.0f}₽",
                "Новая цена": f"{new_total_price_rounded:.0f}₽",
                "Изменение цены": f"{price_change_absolute:+.0f}₽ ({price_change_percent:+.1f}%)",
                "Старая прибыль": f"{current_profit:.0f}₽",
                "Целевая прибыль": f"{product.target_profit:.0f}₽",
                "Изменение прибыли": f"{product.target_profit - current_profit:+.0f}₽",
                "Использовано продаж": len(valid_sales),
                "Скидка WB": f"{api_discount:.1f}%",
                "Логистика (из отчета)": f"{logistics_cost:.0f}₽",
                "Логистика (с буфером)": f"{logistics_cost_buffered:.0f}₽",
                "Буфер": f"{buffer_reason}"
            })

            await self.db_logger.log(
                level="SUCCESS",
                message=f"Цена успешно рассчитана",
                vendor_code=vendor_code,
                details={
                    "old_price": product.current_price_wb,
                    "new_price": new_total_price_rounded,
                    "price_change": new_total_price_rounded - product.current_price_wb,
                    "old_price_wd": avg_price_wd,
                    "new_price_wd": new_price_wd,
                    "price_wd_diff": price_wd_diff,
                    "current_profit": current_profit,
                    "target_profit": product.target_profit,
                    "profit_diff": product.target_profit - current_profit,
                    "avg_ratio_forpay_to_price": avg_ratio,
                    "target_forpay": target_forpay,
                    "expected_profit_with_new_price": expected_profit,
                    "logistics_from_report": logistics_cost,
                    "logistics_with_buffer": logistics_cost_buffered,
                    "buffer_factor": final_buffer_factor,
                    "buffer_reason": buffer_reason,
                    "historical_logistics_data_points": len(historical_logistics) if historical_logistics else 0
                }
            )

            return update

        except Exception as e:
            error_msg = f"❌ КРИТИЧЕСКАЯ ОШИБКА при обработке {vendor_code}: {e}"
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())

            await self.db_logger.log(
                level="ERROR",
                message=f"Ошибка обработки",
                vendor_code=vendor_code,
                details={"error": str(e)}
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

        self.logger.info("📋 ПРОВЕРКА КРИТЕРИЕВ:")

        # 1. Проверка минимальной цены
        if new_price_wd < min_price:
            self.logger.error(f"❌ ЦЕНА НИЖЕ МИНИМАЛЬНОЙ: {new_price_wd:.0f}₽ < {min_price:.0f}₽")
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
        else:
            self.logger.info(f"✅ Минимальная цена: ОК ({new_price_wd:.0f}₽ ≥ {min_price:.0f}₽)")

        # 2. Проверка минимального изменения
        price_change = abs(new_total_price - product.current_price_wb)
        if price_change < Config.MIN_PRICE_CHANGE:
            self.logger.error(f"❌ ИЗМЕНЕНИЕ СЛИШКОМ МАЛО: {price_change:.0f}₽ < {Config.MIN_PRICE_CHANGE}₽")
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
        else:
            self.logger.info(f"✅ Минимальное изменение: ОК ({price_change:.0f}₽ ≥ {Config.MIN_PRICE_CHANGE}₽)")

        # 3. Проверка максимального процентного изменения
        if product.current_price_wb > 0:
            price_change_percent = abs((new_total_price - product.current_price_wb) / product.current_price_wb) * 100
            if price_change_percent > Config.MAX_PRICE_CHANGE_PERCENT:
                self.logger.error(
                    f"❌ ПРОЦЕНТ ИЗМЕНЕНИЯ СЛИШКОМ ВЕЛИК: {price_change_percent:.1f}% > {Config.MAX_PRICE_CHANGE_PERCENT}%")
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
            else:
                self.logger.info(
                    f"✅ Максимальный процент изменения: ОК ({price_change_percent:.1f}% ≤ {Config.MAX_PRICE_CHANGE_PERCENT}%)")

        self.logger.info("✅ ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ")
        return None

    async def get_logistics_history_from_reports(self, vendor_code: str) -> List[float]:
        """Получает историю логистики для артикула из отчетов WB за последние 7 дней"""
        try:
            moscow_tz = pytz.timezone('Europe/Moscow')
            date_to = datetime.now(moscow_tz).strftime("%Y-%m-%d")
            date_from = (datetime.now(moscow_tz) - timedelta(days=7)).strftime("%Y-%m-%d")

            self.logger.debug(f"📊 Запрос истории логистики для {vendor_code} за {date_from} - {date_to}")

            url = "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"
            headers = {"Authorization": Config.WB_SALES_TOKEN}
            params = {
                "dateFrom": date_from,
                "dateTo": date_to,
                "limit": 10000,
                "period": "daily"
            }

            async with self.session.get(url, headers=headers, params=params) as resp:
                if resp.status == 204:
                    return []
                if resp.status != 200:
                    return []

                data = await resp.json()

                logistics_history = []

                for record in data:
                    sa_name = record.get('sa_name', '')
                    if sa_name != vendor_code:
                        continue

                    delivery_amount = record.get('delivery_amount', 0)
                    return_amount = record.get('return_amount', 0)
                    delivery_rub = record.get('delivery_rub', 0)

                    try:
                        delivery_amount_int = int(delivery_amount) if delivery_amount else 0
                        delivery_rub_float = float(delivery_rub) if delivery_rub else 0
                        return_amount_int = int(return_amount) if return_amount else 0
                    except (ValueError, TypeError):
                        continue

                    # Берем только основные доставки (не возвраты) с положительной стоимостью
                    if delivery_amount_int > 0 and delivery_rub_float > 0 and return_amount_int == 0:
                        cost_per_unit = delivery_rub_float / delivery_amount_int
                        logistics_history.append(cost_per_unit)

                # Убираем дубликаты и сортируем по времени (поскольку отчеты могут идти не по порядку)
                # Используем медиану за каждый день
                if logistics_history:
                    # Возвращаем медиану за весь период как единое значение
                    median_cost = statistics.median(logistics_history)

                    # Для более детального анализа можно группировать по дням
                    # Но для простоты возвращаем список всех значений
                    self.logger.debug(f"📊 Найдено {len(logistics_history)} записей логистики для {vendor_code}")

                return logistics_history

        except Exception as e:
            self.logger.warning(f"⚠️  Не удалось получить историю логистики для {vendor_code}: {e}")
            return []

    async def save_nm_id_to_db(self, vendor_code: str, nm_id: int) -> bool:
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
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    current_time = datetime.now(pytz.timezone('Europe/Moscow'))

                    expected_profit_after_change = update.target_profit

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
                        expected_profit_after_change,
                        update.target_profit,
                        update.sales_count,
                        update.spp_used,
                        update.purchase_price,
                        update.logistics_cost,
                        update.vendor_code,
                    ))

                    self.stats['prices_updated'] += 1

                    profit_before = update.current_profit
                    profit_after = expected_profit_after_change
                    profit_diff = profit_after - profit_before

                    self._log_separator("СОХРАНЕНИЕ ЦЕНЫ В БД", "💾")
                    self._log_table("СОХРАНЕННЫЕ ДАННЫЕ", {
                        "Артикул": update.vendor_code,
                        "Старая цена": f"{update.old_price_wb:.0f}₽",
                        "Новая цена": f"{update.new_price_wb:.0f}₽",
                        "Изменение": f"{update.new_price_wb - update.old_price_wb:+.0f}₽",
                        "Прибыль до": f"{profit_before:.0f}₽",
                        "Ожидаемая прибыль после": f"{profit_after:.0f}₽",
                        "Изменение прибыли": f"{profit_diff:+.0f}₽",
                        "Скидка": f"{update.discount or 0:.1f}%",
                        "Логистика": f"{update.logistics_cost:.0f}₽",
                        "Продаж использовано": update.sales_count
                    })

                    await self.db_logger.log(
                        level="SUCCESS",
                        message=f"Цена сохранена с деталями прибыли",
                        vendor_code=update.vendor_code,
                        details={
                            "old_price": update.old_price_wb,
                            "new_price": update.new_price_wb,
                            "price_change": update.new_price_wb - update.old_price_wb,
                            "profit_before": f"{profit_before:.0f} ₽",
                            "expected_profit_after": f"{profit_after:.0f} ₽",
                            "profit_diff": f"{profit_diff:+.0f} ₽",
                            "target_profit": f"{update.target_profit:.0f} ₽",
                            "sales_used": update.sales_count,
                            "logistics": f"{update.logistics_cost:.0f} ₽",
                            "purchase_price": f"{update.purchase_price:.0f} ₽"
                        }
                    )

        except Exception as e:
            error_msg = f"❌ Ошибка сохранения цены для {update.vendor_code}: {e}"
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
            self.logger.info("ℹ️  Нет данных для отправки на WB")
            return

        self._log_separator("ОТПРАВКА ЦЕН НА WILDBERRIES", "🚀")
        self.logger.info(f"📤 Отправка {len(data)} цен на WB API...")

        url = "https://discounts-prices-api.wildberries.ru/api/v2/upload/task"
        headers = {
            "Authorization": Config.WB_PRICES_TOKEN,
            "Content-Type": "application/json"
        }

        # Логируем что отправляем
        self.logger.info("📋 Отправляемые данные:")
        for item in data[:5]:  # Показываем первые 5
            self.logger.info(f"   • nmID: {item['nmID']}, цена: {item['price']}₽, скидка: {item['discount']}%")
        if len(data) > 5:
            self.logger.info(f"   ... и еще {len(data) - 5} товаров")

        try:
            async with self.session.post(url, headers=headers, json={"data": data}) as resp:
                if resp.status == 200:
                    res = await resp.json()
                    task_id = res.get('data', {}).get('id')

                    self._log_separator("УСПЕШНАЯ ОТПРАВКА", "✅")
                    self.logger.info(f"✅ Цены успешно отправлены на Wildberries")
                    self.logger.info(f"📝 ID задачи: {task_id}")
                    self.logger.info(f"📊 Количество товаров: {len(data)}")

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
                    error_msg = f"❌ Ошибка отправки цен на WB: {resp.status}"
                    self.logger.error(error_msg)
                    self.logger.error(f"   Ответ сервера: {text[:200]}")

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
            error_msg = f"❌ Ошибка при отправке цен на WB: {e}"
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
            self.logger.info(f"👷 Воркер #{worker_id} запущен")

            while self.is_running:
                try:
                    vendor_code, sales, product, logistics_cost = await asyncio.wait_for(
                        self.queue.get(),
                        timeout=1.0
                    )

                    self.logger.info(f"👷 Воркер #{worker_id} обрабатывает: {vendor_code}")

                    update = await self.process_product_new_logic(vendor_code, sales, product, logistics_cost)

                    if update.status == ProcessingStatus.SUCCESS:
                        await self.save_price_update(update)
                        self.successful_updates.append(update)
                        self.logger.info(f"👷 Воркер #{worker_id}: {vendor_code} - УСПЕХ")
                    else:
                        self.logger.info(f"👷 Воркер #{worker_id}: {vendor_code} - {update.status.value}")

                    self.stats[update.status.value] += 1
                    self.queue.task_done()

                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    self.logger.error(f"❌ Ошибка в воркере {worker_id}: {e}")
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
            self.logger.info(f"👷 Воркер {worker_id} остановлен")

    async def run_cycle(self):
        self._log_separator(f"ЦИКЛ #{self.current_cycle + 1}", "🔄")
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
            # 1. Получение продаж
            sales_data = await self.fetch_wb_sales()

            if not sales_data:
                self._log_separator("ЦИКЛ ПРЕРВАН", "⚠️")
                self.logger.warning("⚠️  Нет данных о продажах")
                await self.db_logger.log(
                    level="WARNING",
                    message="Нет данных о продажах",
                    details={"cycle_id": self.current_cycle}
                )
                return

            # 2. Подготовка данных
            sa_names_from_sales = []
            for sale in sales_data:
                if sale.vendor_code and sale.vendor_code not in sa_names_from_sales:
                    sa_names_from_sales.append(sale.vendor_code)

            self.logger.info(f"📊 Найдено {len(sa_names_from_sales)} уникальных артикулов в продажах")

            # 3. Расчет логистики
            logistics_by_sa_name = await self.get_logistics_by_sa_name_improved(sa_names_from_sales)

            # 4. Группировка продаж
            sales_by_vendor = defaultdict(list)
            for sale in sales_data:
                if sale.vendor_code:
                    sales_by_vendor[sale.vendor_code].append(sale)

            vendor_codes = list(sales_by_vendor.keys())

            self._log_table("ОБНАРУЖЕННЫЕ АРТИКУЛЫ", {
                "Всего артикулов с продажами": len(vendor_codes),
                "Артикулов с логистикой": len(logistics_by_sa_name)
            })

            # 5. Загрузка товаров из БД
            product_map = await self.fetch_products_batch(vendor_codes)

            if not product_map:
                self._log_separator("ЦИКЛ ПРЕРВАН", "⚠️")
                self.logger.warning("⚠️  Нет товаров для обработки")
                await self.db_logger.log(
                    level="WARNING",
                    message="Нет товаров для обработки в БД",
                    details={"cycle_id": self.current_cycle}
                )
                return

            # 6. Добавление задач в очередь
            queue_tasks = 0
            skipped_tasks = 0

            self._log_separator("ФОРМИРОВАНИЕ ОЧЕРЕДИ ЗАДАЧ", "📋")

            for vendor_code in vendor_codes:
                if vendor_code in product_map:
                    product = product_map[vendor_code]
                    logistics_cost = logistics_by_sa_name.get(vendor_code, 0)

                    await self.queue.put((vendor_code,
                                          sales_by_vendor[vendor_code],
                                          product,
                                          logistics_cost))
                    queue_tasks += 1

                    if queue_tasks <= 5:  # Показываем первые 5 задач
                        self.logger.info(f"📥 Добавлен: {vendor_code} "
                                         f"({len(sales_by_vendor[vendor_code])} продаж, "
                                         f"логистика {logistics_cost:.0f}₽)")
                else:
                    skipped_tasks += 1

            self.logger.info(f"📊 В очередь добавлено: {queue_tasks} задач")
            if skipped_tasks > 0:
                self.logger.info(f"📊 Пропущено (нет в БД): {skipped_tasks} артикулов")

            # 7. Запуск воркеров
            self._log_separator("ЗАПУСК ОБРАБОТКИ", "⚡")
            self.logger.info(f"👥 Запускаем {Config.WORKERS_COUNT} воркеров...")

            workers = []
            for i in range(Config.WORKERS_COUNT):
                worker_task = asyncio.create_task(self.worker(i))
                workers.append(worker_task)

            # 8. Ожидание завершения
            self.logger.info("⏳ Ожидание завершения обработки...")
            await self.queue.join()

            # 9. Остановка воркеров
            for worker_task in workers:
                worker_task.cancel()

            await asyncio.gather(*workers, return_exceptions=True)
            self.logger.info("✅ Обработка завершена")

            # 10. Отправка цен на WB (если включено)
            if LOAD_PRICE_TO_WB and self.successful_updates:
                await self.upload_prices_to_wb(self.successful_updates)
            else:
                self.logger.info("ℹ️  Отправка цен на WB отключена (LOAD_PRICE_TO_WB=False)")

            # 11. Статистика цикла
            cycle_end = datetime.now()
            duration = (cycle_end - cycle_start).total_seconds()

            self._log_separator("СТАТИСТИКА ЦИКЛА", "📊")
            self._log_table("РЕЗУЛЬТАТЫ ОБРАБОТКИ", {
                "Время выполнения": f"{duration:.1f} сек",
                "Всего артикулов": len(vendor_codes),
                "Товаров в БД": len(product_map),
                "✅ Успешно обработано": self.stats.get('success', 0),
                "✅ Обновлено цен": self.stats.get('prices_updated', 0),
                "✅ Отправлено на WB": self.stats.get('prices_uploaded_to_wb', 0),
                "⚠️  Пропущено (мало данных)": self.stats.get('skipped_no_data', 0),
                "⚠️  Пропущено (низкая цена)": self.stats.get('skipped_min_price', 0),
                "⚠️  Пропущено (мало изменений)": self.stats.get('skipped_min_change', 0),
                "❌ Ошибок": self.stats.get('error', 0)
            })

            # 12. Примеры изменений
            if self.successful_updates:
                self._log_separator("ПРИМЕРЫ ИЗМЕНЕНИЙ ЦЕН", "📈")
                sample_updates = self.successful_updates[:3]  # Показываем первые 3
                for update in sample_updates:
                    change = update.new_price_wb - update.old_price_wb
                    percent = (change / update.old_price_wb * 100) if update.old_price_wb > 0 else 0
                    self.logger.info(f"   • {update.vendor_code}: "
                                     f"{update.old_price_wb:.0f}₽ → {update.new_price_wb:.0f}₽ "
                                     f"({change:+.0f}₽, {percent:+.1f}%)")

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
            error_msg = f"❌ КРИТИЧЕСКАЯ ОШИБКА в цикле: {e}"
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

            try:
                await self.run_cycle()

                hours = Config.CYCLE_INTERVAL // 3600
                minutes = (Config.CYCLE_INTERVAL % 3600) // 60

                self._log_separator("ОЖИДАНИЕ СЛЕДУЮЩЕГО ЦИКЛА", "⏰")
                self.logger.info(f"🕒 Следующий цикл через: {hours}ч {minutes}мин")
                self.logger.info(f"📅 Время следующего запуска: "
                                 f"{(datetime.now() + timedelta(seconds=Config.CYCLE_INTERVAL)).strftime('%H:%M:%S')}")

                await asyncio.sleep(Config.CYCLE_INTERVAL)

            except KeyboardInterrupt:
                self._log_separator("ОСТАНОВКА ПОЛЬЗОВАТЕЛЕМ", "🛑")
                self.logger.info("Пользователь запросил остановку")

                await self.db_logger.log(
                    level="INFO",
                    message="Остановка по запросу пользователя",
                    details={"cycle_id": self.current_cycle}
                )
                break
            except Exception as e:
                error_msg = f"❌ ФАТАЛЬНАЯ ОШИБКА: {e}"
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
                self.logger.info(f"⏸️  Пауза {wait_time} сек перед повторной попыткой...")
                await asyncio.sleep(wait_time)

    def _handle_shutdown(self, signum, frame):
        signal_name = {signal.SIGTERM: 'SIGTERM', signal.SIGINT: 'SIGINT'}.get(signum, str(signum))
        self._log_separator("ПОЛУЧЕН СИГНАЛ ОСТАНОВКИ", "🚨")
        self.logger.info(f"Сигнал: {signal_name}")
        self.logger.info("Завершение работы...")
        self.is_running = False

    async def cleanup(self):
        self._log_separator("ЗАВЕРШЕНИЕ РАБОТЫ", "🔚")
        self.logger.info("Очистка ресурсов...")

        await self.db_logger.log(
            level="INFO",
            message="Завершение работы системы",
            details={"cycle_id": self.current_cycle}
        )

        if self.session and not self.session.closed:
            await self.session.close()
            self.logger.info("✅ HTTP сессия закрыта")

        if self.db_pool:
            self.db_pool.close()
            await self.db_pool.wait_closed()
            self.logger.info("✅ Пул БД закрыт")

        self.logger.info("✅ Очистка завершена")
        self._log_separator("РАБОТА ЗАВЕРШЕНА", "🏁")


async def main():
    updater = PriceUpdater()

    try:
        await updater.initialize()
        await updater.run()
    except Exception as e:
        updater.logger.critical(f"❌ ФАТАЛЬНАЯ ОШИБКА: {e}")
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

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Программа остановлена пользователем")
        sys.exit(0)