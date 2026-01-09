import asyncio
import logging
import logging.handlers
import sys
import os
import math
from collections import defaultdict
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple
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

    # Логистика по габаритам
    WB_COMMISSION = float(os.getenv('WB_COMMISSION', 0.11))
    BANK_COMMISSION = float(os.getenv('BANK_COMMISSION', 0.022))
    BUFFER_COEFF = float(os.getenv('BUFFER_COEFF', 1.05))
    WB_TARIFF_URL = "https://common-api.wildberries.ru/api/tariffs/v1/acceptance/coefficients"
    ORDERS_API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
    WAREHOUSES_API_URL = "https://supplies-api.wildberries.ru/api/v1/warehouses"
    DEFAULT_WAREHOUSE_ID = int(os.getenv('WB_WAREHOUSE_ID', 0))
    WB_BOX_TYPE = int(os.getenv('WB_BOX_TYPE', 0))

    BATCH_SIZE = int(os.getenv('BATCH_SIZE'))
    WORKERS_COUNT = int(os.getenv('WORKERS_COUNT'))
    MAX_QUEUE_SIZE = int(os.getenv('MAX_QUEUE_SIZE'))

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
class WarehouseLogistics:
    """Структура для хранения тарифов логистики по складам"""
    warehouse_id: int
    warehouse_name: str
    delivery_base: float
    delivery_liter: float
    storage_base: float
    storage_liter: float
    coefficient: int
    is_sorting_center: bool
    orders_count: int = 0  # Количество заказов с этого склада
    weight: float = 0.0  # Вес в расчете средневзвешенной логистики

    @property
    def is_available(self) -> bool:
        """Проверяет, доступен ли склад для приемки"""
        return self.coefficient in [0, 1] and self.orders_count > 0


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
    warehouse_name: str = ""
    quantity: int = 1

    @classmethod
    def from_api_dict(cls, data: Dict) -> Optional['SaleData']:
        try:
            price_with_desc = data.get('priceWithDisc') or data.get('price_with_desc') or 0
            discount_percent = data.get('discountPercent') or data.get('discount') or 0
            total_price = data.get('totalPrice') or 0
            warehouse_name = data.get('warehouseName') or ""

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
                warehouse_name=warehouse_name,
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
    length: float = 0.0
    width: float = 0.0
    height: float = 0.0
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
                length=float(row.get('length', 0) or 0),
                width=float(row.get('width', 0) or 0),
                height=float(row.get('height', 0) or 0),
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
    action_type: str = ""

    @property
    def reason(self) -> str:
        if self.error_msg:
            return self.error_msg

        if self.status == ProcessingStatus.SUCCESS:
            if self.action_type == "discount_change":
                return f"Изменена скидка: {self.discount:.1f}%"
            else:
                direction = "↑" if self.new_price_wb > self.old_price_wb else "↓"
                return f"Цена {direction} на {abs(self.new_price_wb - self.old_price_wb):.0f} ₽ (скидка: {self.discount:.1f}%)"

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
        self.warehouse_logistics: Dict[int, WarehouseLogistics] = {}
        self.warehouse_orders_stats: Dict[str, int] = {}
        self.weighted_delivery_base = 0.0
        self.weighted_delivery_liter = 0.0

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
        if title:
            padding = (length - len(title) - 4) // 2
            self.logger.info(f"\n{char * padding} {title} {char * padding}")
        else:
            self.logger.info(f"\n{char * length}")

    def _log_table(self, title: str, data: dict):
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

        await self._init_weighted_logistics()

        self._log_table("КОНФИГУРАЦИЯ СИСТЕМЫ", {
            "WORKERS_COUNT": Config.WORKERS_COUNT,
            "BATCH_SIZE": Config.BATCH_SIZE,
            "SALES_HOURS_FILTER": f"{Config.SALES_HOURS_FILTER}ч",
            "CYCLE_INTERVAL": f"{Config.CYCLE_INTERVAL // 3600}ч {(Config.CYCLE_INTERVAL % 3600) // 60}м",
            "MIN_SALES_FOR_CALC": Config.MIN_SALES_FOR_CALC,
            "MIN_MARGIN_FACTOR": f"×{Config.MIN_MARGIN_FACTOR}",
            "BANK_COMMISSION": f"{Config.BANK_COMMISSION * 100}%",
            "Логистика по габаритам": "ВКЛЮЧЕНА",
            "Средневзвешенная логистика": "ВКЛЮЧЕНА",
            "Период статистики": "14 дней",
            "Тариф доставки": f"{self.weighted_delivery_base:.2f} + {self.weighted_delivery_liter:.2f} ₽/л",
            "Складов в расчете": len(self.warehouse_logistics)
        })

        self.logger.info("✅ Инициализация завершена успешно")
        self._log_separator()

    async def _init_weighted_logistics(self):
        """Инициализация средневзвешенной логистики по всем складам за 14 дней"""
        self._log_separator("РАСЧЕТ СРЕДНЕВЗВЕШЕННОЙ ЛОГИСТИКИ", "⚖️")

        try:
            self.logger.info("📊 Получение статистики заказов за 14 дней...")
            await self._get_warehouse_orders_stats()

            if not self.warehouse_orders_stats:
                self.logger.warning("⚠️  Нет данных о заказах за 14 дней")
                self.weighted_delivery_base = 30.0
                self.weighted_delivery_liter = 10.0
                return

            self.logger.info("📋 Получение информации о всех складах...")
            warehouses_list = await self._get_all_warehouses()

            if not warehouses_list:
                self.logger.error("❌ Не удалось получить список складов")
                self.weighted_delivery_base = 30.0
                self.weighted_delivery_liter = 10.0
                return

            self.logger.info("💰 Получение тарифов логистики для складов с заказами...")
            await self._get_warehouse_tariffs(warehouses_list)

            if not self.warehouse_logistics:
                self.logger.error("❌ Не удалось получить тарифы логистики")
                self.weighted_delivery_base = 30.0
                self.weighted_delivery_liter = 10.0
                return

            self.logger.info("🧮 Расчет средневзвешенных тарифов...")
            self._calculate_weighted_logistics()

            self._log_weighted_logistics_results()

        except Exception as e:
            self.logger.error(f"❌ Ошибка расчета средневзвешенной логистики: {e}")
            self.weighted_delivery_base = 30.0
            self.weighted_delivery_liter = 10.0

    async def _get_warehouse_orders_stats(self):
        """Получает статистику заказов по складам за последние 14 дней"""
        try:
            date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

            headers = {"Authorization": Config.WB_PRICES_TOKEN}
            params = {"dateFrom": date_from, "flag": 1}

            self.logger.info(f"📅 Запрашиваем заказы с {date_from}...")

            async with self.session.get(Config.ORDERS_API_URL, headers=headers, params=params, timeout=30) as resp:
                if resp.status != 200:
                    self.logger.warning(f"❌ Ошибка Orders API: {resp.status}")
                    return

                orders = await resp.json()

                if not orders:
                    self.logger.warning("⚠️  Нет заказов за последние 14 дней")
                    return

                for order in orders:
                    warehouse_name = order.get('warehouseName')
                    if warehouse_name and warehouse_name.strip():
                        self.warehouse_orders_stats[warehouse_name] = self.warehouse_orders_stats.get(warehouse_name,
                                                                                                      0) + 1

                total_orders = sum(self.warehouse_orders_stats.values())
                self.logger.info(f"✅ Получено {total_orders} заказов с {len(self.warehouse_orders_stats)} складов")

                sorted_stats = sorted(self.warehouse_orders_stats.items(), key=lambda x: x[1], reverse=True)
                self.logger.info("🏢 Топ-10 складов по количеству заказов:")
                for i, (warehouse_name, count) in enumerate(sorted_stats[:10], 1):
                    percentage = (count / total_orders * 100) if total_orders > 0 else 0
                    self.logger.info(f"   {i:2d}. {warehouse_name[:40]:40} | Заказов: {count:4} ({percentage:5.1f}%)")

        except Exception as e:
            self.logger.error(f"❌ Ошибка получения статистики заказов: {e}")

    async def _get_all_warehouses(self) -> List[Dict]:
        """Получает список всех складов"""
        try:
            headers = {"Authorization": Config.WB_PRICES_TOKEN}

            async with self.session.get(Config.WAREHOUSES_API_URL, headers=headers, timeout=15) as resp:
                if resp.status != 200:
                    self.logger.warning(f"❌ Ошибка Warehouses API: {resp.status}")
                    return []

                warehouses_list = await resp.json()
                self.logger.info(f"✅ Получено {len(warehouses_list)} складов")
                return warehouses_list

        except Exception as e:
            self.logger.error(f"❌ Ошибка получения списка складов: {e}")
            return []

    async def _get_warehouse_tariffs(self, warehouses_list: List[Dict]):
        """Получает тарифы логистики для всех складов с заказами"""
        try:
            warehouse_name_to_id = {}
            for wh in warehouses_list:
                warehouse_name = wh.get('name')
                warehouse_id = wh.get('ID')
                if warehouse_name and warehouse_id:
                    warehouse_name_to_id[warehouse_name] = warehouse_id

            warehouse_ids_to_fetch = []
            warehouse_info = {}

            for warehouse_name, orders_count in self.warehouse_orders_stats.items():
                if warehouse_name in warehouse_name_to_id:
                    warehouse_id = warehouse_name_to_id[warehouse_name]
                    warehouse_ids_to_fetch.append(warehouse_id)

                    for wh in warehouses_list:
                        if wh.get('ID') == warehouse_id:
                            warehouse_info[warehouse_id] = {
                                'name': warehouse_name,
                                'city': wh.get('city', 'Не указан'),
                                'address': wh.get('address', 'Не указан'),
                                'is_sc': wh.get('isSC', False)
                            }
                            break

            if not warehouse_ids_to_fetch:
                self.logger.warning("⚠️  Не найдено складов с заказами")
                return

            self.logger.info(f"📋 Запрашиваем тарифы для {len(warehouse_ids_to_fetch)} складов с заказами")

            batch_size = 10
            all_tariffs = []

            for i in range(0, len(warehouse_ids_to_fetch), batch_size):
                batch = warehouse_ids_to_fetch[i:i + batch_size]
                self.logger.info(f"   Батч {i // batch_size + 1}: {len(batch)} складов")

                headers = {"Authorization": Config.WB_PRICES_TOKEN}
                params = {"warehouseIDs": ",".join(str(id) for id in batch)}

                async with self.session.get(Config.WB_TARIFF_URL, headers=headers, params=params, timeout=30) as resp:
                    if resp.status == 429:
                        self.logger.warning("⚠️  Лимит запросов, ждем 10 секунд...")
                        await asyncio.sleep(10)
                        continue

                    if resp.status != 200:
                        self.logger.warning(f"   ❌ Ошибка API: {resp.status}")
                        continue

                    batch_tariffs = await resp.json()
                    all_tariffs.extend(batch_tariffs)

                    if i + batch_size < len(warehouse_ids_to_fetch):
                        await asyncio.sleep(1)

            if not all_tariffs:
                self.logger.error("❌ API вернуло пустой ответ")
                return

            self.logger.info(f"✅ Получено {len(all_tariffs)} записей о тарифах")

            today_date = date.today().strftime('%Y-%m-%d')
            tariffs_found = 0

            for tariff in all_tariffs:
                warehouse_id = tariff.get('warehouseID')
                box_type_id = tariff.get('boxTypeID')

                if box_type_id != Config.WB_BOX_TYPE or warehouse_id not in warehouse_info:
                    continue

                coefficient = tariff.get('coefficient')
                allow_unload = tariff.get('allowUnload')

                if coefficient in [0, 1] and allow_unload is True:
                    api_date_str = tariff.get('date', '')
                    try:
                        if api_date_str.endswith('Z'):
                            api_date = datetime.fromisoformat(api_date_str.replace('Z', '+00:00'))
                        else:
                            api_date = datetime.fromisoformat(api_date_str)

                        api_date_simple = api_date.strftime('%Y-%m-%d')

                        if api_date_simple <= today_date:
                            warehouse_name = warehouse_info[warehouse_id]['name']
                            orders_count = self.warehouse_orders_stats.get(warehouse_name, 0)

                            self.warehouse_logistics[warehouse_id] = WarehouseLogistics(
                                warehouse_id=warehouse_id,
                                warehouse_name=warehouse_name,
                                delivery_base=self._parse_float(tariff.get('deliveryBaseLiter')),
                                delivery_liter=self._parse_float(tariff.get('deliveryAdditionalLiter')),
                                storage_base=self._parse_float(tariff.get('storageBaseLiter')),
                                storage_liter=self._parse_float(tariff.get('storageAdditionalLiter')),
                                coefficient=coefficient,
                                is_sorting_center=warehouse_info[warehouse_id]['is_sc'],
                                orders_count=orders_count
                            )
                            tariffs_found += 1

                    except Exception as e:
                        self.logger.debug(f"Ошибка парсинга даты {api_date_str}: {e}")
                        continue

            self.logger.info(f"✅ Получены тарифы для {tariffs_found} складов")

        except Exception as e:
            self.logger.error(f"❌ Ошибка получения тарифов: {e}")

    def _parse_float(self, val) -> float:
        if val is None:
            return 0.0
        try:
            return float(str(val).replace(',', '.'))
        except:
            return 0.0

    def _calculate_weighted_logistics(self):
        """Рассчитывает средневзвешенные тарифы логистики"""
        total_orders = sum(wh.orders_count for wh in self.warehouse_logistics.values())

        if total_orders == 0:
            self.logger.error("❌ Нет заказов для расчета весов")
            return

        weighted_base_sum = 0.0
        weighted_liter_sum = 0.0

        for wh in self.warehouse_logistics.values():
            weight = wh.orders_count / total_orders
            wh.weight = weight

            weighted_base_sum += wh.delivery_base * weight
            weighted_liter_sum += wh.delivery_liter * weight

        self.weighted_delivery_base = weighted_base_sum
        self.weighted_delivery_liter = weighted_liter_sum

    def _log_weighted_logistics_results(self):
        """Логирует результаты расчета средневзвешенной логистики"""
        total_orders = sum(wh.orders_count for wh in self.warehouse_logistics.values())

        self._log_separator("РЕЗУЛЬТАТЫ РАСЧЕТА ЛОГИСТИКИ", "📊")
        self.logger.info(f"📈 ОБЩАЯ СТАТИСТИКА:")
        self.logger.info(f"   • Всего заказов за 14 дней: {total_orders}")
        self.logger.info(f"   • Складов с тарифами: {len(self.warehouse_logistics)}")
        self.logger.info(
            f"   • Средневзвешенная доставка: {self.weighted_delivery_base:.2f} + {self.weighted_delivery_liter:.2f} ₽/л")

        sorted_warehouses = sorted(
            self.warehouse_logistics.values(),
            key=lambda x: x.orders_count,
            reverse=True
        )

        self.logger.info("🏢 ТАРИФЫ ПО СКЛАДАМ (топ-10):")
        for i, wh in enumerate(sorted_warehouses[:10], 1):
            percentage = (wh.orders_count / total_orders * 100) if total_orders > 0 else 0
            weight_percentage = wh.weight * 100 if wh.weight else 0
            self.logger.info(f"   {i:2d}. {wh.warehouse_name[:30]:30} | "
                             f"Заказов: {wh.orders_count:4} ({percentage:5.1f}%) | "
                             f"Вес: {weight_percentage:4.1f}% | "
                             f"Тариф: {wh.delivery_base:5.1f} + {wh.delivery_liter:4.1f} ₽/л")

        if len(sorted_warehouses) > 10:
            self.logger.info(f"   ... и еще {len(sorted_warehouses) - 10} складов")

        if self.warehouse_logistics:
            bases = [wh.delivery_base for wh in self.warehouse_logistics.values()]
            liters = [wh.delivery_liter for wh in self.warehouse_logistics.values()]

            self.logger.info("📊 СТАТИСТИКА ТАРИФОВ:")
            self.logger.info(f"   • Доставка (база): мин={min(bases):.1f}, макс={max(bases):.1f}, "
                             f"сред={statistics.mean(bases):.1f}, медиана={statistics.median(bases):.1f}")
            self.logger.info(f"   • Доставка (литр): мин={min(liters):.1f}, макс={max(liters):.1f}, "
                             f"сред={statistics.mean(liters):.1f}, медиана={statistics.median(liters):.1f}")

            sc_count = sum(1 for wh in self.warehouse_logistics.values() if wh.is_sorting_center)
            warehouse_count = len(self.warehouse_logistics) - sc_count

            self.logger.info("🏭 РАСПРЕДЕЛЕНИЕ ПО ТИПАМ СКЛАДОВ:")
            self.logger.info(f"   • Сортировочные центры (SC): {sc_count}")
            self.logger.info(f"   • Обычные склады: {warehouse_count}")

    def _calc_volume(self, length: float, width: float, height: float) -> float:
        try:
            l = float(length) if length is not None else 0.0
            w = float(width) if width is not None else 0.0
            h = float(height) if height is not None else 0.0
        except (TypeError, ValueError):
            l = w = h = 0.0

        if not all([l, w, h]):
            return 1.0

        vol = (l * w * h) / 1000.0
        return max(vol, 1.0)

    def _calc_logistics(self, volume: float) -> float:
        first = self.weighted_delivery_base
        extra_vol = max(volume - 1.0, 0.0)
        extra_cost = extra_vol * self.weighted_delivery_liter
        return first + extra_cost

    async def get_logistics_by_volume(self, product: ProductData) -> Tuple[float, Dict]:
        self.logger.debug(f"📦 Расчет логистики для {product.vendor_code} по габаритам")

        volume = self._calc_volume(product.length, product.width, product.height)
        logistics_cost = self._calc_logistics(volume)

        calculation_details = {
            'dimensions': f"{product.length}x{product.width}x{product.height} см",
            'volume_liters': volume,
            'weighted_base': self.weighted_delivery_base,
            'weighted_liter': self.weighted_delivery_liter,
            'warehouses_count': len(self.warehouse_logistics),
            'total_orders': sum(wh.orders_count for wh in self.warehouse_logistics.values()),
            'calculation_formula': f"{self.weighted_delivery_base:.2f} + (max({volume:.2f} - 1, 0) × {self.weighted_delivery_liter:.2f})"
        }

        self.logger.debug(f"   Объем: {volume:.2f} л")
        self.logger.debug(f"   Тариф: {self.weighted_delivery_base:.2f} + {self.weighted_delivery_liter:.2f} ₽/л")
        self.logger.debug(
            f"   Расчет: {self.weighted_delivery_base:.2f} + (max({volume:.2f} - 1, 0) × {self.weighted_delivery_liter:.2f}) = {logistics_cost:.2f} ₽")

        return logistics_cost, calculation_details

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
                                   price_wb, wb_real_price, length, width, height, sku_wb, status
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

        self._log_separator("РЕЗУЛЬТАТЫ ЗАГРУЗКИ ТОВАРОВ", "-")
        self.logger.info(f"✅ Успешно загружено: {loaded_count} товаров")
        self.logger.info(f"⚠️  Пропущено (нет в БД/неактивны): {skipped_count}")

        if loaded_count > 0:
            self.logger.info("📋 Примеры загруженных товаров:")
            sample_items = list(product_map.items())[:3]
            for vendor_code, product in sample_items:
                logistics_cost, _ = await self.get_logistics_by_volume(product)
                self.logger.info(f"   • {vendor_code}: закуп={product.purchase_price:.0f}₽, "
                                 f"цель={product.target_profit:.0f}₽, цена={product.current_price_wb:.0f}₽, "
                                 f"логистика={logistics_cost:.0f}₽, габариты={product.length}x{product.width}x{product.height}см")

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

        self._log_separator("ПОЛУЧЕНИЕ АКТУАЛЬНЫХ СКИДОК С WB API v2", "-")
        self.logger.info(f"🎯 Запрашиваем скидки для {len(nm_ids)} товаров")

        results = {}
        url = "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter"
        headers = {
            "Authorization": Config.WB_PRICES_TOKEN,
            "Content-Type": "application/json"
        }

        success_count = 0
        error_count = 0
        no_data_count = 0

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

                        if data.get("error", True):
                            error_text = data.get("errorText", "Unknown error")
                            self.logger.warning(f"   ⚠️  Ошибка API: {error_text}")
                            results[nm_id] = {"discount": 0}
                            error_count += 1
                            continue

                        list_goods = data.get("data", {}).get("listGoods", [])

                        if not list_goods:
                            self.logger.warning(f"   ⚠️  Товар с nmID {nm_id} не найден в ответе API")
                            results[nm_id] = {"discount": 0}
                            no_data_count += 1
                            continue

                        goods_info = list_goods[0]
                        discount = float(goods_info.get("discount", 0))

                        self.logger.info(f"   ✅ Получена скидка продавца: {discount:.1f}%")

                        results[nm_id] = {
                            "discount": discount,
                            "effective_discount": discount
                        }
                        success_count += 1

                    elif resp.status == 401:
                        self.logger.error("❌ Ошибка авторизации: неверный токен Prices API")
                        results[nm_id] = {"discount": 0}
                        error_count += 1
                    elif resp.status == 403:
                        self.logger.error("❌ Доступ запрещен: нет прав для использования API")
                        results[nm_id] = {"discount": 0}
                        error_count += 1
                    elif resp.status == 400:
                        text = await resp.text()
                        self.logger.error(f"❌ Неправильный запрос: {text[:200]}")
                        results[nm_id] = {"discount": 0}
                        error_count += 1
                    else:
                        text = await resp.text()
                        self.logger.warning(f"   ⚠️  Ошибка API: {resp.status}")
                        self.logger.debug(f"   Ответ: {text[:200]}")
                        results[nm_id] = {"discount": 0}
                        error_count += 1

                await asyncio.sleep(0.6)

            except asyncio.TimeoutError:
                self.logger.error(f"   ⏰ Таймаут при запросе скидки для nmID {nm_id}")
                results[nm_id] = {"discount": 0}
                error_count += 1
                await asyncio.sleep(1.0)
            except Exception as e:
                self.logger.error(f"   ❌ Ошибка при запросе скидки: {e}")
                results[nm_id] = {"discount": 0}
                error_count += 1
                await asyncio.sleep(0.5)

        self._log_separator("ИТОГИ ПОЛУЧЕНИЯ СКИДОК", "-")
        self.logger.info(f"✅ Успешно: {success_count} товаров")
        if no_data_count > 0:
            self.logger.info(f"⚠️  Данные не найдены: {no_data_count} товаров")
        if error_count > 0:
            self.logger.warning(f"❌ С ошибками: {error_count} товаров")

        if results:
            discounts = [info.get("discount", 0) for info in results.values() if info.get("discount", 0) > 0]
            if discounts:
                self.logger.info(f"📊 Статистика скидок:")
                self.logger.info(f"   Мин. скидка: {min(discounts):.1f}%")
                self.logger.info(f"   Ср. скидка: {statistics.mean(discounts):.1f}%")
                self.logger.info(f"   Макс. скидка: {max(discounts):.1f}%")
                self.logger.info(f"   Медиана: {statistics.median(discounts):.1f}%")

        return results

    async def process_product_new_logic(self, vendor_code: str,
                                        sales: List[SaleData],
                                        product: ProductData) -> PriceUpdate:
        try:
            self._log_separator(f"АНАЛИЗ ТОВАРА: {vendor_code}", "=")

            logistics_cost, logistics_details = await self.get_logistics_by_volume(product)

            self.logger.info(f"📊 ИСХОДНЫЕ ДАННЫЕ:")
            logistics_info = {
                "Артикул": vendor_code,
                "Закупочная цена": f"{product.purchase_price:.2f}₽",
                "Целевая прибыль": f"{product.target_profit:.2f}₽",
                "Текущая цена WB": f"{product.current_price_wb:.0f}₽",
                "Габариты": logistics_details['dimensions'],
                "Объем": f"{logistics_details['volume_liters']:.2f} л",
                "Логистика (средневзвешенная)": f"{logistics_cost:.2f}₽",
                "Тариф (средневзвешенный)": f"{self.weighted_delivery_base:.2f} + {self.weighted_delivery_liter:.2f} ₽/л",
                "Складов в расчете": f"{logistics_details['warehouses_count']}",
                "Заказов в расчете": f"{logistics_details['total_orders']}",
                "Формула расчета": logistics_details['calculation_formula'],
                "nmID": product.sku_wb if product.sku_wb else "не установлен"
            }

            self._log_table("ПАРАМЕТРЫ ТОВАРА", logistics_info)

            self.logger.info(f"📈 ДАННЫЕ О ПРОДАЖАХ:")
            self.logger.info(f"   Всего продаж: {len(sales)}")
            self.logger.info(f"   Требуется для расчета: {Config.MIN_SALES_FOR_CALC}")

            if len(sales) <= 10:
                self.logger.info("   Детали продаж:")
                for i, sale in enumerate(sales, 1):
                    self.logger.info(f"   {i:2d}. totalPrice: {sale.total_price:7.0f}₽ | "
                                     f"discount: {sale.discount_percent:5.1f}% | "
                                     f"priceWithDisc: {sale.price_with_desc:7.0f}₽ | "
                                     f"forPay: {sale.for_pay:7.0f}₽")
            else:
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
                    details={
                        "valid_sales": len(valid_sales),
                        "required": Config.MIN_SALES_FOR_CALC,
                        "logistics_details": logistics_details
                    }
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
                        logistics_cost=logistics_cost,
                        finished_price=0
                    )
                await self.save_nm_id_to_db(vendor_code, new_nm_id)
                product.sku_wb = new_nm_id
                self.logger.info(f"✅ nmID сохранен: {new_nm_id}")

            self.logger.info("🎯 РАСЧЕТ СКИДКИ ИЗ ПРОДАЖ:")

            discount_list = [sale.discount_percent for sale in valid_sales if sale.discount_percent > 0]

            if discount_list:
                median_discount = statistics.median(discount_list)
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

                discount_from_sales = median_discount
                self.logger.info(f"📊 Скидка из продаж: {discount_from_sales:.1f}%")
            else:
                self.logger.warning("⚠️  В продажах нет данных о скидках")
                discount_from_sales = 0.1

            self.logger.info("🔄 ПОЛУЧЕНИЕ ТЕКУЩЕЙ СКИДКИ С WB API:")
            discounts_info = await self.get_wb_discounts([product.sku_wb])
            current_discount_from_api = discounts_info.get(product.sku_wb, {}).get("discount", 0)

            if current_discount_from_api > 0:
                self.logger.info(f"✅ Текущая скидка с WB API: {current_discount_from_api:.1f}%")
                api_discount = current_discount_from_api
            else:
                self.logger.warning(f"⚠️  Нет данных о скидке из API, используем из продаж")
                api_discount = discount_from_sales

            api_discount = max(0.1, min(99.9, api_discount))

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
                "Скидка из API": f"{api_discount:.1f}%",
                "Скидка из продаж": f"{discount_from_sales:.1f}%"
            })

            self.logger.info("🧮 РАСЧЕТ ТЕКУЩЕЙ ПРИБЫЛИ:")

            bank_commission_current = avg_forpay * Config.BANK_COMMISSION
            current_profit = avg_forpay - logistics_cost - bank_commission_current - product.purchase_price

            self._log_table("РАСЧЕТ ТЕКУЩЕЙ ПРИБЫЛИ", {
                "Средний forPay": f"{avg_forpay:.2f}₽",
                "Логистика (средневзвешенная)": f"-{logistics_cost:.2f}₽",
                "Банковская комиссия": f"-{bank_commission_current:.2f}₽ ({Config.BANK_COMMISSION * 100}%)",
                "Закупочная цена": f"-{product.purchase_price:.2f}₽",
                "ИТОГО ПРИБЫЛЬ": f"{current_profit:.2f}₽"
            })

            self.logger.info("🎯 РАСЧЕТ ЦЕЛЕВЫХ ПОКАЗАТЕЛЕЙ:")

            target_forpay = (product.target_profit + logistics_cost + product.purchase_price) / (
                    1 - Config.BANK_COMMISSION)

            bank_commission_target = target_forpay * Config.BANK_COMMISSION
            expected_profit = target_forpay - logistics_cost - bank_commission_target - product.purchase_price

            self._log_table("РАСЧЕТ ДЛЯ ЦЕЛЕВОЙ ПРИБЫЛИ", {
                "Целевая прибыль": f"{product.target_profit:.2f}₽",
                "+ Логистика (средневзвешенная)": f"+{logistics_cost:.2f}₽",
                "+ Закупка": f"+{product.purchase_price:.2f}₽",
                "Сумма до комиссии": f"{(product.target_profit + logistics_cost + product.purchase_price):.2f}₽",
                "Банковская комиссия": f"{Config.BANK_COMMISSION * 100}%",
                "Требуемый forPay": f"{target_forpay:.2f}₽",
                "Ожидаемая прибыль": f"{expected_profit:.2f}₽"
            })

            self.logger.info("💵 РАСЧЕТ НОВОЙ ЦЕНЫ СО СКИДКОЙ:")

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

            spp = self._get_last_non_zero_spp(sales)
            finished_price = new_price_wd * (1 - spp / 100)
            self.logger.info(f"📉 СПП (не учитывается): {spp:.1f}%")
            self.logger.info(f"   Цена со скидкой СПП: {finished_price:.2f}₽")

            self._log_separator("РАСЧЕТ НОВОЙ СКИДКИ И РЕШЕНИЕ", "⭐")

            if product.current_price_wb > 0:
                new_discount_needed = (1 - new_price_wd / product.current_price_wb) * 100
                self.logger.info(f"📊 Расчет новой скидки:")
                self.logger.info(f"   Текущая полная цена: {product.current_price_wb:.0f}₽")
                self.logger.info(f"   Нужный priceWithDisc: {new_price_wd:.0f}₽")
                self.logger.info(f"   Нужная скидка: {new_discount_needed:.1f}%")
            else:
                new_discount_needed = api_discount
                self.logger.warning(f"⚠️  Нет текущей цены в БД, используем текущую скидку: {api_discount:.1f}%")

            new_discount_needed = max(0.1, min(99.9, new_discount_needed))

            discount_change = abs(new_discount_needed - api_discount)
            self.logger.info(f"📊 Изменение скидки:")
            self.logger.info(f"   Текущая скидка (API): {api_discount:.1f}%")
            self.logger.info(f"   Нужная скидка: {new_discount_needed:.1f}%")
            self.logger.info(f"   Изменение: {discount_change:.1f}%")

            action_type = ""
            new_total_price_rounded = 0
            final_discount = 0

            if discount_change <= 20:
                action_type = "discount_change"
                new_total_price_rounded = round(product.current_price_wb, 0)
                final_discount = new_discount_needed
                self._log_table("РЕШЕНИЕ: ИЗМЕНЕНИЕ СКИДКИ", {
                    "Действие": "Изменена только скидка",
                    "Полная цена": f"{new_total_price_rounded:.0f}₽ (без изменений)",
                    "Старая скидка": f"{api_discount:.1f}%",
                    "Новая скидка": f"{final_discount:.1f}%",
                    "Изменение скидки": f"{discount_change:.1f}%",
                    "Новый priceWithDisc": f"{new_price_wd:.0f}₽"
                })
            else:
                action_type = "price_change"
                reasonable_discount = max(0.1, min(50, api_discount))
                new_total_price = new_price_wd / (1 - reasonable_discount / 100)
                new_total_price_rounded = round(new_total_price, 0)
                final_discount = reasonable_discount
                self._log_table("РЕШЕНИЕ: ИЗМЕНЕНИЕ ПОЛНОЙ ЦЕНЫ", {
                    "Действие": "Изменена полная цена",
                    "Причина": f"Изменение скидки слишком большое ({discount_change:.1f}% > 20%)",
                    "Старая полная цена": f"{product.current_price_wb:.0f}₽",
                    "Новая полная цена": f"{new_total_price_rounded:.0f}₽",
                    "Скидка": f"{final_discount:.1f}% (близко к текущей {api_discount:.1f}%)",
                    "Новый priceWithDisc": f"{new_price_wd:.0f}₽"
                })

            price_change_absolute = new_total_price_rounded - product.current_price_wb
            price_change_percent = (
                    price_change_absolute / product.current_price_wb * 100) if product.current_price_wb > 0 else 0

            self._log_table("ИТОГОВЫЕ ЦЕНЫ", {
                "Новый priceWithDisc": f"{new_price_wd:.2f}₽",
                "Скидка WB": f"{final_discount:.1f}%",
                "Цена без скидки": f"{new_total_price_rounded:.0f}₽",
                "Текущая цена WB": f"{product.current_price_wb:.0f}₽",
                "Изменение цены": f"{price_change_absolute:+.0f}₽",
                "Процент изменения": f"{price_change_percent:+.1f}%",
                "Действие": action_type
            })

            self.logger.info("✅ ПРОВЕРКА ВАЛИДАЦИИ:")
            validation = self._validate_price_update(
                vendor_code, product, new_price_wd, new_total_price_rounded, price_wd_diff
            )

            if validation:
                validation.discount = final_discount
                validation.logistics_cost = logistics_cost
                validation.action_type = action_type

                self._log_separator("РЕШЕНИЕ: НЕ ИЗМЕНЯТЬ ЦЕНУ", "!")
                self.logger.warning(f"❌ {validation.reason}")

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
                profit_correction=abs(price_wd_diff),
                status=ProcessingStatus.SUCCESS,
                error_msg=f"Корректировка прибыли: {product.target_profit - current_profit:+.0f} ₽",
                discount=final_discount,
                sku_wb=product.sku_wb,
                logistics_cost=logistics_cost,
                finished_price=finished_price,
                current_profit=current_profit,
                target_profit=product.target_profit,
                sales_count=len(valid_sales),
                spp_used=spp,
                purchase_price=product.purchase_price,
                target_forpay=target_forpay,
                action_type=action_type
            )

            self._log_separator("ИТОГОВОЕ РЕШЕНИЕ", "✅")
            self._log_table("РЕЗУЛЬТАТ РАСЧЕТА", {
                "Артикул": vendor_code,
                "Старая цена": f"{product.current_price_wb:.0f}₽",
                "Новая цена": f"{new_total_price_rounded:.0f}₽",
                "Изменение цены": f"{price_change_absolute:+.0f}₽ ({price_change_percent:+.1f}%)",
                "Старая скидка": f"{api_discount:.1f}%",
                "Новая скидка": f"{final_discount:.1f}%",
                "Изменение скидки": f"{final_discount - api_discount:+.1f}%",
                "Старая прибыль": f"{current_profit:.0f}₽",
                "Целевая прибыль": f"{product.target_profit:.0f}₽",
                "Изменение прибыли": f"{product.target_profit - current_profit:+.0f}₽",
                "Использовано продаж": len(valid_sales),
                "Тип действия": action_type,
                "Логистика (средневзвешенная)": f"{logistics_cost:.0f}₽",
                "Тариф логистики": f"{self.weighted_delivery_base:.2f} + {self.weighted_delivery_liter:.2f} ₽/л",
                "Габариты": logistics_details['dimensions'],
                "Объем": f"{logistics_details['volume_liters']:.2f} л",
                "Складов в расчете": logistics_details['warehouses_count'],
                "Заказов в расчете": logistics_details['total_orders']
            })

            await self.db_logger.log(
                level="SUCCESS",
                message=f"Цена успешно рассчитана",
                vendor_code=vendor_code,
                details={
                    "old_price": product.current_price_wb,
                    "new_price": new_total_price_rounded,
                    "price_change": new_total_price_rounded - product.current_price_wb,
                    "old_discount": api_discount,
                    "new_discount": final_discount,
                    "discount_change": final_discount - api_discount,
                    "old_price_wd": avg_price_wd,
                    "new_price_wd": new_price_wd,
                    "price_wd_diff": price_wd_diff,
                    "current_profit": current_profit,
                    "target_profit": product.target_profit,
                    "profit_diff": product.target_profit - current_profit,
                    "action_type": action_type,
                    "avg_ratio_forpay_to_price": avg_ratio,
                    "target_forpay": target_forpay,
                    "expected_profit_with_new_price": expected_profit,
                    "logistics_calculated": logistics_cost,
                    "logistics_details": logistics_details,
                    "weighted_base": self.weighted_delivery_base,
                    "weighted_liter": self.weighted_delivery_liter,
                    "warehouses_in_calculation": len(self.warehouse_logistics),
                    "total_orders_for_calculation": logistics_details['total_orders']
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
                logistics_cost=0,
                finished_price=0
            )

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

    def _validate_price_update(self, vendor_code: str,
                               product: ProductData,
                               new_price_wd: float,
                               new_total_price: float,
                               profit_diff: float) -> Optional[PriceUpdate]:
        min_price = product.purchase_price * Config.MIN_MARGIN_FACTOR

        self.logger.info("📋 ПРОВЕРКА КРИТЕРИЕВ:")

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
                    if update.action_type == "discount_change":
                        self.stats['discounts_changed'] += 1
                    else:
                        self.stats['prices_changed'] += 1

                    profit_before = update.current_profit
                    profit_after = expected_profit_after_change
                    profit_diff = profit_after - profit_before

                    self._log_separator("СОХРАНЕНИЕ ЦЕНЫ В БД", "💾")
                    self._log_table("СОХРАНЕННЫЕ ДАННЫЕ", {
                        "Артикул": update.vendor_code,
                        "Старая цена": f"{update.old_price_wb:.0f}₽",
                        "Новая цена": f"{update.new_price_wb:.0f}₽",
                        "Изменение": f"{update.new_price_wb - update.old_price_wb:+.0f}₽",
                        "Старая скидка": f"{update.discount or 0:.1f}%",
                        "Тип действия": update.action_type,
                        "Прибыль до": f"{profit_before:.0f}₽",
                        "Ожидаемая прибыль после": f"{profit_after:.0f}₽",
                        "Изменение прибыли": f"{profit_diff:+.0f}₽",
                        "Продаж использовано": update.sales_count,
                        "Логистика (средневзвешенная)": f"{update.logistics_cost:.0f}₽"
                    })

                    await self.db_logger.log(
                        level="SUCCESS",
                        message=f"Цена сохранена с деталями прибыли",
                        vendor_code=update.vendor_code,
                        details={
                            "old_price": update.old_price_wb,
                            "new_price": update.new_price_wb,
                            "price_change": update.new_price_wb - update.old_price_wb,
                            "discount": f"{update.discount or 0:.1f}%",
                            "action_type": update.action_type,
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
        self.logger.info(
            f"📊 Статистика: {len([u for u in updates if u.action_type == 'price_change'])} изменений цены, "
            f"{len([u for u in updates if u.action_type == 'discount_change'])} изменений скидки")

        url = "https://discounts-prices-api.wildberries.ru/api/v2/upload/task"
        headers = {
            "Authorization": Config.WB_PRICES_TOKEN,
            "Content-Type": "application/json"
        }

        self.logger.info("📋 Отправляемые данные:")
        for item in data[:5]:
            update = next((u for u in updates if u.sku_wb == item['nmID']), None)
            action_type = update.action_type if update else "unknown"
            self.logger.info(f"   • nmID: {item['nmID']}, цена: {item['price']}₽, "
                             f"скидка: {item['discount']}%, действие: {action_type}")
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
                            "cycle_id": self.current_cycle,
                            "price_changes": len([u for u in updates if u.action_type == 'price_change']),
                            "discount_changes": len([u for u in updates if u.action_type == 'discount_change'])
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
                    vendor_code, sales, product = await asyncio.wait_for(
                        self.queue.get(),
                        timeout=1.0
                    )

                    self.logger.info(f"👷 Воркер #{worker_id} обрабатывает: {vendor_code}")

                    update = await self.process_product_new_logic(vendor_code, sales, product)

                    if update.status == ProcessingStatus.SUCCESS:
                        await self.save_price_update(update)
                        self.successful_updates.append(update)
                        self.logger.info(f"👷 Воркер #{worker_id}: {vendor_code} - УСПЕХ ({update.action_type})")
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

            sa_names_from_sales = []
            for sale in sales_data:
                if sale.vendor_code and sale.vendor_code not in sa_names_from_sales:
                    sa_names_from_sales.append(sale.vendor_code)

            self.logger.info(f"📊 Найдено {len(sa_names_from_sales)} уникальных артикулов в продажах")

            sales_by_vendor = defaultdict(list)
            for sale in sales_data:
                if sale.vendor_code:
                    sales_by_vendor[sale.vendor_code].append(sale)

            vendor_codes = list(sales_by_vendor.keys())

            self._log_table("ОБНАРУЖЕННЫЕ АРТИКУЛЫ", {
                "Всего артикулов с продажами": len(vendor_codes),
                "Тариф логистики (средневзвешенный)": f"{self.weighted_delivery_base:.2f} + {self.weighted_delivery_liter:.2f} ₽/л",
                "Складов в расчете логистики": len(self.warehouse_logistics)
            })

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

            queue_tasks = 0
            skipped_tasks = 0

            self._log_separator("ФОРМИРОВАНИЕ ОЧЕРЕДИ ЗАДАЧ", "📋")

            for vendor_code in vendor_codes:
                if vendor_code in product_map:
                    product = product_map[vendor_code]

                    await self.queue.put((vendor_code,
                                          sales_by_vendor[vendor_code],
                                          product))
                    queue_tasks += 1

                    if queue_tasks <= 5:
                        logistics_cost, _ = await self.get_logistics_by_volume(product)
                        self.logger.info(f"📥 Добавлен: {vendor_code} "
                                         f"({len(sales_by_vendor[vendor_code])} продаж, "
                                         f"логистика {logistics_cost:.0f}₽, "
                                         f"габариты {product.length}x{product.width}x{product.height}см)")
                else:
                    skipped_tasks += 1

            self.logger.info(f"📊 В очередь добавлено: {queue_tasks} задач")
            if skipped_tasks > 0:
                self.logger.info(f"📊 Пропущено (нет в БД): {skipped_tasks} артикулов")

            self._log_separator("ЗАПУСК ОБРАБОТКИ", "⚡")
            self.logger.info(f"👥 Запускаем {Config.WORKERS_COUNT} воркеров...")

            workers = []
            for i in range(Config.WORKERS_COUNT):
                worker_task = asyncio.create_task(self.worker(i))
                workers.append(worker_task)

            self.logger.info("⏳ Ожидание завершения обработки...")
            await self.queue.join()

            for worker_task in workers:
                worker_task.cancel()

            await asyncio.gather(*workers, return_exceptions=True)
            self.logger.info("✅ Обработка завершена")

            if LOAD_PRICE_TO_WB and self.successful_updates:
                await self.upload_prices_to_wb(self.successful_updates)
            else:
                self.logger.info("ℹ️  Отправка цен на WB отключена (LOAD_PRICE_TO_WB=False)")

            cycle_end = datetime.now()
            duration = (cycle_end - cycle_start).total_seconds()

            price_changes = len([u for u in self.successful_updates if u.action_type == 'price_change'])
            discount_changes = len([u for u in self.successful_updates if u.action_type == 'discount_change'])

            self._log_separator("СТАТИСТИКА ЦИКЛА", "📊")
            self._log_table("РЕЗУЛЬТАТЫ ОБРАБОТКИ", {
                "Время выполнения": f"{duration:.1f} сек",
                "Всего артикулов": len(vendor_codes),
                "Товаров в БД": len(product_map),
                "✅ Успешно обработано": self.stats.get('success', 0),
                "📈 Изменений цены": price_changes,
                "🎯 Изменений скидки": discount_changes,
                "✅ Обновлено цен": self.stats.get('prices_updated', 0),
                "✅ Отправлено на WB": self.stats.get('prices_uploaded_to_wb', 0),
                "⚠️  Пропущено (мало данных)": self.stats.get('skipped_no_data', 0),
                "⚠️  Пропущено (низкая цена)": self.stats.get('skipped_min_price', 0),
                "⚠️  Пропущено (мало изменений)": self.stats.get('skipped_min_change', 0),
                "❌ Ошибок": self.stats.get('error', 0)
            })

            if self.successful_updates:
                self._log_separator("ПРИМЕРЫ ИЗМЕНЕНИЙ", "📈")
                sample_updates = self.successful_updates[:3]
                for update in sample_updates:
                    if update.action_type == "discount_change":
                        self.logger.info(f"   🎯 {update.vendor_code}: "
                                         f"Скидка {update.discount:.1f}% (изменена скидка)")
                    else:
                        change = update.new_price_wb - update.old_price_wb
                        percent = (change / update.old_price_wb * 100) if update.old_price_wb > 0 else 0
                        self.logger.info(f"   📈 {update.vendor_code}: "
                                         f"{update.old_price_wb:.0f}₽ → {update.new_price_wb:.0f}₽ "
                                         f"({change:+.0f}₽, {percent:+.1f}%), скидка: {update.discount:.1f}%")

            await self.db_logger.log(
                level="INFO",
                message=f"Цикл #{self.current_cycle} завершен",
                details={
                    "duration_sec": duration,
                    "vendor_codes": len(vendor_codes),
                    "products_in_db": len(product_map),
                    "success": self.stats.get('success', 0),
                    "price_changes": price_changes,
                    "discount_changes": discount_changes,
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