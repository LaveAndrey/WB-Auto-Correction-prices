import asyncio
import json
import signal
import atexit
import traceback
import statistics
from collections import defaultdict
from datetime import datetime, timedelta, timezone, date
from typing import Dict, List, Optional, Tuple
import pymysql

import pytz
import aiomysql
from aiohttp import ClientTimeout, ClientSession

from config import Config

from WB_integration.structurs.enums import ProcessingStatus
from WB_integration.structurs.dataclass import WarehouseLogistics, OrderData, ProductData, PriceUpdate
from WB_integration.core.database_logger import DatabaseLogger
from WB_integration.api.wb_api_client import WBApiClient
from WB_integration.api.promo_client import PromoClient
from WB_integration.utils.logging_utils import setup_logger, log_separator, log_table
from WB_integration.utils.calculations import calculate_volume, _get_last_non_zero_spp
from WB_integration.api.promotions_client_wb import WBPromotionManager


class PriceUpdater:
    def __init__(self):
        self.logger = setup_logger('price_updater')
        self.db_pool = None
        self.session = None
        self.db_logger = None
        self.wb_client = None
        self.promo_client = None
        self.is_running = False
        self.queue = None
        self.stats = defaultdict(int)
        self.successful_updates = []
        self.current_cycle = 0
        self.warehouse_logistics: Dict[int, WarehouseLogistics] = {}
        self.warehouse_orders_stats: Dict[str, int] = {}
        self.weighted_delivery_base = 0.0
        self.weighted_delivery_liter = 0.0
        self.cycle_start_time = None
        self.current_cycle_id = None


        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _log_separator(self, title: str = "", char: str = "=", length: int = 80):
        """Логирование разделителя (только если не в объединенной системе)"""
        # Проверяем, не запущены ли мы из объединенной системы
        import inspect
        frame = inspect.currentframe()
        caller_frame = frame.f_back
        caller_class = caller_frame.f_locals.get('self', None).__class__.__name__ if caller_frame else None

        # Если нас вызвали из UnifiedPriceUpdater, не логируем
        if caller_class == 'UnifiedPriceUpdater':
            return

        log_separator(self.logger, title, char, length)

    def _log_table(self, title: str, data: dict):
        """Логирование таблицы (только если не в объединенной системе)"""
        import inspect
        frame = inspect.currentframe()
        caller_frame = frame.f_back
        caller_class = caller_frame.f_locals.get('self', None).__class__.__name__ if caller_frame else None

        # Если нас вызвали из UnifiedPriceUpdater, не логируем
        if caller_class == 'UnifiedPriceUpdater':
            return

        log_table(self.logger, title, data)

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
        self.wb_client = WBApiClient(self.session)
        self.wb_client.set_logger(self.logger)
        self.promo_client = PromoClient(self.session)
        self.queue = asyncio.Queue(maxsize=Config.MAX_QUEUE_SIZE)
        await self._ensure_tables_exist()


        self.promotion_manager = WBPromotionManager(self.session, self.db_pool, self.logger, self.db_logger)
        self.promotion_manager.set_logger(self.logger)

        await self._init_weighted_logistics()

        self._log_table("КОНФИГУРАЦИЯ СИСТЕМЫ", {
            "WORKERS_COUNT": Config.WORKERS_COUNT,
            "BATCH_SIZE": Config.BATCH_SIZE,
            "SALES_HOURS_FILTER": f"{Config.SALES_HOURS_FILTER}ч",
            "CYCLE_INTERVAL": f"{Config.WB_INTERVAL // 3600}ч {(Config.WB_INTERVAL % 3600) // 60}м",
            "MIN_SALES_FOR_CALC": Config.MIN_SALES_FOR_CALC,
            "MIN_MARGIN_FACTOR": f"×{Config.MIN_MARGIN_FACTOR}",
            "BANK_COMMISSION": f"{Config.BANK_COMMISSION * 100}%",
            "FORPAY_RATIO": f"{Config.WB_FORPAY_TO_PRICEWITHDISC_RATIO:.3f}",
            "Логистика по габаритам": "ВКЛЮЧЕНА",
            "Средневзвешенная логистика": "ВКЛЮЧЕНА",
            "Период статистики": "14 дней",
            "Тариф доставки": f"{self.weighted_delivery_base:.2f} + {self.weighted_delivery_liter:.2f} ₽/л",
            "Складов в расчете": len(self.warehouse_logistics)
        })

        self.logger.info("✅ Инициализация завершена успешно")
        self._log_separator()

    async def _ensure_tables_exist(self):
        """Проверяет и создает необходимые таблицы"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # Таблица циклов
                    await cursor.execute(f"""
                        CREATE TABLE IF NOT EXISTS {Config.WB_CYCLE_INFO_TABLE} (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            start_time DATETIME NOT NULL,
                            end_time DATETIME,
                            duration_sec INT,
                            status VARCHAR(20),
                            products_processed INT DEFAULT 0,
                            successful_updates INT DEFAULT 0,
                            price_changes INT DEFAULT 0,
                            discount_changes INT DEFAULT 0,
                            skipped_no_data INT DEFAULT 0,
                            skipped_min_price INT DEFAULT 0,
                            skipped_min_change INT DEFAULT 0,
                            errors INT DEFAULT 0,
                            promotions_scheduled INT DEFAULT 0,
                            next_cycle_time DATETIME,
                            INDEX idx_start_time (start_time)
                        )
                    """)

                    # Таблица статуса системы
                    await cursor.execute(f"""
                        CREATE TABLE IF NOT EXISTS {Config.WB_SYSTEM_STATUS_TABLE} (
                            id INT DEFAULT 1,
                            is_running BOOLEAN DEFAULT FALSE,
                            last_start DATETIME,
                            last_stop DATETIME,
                            next_cycle DATETIME,
                            current_cycle_id INT,
                            updated_at DATETIME,
                            PRIMARY KEY (id)
                        )
                    """)

                    await cursor.execute(f"""
                        CREATE TABLE IF NOT EXISTS {Config.WB_LOGIDTIC_TABLE} (
                            vendor_code VARCHAR(100) NOT NULL,
                            warehouse_id INT NOT NULL,
                            success_orders INT DEFAULT 0,
                            returned_orders INT DEFAULT 0,
                            total_delivery_cost DECIMAL(10,2) DEFAULT 0,
                            total_return_cost DECIMAL(10,2) DEFAULT 0,
                            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                            PRIMARY KEY (vendor_code, warehouse_id),
                            INDEX idx_vendor (vendor_code),
                            INDEX idx_updated (updated_at)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """)

                    # Таблица дневной статистики
                    await cursor.execute(f"""
                        CREATE TABLE IF NOT EXISTS {Config.WB_DAILY_STATS_TABLE} (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            date DATE NOT NULL,
                            vendor_code VARCHAR(50) NOT NULL,
                            orders_count INT DEFAULT 0,
                            target_profit_per_item DECIMAL(10,2) DEFAULT 0,
                            total_profit DECIMAL(10,2) DEFAULT 0,
                            total_revenue DECIMAL(10,2) DEFAULT 0,
                            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                            UNIQUE KEY unique_date_vendor (date, vendor_code),
                            INDEX idx_date (date),
                            INDEX idx_vendor (vendor_code)
                        )
                    """)

            self.logger.info("✅ Таблицы для статистики проверены/созданы")

        except Exception as e:
            self.logger.error(f"❌ Ошибка создания таблиц: {e}")

    async def update_wb_logistics_stats(self, vendor_code: str, warehouse_id: int, delivery_cost: float,
                                        is_returned: bool):
        """Обновляет статистику логистики для товара на WB"""
        return_cost = Config.WB_RETURN_COST if is_returned else 0
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"""
                        INSERT INTO {Config.WB_LOGIDTIC_TABLE} 
                        (vendor_code, warehouse_id, success_orders, returned_orders, 
                         total_delivery_cost, total_return_cost, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, NOW())
                        ON DUPLICATE KEY UPDATE
                            success_orders = success_orders + VALUES(success_orders),
                            returned_orders = returned_orders + VALUES(returned_orders),
                            total_delivery_cost = total_delivery_cost + VALUES(total_delivery_cost),
                            total_return_cost = total_return_cost + VALUES(total_return_cost),
                            updated_at = NOW()
                    """, (
                        vendor_code,
                        warehouse_id,
                        0 if is_returned else 1,
                        1 if is_returned else 0,
                        delivery_cost,
                        return_cost
                    ))
        except Exception as e:
            self.logger.error(f"Ошибка обновления статистики WB для {vendor_code}: {e}")

    async def get_weighted_wb_logistics(self, vendor_code: str) -> Optional[float]:
        """Возвращает средневзвешенную стоимость логистики для товара на WB с учётом возвратов за последние N дней"""
        days = 30  # можно вынести в конфиг
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"""
                        SELECT 
                            SUM(total_delivery_cost) + SUM(total_return_cost) as total_cost,
                            SUM(success_orders) as total_success
                        FROM {Config.WB_LOGIDTIC_TABLE}
                        WHERE vendor_code = %s AND updated_at >= NOW() - INTERVAL %s DAY
                    """, (vendor_code, days))
                    row = await cursor.fetchone()
                    if row and row[1] and row[1] > 0:
                        return float(row[0] / row[1])  # преобразуем в float
        except Exception as e:
            self.logger.error(f"Ошибка получения средней логистики WB для {vendor_code}: {e}")
        return None

    async def _init_weighted_logistics(self):
        """Инициализация средневзвешенной логистики по всем складам за 14 дней"""
        self._log_separator("РАСЧЕТ СРЕДНЕВЗВЕШЕННОЙ ЛОГИСТИКИ", "⚖️")

        try:
            self.logger.info("📊 Получение статистики заказов за 14 дней...")
            self.warehouse_orders_stats = await self.wb_client.get_warehouse_orders_stats()

            if not self.warehouse_orders_stats:
                self.logger.warning("⚠️  Нет данных о заказах за 14 дней")
                self.weighted_delivery_base = 30.0
                self.weighted_delivery_liter = 10.0
                return

            self.logger.info("📋 Получение информации о всех складах...")
            warehouses_list = await self.wb_client.get_all_warehouses()

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
            all_tariffs = await self.wb_client.get_warehouse_tariffs(warehouse_ids_to_fetch)
            #print(json.dumps(all_tariffs, indent=2, ensure_ascii=False))

            if not all_tariffs:
                self.logger.error("❌ API вернуло пустой ответ")
                return

            self.logger.info(f"✅ Получено {len(all_tariffs)} записей о тарифах")

            # Группируем тарифы по складам
            warehouse_tariffs = defaultdict(list)
            for tariff in all_tariffs:
                warehouse_id = tariff.get('warehouseID')
                if warehouse_id:
                    warehouse_tariffs[warehouse_id].append(tariff)

            tariffs_found = 0

            # Для каждого склада выбираем самый актуальный валидный тариф
            for warehouse_id, tariffs in warehouse_tariffs.items():
                if warehouse_id not in warehouse_info:
                    continue

                # Фильтруем только валидные тарифы (сортировка по типу коробки)
                valid_tariffs = []
                print(valid_tariffs)
                for tariff in tariffs:
                    box_type_id = tariff.get('boxTypeID')
                    coefficient = tariff.get('coefficient')
                    allow_unload = tariff.get('allowUnload')

                    if box_type_id != Config.WB_BOX_TYPE:
                        continue

                    if coefficient in [0, 1, -1] and allow_unload is True:
                        valid_tariffs.append(tariff)

                if not valid_tariffs:
                    self.logger.debug(f"   ⚠️  Для склада {warehouse_id} нет валидных тарифов")
                    continue

                # Выбираем самый актуальный тариф (самую позднюю дату)
                best_tariff = None
                best_tariff_date = None

                current_utc = datetime.now(timezone.utc)  # Текущее время в UTC

                for tariff in valid_tariffs:
                    try:
                        # Парсинг даты с приведением к UTC
                        date_str = tariff.get('date', '')
                        if date_str.endswith('Z'):
                            api_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                        else:
                            api_date = datetime.fromisoformat(date_str)
                        if api_date.tzinfo is None:
                            api_date = api_date.replace(tzinfo=timezone.utc)

                        # Пропускаем тарифы из будущего
                        if api_date > current_utc:
                            continue

                        # Выбираем самый свежий ИЗ ДЕЙСТВУЮЩИХ НА СЕГОДНЯ
                        if best_tariff is None or api_date > best_tariff_date:
                            best_tariff = tariff
                            best_tariff_date = api_date
                    except Exception:
                        continue

                if best_tariff:
                    warehouse_name = warehouse_info[warehouse_id]['name']
                    orders_count = self.warehouse_orders_stats.get(warehouse_name, 0)

                    self.warehouse_logistics[warehouse_id] = WarehouseLogistics(
                        warehouse_id=warehouse_id,
                        warehouse_name=warehouse_name,
                        delivery_base=self._parse_float(best_tariff.get('deliveryBaseLiter')),
                        delivery_liter=self._parse_float(best_tariff.get('deliveryAdditionalLiter')),
                        storage_base=self._parse_float(best_tariff.get('storageBaseLiter')),
                        storage_liter=self._parse_float(best_tariff.get('storageAdditionalLiter')),
                        coefficient=best_tariff.get('coefficient'),
                        is_sorting_center=warehouse_info[warehouse_id]['is_sc'],
                        orders_count=orders_count
                    )
                    tariffs_found += 1

            self.logger.info(f"✅ Получены тарифы для {tariffs_found} складов")

        except Exception as e:
            self.logger.error(f"❌ Ошибка получения тарифов: {e}")

    async def get_primary_warehouse_for_product(self, vendor_code: str) -> Optional[WarehouseLogistics]:
        """
        Возвращает основной склад для товара (с максимальным числом заказов из статистики).
        Если данных нет, возвращает None.
        """
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute(f"""
                        SELECT warehouse_id, SUM(success_orders) as total_orders
                        FROM {Config.WB_LOGIDTIC_TABLE}
                        WHERE vendor_code = %s
                        GROUP BY warehouse_id
                        ORDER BY total_orders DESC
                        LIMIT 1
                    """, (vendor_code,))
                    row = await cursor.fetchone()
                    if row and row['warehouse_id']:
                        warehouse_id = row['warehouse_id']
                        return self.warehouse_logistics.get(warehouse_id)
        except Exception as e:
            self.logger.error(f"Ошибка при определении основного склада для {vendor_code}: {e}")
        return None

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

        self.logger.info(f"📍 Индекс локализации (ИЛ): ×{Config.LOCALIZATION_INDEX:.2f}")
        if Config.LOCALIZATION_INDEX > 1.0:
            self.logger.warning(
                f"   ⚠️  ИЛ > 1.0 → наценка на логистику {((Config.LOCALIZATION_INDEX - 1) * 100):.0f}%")
        elif Config.LOCALIZATION_INDEX < 1.0:
            self.logger.info(f"   ✅ ИЛ < 1.0 → скидка на логистику {((1 - Config.LOCALIZATION_INDEX) * 100):.0f}%")
        else:
            self.logger.info(f"   ➖ ИЛ = 1.0 → логистика без изменений")

    async def get_logistics_by_volume(self, product: ProductData, vendor_code: str = None) -> Tuple[float, Dict]:
        """
        Расчёт логистики по габаритам товара.
        Если передан vendor_code, пытается найти основной склад для этого товара.
        Если основного склада нет, использует глобальный топ-склад (по всем товарам).
        """
        if Config.USE_FIXED_TARIFF:
            self.logger.info("🔧 Используем временные фиксированные тарифы (заглушка 34.5 + 10.5)")
            delivery_base = Config.FIXED_DELIVERY_BASE
            delivery_liter = Config.FIXED_DELIVERY_LITER
            warehouse_name = "Фиксированный тариф (временный)"

            volume = calculate_volume(product.length, product.width, product.height)
            base_logistics = delivery_base + max(volume - 1.0, 0.0) * delivery_liter
            total_logistics = base_logistics * Config.LOCALIZATION_INDEX

            calculation_details = {
                'dimensions': f"{product.length}x{product.width}x{product.height} см",
                'volume_liters': volume,
                'delivery_base': delivery_base,
                'delivery_liter': delivery_liter,
                'warehouse_name': warehouse_name,
                'localization_index': Config.LOCALIZATION_INDEX,
                'base_logistics': base_logistics,
                'total_logistics': total_logistics,
                'calculation_formula': (
                    f"({delivery_base:.2f} + max({volume:.2f} - 1, 0) × {delivery_liter:.2f}) "
                    f"× {Config.LOCALIZATION_INDEX:.2f} = {total_logistics:.2f} ₽"
                ),
                'source': 'fixed_tariff_fallback'
            }

            self.logger.debug(f"   Объем: {volume:.2f} л")
            self.logger.debug(f"   Базовая логистика: {base_logistics:.2f} ₽")
            self.logger.debug(f"   Итоговая логистика: {total_logistics:.2f} ₽")
            return total_logistics, calculation_details
        primary_warehouse = None
        if vendor_code:
            primary_warehouse = await self.get_primary_warehouse_for_product(vendor_code)

        if primary_warehouse:
            delivery_base = primary_warehouse.delivery_base
            delivery_liter = primary_warehouse.delivery_liter
            warehouse_name = primary_warehouse.warehouse_name
            self.logger.info(
                f"📦 Для товара {vendor_code} используется основной склад: {warehouse_name} ({primary_warehouse.orders_count} заказов)")
        else:
            # Если основного склада нет, берём глобальный топ-склад
            if self.warehouse_logistics:
                top_warehouse = max(self.warehouse_logistics.values(), key=lambda wh: wh.orders_count)
                delivery_base = top_warehouse.delivery_base
                delivery_liter = top_warehouse.delivery_liter
                warehouse_name = top_warehouse.warehouse_name
                self.logger.info(
                    f"🏆 Используем глобальный топ-склад: {warehouse_name} ({top_warehouse.orders_count} заказов)")
            else:
                # fallback на дефолтные значения
                delivery_base = 30.0
                delivery_liter = 10.0
                warehouse_name = "дефолтный"
                self.logger.warning("⚠️ Нет данных о складах, используем дефолтные тарифы")

        volume = calculate_volume(product.length, product.width, product.height)
        base_logistics = delivery_base + max(volume - 1.0, 0.0) * delivery_liter
        total_logistics = base_logistics * Config.LOCALIZATION_INDEX

        calculation_details = {
            'dimensions': f"{product.length}x{product.width}x{product.height} см",
            'volume_liters': volume,
            'delivery_base': delivery_base,
            'delivery_liter': delivery_liter,
            'warehouse_name': warehouse_name,
            'warehouse_orders': primary_warehouse.orders_count if primary_warehouse else (
                top_warehouse.orders_count if 'top_warehouse' in locals() else 0),
            'localization_index': Config.LOCALIZATION_INDEX,
            'base_logistics': base_logistics,
            'total_logistics': total_logistics,
            'calculation_formula': (
                f"({delivery_base:.2f} + max({volume:.2f} - 1, 0) × {delivery_liter:.2f}) "
                f"× {Config.LOCALIZATION_INDEX:.2f} = {total_logistics:.2f} ₽"
            )
        }

        self.logger.debug(f"   Объем: {volume:.2f} л")
        self.logger.debug(f"   Базовая логистика: {base_logistics:.2f} ₽")
        self.logger.debug(f"   Индекс локализации: ×{Config.LOCALIZATION_INDEX:.2f}")
        self.logger.debug(f"   Итоговая логистика: {total_logistics:.2f} ₽")

        return total_logistics, calculation_details

    async def fetch_wb_orders(self) -> Dict:
        """Прокси метод к WBApiClient"""
        return await self.wb_client.fetch_wb_orders()

    async def fetch_all_products(self) -> Dict[str, ProductData]:
        """Загружает все товары из базы данных"""
        self._log_separator("ЗАГРУЗКА ВСЕХ ТОВАРОВ ИЗ БАЗЫ ДАННЫХ", "-")

        product_map = {}
        nm_id_to_product = {}

        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute(f"""
                        SELECT model_wb, purchase_price, target_profit_rub, 
                               price_wb, wb_real_price, length, width, height, 
                               sku_wb, status, wb_seller_discount,
                               promotion_active_wb, promotion_lock_until_wb,
                               has_sales_last_period_wb 
                        FROM {Config.PPODUCT_TABLE}
                        WHERE purchase_price > 0
                          AND target_profit_rub > 0
                          AND status = 1
                    """)

                    rows = await cursor.fetchall()

                    loaded_count = 0
                    with_nm_id = 0

                    for row in rows:
                        product = ProductData.from_db_row(row)
                        if product:
                            product.wb_seller_discount = float(row.get('wb_seller_discount', 0) or 0)
                            product_map[product.vendor_code] = product
                            loaded_count += 1

                            if product.sku_wb > 0:
                                nm_id_to_product[product.sku_wb] = product
                                with_nm_id += 1

            self._log_table("РЕЗУЛЬТАТЫ ЗАГРУЗКИ ТОВАРОВ", {
                "Всего загружено товаров": loaded_count,
                "С nmId в БД (sku_wb)": with_nm_id,
                "Без nmId в БД": loaded_count - with_nm_id
            })

            if loaded_count > 0:
                self.logger.info("📋 Примеры загруженных товаров:")
                sample_items = list(product_map.items())[:3]
                for vendor_code, product in sample_items:
                    logistics_cost, _ = await self.get_logistics_by_volume(product)
                    self.logger.info(f"   • {vendor_code}: "
                                     f"nmId={product.sku_wb if product.sku_wb else 'нет'}, "
                                     f"закуп={product.purchase_price:.0f}₽, "
                                     f"цель={product.target_profit:.0f}₽")

            return {
                'by_vendor_code': product_map,
                'by_nm_id': nm_id_to_product
            }

        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки товаров: {e}")
            return {'by_vendor_code': {}, 'by_nm_id': {}}

    async def fetch_nm_id(self, vendor_code: str) -> int:
        """Прокси метод к WBApiClient"""
        return await self.wb_client.fetch_nm_id(vendor_code)

    async def get_wb_discounts(self, nm_ids: List[int]) -> Dict[int, Dict[str, float]]:
        """Прокси метод к PromoClient"""
        return await self.promo_client.get_wb_discounts(nm_ids)

    async def update_sales_flags(self, processed_vendor_codes: set):
        """Обновляет флаги наличия продаж для товаров"""
        try:
            self._log_separator("ОБНОВЛЕНИЕ ФЛАГОВ ПРОДАЖ", "🏷️")

            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # 1. Сначала все товары помечаем как "нет продаж" (по умолчанию)
                    await cursor.execute(f"""
                        UPDATE {Config.PPODUCT_TABLE}
                        SET has_sales_last_period_wb = 0
                        WHERE status = 1 AND target_profit_rub > 0
                    """)

                    # 2. Помечаем товары, которые были обработаны в этом цикле (есть продажи)
                    if processed_vendor_codes:
                        placeholders = ','.join(['%s'] * len(processed_vendor_codes))
                        await cursor.execute(f"""
                            UPDATE {Config.PPODUCT_TABLE}
                            SET has_sales_last_period_wb = 1,
                                last_sale_time = NOW()
                            WHERE model_wb IN ({placeholders})
                        """, list(processed_vendor_codes))

                    # 3. Получаем статистику
                    await cursor.execute(f"""
                        SELECT 
                            COUNT(*) as total,
                            SUM(has_sales_last_period_wb) as with_sales,
                            SUM(CASE WHEN has_sales_last_period_wb = 0 THEN 1 ELSE 0 END) as without_sales
                        FROM {Config.PPODUCT_TABLE} 
                        WHERE status = 1 AND target_profit_rub > 0
                    """)

                    stats = await cursor.fetchone()

                    self.logger.info(f"📊 СТАТИСТИКА ФЛАГОВ ПРОДАЖ:")
                    self.logger.info(f"   • Всего активных товаров: {stats[0]}")
                    self.logger.info(f"   • С продажами: {stats[1]}")
                    self.logger.info(f"   • Без продаж: {stats[2]}")

        except Exception as e:
            self.logger.error(f"❌ Ошибка обновления флагов продаж: {e}")

    async def process_product_new_logic(self, vendor_code: str,
                                        orders: List[OrderData],
                                        product: ProductData,
                                        source: str = "unknown") -> PriceUpdate:
        """Основная логика обработки товара с приоритетом скидки из БД"""
        try:
            if Config.PROMOTIONS_ENABLED:
                if hasattr(product, 'promotion_active_wb') and product.promotion_active_wb and Config.PROMOTION_LOCK_PRICES:
                    self.logger.info(f"🔒 Товар {vendor_code} УЧАСТВУЕТ В АКЦИИ - пропускаем изменение цены")
                    await self.db_logger.log_skip(
                        vendor_code=vendor_code,
                        nm_id=product.sku_wb,
                        reason="Товар участвует в акции (promotion_active_wb)",
                        details={
                            "promotion_active_wb": True,
                            "promotion_lock_until_wb": str(
                                product.promotion_lock_until_wb) if product.promotion_lock_until_wb else None
                        }
                    )
                    self.stats['products_in_promotion_wb'] = self.stats.get('products_in_promotion_wb', 0) + 1
                    return PriceUpdate(
                        vendor_code=vendor_code,
                        new_price_wb=0,
                        new_real_price=0,
                        old_price_wb=product.current_price_wb,
                        old_real_price=product.current_real_price,
                        profit_correction=0,
                        status=ProcessingStatus.SKIPPED_PROMOTION,
                        error_msg="Товар участвует в акции",
                        sku_wb=product.sku_wb,
                        logistics_cost=0,
                        finished_price=0,
                        source=source
                    )

                # ===== ПРОВЕРКА НА ЗАЩИТУ ДО НАЧАЛА АКЦИИ =====
                if product.promotion_lock_until_wb:
                    lock_until = product.promotion_lock_until_wb
                    if lock_until.tzinfo is not None:
                        lock_until = lock_until.replace(tzinfo=None)
                    if datetime.now() < lock_until:
                        self.logger.info(
                            f"🛡️ Товар {vendor_code} защищён до {lock_until} (участвует в будущей акции) – пропускаем")
                        await self.db_logger.log_skip(
                            vendor_code=vendor_code,
                            nm_id=product.sku_wb,
                            reason="Товар защищён до начала акции",
                            details={"promotion_lock_until_wb": str(product.promotion_lock_until_wb)}
                        )
                        self.stats['products_protected_for_promotion'] = self.stats.get('products_protected_for_promotion',
                                                                                        0) + 1
                        return PriceUpdate(
                            vendor_code=vendor_code,
                            new_price_wb=0,
                            new_real_price=0,
                            old_price_wb=product.current_price_wb,
                            old_real_price=product.current_real_price,
                            profit_correction=0,
                            status=ProcessingStatus.SKIPPED_PROMOTION,
                            error_msg="Товар защищён до начала акции",
                            sku_wb=product.sku_wb,
                            logistics_cost=0,
                            finished_price=0,
                            source=source
                        )

            self._log_separator(f"АНАЛИЗ ТОВАРА: {vendor_code}", "=")

            # Получаем целевую скидку из БД
            discount_from_db = product.wb_seller_discount if hasattr(product, 'wb_seller_discount') else 0

            # Логируем информацию о nmId и источнике
            nm_ids_in_orders = set(o.nm_id for o in orders if o.nm_id > 0)
            current_nm_id = product.sku_wb

            self.logger.info(f"📊 ИНФОРМАЦИЯ О nmId И СКИДКАХ:")
            self.logger.info(f"   Артикул поставщика: {vendor_code}")
            self.logger.info(f"   nmId в БД (sku_wb): {current_nm_id}")
            self.logger.info(f"   Целевая скидка из БД (wb_seller_discount): {discount_from_db:.1f}%")
            self.logger.info(f"   nmId в заказах: {', '.join(map(str, list(nm_ids_in_orders)[:3])) or 'нет'}")
            self.logger.info(f"   Источник поиска: {source}")
            self.logger.info(f"   Всего заказов: {len(orders)}")

            # Проверяем совпадение nmId
            if current_nm_id > 0 and current_nm_id in nm_ids_in_orders:
                self.logger.info(f"✅ nmId СОВПАДАЕТ: {current_nm_id}")
            elif current_nm_id > 0:
                self.logger.warning(f"⚠️  nmId НЕ СОВПАДАЕТ: в БД={current_nm_id}, в заказах={nm_ids_in_orders}")
            else:
                self.logger.warning(f"⚠️  nmId ОТСУТСТВУЕТ в БД")

            # ===== НОВЫЙ БЛОК: ПОЛУЧЕНИЕ СТОИМОСТИ ЛОГИСТИКИ С УЧЁТОМ ВОЗВРАТОВ =====
            # Сначала пробуем получить средневзвешенную из статистики
            avg_logistics = await self.get_weighted_wb_logistics(vendor_code)

            if avg_logistics is not None and avg_logistics > 0:
                logistics_cost = avg_logistics
                logistics_details = {
                    'source': 'weighted_stats',
                    'avg_logistics': avg_logistics,
                    'stats_days': 30  # можно вынести в конфиг
                }
                self.logger.info(f"📊 Используем средневзвешенную логистику: {logistics_cost:.2f}₽")
            else:
                # Если статистики нет, собираем данные по текущим заказам
                # Для каждого заказа определяем склад и рассчитываем логистику по его тарифам
                order_costs = []
                for order in orders:
                    # Определяем склад по warehouse_name
                    warehouse = None
                    for wh in self.warehouse_logistics.values():
                        if wh.warehouse_name == order.warehouse_name:
                            warehouse = wh
                            break

                    if warehouse:
                        # Рассчитываем логистику для данного склада с учётом габаритов товара
                        volume = calculate_volume(product.length, product.width, product.height)
                        base_logistics = warehouse.delivery_base + max(volume - 1.0, 0.0) * warehouse.delivery_liter
                        cost = base_logistics * Config.LOCALIZATION_INDEX
                    else:
                        # Если склад не найден, используем средневзвешенную (как раньше)
                        cost, _ = await self.get_logistics_by_volume(product)

                    order_costs.append(cost)

                    # Обновляем статистику (is_returned = order.is_cancel)
                    await self.update_wb_logistics_stats(
                        vendor_code=vendor_code,
                        warehouse_id=warehouse.warehouse_id if warehouse else 0,
                        delivery_cost=cost,
                        is_returned=order.is_cancel
                    )

                if order_costs:
                    logistics_cost = sum(order_costs) / len(order_costs)
                    logistics_details = {
                        'source': 'current_orders_avg',
                        'avg_logistics': logistics_cost,
                        'orders_processed': len(order_costs)
                    }
                else:
                    # Аварийный вариант – fallback
                    logistics_cost, logistics_details = await self.get_logistics_by_volume(product, vendor_code)
                    logistics_details['source'] = 'fallback_volume'

            self.logger.debug(
                f"📦 Логистика для {vendor_code}: {logistics_cost:.2f}₽ (источник: {logistics_details['source']})")
            # ===== КОНЕЦ НОВОГО БЛОКА =====

            # Получаем текущую скидку с WB API
            self.logger.info("🔄 ПОЛУЧЕНИЕ ТЕКУЩЕЙ СКИДКИ С WB API:")
            discounts_info = await self.get_wb_discounts([product.sku_wb])
            current_discount_from_api = discounts_info.get(product.sku_wb, {}).get("discount", 0)

            # Рассчитываем медианную скидку из заказов (для информации)
            valid_orders_for_discount = []
            for order in orders:
                if (order.price_with_desc > 0 and
                        order.discount_percent > 0 and
                        not order.is_cancel):
                    valid_orders_for_discount.append(order)

            discount_list = [order.discount_percent for order in valid_orders_for_discount if
                             order.discount_percent > 0]
            discount_from_orders = statistics.median(discount_list) if discount_list else 0

            self.logger.info(f"📊 ВСЕ ДОСТУПНЫЕ СКИДКИ:")
            self.logger.info(f"   • Из БД (wb_seller_discount): {discount_from_db:.1f}%")
            self.logger.info(f"   • Из WB API (текущая): {current_discount_from_api:.1f}%")
            self.logger.info(f"   • Из заказов (медианная): {discount_from_orders:.1f}%")

            # ОПРЕДЕЛЯЕМ ИСПОЛЬЗУЕМУЮ СКИДКУ ПО НОВОЙ ЛОГИКЕ
            if discount_from_db > 0:
                # ИСПОЛЬЗУЕМ ЦЕЛЕВУЮ СКИДКУ ИЗ БД как ориентир
                api_discount = discount_from_db
                discount_source = "БД (целевая скидка)"
                self.logger.info(f"🎯 ИСПОЛЬЗУЕМ ЦЕЛЕВУЮ СКИДКУ ИЗ БД: {api_discount:.1f}%")

                if current_discount_from_api > 0:
                    diff = abs(api_discount - current_discount_from_api)
                    if diff > 1:
                        self.logger.info(f"   🔄 Текущая скидка на WB: {current_discount_from_api:.1f}%")
                        self.logger.info(
                            f"   📈 Нужно восстановить: {current_discount_from_api:.1f}% → {api_discount:.1f}%")
            elif current_discount_from_api > 0:
                # Если в БД нет целевой скидки, используем текущую из WB API
                api_discount = current_discount_from_api
                discount_source = "API Wildberries (текущая)"
                self.logger.info(f"✅ Используем текущую скидку из API WB: {api_discount:.1f}%")
            else:
                # Если нет нигде, используем скидку из заказов
                api_discount = discount_from_orders
                discount_source = "заказы (медианная)"
                self.logger.warning(f"⚠️  Нет данных о скидке из API/БД, используем из заказов: {api_discount:.1f}%")

            api_discount = max(0.1, min(99.9, api_discount))
            self.logger.info(f"📊 Итоговая используемая скидка: {api_discount:.1f}% (источник: {discount_source})")

            # ДОБАВЛЯЕМ ИНФОРМАЦИЮ О НДС
            vat_percent = Config.VAT_PERCENT  # Получаем процент НДС из конфига
            self.logger.info(f"🧾 ПАРАМЕТРЫ НДС:")
            self.logger.info(f"   Ставка НДС: {vat_percent}%")
            self.logger.info(f"   НДС считается от priceWithDisc (цены для покупателя)")
            self.logger.info(f"   Формула: НДС = priceWithDisc × ({vat_percent} / (100 + {vat_percent}))")

            self.logger.info(f"📊 ИСХОДНЫЕ ДАННЫЕ:")
            logistics_info = {
                "Артикул": vendor_code,
                "nmId в БД": current_nm_id if current_nm_id else "нет",
                "Целевая скидка из БД": f"{discount_from_db:.1f}%" if discount_from_db > 0 else "нет",
                "Текущая скидка на WB": f"{current_discount_from_api:.1f}%",
                "Используемая скидка": f"{api_discount:.1f}% ({discount_source})",
                "Ставка НДС": f"{vat_percent}%",
                "Источник": source,
                "Закупочная цена": f"{product.purchase_price:.2f}₽",
                "Целевая прибыль": f"{product.target_profit:.2f}₽",
                "Текущая цена WB": f"{product.current_price_wb:.0f}₽",
                "Габариты": logistics_details.get('dimensions', 'не указаны'),
                "Объем": f"{logistics_details.get('volume_liters', 0):.2f} л",
                "Логистика (средневзвешенная)": f"{logistics_cost:.2f}₽",
                "Источник логистики": logistics_details['source'],
                "Тариф (средневзвешенный)": f"{self.weighted_delivery_base:.2f} + {self.weighted_delivery_liter:.2f} ₽/л",
                "Складов в расчете": f"{logistics_details.get('warehouses_count', len(self.warehouse_logistics))}",
                "Заказов в расчете": f"{logistics_details.get('total_orders', 0)}",
                "Формула расчета логистики": logistics_details.get('calculation_formula', 'не указана'),
                "Соотношение forPay/priceWithDisc": f"{Config.WB_FORPAY_TO_PRICEWITHDISC_RATIO:.3f}"
            }

            self._log_table("ПАРАМЕТРЫ ТОВАРА", logistics_info)

            self.logger.info(f"📈 ДАННЫЕ О ЗАКАЗАХ:")
            self.logger.info(f"   Всего заказов: {len(orders)}")
            self.logger.info(f"   Требуется для расчета: {Config.MIN_SALES_FOR_CALC}")

            if len(orders) <= 10:
                self.logger.info("   Детали заказов:")
                for i, order in enumerate(orders, 1):
                    # Расчет НДС для текущих заказов (для информации)
                    vat_in_order = order.price_with_desc * (vat_percent / (100 + vat_percent))
                    self.logger.info(f"   {i:2d}. nmId: {order.nm_id} | "
                                     f"priceWithDisc: {order.price_with_desc:7.0f}₽ | "
                                     f"discount: {order.discount_percent:5.1f}% | "
                                     f"forPay: {order.for_pay:7.0f}₽ | "
                                     f"НДС: {vat_in_order:5.0f}₽")
            else:
                price_stats = {
                    "min": min(o.price_with_desc for o in orders),
                    "max": max(o.price_with_desc for o in orders),
                    "avg": statistics.mean(o.price_with_desc for o in orders)
                }
                self.logger.info(f"   Статистика priceWithDisc: "
                                 f"мин={price_stats['min']:.0f}₽, "
                                 f"макс={price_stats['max']:.0f}₽, "
                                 f"сред={price_stats['avg']:.0f}₽")

            # Фильтруем валидные заказы
            valid_orders = []
            for order in orders:
                if (order.price_with_desc > 0 and
                        order.discount_percent > 0 and
                        not order.is_cancel):
                    valid_orders.append(order)

            self.logger.info(f"✅ Валидных заказов: {len(valid_orders)}")

            if len(valid_orders) < Config.MIN_SALES_FOR_CALC:
                self._log_separator("РЕШЕНИЕ: ПРОПУСК", "!")
                self.logger.warning(f"⚠️  НЕДОСТАТОЧНО ДАННЫХ")
                self.logger.warning(f"   Требуется: {Config.MIN_SALES_FOR_CALC}, имеется: {len(valid_orders)}")

                await self.db_logger.log_skip(
                    vendor_code=vendor_code,
                    nm_id=product.sku_wb,
                    reason="Недостаточно данных для расчета",
                    details={
                        "valid_orders": len(valid_orders),
                        "required": Config.MIN_SALES_FOR_CALC,
                        "source": source,
                        "target_discount_from_db": discount_from_db,
                        "current_discount_from_wb": current_discount_from_api
                    }
                )
                return PriceUpdate(
                    vendor_code=vendor_code,
                    new_price_wb=0,
                    new_real_price=0,
                    old_price_wb=product.current_price_wb,
                    profit_correction=0,
                    status=ProcessingStatus.SKIPPED_NO_DATA,
                    error_msg=f"Недостаточно данных: {len(valid_orders)}",
                    sku_wb=product.sku_wb,
                    logistics_cost=logistics_cost,
                    finished_price=0,
                    source=source
                )

            # Поиск nmId если отсутствует в БД
            if product.sku_wb == 0:
                self.logger.info("🔍 Поиск nmId...")
                new_nm_id = await self.fetch_nm_id(vendor_code)
                if new_nm_id == 0:
                    self.logger.error("❌ Не удалось получить nmId")
                    await self.db_logger.log_error(
                        vendor_code=vendor_code,
                        nm_id=0,
                        error="Не удалось получить nmId",
                        details={
                            "source": source,
                            "target_discount_from_db": discount_from_db
                        }
                    )
                    return PriceUpdate(
                        vendor_code=vendor_code,
                        new_price_wb=0,
                        new_real_price=0,
                        old_price_wb=product.current_price_wb,
                        profit_correction=0,
                        status=ProcessingStatus.ERROR,
                        error_msg="Не удалось получить nmId",
                        sku_wb=0,
                        logistics_cost=logistics_cost,
                        finished_price=0,
                        source=source
                    )
                await self.save_nm_id_to_db(vendor_code, new_nm_id)
                product.sku_wb = new_nm_id
                self.logger.info(f"✅ nmId сохранен: {new_nm_id}")

            self.logger.info("🎯 РАСЧЕТ СКИДКИ ИЗ ЗАКАЗОВ (для информации):")

            discount_list = [order.discount_percent for order in valid_orders if order.discount_percent > 0]

            if discount_list:
                median_discount = statistics.median(discount_list)
                min_discount = min(discount_list)
                max_discount = max(discount_list)
                mean_discount = statistics.mean(discount_list)

                self._log_table("СТАТИСТИКА СКИДОК ИЗ ЗАКАЗОВ", {
                    "Всего значений": len(discount_list),
                    "Мин. скидка": f"{min_discount:.1f}%",
                    "Макс. скидка": f"{max_discount:.1f}%",
                    "Средняя скидка": f"{mean_discount:.1f}%",
                    "Медианная скидка": f"{median_discount:.1f}%"
                })
            else:
                self.logger.warning("⚠️  В заказах нет данных о скидках")

            self.logger.info("📊 АНАЛИЗ ЗАКАЗОВ:")

            price_wd_list = []
            forpay_list = []
            vat_list = []

            for order in valid_orders:
                price_wd_list.append(order.price_with_desc)
                forpay_list.append(order.for_pay)
                # Расчет НДС в каждом заказе
                vat_in_order = order.price_with_desc * (vat_percent / (100 + vat_percent))
                vat_list.append(vat_in_order)

            avg_price_wd = statistics.mean(price_wd_list)
            avg_forpay = statistics.mean(forpay_list)
            avg_vat = statistics.mean(vat_list)

            # Используем константное соотношение вместо расчета из данных
            avg_ratio = Config.WB_FORPAY_TO_PRICEWITHDISC_RATIO

            self._log_table("СТАТИСТИКА ЗАКАЗОВ С НДС", {
                "Средний priceWithDisc": f"{avg_price_wd:.2f}₽",
                "Средний forPay (расчетный)": f"{avg_forpay:.2f}₽",
                "Средний НДС в priceWithDisc": f"{avg_vat:.2f}₽ ({vat_percent}%)",
                "priceWithDisc без НДС": f"{avg_price_wd - avg_vat:.2f}₽",
                "Соотношение forPay/priceWithDisc (константа)": f"{avg_ratio:.3f}",
                "Заказов использовано": len(valid_orders),
                "Целевая скидка из БД": f"{discount_from_db:.1f}%" if discount_from_db > 0 else "нет",
                "Текущая скидка на WB": f"{current_discount_from_api:.1f}%",
                "Используемая скидка": f"{api_discount:.1f}%",
                "Скидка из заказов (медианная)": f"{discount_from_orders:.1f}%"
            })

            self.logger.info("🧮 РАСЧЕТ ТЕКУЩЕЙ ПРИБЫЛИ С УЧЕТОМ НДС:")

            # Расчет НДС из текущей цены
            current_vat = avg_price_wd * (vat_percent / (100 + vat_percent))
            price_wd_without_vat = avg_price_wd - current_vat

            total_commission = Config.BANK_COMMISSION + Config.ACQUIRING_RATE

            # ForPay уже не содержит НДС (по условию)
            bank_commission_current = avg_forpay * Config.BANK_COMMISSION
            acquiring_commission_current = avg_forpay * Config.ACQUIRING_RATE
            total_commission_current = avg_forpay * total_commission

            current_profit = avg_forpay - float(logistics_cost) - bank_commission_current - acquiring_commission_current - product.purchase_price

            self._log_table("РАСЧЕТ ТЕКУЩЕЙ ПРИБЫЛИ С НДС И ЭКВАЙРИНГОМ", {
                "Средний priceWithDisc": f"{avg_price_wd:.2f}₽",
                "НДС в цене ({vat_percent}%)": f"-{current_vat:.2f}₽",
                "Цена без НДС": f"{price_wd_without_vat:.2f}₽",
                "Средний forPay": f"{avg_forpay:.2f}₽",
                "Логистика (средневзвешенная)": f"-{logistics_cost:.2f}₽",
                f"Банковская комиссия": f"-{bank_commission_current:.2f}₽ ({Config.BANK_COMMISSION * 100}%)",
                f"Эквайринг": f"-{acquiring_commission_current:.2f}₽ ({Config.ACQUIRING_RATE * 100}%)",
                f"Общая комиссия": f"-{total_commission_current:.2f}₽ ({total_commission * 100}%)",
                "Закупочная цена": f"-{product.purchase_price:.2f}₽",
                "ИТОГО ПРИБЫЛЬ": f"{current_profit:.2f}₽",
                "Примечание": "НДС учитывается отдельно в налоговой отчетности"
            })
            self.logger.info("🎯 РАСЧЕТ ЦЕЛЕВЫХ ПОКАЗАТЕЛЕЙ С УЧЕТОМ НДС:")

            # ВАЖНО: Формула меняется с учетом НДС
            # НДС = priceWithDisc × (VAT_PERCENT / (100 + VAT_PERCENT))
            # НДС платится отдельно, поэтому в целевой прибыли его не вычитаем

            # 1. Рассчитываем необходимый forPay для достижения целевой прибыли
            target_forpay = (product.target_profit + logistics_cost + product.purchase_price) / (
                    1 - total_commission)

            # 2. Рассчитываем необходимый priceWithDisc из forPay
            required_price_wd = target_forpay / avg_ratio

            # 3. Рассчитываем НДС в новой цене
            new_vat = required_price_wd * (vat_percent / (100 + vat_percent))
            new_price_wd_without_vat = required_price_wd - new_vat

            bank_commission_target = target_forpay * Config.BANK_COMMISSION
            acquiring_commission_target = target_forpay * Config.ACQUIRING_RATE
            total_commission_target = target_forpay * total_commission
            expected_profit = target_forpay - logistics_cost - bank_commission_target - product.purchase_price - acquiring_commission_target

            self._log_table("РАСЧЕТ ДЛЯ ЦЕЛЕВОЙ ПРИБЫЛИ С НДС И ЭКВАЙРИНГОМ", {
                "Целевая прибыль": f"{product.target_profit:.2f}₽",
                "+ Логистика (средневзвешенная)": f"+{logistics_cost:.2f}₽",
                "+ Закупка": f"+{product.purchase_price:.2f}₽",
                "Сумма до комиссии": f"{(product.target_profit + logistics_cost + product.purchase_price):.2f}₽",
                f"Банковская комиссия": f"{Config.BANK_COMMISSION * 100}%",
                f"Эквайринг": f"{Config.ACQUIRING_RATE * 100}%",
                f"Общая комиссия": f"{total_commission * 100}%",
                "Требуемый forPay": f"{target_forpay:.2f}₽",
                "Соотношение forPay/priceWithDisc": f"{avg_ratio:.3f}",
                "Требуемый priceWithDisc": f"{required_price_wd:.2f}₽",
                "НДС {vat_percent}% в priceWithDisc": f"{new_vat:.2f}₽",
                "priceWithDisc без НДС": f"{new_price_wd_without_vat:.2f}₽",
                "Ожидаемая прибыль": f"{expected_profit:.2f}₽",
                "Формула НДС": f"priceWithDisc × ({vat_percent} / (100 + {vat_percent}))"
            })
            self.logger.info("💵 РАСЧЕТ НОВОЙ ЦЕНЫ СО СКИДКОЙ:")

            new_price_wd = required_price_wd
            price_wd_diff = new_price_wd - avg_price_wd

            self._log_table("РАСЧЕТ НОВОГО PRICEWITHDISC С НДС", {
                "Требуемый forPay": f"{target_forpay:.2f}₽",
                "Соотношение forPay/priceWithDisc": f"{avg_ratio:.3f}",
                "Нужный priceWithDisc": f"{required_price_wd:.2f}₽",
                "НДС {vat_percent}% в цене": f"{new_vat:.2f}₽",
                "Цена без НДС": f"{new_price_wd_without_vat:.2f}₽",
                "Текущий priceWithDisc": f"{avg_price_wd:.2f}₽",
                "Текущий НДС": f"{current_vat:.2f}₽",
                "Изменение цены": f"{price_wd_diff:+.2f}₽",
                "Изменение НДС": f"{new_vat - current_vat:+.2f}₽"
            })

            # Проверка минимальной цены с учетом НДС
            min_price_without_vat = product.purchase_price * Config.MIN_MARGIN_FACTOR
            min_price_with_vat = min_price_without_vat * (1 + vat_percent / 100)

            self.logger.info(f"📏 ПРОВЕРКА МИНИМАЛЬНОЙ ЦЕНЫ С НДС:")
            self.logger.info(
                f"   Минимальная цена без НДС: {min_price_without_vat:.2f}₽ (закупка × {Config.MIN_MARGIN_FACTOR})")
            self.logger.info(f"   Минимальная цена с НДС {vat_percent}%: {min_price_with_vat:.2f}₽")
            self.logger.info(f"   Рассчитанная цена с НДС: {new_price_wd:.2f}₽")

            if new_price_wd < min_price_with_vat:
                self.logger.warning(f"⚠️  ЦЕНА НИЖЕ МИНИМАЛЬНОЙ С УЧЕТОМ НДС!")
                self.logger.warning(f"   {new_price_wd:.0f}₽ < {min_price_with_vat:.0f}₽")
                self.logger.info(f"   Корректируем до минимальной: {min_price_with_vat:.2f}₽")

                await self.db_logger.log_skip(
                    vendor_code=vendor_code,
                    nm_id=product.sku_wb,
                    reason="Цена ниже минимальной с учетом НДС",
                    details={
                        "new_price": new_price_wd,
                        "min_price": min_price_with_vat,
                        "adjustment": "приведено к минимальной",
                        "source": source,
                        "target_discount_from_db": discount_from_db,
                        "vat_percent": vat_percent
                    }
                )
                new_price_wd = min_price_with_vat
                # Пересчитываем НДС для новой цены
                new_vat = new_price_wd * (vat_percent / (100 + vat_percent))
                price_wd_diff = new_price_wd - avg_price_wd

            # Расчет СПП
            spp = _get_last_non_zero_spp(valid_orders)
            finished_price = new_price_wd * (1 - spp / 100)
            self.logger.info(f"📉 СПП (не учитывается): {spp:.1f}%")
            self.logger.info(f"   Цена со скидкой СПП: {finished_price:.2f}₽")
            self.logger.info(f"   НДС в цене со СПП: {finished_price * (vat_percent / (100 + vat_percent)):.2f}₽")

            self._log_separator("РАСЧЕТ НОВОЙ СКИДКИ И РЕШЕНИЕ", "⭐")

            # ОСНОВНАЯ ЛОГИКА ВЫБОРА СКИДКИ
            new_discount_needed = 0
            new_total_price_rounded = 0
            action_type = ""
            final_discount = 0

            if discount_from_db > 0:
                # СЛУЧАЙ 1: ЕСТЬ ЦЕЛЕВАЯ СКИДКИ В БД
                new_discount_needed = discount_from_db
                self.logger.info(f"🎯 ИСПОЛЬЗУЕМ ЦЕЛЕВУЮ СКИДКУ ИЗ БД: {new_discount_needed:.1f}%")
                self.logger.info(f"   Текущая скидка на WB: {current_discount_from_api:.1f}%")

                # Рассчитываем полную цену для достижения целевой прибыли при этой скидке
                needed_full_price = new_price_wd / (1 - new_discount_needed / 100)

                # Проверяем разницу между текущей и целевой скидкой
                discount_diff = abs(new_discount_needed - current_discount_from_api)

                if discount_diff <= 20:
                    # Скидка близка к целевой - меняем только скидку
                    action_type = "discount_change"
                    new_total_price_rounded = round(product.current_price_wb, 0)  # Цену не меняем
                    final_discount = new_discount_needed

                    self._log_table("РЕШЕНИЕ: ВОССТАНОВЛЕНИЕ ЦЕЛЕВОЙ СКИДКИ", {
                        "Действие": "Восстановление целевой скидки из БД",
                        "Причина": f"Текущая скидка ({current_discount_from_api:.1f}%) близка к целевой ({new_discount_needed:.1f}%)",
                        "Полная цена": f"{new_total_price_rounded:.0f}₽ (без изменений)",
                        "Старая скидка": f"{current_discount_from_api:.1f}%",
                        "Новая скидка": f"{final_discount:.1f}%",
                        "Изменение скидки": f"{final_discount - current_discount_from_api:+.1f}%",
                        "Новый priceWithDisc": f"{new_price_wd:.0f}₽",
                        "НДС в новой цене": f"{new_vat:.0f}₽",
                        "Расчетная полная цена": f"{needed_full_price:.0f}₽"
                    })
                else:
                    # Скидка сильно отличается - меняем и цену и скидку
                    action_type = "price_change"
                    new_total_price_rounded = round(needed_full_price, 0)
                    final_discount = new_discount_needed

                    self._log_table("РЕШЕНИЕ: ИЗМЕНЕНИЕ ЦЕНЫ ДЛЯ ЦЕЛЕВОЙ СКИДКИ", {
                        "Действие": "Изменение цены для целевой скидки из БД",
                        "Причина": f"Текущая скидка ({current_discount_from_api:.1f}%) сильно отличается от целевой ({new_discount_needed:.1f}%)",
                        "Старая полная цена": f"{product.current_price_wb:.0f}₽",
                        "Новая полная цена": f"{new_total_price_rounded:.0f}₽",
                        "Изменение цены": f"{new_total_price_rounded - product.current_price_wb:+.0f}₽",
                        "Скидка": f"{final_discount:.1f}% (целевая из БД)",
                        "Новый priceWithDisc": f"{new_price_wd:.0f}₽",
                        "НДС в новой цене": f"{new_vat:.0f}₽"
                    })

            elif product.current_price_wb > 0:
                # СЛУЧАЙ 2: НЕТ ЦЕЛЕВОЙ СКИДКИ В БД, НО ЕСТЬ ЦЕНА
                new_discount_needed = (1 - new_price_wd / product.current_price_wb) * 100
                self.logger.info(f"📊 Расчет новой скидки (без целевой скидки в БД):")
                self.logger.info(f"   Текущая полная цена: {product.current_price_wb:.0f}₽")
                self.logger.info(f"   Нужный priceWithDisc: {new_price_wd:.0f}₽")
                self.logger.info(f"   НДС в новой цене: {new_vat:.0f}₽")
                self.logger.info(f"   Нужная скидка: {new_discount_needed:.1f}%")
            else:
                # СЛУЧАЙ 3: НЕТ ЦЕНЫ В БД
                new_discount_needed = api_discount
                self.logger.warning(f"⚠️  Нет текущей цены в БД, используем текущую скидку: {api_discount:.1f}%")

            new_discount_needed = max(0.1, min(99.9, new_discount_needed))

            # Для случаев без целевой скидки в БД используем старую логику
            if discount_from_db == 0:
                discount_change = abs(new_discount_needed - api_discount)
                self.logger.info(f"📊 Изменение скидки:")
                self.logger.info(f"   Текущая скидка: {api_discount:.1f}%")
                self.logger.info(f"   Нужная скидка: {new_discount_needed:.1f}%")
                self.logger.info(f"   Изменение: {discount_change:.1f}%")

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
                        "Новый priceWithDisc": f"{new_price_wd:.0f}₽",
                        "НДС в новой цене": f"{new_vat:.0f}₽"
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
                        "Новый priceWithDisc": f"{new_price_wd:.0f}₽",
                        "НДС в новой цене": f"{new_vat:.0f}₽"
                    })

            # Расчет изменений
            price_change_absolute = new_total_price_rounded - product.current_price_wb
            price_change_percent = (
                    price_change_absolute / product.current_price_wb * 100) if product.current_price_wb > 0 else 0

            self._log_table("ИТОГОВЫЕ ЦЕНЫ С УЧЕТОМ НДС", {
                "Новый priceWithDisc": f"{new_price_wd:.2f}₽",
                "НДС в цене ({vat_percent}%)": f"{new_vat:.2f}₽",
                "Цена без НДС": f"{new_price_wd - new_vat:.2f}₽",
                "Скидка WB": f"{final_discount:.1f}%",
                "Цена без скидки": f"{new_total_price_rounded:.0f}₽",
                "Текущая цена WB": f"{product.current_price_wb:.0f}₽",
                "Изменение цены": f"{price_change_absolute:+.0f}₽",
                "Процент изменения": f"{price_change_percent:+.1f}%",
                "Действие": action_type,
                "Источник": source,
                "Целевая скидка из БД": f"{discount_from_db:.1f}%" if discount_from_db > 0 else "нет"
            })

            # Валидация с учетом НДС
            self.logger.info("✅ ПРОВЕРКА ВАЛИДАЦИИ С УЧЕТОМ НДС:")
            validation = self._validate_price_update_with_vat(
                vendor_code, product, new_price_wd, new_total_price_rounded, price_wd_diff, vat_percent
            )

            if validation:
                validation.discount = final_discount
                validation.logistics_cost = logistics_cost
                validation.action_type = action_type
                validation.source = source

                self._log_separator("РЕШЕНИЕ: НЕ ИЗМЕНЯТЬ ЦЕНУ", "!")
                self.logger.warning(f"❌ {validation.reason}")

                await self.db_logger.log_skip(
                    vendor_code=vendor_code,
                    nm_id=product.sku_wb,
                    reason=f"Валидация не пройдена: {validation.status.value}",
                    details={
                        "status": validation.status.value,
                        "reason": validation.reason,
                        "source": source,
                        "target_discount_from_db": discount_from_db,
                        "current_discount_from_wb": current_discount_from_api,
                        "final_discount": final_discount,
                        "vat_percent": vat_percent
                    }
                )
                return validation

            # Создание объекта обновления с НДС
            update = PriceUpdate(
                vendor_code=vendor_code,
                new_price_wb=new_total_price_rounded,
                new_real_price=round(new_price_wd, 2),
                old_price_wb=product.current_price_wb,
                old_real_price=product.current_real_price,
                profit_correction=abs(price_wd_diff),
                status=ProcessingStatus.SUCCESS,
                error_msg=f"Корректировка прибыли: {product.target_profit - current_profit:+.0f}",
                discount=final_discount,
                sku_wb=product.sku_wb,
                logistics_cost=logistics_cost,
                finished_price=finished_price,
                current_profit=current_profit,
                target_profit=product.target_profit,
                sales_count=len(valid_orders),
                spp_used=spp,
                purchase_price=product.purchase_price,
                target_forpay=target_forpay,
                action_type=action_type,
                source=source,
                vat_amount=new_vat,  # Добавляем НДС в объект
                vat_percent=vat_percent,
                price_without_vat=new_price_wd - new_vat
            )

            if valid_orders and update.status == ProcessingStatus.SUCCESS:
                self.logger.info(f"✅ Товар {vendor_code} успешно обработан")

                self.logger.info(f"📊 Сохранена маржа для товара {vendor_code}")

            self._log_separator("ИТОГОВОЕ РЕШЕНИЕ", "✅")
            self._log_table("РЕЗУЛЬТАТ РАСЧЕТА С НДС", {
                "Артикул": vendor_code,
                "nmId": product.sku_wb,
                "Источник": source,
                "Целевая скидка из БД": f"{discount_from_db:.1f}%" if discount_from_db > 0 else "нет",
                "Текущая скидка на WB": f"{current_discount_from_api:.1f}%",
                "Итоговая скидка": f"{final_discount:.1f}%",
                "Ставка НДС": f"{vat_percent}%",
                "Старая цена": f"{product.current_price_wb:.0f}₽",
                "Новая цена": f"{new_total_price_rounded:.0f}₽",
                "Изменение цены": f"{price_change_absolute:+.0f}₽ ({price_change_percent:+.1f}%)",
                "Новый priceWithDisc": f"{new_price_wd:.0f}₽",
                "НДС в цене": f"{new_vat:.0f}₽",
                "Цена без НДС": f"{new_price_wd - new_vat:.0f}₽",
                "Старая прибыль": f"{current_profit:.0f}₽",
                "Целевая прибыль": f"{product.target_profit:.0f}₽",
                "Изменение прибыли": f"{product.target_profit - current_profit:+.0f}₽",
                "Использовано заказов": len(valid_orders),
                "Тип действия": action_type,
                "Логистика (средневзвешенная)": f"{logistics_cost:.0f}₽",
                "Источник логистики": logistics_details['source'],
                "Тариф логистики": f"{self.weighted_delivery_base:.2f} + {self.weighted_delivery_liter:.2f} ₽/л",
                "Габариты": logistics_details.get('dimensions', 'не указаны'),
                "Объем": f"{logistics_details.get('volume_liters', 0):.2f} л",
                "Соотношение forPay/priceWithDisc": f"{Config.WB_FORPAY_TO_PRICEWITHDISC_RATIO:.3f}",
                "Складов в расчете": logistics_details.get('warehouses_count', len(self.warehouse_logistics)),
                "Заказов в расчете": logistics_details.get('total_orders', 0)
            })

            await self.db_logger.log_price_calculation(
                vendor_code=vendor_code,
                nm_id=product.sku_wb,
                old_price=product.current_price_wb,
                new_price=new_total_price_rounded,
                old_discount=current_discount_from_api,
                new_discount=final_discount,
                current_profit=current_profit,
                target_profit=product.target_profit,
                action_type=action_type,
                source=source,
                logistics_cost=logistics_cost,
                sales_count=len(valid_orders),
                details={
                    "old_price_wd": avg_price_wd,
                    "new_price_wd": new_price_wd,
                    "price_wd_diff": price_wd_diff,
                    "target_discount_from_db": discount_from_db,
                    "discount_change": final_discount - current_discount_from_api,
                    "forpay_ratio": Config.WB_FORPAY_TO_PRICEWITHDISC_RATIO,
                    "target_forpay": target_forpay,
                    "expected_profit": expected_profit,
                    "logistics_details": logistics_details,
                    "weighted_base": self.weighted_delivery_base,
                    "weighted_liter": self.weighted_delivery_liter,
                    "warehouses_count": len(self.warehouse_logistics),
                    "total_orders": logistics_details.get('total_orders', 0),
                    "vat_percent": vat_percent,
                    "vat_amount": new_vat,
                    "price_without_vat": new_price_wd - new_vat,
                    "volume": logistics_details.get('volume_liters', 0),
                    "dimensions": logistics_details.get('dimensions', 'не указаны')
                }
            )

            if update.status == ProcessingStatus.SUCCESS:
                # Обновляем статистику цикла
                self.stats['total_processed'] = self.stats.get('total_processed', 0) + 1
                self.stats['success'] = self.stats.get('success', 0) + 1

                if update.action_type == 'price_change':
                    self.stats['price_changes'] = self.stats.get('price_changes', 0) + 1
                elif update.action_type == 'discount_change':
                    self.stats['discount_changes'] = self.stats.get('discount_changes', 0) + 1

            return update

        except Exception as e:
            error_msg = f"❌ КРИТИЧЕСКАЯ ОШИБКА при обработке {vendor_code}: {e}"
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())

            await self.db_logger.log_error(
                vendor_code=vendor_code,
                nm_id=product.sku_wb if 'product' in locals() else 0,
                error=str(e),
                trace=traceback.format_exc(),
                details={
                    "source": source,
                    "target_discount_from_db": discount_from_db if 'discount_from_db' in locals() else 0,
                    "vat_percent": vat_percent if 'vat_percent' in locals() else Config.VAT_PERCENT
                }
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
                finished_price=0,
                source=source
            )

    def _validate_price_update_with_vat(self, vendor_code: str,
                                        product: ProductData,
                                        new_price_wd: float,
                                        new_total_price: float,
                                        profit_diff: float,
                                        vat_percent: float) -> Optional[PriceUpdate]:
        # Минимальная цена с учетом НДС
        min_price_without_vat = product.purchase_price * Config.MIN_MARGIN_FACTOR
        min_price_with_vat = min_price_without_vat * (1 + vat_percent / 100)

        self.logger.info("📋 ПРОВЕРКА КРИТЕРИЕВ С УЧЕТОМ НДС:")

        if new_price_wd < min_price_with_vat:
            self.logger.error(f"❌ ЦЕНА НИЖЕ МИНИМАЛЬНОЙ С НДС: {new_price_wd:.0f}₽ < {min_price_with_vat:.0f}₽")
            self.logger.error(
                f"   Без НДС: {new_price_wd * (100 / (100 + vat_percent)):.0f}₽ < {min_price_without_vat:.0f}₽")
            return PriceUpdate(
                vendor_code=vendor_code,
                new_price_wb=0,
                new_real_price=new_price_wd,
                old_price_wb=product.current_price_wb,
                old_real_price=product.current_real_price,
                profit_correction=profit_diff,
                status=ProcessingStatus.SKIPPED_MIN_PRICE,
                error_msg=f"Цена ниже минимальной с учетом НДС: {new_price_wd:.0f} < {min_price_with_vat:.0f}",
                sku_wb=product.sku_wb,
                finished_price=0,
                current_profit=0,
                target_profit=product.target_profit,
                sales_count=0,
                spp_used=0,
                purchase_price=product.purchase_price,
                source="validation_with_vat"
            )
        else:
            self.logger.info(f"✅ Минимальная цена с НДС: ОК ({new_price_wd:.0f}₽ ≥ {min_price_with_vat:.0f}₽)")

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
                purchase_price=product.purchase_price,
                source="validation_with_vat"
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
                    purchase_price=product.purchase_price,
                    source="validation_with_vat"
                )
            else:
                self.logger.info(
                    f"✅ Максимальный процент изменения: ОК ({price_change_percent:.1f}% ≤ {Config.MAX_PRICE_CHANGE_PERCENT}%)")

        self.logger.info("✅ ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ С УЧЕТОМ НДС")
        return None

    async def save_nm_id_to_db(self, vendor_code: str, nm_id: int) -> bool:
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"""
                        UPDATE {Config.PPODUCT_TABLE} 
                        SET sku_wb = %s
                        WHERE model_wb = %s
                    """, (nm_id, vendor_code))

                    self.logger.info(f"💾 nmId {nm_id} сохранен в БД для {vendor_code}")
                    return True

        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения nmId для {vendor_code}: {e}")
            return False

    async def save_price_update(self, update: PriceUpdate):
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    current_time = datetime.now(pytz.timezone('Europe/Moscow'))

                    expected_profit_after_change = update.target_profit

                    await cur.execute(f"""
                        UPDATE {Config.PPODUCT_TABLE}
                        SET price_wb = %s, 
                            wb_real_price = %s,
                            sku_wb = %s,
                            last_price_update = %s
                        WHERE model_wb = %s
                    """, (
                        update.new_price_wb,
                        update.new_real_price,
                        update.sku_wb,
                        current_time,
                        update.vendor_code
                    ))

                    await cur.execute(f"""
                        INSERT INTO {Config.WB_PRICE_HISTORY_TABLE} 
                        (product_id, vendor_code, old_price_wb, new_price_wb,
                         old_real_price, new_real_price, profit_correction, avg_finished_price,
                         discount, change_reason, status, created_at,
                         current_margin, target_margin, sales_count, spp_used,
                         purchase_price, logistics_cost, source)
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
                            %s,
                            %s
                        FROM {Config.PPODUCT_TABLE} p
                        WHERE p.model_wb = %s
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
                        update.source,
                        update.vendor_code,
                    ))

                    self.stats['prices_updated'] += 1
                    if update.action_type == "discount_change":
                        self.stats['discounts_changed'] += 1
                    else:
                        self.stats['prices_changed'] += 1

                    # Статистика по источникам
                    if update.source:
                        self.stats[f'source_{update.source}'] = self.stats.get(f'source_{update.source}', 0) + 1

                    profit_before = update.current_profit
                    profit_after = expected_profit_after_change
                    profit_diff = profit_after - profit_before

                    self._log_separator("СОХРАНЕНИЕ ЦЕНЫ В БД", "💾")
                    self._log_table("СОХРАНЕННЫЕ ДАННЫЕ", {
                        "Артикул": update.vendor_code,
                        "nmId": update.sku_wb,
                        "Источник": update.source,
                        "Старая цена": f"{update.old_price_wb:.0f}₽",
                        "Новая цена": f"{update.new_price_wb:.0f}₽",
                        "Изменение": f"{update.new_price_wb - update.old_price_wb:+.0f}₽",
                        "Старая скидка": f"{update.discount or 0:.1f}%",
                        "Тип действия": update.action_type,
                        "Прибыль до": f"{profit_before:.0f}₽",
                        "Ожидаемая прибыль после": f"{profit_after:.0f}₽",
                        "Изменение прибыли": f"{profit_diff:+.0f}₽",
                        "Заказов использовано": update.sales_count,
                        "Логистика (средневзвешенная)": f"{update.logistics_cost:.0f}₽"
                    })

                    await self.db_logger.log(
                        level="SUCCESS",
                        message=f"💾 Цена сохранена в БД",
                        vendor_code=update.vendor_code,
                        mp="wb",
                        nm_id=update.sku_wb,
                        details={
                            "event": "price_saved",
                            "old_price": update.old_price_wb,
                            "new_price": update.new_price_wb,
                            "price_change": update.new_price_wb - update.old_price_wb,
                            "discount": update.discount,
                            "action_type": update.action_type,
                            "profit_before": profit_before,
                            "profit_after": profit_after,
                            "profit_diff": profit_diff,
                            "target_profit": update.target_profit,
                            "sales_used": update.sales_count,
                            "logistics_cost": update.logistics_cost,
                            "purchase_price": update.purchase_price,
                            "source": update.source
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

    async def save_cycle_start(self):
        """Сохраняет информацию о начале цикла"""
        try:
            moscow_tz = pytz.timezone('Europe/Moscow')
            now_moscow = datetime.now(moscow_tz)

            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # Рассчитываем время следующего цикла
                    next_cycle = now_moscow + timedelta(seconds=Config.WB_INTERVAL)

                    # ✅ Всегда создаем новую запись цикла
                    await cursor.execute(f"""
                        INSERT INTO {Config.WB_CYCLE_INFO_TABLE} 
                        (start_time, status, next_cycle_time)
                        VALUES (%s, 'running', %s)
                    """, (
                        now_moscow.replace(tzinfo=None),
                        next_cycle.replace(tzinfo=None)
                    ))

                    # ✅ Сохраняем ID созданной записи
                    self.current_cycle_id = cursor.lastrowid
                    self.logger.info(f"✅ Создан новый цикл #{self.current_cycle_id}")

                    # ✅ Проверяем, есть ли запись в таблице статуса
                    await cursor.execute(f"SELECT COUNT(*) FROM {Config.WB_SYSTEM_STATUS_TABLE} WHERE id = 1")
                    count = (await cursor.fetchone())[0]

                    if count == 0:
                        # Первый запуск - создаем запись
                        await cursor.execute(f"""
                            INSERT INTO {Config.WB_SYSTEM_STATUS_TABLE} 
                            (id, is_running, last_start, current_cycle_id, next_cycle, updated_at)
                            VALUES (1, TRUE, %s, %s, %s, %s)
                        """, (
                            now_moscow.replace(tzinfo=None),
                            self.current_cycle_id,
                            next_cycle.replace(tzinfo=None),  # ✅ Устанавливаем время следующего цикла
                            now_moscow.replace(tzinfo=None)
                        ))
                    else:
                        # Обновляем существующую запись - is_running остается TRUE
                        await cursor.execute(f"""
                            UPDATE {Config.WB_SYSTEM_STATUS_TABLE}
                            SET 
                                is_running = TRUE,
                                last_start = %s,
                                current_cycle_id = %s,
                                next_cycle = %s,  # ✅ Обновляем время следующего цикла
                                updated_at = %s
                            WHERE id = 1
                        """, (
                            now_moscow.replace(tzinfo=None),
                            self.current_cycle_id,
                            next_cycle.replace(tzinfo=None),  # ✅ Вот ключевое изменение!
                            now_moscow.replace(tzinfo=None)
                        ))

                    self.logger.info(
                        f"✅ Цикл #{self.current_cycle_id} начат в {now_moscow.strftime('%Y-%m-%d %H:%M:%S')} МСК")
                    self.logger.info(f"📅 Следующий цикл запланирован на: {next_cycle.strftime('%Y-%m-%d %H:%M:%S')}")

        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения начала цикла: {e}")

    async def save_cycle_stats(self):
        """Сохраняет статистику цикла в БД"""
        try:
            moscow_tz = pytz.timezone('Europe/Moscow')
            now_moscow = datetime.now(moscow_tz)
            cycle_end = now_moscow

            # Приводим cycle_start_time к московскому времени
            if hasattr(self, 'cycle_start_time') and self.cycle_start_time:
                if self.cycle_start_time.tzinfo is None:
                    start_moscow = moscow_tz.localize(self.cycle_start_time)
                else:
                    start_moscow = self.cycle_start_time.astimezone(moscow_tz)

                duration = (cycle_end - start_moscow).total_seconds()
            else:
                start_moscow = now_moscow
                duration = 0

            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # Рассчитываем время следующего цикла
                    next_cycle = now_moscow + timedelta(seconds=Config.WB_INTERVAL)

                    # ✅ ИСПРАВЛЕНО: Обновляем по ID цикла, а не по start_time
                    if self.current_cycle_id:
                        await cursor.execute(f"""
                            UPDATE {Config.WB_CYCLE_INFO_TABLE}
                            SET 
                                end_time = %s,
                                duration_sec = %s,
                                status = 'completed',
                                products_processed = %s,
                                successful_updates = %s,
                                price_changes = %s,
                                discount_changes = %s,
                                skipped_no_data = %s,
                                skipped_min_price = %s,
                                skipped_min_change = %s,
                                errors = %s,
                                promotions_scheduled = %s,
                                next_cycle_time = %s
                            WHERE id = %s
                        """, (
                            cycle_end.replace(tzinfo=None),
                            duration,
                            self.stats.get('total_processed', 0),
                            self.stats.get('success', 0),
                            self.stats.get('price_changes', 0),
                            self.stats.get('discount_changes', 0),
                            self.stats.get('skipped_no_data', 0),
                            self.stats.get('skipped_min_price', 0),
                            self.stats.get('skipped_min_change', 0),
                            self.stats.get('error', 0),
                            self.stats.get('promotions_scheduled', 0),
                            next_cycle.replace(tzinfo=None),  # ✅ Сохраняем время следующего цикла
                            self.current_cycle_id  # ✅ Используем ID
                        ))

                        affected = cursor.rowcount
                        self.logger.info(f"🔄 Обновлено записей: {affected} (cycle_id: {self.current_cycle_id})")
                    else:
                        self.logger.error("❌ Нет current_cycle_id для обновления")

                    # ✅ Обновляем статус системы - ОБЯЗАТЕЛЬНО меняем next_cycle!
                    # Проверяем, есть ли запись
                    await cursor.execute(f"SELECT COUNT(*) FROM {Config.WB_SYSTEM_STATUS_TABLE} WHERE id = 1")
                    count = (await cursor.fetchone())[0]

                    if count == 0:
                        # Создаем запись, если её нет
                        await cursor.execute(f"""
                            INSERT INTO {Config.WB_SYSTEM_STATUS_TABLE} 
                            (id, is_running, next_cycle, updated_at)
                            VALUES (1, FALSE, %s, %s)
                        """, (
                            next_cycle.replace(tzinfo=None),  # ✅ Устанавливаем время следующего цикла
                            now_moscow.replace(tzinfo=None)
                        ))
                    else:
                        # Обновляем существующую запись - НЕ МЕНЯЕМ is_running!
                        await cursor.execute(f"""
                            UPDATE {Config.WB_SYSTEM_STATUS_TABLE}
                            SET 
                                next_cycle = %s,  # ✅ Вот ключевое изменение!
                                updated_at = %s
                            WHERE id = 1
                        """, (
                            next_cycle.replace(tzinfo=None),  # ✅ Обновляем время следующего цикла
                            now_moscow.replace(tzinfo=None)
                        ))

                    self.logger.info(f"✅ Статистика цикла #{self.current_cycle_id} сохранена в БД")
                    self.logger.info(f"📅 Следующий цикл запланирован на: {next_cycle.strftime('%Y-%m-%d %H:%M:%S')}")

        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения статистики цикла: {e}")

    def sync_update_system_running_status(self, is_running: bool):
        """Синхронно обновляет статус работы системы в БД"""
        try:
            moscow_tz = pytz.timezone('Europe/Moscow')
            now_moscow = datetime.now(moscow_tz)

            conn = pymysql.connect(
                host=Config.DB_HOST,
                user=Config.DB_USER,
                password=Config.DB_PASSWORD,
                db=Config.DB_NAME,
                port=Config.DB_PORT,
                charset='utf8mb4'
            )

            with conn.cursor() as cursor:
                # Проверяем, есть ли запись
                cursor.execute(f"SELECT COUNT(*) FROM {Config.WB_SYSTEM_STATUS_TABLE} WHERE id = 1")
                count = cursor.fetchone()[0]

                if count == 0:
                    # Создаем запись
                    cursor.execute(f"""
                        INSERT INTO {Config.WB_SYSTEM_STATUS_TABLE} 
                        (id, is_running, {'last_start' if is_running else 'last_stop'}, updated_at)
                        VALUES (1, %s, %s, %s)
                    """, (
                        1 if is_running else 0,
                        now_moscow.replace(tzinfo=None),
                        now_moscow.replace(tzinfo=None)
                    ))
                else:
                    # Обновляем существующую запись
                    if is_running:
                        cursor.execute(f"""
                            UPDATE {Config.WB_SYSTEM_STATUS_TABLE}
                            SET 
                                is_running = %s,
                                last_start = %s,
                                updated_at = %s
                            WHERE id = 1
                        """, (
                            1,
                            now_moscow.replace(tzinfo=None),
                            now_moscow.replace(tzinfo=None)
                        ))
                    else:
                        cursor.execute(f"""
                            UPDATE {Config.WB_SYSTEM_STATUS_TABLE}
                            SET 
                                is_running = %s,
                                last_stop = %s,
                                updated_at = %s
                            WHERE id = 1
                        """, (
                            0,
                            now_moscow.replace(tzinfo=None),
                            now_moscow.replace(tzinfo=None)
                        ))

                conn.commit()

            conn.close()
            print(f"✅ Статус системы синхронно обновлен: {'РАБОТАЕТ' if is_running else 'ОСТАНОВЛЕНА'}")

        except Exception as e:
            print(f"❌ Ошибка синхронного обновления статуса: {e}")

    async def collect_daily_profit_stats(self, orders_by_nm_id: Dict,
                                         product_map: Dict[str, ProductData],
                                         nm_id_to_vendor_code: Dict[int, str]):
        """
        Сбор дневной статистики прибыли на основе заказов за 24 часа
        Прибыль = сумма target_profit из таблицы продуктов по всем заказам
        """
        self._log_separator("СБОР ДНЕВНОЙ СТАТИСТИКИ ПРИБЫЛИ", "📊")

        moscow_tz = pytz.timezone('Europe/Moscow')
        now = datetime.now(moscow_tz)
        sale_date = now.date()

        # Структура для агрегации
        daily_stats = {
            'total_profit': 0.0,
            'total_revenue': 0.0,
            'products_updated': 0,
            'orders_count': 0,
            'vendor_codes': set()
        }

        # Проходим по всем заказам
        for nm_id, orders in orders_by_nm_id.items():
            # Определяем vendor_code
            vendor_code = nm_id_to_vendor_code.get(nm_id)
            if not vendor_code or vendor_code not in product_map:
                continue

            product = product_map[vendor_code]

            for order in orders:
                if order.is_cancel or order.price_with_desc <= 0:
                    continue

                # ===== БЕРЕМ TARGET_PROFIT ИЗ ПРОДУКТА =====
                target_profit = product.target_profit

                # Суммируем
                daily_stats['total_profit'] += target_profit
                daily_stats['total_revenue'] += order.price_with_desc
                daily_stats['orders_count'] += 1
                daily_stats['vendor_codes'].add(vendor_code)

        daily_stats['products_updated'] = len(daily_stats['vendor_codes'])

        # ===== СОХРАНЕНИЕ В БД =====
        await self._save_daily_stats_to_db(daily_stats, sale_date)

        # ===== ЛОГИРОВАНИЕ =====
        self._log_daily_stats_summary(daily_stats)

        return daily_stats

    async def _save_daily_stats_to_db(self, daily_stats: Dict, sale_date: date):
        """
        Сохранение дневной статистики в БД
        ТАБЛИЦА: wb_daily_stats (id, date, total_profit, total_revenue, products_updated, avg_margin)
        """
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # Расчет средней маржинальности в процентах
                    avg_margin = (
                        (daily_stats['total_profit'] / daily_stats['total_revenue'] * 100)
                        if daily_stats['total_revenue'] > 0 else 0
                    )

                    # INSERT ... ON DUPLICATE KEY UPDATE
                    await cursor.execute(f"""
                        INSERT INTO {Config.WB_DAILY_STATS_TABLE} 
                        (date, total_profit, total_revenue, products_updated, avg_margin)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            total_profit = VALUES(total_profit),
                            total_revenue = VALUES(total_revenue),
                            products_updated = VALUES(products_updated),
                            avg_margin = VALUES(avg_margin)
                    """, (
                        sale_date,
                        round(daily_stats['total_profit'], 2),
                        round(daily_stats['total_revenue'], 2),
                        daily_stats['products_updated'],
                        round(avg_margin, 2)
                    ))

                    await conn.commit()

                    self.logger.info(f"✅ Дневная статистика сохранена за {sale_date}")

        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения дневной статистики: {e}")
            await self.db_logger.log_error(
                vendor_code=None,
                nm_id=None,
                error=f"Ошибка сохранения дневной статистики: {e}",
                details={"date": str(sale_date)}
            )

    def _log_daily_stats_summary(self, daily_stats: Dict):
        """
        Логирование итогов сбора дневной статистики
        """
        self._log_separator("ИТОГИ ДНЕВНОЙ СТАТИСТИКИ", "📈")
        self.logger.info(f"📊 ОБЩАЯ СТАТИСТИКА:")
        self.logger.info(f"   • Всего товаров: {daily_stats['products_updated']}")
        self.logger.info(f"   • Всего заказов: {daily_stats['orders_count']}")
        self.logger.info(f"   • Общая выручка: {daily_stats['total_revenue']:,.2f}₽")
        self.logger.info(f"   • ОБЩАЯ ПРИБЫЛЬ (target_profit): {daily_stats['total_profit']:,.2f}₽")

        if daily_stats['total_revenue'] > 0:
            overall_margin = (daily_stats['total_profit'] / daily_stats['total_revenue']) * 100
            self.logger.info(f"   • Средняя маржинальность: {overall_margin:.2f}%")

    async def should_collect_daily_stats(self) -> bool:
        """
        Проверяет, нужно ли запускать сбор дневной статистики сегодня
        Запускается 1 раз в 24 часа в указанное время
        """
        if not Config.DAILY_STATS_ENABLED:
            return False

        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:

                    # Проверяем, есть ли запись за сегодня
                    moscow_tz = pytz.timezone('Europe/Moscow')
                    now = datetime.now(moscow_tz)
                    today = now.date()

                    await cursor.execute(f"""
                        SELECT id, date FROM {Config.WB_DAILY_STATS_TABLE}
                        WHERE date = %s
                    """, (today,))

                    row = await cursor.fetchone()

                    if row:
                        self.logger.info(f"ℹ️  Дневная статистика за {today} уже собрана")
                        return False
                    else:
                        # Проверяем время (запускаем в Config.DAILY_STATS_HOUR)
                        current_hour = now.hour

                        # Можно запускать если наступил нужный час
                        if current_hour >= Config.DAILY_STATS_HOUR:
                            self.logger.info(f"✅ Время для сбора статистики (час: {current_hour})")
                            return True
                        else:
                            self.logger.info(f"⏰ Ждем {Config.DAILY_STATS_HOUR} часов (сейчас: {current_hour})")
                            return False

        except Exception as e:
            self.logger.error(f"❌ Ошибка проверки дневной статистики: {e}")
            return False

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

        self.logger.info("📋 Отправляемые данные:")
        for item in data[:5]:
            update = next((u for u in updates if u.sku_wb == item['nmID']), None)
            action_type = update.action_type if update else "unknown"
            source = update.source if update else "unknown"
            self.logger.info(f"   • nmId: {item['nmID']}, цена: {item['price']}₽, "
                             f"скидка: {item['discount']}%, действие: {action_type}, источник: {source}")
        if len(data) > 5:
            self.logger.info(f"   ... и еще {len(data) - 5} товаров")

        try:
            result = await self.promo_client.upload_prices_to_wb(data)

            if result and not result.get('error'):
                task_id = result.get('data', {}).get('id')

                self._log_separator("УСПЕШНАЯ ОТПРАВКА", "✅")
                self.logger.info(f"✅ Цены успешно отправлены на Wildberries")
                self.logger.info(f"📝 ID задачи: {task_id}")
                self.logger.info(f"📊 Количество товаров: {len(data)}")

                self.stats['prices_uploaded_to_wb'] = len(data)

                await self.db_logger.log(
                    level="SUCCESS",
                    message=f"🚀 Цены отправлены на Wildberries",
                    mp="wb",
                    details={
                        "event": "prices_uploaded",
                        "count": len(data),
                        "task_id": task_id,
                        "cycle_id": self.current_cycle,
                        "price_changes": len([u for u in updates if u.action_type == 'price_change']),
                        "discount_changes": len([u for u in updates if u.action_type == 'discount_change']),
                        "sample_nm_ids": [u.sku_wb for u in updates[:5]]
                    }
                )
            else:
                error_msg = f"❌ Ошибка отправки цен на WB"
                self.logger.error(error_msg)

                await self.db_logger.log_error(
                    vendor_code=None,
                    nm_id=None,
                    error=f"Ошибка отправки цен на WB: {result}",
                    details={
                        "cycle_id": self.current_cycle,
                        "data_count": len(data)
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
                    vendor_code, orders, product, source = await asyncio.wait_for(
                        self.queue.get(),
                        timeout=1.0
                    )

                    self.logger.info(f"👷 Воркер #{worker_id} обрабатывает: {vendor_code} (источник: {source})")

                    update = await self.process_product_new_logic(vendor_code, orders, product, source)

                    if update.status == ProcessingStatus.SUCCESS:
                        await self.save_price_update(update)
                        self.successful_updates.append(update)
                        self.logger.info(
                            f"👷 Воркер #{worker_id}: {vendor_code} - УСПЕХ ({update.action_type}, источник: {source})")
                    else:
                        self.logger.info(
                            f"👷 Воркер #{worker_id}: {vendor_code} - {update.status.value} (источник: {source})")

                    self.stats[update.status.value] += 1
                    self.queue.task_done()

                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    self.logger.error(f"❌ Ошибка в воркере {worker_id}: {e}")
                    self.stats['error'] = self.stats.get('error', 0) + 1
                    await self.db_logger.log_error(
                        vendor_code=vendor_code if 'vendor_code' in locals() else None,
                        nm_id=product.sku_wb if 'product' in locals() else None,
                        error=f"Ошибка в воркере #{worker_id}: {e}",
                        details={
                            "worker_id": worker_id,
                            "cycle_id": self.current_cycle
                        }
                    )

        except asyncio.CancelledError:
            self.logger.info(f"👷 Воркер {worker_id} остановлен")

    async def run_cycle(self):
        self._log_separator(f"ЦИКЛ #{self.current_cycle + 1}", "🔄")
        cycle_start = datetime.now()
        self.cycle_start_time = cycle_start
        self.current_cycle += 1

        await self.save_cycle_start()
        await self.db_logger.set_cycle_id(self.current_cycle)

        await self.db_logger.log(
            level="INFO",
            message=f"Начало цикла #{self.current_cycle}",
            details={"cycle_id": self.current_cycle}
        )

        self.stats = defaultdict(int)
        self.successful_updates = []
        self.is_running = True

        try:
            # ========== 1. ПОЛУЧАЕМ ЗАКАЗЫ ==========
            orders_data = await self.fetch_wb_orders()
            orders_by_nm_id = orders_data['by_nm_id']
            nm_id_to_vendor_code = orders_data['nm_id_to_vendor_code']

            # ========== 2. ЗАГРУЖАЕМ ТОВАРЫ ==========
            products_data = await self.fetch_all_products()
            product_map = products_data['by_vendor_code']
            nm_id_to_product = products_data['by_nm_id']

            # ========== 3. СОБИРАЕМ ЗАДАЧИ ДЛЯ ОБРАБОТКИ ==========
            tasks_to_process = {}
            processed_vendor_codes = set()

            # Поиск по nmId
            for nm_id, orders in orders_by_nm_id.items():
                if nm_id in nm_id_to_product:
                    product = nm_id_to_product[nm_id]
                    if product.vendor_code not in processed_vendor_codes:
                        tasks_to_process[product.vendor_code] = {
                            'orders': orders,
                            'product': product,
                            'source': 'nm_id_match',
                            'nm_id': nm_id
                        }
                        processed_vendor_codes.add(product.vendor_code)

            # Поиск по артикулу (резерв)
            for nm_id, orders in orders_by_nm_id.items():
                if nm_id in processed_vendor_codes:
                    continue
                vendor_code = nm_id_to_vendor_code.get(nm_id)
                if vendor_code and vendor_code in product_map:
                    product = product_map[vendor_code]
                    if product.vendor_code not in processed_vendor_codes:
                        tasks_to_process[vendor_code] = {
                            'orders': orders,
                            'product': product,
                            'source': 'vendor_code_match',
                            'nm_id': nm_id
                        }
                        processed_vendor_codes.add(product.vendor_code)

            if not tasks_to_process:
                self.logger.warning("⚠️ Нет задач для обработки")
                return

            # ========== 4. ОБРАБАТЫВАЕМ ТОВАРЫ ==========
            for vendor_code, task_data in tasks_to_process.items():
                await self.queue.put((
                    vendor_code,
                    task_data['orders'],
                    task_data['product'],
                    task_data['source']
                ))

            # Запускаем воркеров
            workers = []
            for i in range(Config.WORKERS_COUNT):
                worker_task = asyncio.create_task(self.worker(i))
                workers.append(worker_task)

            await self.queue.join()

            for worker_task in workers:
                worker_task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

            # ========== 5. ОБНОВЛЯЕМ ФЛАГИ ПРОДАЖ ==========
            successful_vendor_codes = {update.vendor_code for update in self.successful_updates}
            await self.update_sales_flags(successful_vendor_codes)

            # ========== 6. ОТПРАВЛЯЕМ ЦЕНЫ НА WB (ЕСЛИ ВКЛЮЧЕНО) ==========
            if Config.LOAD_PRICE_TO_WB and self.successful_updates:
                await self.upload_prices_to_wb(self.successful_updates)

            if Config.PROMOTIONS_ENABLED:
                if hasattr(self, 'promotion_manager') and self.promotion_manager:

                    # 7.1 ПОЛУЧАЕМ АКЦИИ ИЗ WB API
                    self._log_separator("ПОЛУЧЕНИЕ АКЦИЙ WB", "🎯")
                    await self.promotion_manager.sync_promotions_from_wb()

                    # 👇 ВАЖНО: берём ТОЛЬКО товары с продажами
                    products_with_sales = []
                    for product in product_map.values():
                        if hasattr(product, 'has_sales_last_period_wb') and product.has_sales_last_period_wb:
                            products_with_sales.append(product)

                    self.logger.info(f"📊 Товаров с продажами для акций: {len(products_with_sales)} из {len(product_map)}")
                    await self.promotion_manager.check_and_create_ramp_plans(products_with_sales)

                    stats = await self.promotion_manager.get_promotion_stats()
                    self.logger.info(f"📊 Статистика акций: {stats}")
                    self.stats['promotions_scheduled_wb'] = stats.get('pending_products', 0) + stats.get('ramping_products',
                                                                                                         0)

                    # 7.2 ПОВЫШАЕМ ЦЕНЫ (каждый 24-й цикл)
                    if self.current_cycle % 24 == 0:
                        self.logger.info(f"📅 Цикл #{self.current_cycle} кратен 24 - выполняем дневное повышение")
                        updated, locked = await self.promotion_manager.execute_daily_price_ramp()
                        if updated > 0 or locked > 0:
                            self.logger.info(f"📈 Акции: повышено {updated}, заблокировано {locked}")
                    else:
                        self.logger.debug(f"⏰ Цикл #{self.current_cycle} - пропускаем повышение")

                    # 7.3 БЛОКИРОВКИ ПРОВЕРЯЕМ КАЖДЫЙ ЦИКЛ
                    locked = await self.promotion_manager.update_promotion_locks()
                    self.stats['products_in_promotion_wb'] = locked
            # ========== 8. СБОР СТАТИСТИКИ ==========
            cycle_end = datetime.now()
            duration = (cycle_end - cycle_start).total_seconds()

            # Статистика по источникам
            source_stats = {}
            for update in self.successful_updates:
                if update.source:
                    source_stats[update.source] = source_stats.get(update.source, 0) + 1

            price_changes = len([u for u in self.successful_updates if u.action_type == 'price_change'])
            discount_changes = len([u for u in self.successful_updates if u.action_type == 'discount_change'])

            # Логируем результаты
            self._log_separator("СТАТИСТИКА ЦИКЛА", "📊")

            stats_table = {
                "Время выполнения": f"{duration:.1f} сек",
                "Всего задач": len(tasks_to_process),
                "✅ Успешно": self.stats.get('success', 0),
                "📈 Изменений цены": price_changes,
                "🎯 Изменений скидки": discount_changes,
                "✅ Обновлено в БД": self.stats.get('prices_updated', 0),
                "⚠️ Пропущено": self.stats.get('skipped_no_data', 0) + self.stats.get('skipped_min_price', 0),
                "❌ Ошибок": self.stats.get('error', 0),
                "🔒 В акциях": self.stats.get('products_in_promotion_wb', 0)
            }

            if self.stats.get('auto_promotions_scheduled', 0) > 0:
                stats_table["🤖 Запланировано акций"] = self.stats.get('auto_promotions_scheduled', 0)

            self._log_table("РЕЗУЛЬТАТЫ ОБРАБОТКИ", stats_table)

            if source_stats:
                self._log_table("СТАТИСТИКА ПО ИСТОЧНИКАМ", source_stats)

            # ========== 9. СОХРАНЯЕМ СТАТИСТИКУ ЦИКЛА ==========
            await self.save_cycle_stats()

            # ========== 10. ДНЕВНАЯ СТАТИСТИКА ==========
            try:
                should_collect = await self.should_collect_daily_stats()
                if should_collect:
                    self._log_separator("СБОР ДНЕВНОЙ СТАТИСТИКИ", "📅")
                    await self.collect_daily_profit_stats(
                        orders_by_nm_id=orders_by_nm_id,
                        product_map=product_map,
                        nm_id_to_vendor_code=nm_id_to_vendor_code
                    )
            except Exception as e:
                self.logger.error(f"❌ Ошибка сбора дневной статистики: {e}")

            # Логируем завершение цикла
            await self.db_logger.log(
                level="INFO",
                message=f"Цикл #{self.current_cycle} завершен",
                details={
                    "duration_sec": duration,
                    "success": self.stats.get('success', 0),
                    "price_changes": price_changes,
                    "discount_changes": discount_changes,
                    "products_in_promotion": self.stats.get('products_in_promotion_wb', 0)
                }
            )

        except Exception as e:
            error_msg = f"❌ КРИТИЧЕСКАЯ ОШИБКА в цикле: {e}"
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())

            await self.db_logger.log_error(
                vendor_code=None,
                nm_id=None,
                error=str(e),
                trace=traceback.format_exc(),
                details={"cycle_id": self.current_cycle, "phase": "run_cycle"}
            )

    async def run(self):
        self.is_running = True
        cycle_count = 0

        self.sync_update_system_running_status(True)

        try:
            while self.is_running:
                cycle_count += 1

                try:
                    await self.run_cycle()

                    if not self.is_running:
                        break

                    hours = Config.WB_INTERVAL // 3600
                    minutes = (Config.WB_INTERVAL % 3600) // 60

                    self._log_separator("ОЖИДАНИЕ СЛЕДУЮЩЕГО ЦИКЛА", "⏰")
                    self.logger.info(f"🕒 Следующий цикл через: {hours}ч {minutes}мин")
                    self.logger.info(f"📅 Время следующего запуска: "
                                     f"{(datetime.now() + timedelta(seconds=Config.WB_INTERVAL)).strftime('%H:%M:%S')}")

                    # Разбиваем сон на маленькие интервалы
                    sleep_interval = 60
                    slept = 0
                    while slept < Config.WB_INTERVAL and self.is_running:
                        await asyncio.sleep(min(sleep_interval, Config.WB_INTERVAL - slept))
                        slept += sleep_interval

                except KeyboardInterrupt:
                    self._log_separator("ОСТАНОВКА ПОЛЬЗОВАТЕЛЕМ", "🛑")
                    self.logger.info("Пользователь запросил остановку")
                    break
                except Exception as e:
                    error_msg = f"❌ ФАТАЛЬНАЯ ОШИБКА: {e}"
                    self.logger.error(error_msg)
                    self.logger.error(traceback.format_exc())

                    await self.db_logger.log_error(
                        vendor_code=None,
                        nm_id=None,
                        error=f"ФАТАЛЬНАЯ ОШИБКА: {e}",
                        trace=traceback.format_exc(),
                        details={
                            "cycle_id": self.current_cycle,
                            "phase": "main_loop"
                        }
                    )

                    wait_time = min(300 * (2 ** (cycle_count % 5)), 3600)
                    self.logger.info(f"⏸️  Пауза {wait_time} сек перед повторной попыткой...")

                    sleep_interval = 60
                    slept = 0
                    while slept < wait_time and self.is_running:
                        await asyncio.sleep(min(sleep_interval, wait_time - slept))
                        slept += sleep_interval
        finally:
            # ✅ Гарантированно обновляем статус при любом выходе из цикла
            self.logger.info("🔄 Завершение работы программы...")
            await self.cleanup()

    def _handle_shutdown(self, signum, frame):
        signal_name = {signal.SIGTERM: 'SIGTERM', signal.SIGINT: 'SIGINT'}.get(signum, str(signum))
        self.logger.info(f"🚨 Получен сигнал {signal_name}, останавливаемся...")
        self.sync_update_system_running_status(False)
        self.is_running = False

    async def cleanup(self):
        """Асинхронная очистка ресурсов"""
        self._log_separator("ЗАВЕРШЕНИЕ РАБОТЫ", "🔚")
        self.logger.info("Очистка ресурсов...")

        await self.db_logger.log(
            level="INFO",
            message="🔚 Завершение работы системы",
            vendor_code=None,
            mp="wb",
            details={
                "event": "shutdown",
                "cycle_id": self.current_cycle,
                "final_stats": dict(self.stats) if hasattr(self, 'stats') else {}
            }
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