#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Основной класс системы расчёта и обновления цен для Ozon
"""

import asyncio
import logging
import logging.handlers
import sys
import traceback
import signal
import atexit
import statistics
from collections import defaultdict
from datetime import datetime, timedelta, date
from typing import Dict, List, Tuple, Optional
import pymysql

import aiomysql
import pytz

from OZON_integration.structures.dataclass import ProductData, OzonOrderData, PriceUpdate, ProcessingStatus
from OZON_integration.api.ozon_api_client import OzonAPI
from OZON_integration.api.cdek_api_client import CdekAPI
from OZON_integration.core.database_logger import DatabaseLogger
from config import Config
from WB_integration.utils.logging_utils import setup_logger, log_separator, log_table
from OZON_integration.api.promotions_client import OzonPromotionManager


class OzonPriceUpdater:
    """Основной класс системы расчёта и обновления цен для Ozon"""

    def __init__(self):
        self.logger = setup_logger('ozon_price_updater')
        self.db_pool = None
        self.session = None
        self.db_logger = None
        self.is_running = False
        self.queue = None
        self.stats = defaultdict(int)
        self.successful_updates = []
        self.current_cycle = 0
        self.cycle_start_time = None
        self.current_cycle_id = None
        self.promotion_manager = None

        # Регистрация обработчиков сигналов
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)
        atexit.register(self.cleanup)

    def _log_separator(self, title: str = "", char: str = "=", length: int = 80):
        """Логирование разделителя через общую утилиту"""
        log_separator(self.logger, title, char, length)

    def _log_table(self, title: str, data: dict):
        """Логирование таблицы через общую утилиту"""
        log_table(self.logger, title, data)

    async def initialize(self):
        """Инициализация системы"""
        self._log_separator("НАЧАЛО РАБОТЫ СИСТЕМЫ ЦЕНООБРАЗОВАНИЯ OZON", "=")
        self.logger.info("Инициализация системы...")

        # Проверка обязательных переменных окружения
        required_vars = ['OZON_CLIENT_ID', 'OZON_API_KEY', 'CDEK_CLIENT_ID', 'CDEK_CLIENT_SECRET']
        missing = [var for var in required_vars if not getattr(Config, var, None)]
        if missing:
            raise ValueError(f"❌ Отсутствуют обязательные переменные окружения: {', '.join(missing)}")

        # Подключение к БД
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

        await self._ensure_tables_exist()

        # Инициализация очереди
        self.queue = asyncio.Queue(maxsize=Config.MAX_QUEUE_SIZE)

        self.promotion_manager = OzonPromotionManager(self.db_pool, self.logger, self.db_logger)
        await self.promotion_manager.initialize()

        await self.promotion_manager._ensure_product_fields_exist()

        # Вывод конфигурации
        self._log_table("КОНФИГУРАЦИЯ СИСТЕМЫ", {
            "Склад отправки": "Ростов-на-Дону (код СДЭК: 66)",
            "Схема доставки": "rFBS через СДЭК",
            "WORKERS_COUNT": Config.WORKERS_COUNT,
            "BATCH_SIZE": Config.BATCH_SIZE,
            "SALES_HOURS_FILTER": f"{Config.SALES_HOURS_FILTER}ч",
            "CYCLE_INTERVAL": f"{Config.OZON_CYCLE_INTERVAL // 3600}ч {(Config.OZON_CYCLE_INTERVAL % 3600) // 60}м",
            "MIN_SALES_FOR_CALC": Config.MIN_SALES_FOR_CALC,
            "MIN_MARGIN_FACTOR": f"×{Config.MIN_MARGIN_FACTOR}",
            "BANK_COMMISSION": f"{Config.BANK_COMMISSION * 100}%",
            "MAX_PRICE_CHANGE": f"{Config.MAX_PRICE_CHANGE_PERCENT}%"
        })

        self.logger.info("✅ Инициализация завершена успешно")
        self._log_separator()

    async def fetch_products_batch(self, offer_ids: List[str]) -> Dict[str, ProductData]:
        """Загружает информацию о товарах из базы данных"""
        if not offer_ids:
            return {}

        self._log_separator("ЗАГРУЗКА ТОВАРОВ ИЗ БАЗЫ ДАННЫХ", "-")
        self.logger.info(f"🛒 Загружаем информацию о {len(offer_ids)} товарах")

        product_map = {}
        loaded_count = 0
        skipped_count = 0

        for i in range(0, len(offer_ids), Config.BATCH_SIZE):
            batch = offer_ids[i:i + Config.BATCH_SIZE]
            batch_num = i // Config.BATCH_SIZE + 1
            total_batches = (len(offer_ids) + Config.BATCH_SIZE - 1) // Config.BATCH_SIZE

            self.logger.info(f"📦 Батч {batch_num}/{total_batches}: {len(batch)} товаров")

            placeholders = ', '.join(['%s'] * len(batch))

            try:
                async with self.db_pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cursor:
                        await cursor.execute(f"""
                            SELECT model_ozon, purchase_price, target_profit_rub, ozon_seller_discount,
                                   price_ozon, ozon_real_price, length, width, height, weight, sku_ozon, status,
                                   promotion_active, promotion_lock_until
                            FROM {Config.PPODUCT_TABLE}
                            WHERE model IN ({placeholders})
                              AND purchase_price > 0
                              AND target_profit_rub > 0
                              AND status = 1
                        """, batch)

                        rows = await cursor.fetchall()

                        for row in rows:
                            product = ProductData.from_db_row(row)
                            if product:
                                # 👋 Добавляем поля акций в объект ProductData
                                product.promotion_active = row.get('promotion_active', False)
                                product.promotion_lock_until = row.get('promotion_lock_until')
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
        self.logger.info(f"⚠️ Пропущено (нет в БД/неактивны): {skipped_count}")

        if loaded_count > 0:
            self.logger.info("📋 Примеры загруженных товаров:")
            sample_items = list(product_map.items())[:3]
            for vendor_code, product in sample_items:
                self.logger.info(
                    f"   • {vendor_code}: закуп={product.purchase_price:.0f}₽, "
                    f"цель={product.target_profit:.0f}₽, цена={product.current_price_ozon:.0f}₽, "
                    f"вес={product.weight:.2f}кг, габариты={product.length}x{product.width}x{product.height}см"
                )

        return product_map

    async def _ensure_tables_exist(self):
        """Проверяет и создает необходимые таблицы для статистики Ozon"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # Таблица циклов
                    await cursor.execute(f"""
                        CREATE TABLE IF NOT EXISTS {Config.OZON_CYCLE_INFO_TABLE}  (
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
                        CREATE TABLE IF NOT EXISTS {Config.OZON_SYSTEM_STATUS_TABLE} (
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
                        CREATE TABLE IF NOT EXISTS {Config.OZON_LOGISTIC_TABLE} (
                            vendor_code VARCHAR(100) NOT NULL,
                            region VARCHAR(255) NOT NULL,
                            success_orders INT DEFAULT 0,
                            returned_orders INT DEFAULT 0,
                            total_delivery_cost DECIMAL(10,2) DEFAULT 0,
                            total_return_cost DECIMAL(10,2) DEFAULT 0,
                            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                            PRIMARY KEY (vendor_code, region),
                            INDEX idx_vendor (vendor_code),
                            INDEX idx_updated (updated_at)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """)

                    # Таблица дневной статистики
                    await cursor.execute(f"""
                        CREATE TABLE IF NOT EXISTS {Config.OZON_DAILY_STATS_TABLE} (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            date DATE NOT NULL,
                            total_profit DECIMAL(10,2) DEFAULT 0,
                            total_revenue DECIMAL(10,2) DEFAULT 0,
                            products_updated INT DEFAULT 0,
                            avg_margin DECIMAL(5,2) DEFAULT 0,
                            UNIQUE KEY unique_date (date)
                        )
                    """)

            self.logger.info("✅ Таблицы для статистики Ozon проверены/созданы")

        except Exception as e:
            self.logger.error(f"❌ Ошибка создания таблиц Ozon: {e}")

    async def update_logistics_stats(self, vendor_code: str, region: str, delivery_cost: float, is_returned: bool):
        """Обновляет статистику логистики для товара"""
        try:
            return_cost_percent = Config.OZON_RETURN_COST_PERCENT / 100
            return_cost = delivery_cost * return_cost_percent if is_returned else 0

            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"""
                        INSERT INTO {Config.OZON_LOGISTIC_TABLE}
                        (vendor_code, region, success_orders, returned_orders, 
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
                        region,
                        0 if is_returned else 1,
                        1 if is_returned else 0,
                        delivery_cost,
                        return_cost
                    ))
        except Exception as e:
            self.logger.error(f"Ошибка обновления статистики логистики для {vendor_code}: {e}")

    async def get_weighted_logistics(self, vendor_code: str) -> Optional[float]:
        """Возвращает средневзвешенную стоимость логистики для товара с учётом возвратов за последние N дней"""
        days = Config.OZON_LOGISTICS_STATS_DAYS
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"""
                        SELECT 
                            SUM(total_delivery_cost) + SUM(total_return_cost) as total_cost,
                            SUM(success_orders) as total_success
                        FROM {Config.OZON_LOGISTIC_TABLE}
                        WHERE vendor_code = %s 
                        AND updated_at >= NOW() - INTERVAL %s DAY
                    """, (vendor_code, days))

                    row = await cursor.fetchone()
                    if row and row[1] and row[1] > 0:
                        avg_logistics = row[0] / row[1]
                        return float(avg_logistics)
        except Exception as e:
            self.logger.error(f"Ошибка получения средневзвешенной логистики для {vendor_code}: {e}")
        return None

    async def save_cycle_start(self):
        """Сохраняет информацию о начале цикла"""
        try:
            moscow_tz = pytz.timezone('Europe/Moscow')
            now_moscow = datetime.now(moscow_tz)

            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    next_cycle = now_moscow + timedelta(seconds=Config.OZON_CYCLE_INTERVAL)

                    # ✅ Всегда создаем новую запись цикла
                    await cursor.execute(f"""
                        INSERT INTO {Config.OZON_CYCLE_INFO_TABLE} 
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
                    await cursor.execute(f"SELECT COUNT(*) FROM {Config.OZON_SYSTEM_STATUS_TABLE} WHERE id = 1")
                    count = (await cursor.fetchone())[0]

                    if count == 0:
                        # Первый запуск - создаем запись
                        await cursor.execute(f"""
                            INSERT INTO {Config.OZON_SYSTEM_STATUS_TABLE} 
                            (id, is_running, last_start, current_cycle_id, next_cycle, updated_at)
                            VALUES (1, TRUE, %s, %s, %s, %s)
                        """, (
                            now_moscow.replace(tzinfo=None),
                            self.current_cycle_id,
                            next_cycle.replace(tzinfo=None),
                            now_moscow.replace(tzinfo=None)
                        ))
                    else:
                        # Обновляем существующую запись - is_running остается TRUE
                        await cursor.execute(f"""
                            UPDATE {Config.OZON_SYSTEM_STATUS_TABLE}
                            SET 
                                is_running = TRUE,
                                last_start = %s,
                                current_cycle_id = %s,
                                next_cycle = %s,
                                updated_at = %s
                            WHERE id = 1
                        """, (
                            now_moscow.replace(tzinfo=None),
                            self.current_cycle_id,
                            next_cycle.replace(tzinfo=None),
                            now_moscow.replace(tzinfo=None)
                        ))

                    self.logger.info(
                        f"✅ Цикл #{self.current_cycle_id} начат в {now_moscow.strftime('%Y-%m-%d %H:%M:%S')} МСК")

        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения начала цикла: {e}")

    async def save_cycle_stats(self):
        """Сохраняет статистику цикла в БД"""
        try:
            moscow_tz = pytz.timezone('Europe/Moscow')
            now_moscow = datetime.now(moscow_tz)
            cycle_end = now_moscow

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
                    next_cycle = now_moscow + timedelta(seconds=Config.OZON_CYCLE_INTERVAL)

                    if self.current_cycle_id:
                        # Определяем статус на основе флага прерывания

                        await cursor.execute(f"""
                            UPDATE {Config.OZON_CYCLE_INFO_TABLE}
                            SET 
                                end_time = %s,
                                duration_sec = %s,
                                status = %s,
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
                            'completed',  # 'interrupted' или 'completed'
                            self.stats.get('total_processed', 0),
                            self.stats.get('success', 0),
                            self.stats.get('price_changes', 0),
                            self.stats.get('discount_changes', 0),
                            self.stats.get('skipped_no_data', 0),
                            self.stats.get('skipped_min_price', 0),
                            self.stats.get('skipped_min_change', 0),
                            self.stats.get('error', 0),
                            self.stats.get('promotions_scheduled', 0),
                            next_cycle.replace(tzinfo=None),
                            self.current_cycle_id
                        ))

                        self.logger.info(f"✅ Статистика цикла #{self.current_cycle_id} сохранена ")
                    else:
                        self.logger.error("❌ Нет current_cycle_id для обновления")

                    # Обновляем статус системы
                    await cursor.execute(f"""
                        UPDATE {Config.OZON_SYSTEM_STATUS_TABLE}
                        SET 
                            next_cycle = %s,
                            updated_at = %s
                        WHERE id = 1
                    """, (
                        next_cycle.replace(tzinfo=None),
                        now_moscow.replace(tzinfo=None)
                    ))

        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения статистики цикла: {e}")

    async def collect_daily_profit_stats(self, orders_by_offer: Dict,
                                         product_map: Dict[str, ProductData]):
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
            'products_count': 0,
            'orders_count': 0,
            'offer_ids': set()
        }

        # Проходим по всем заказам
        for offer_id, orders in orders_by_offer.items():
            if offer_id not in product_map:
                continue

            product = product_map[offer_id]

            for order in orders:
                if order.is_cancelled or order.price <= 0:
                    continue

                # ===== БЕРЕМ TARGET_PROFIT ИЗ ПРОДУКТА =====
                target_profit = product.target_profit

                # Суммируем
                daily_stats['total_profit'] += target_profit
                daily_stats['total_revenue'] += order.price
                daily_stats['orders_count'] += 1
                daily_stats['offer_ids'].add(offer_id)

        daily_stats['products_count'] = len(daily_stats['offer_ids'])

        # ===== СОХРАНЕНИЕ В БД =====
        await self._save_daily_stats_to_db(daily_stats, sale_date)

        # ===== ЛОГИРОВАНИЕ =====
        self._log_daily_stats_summary(daily_stats)

        return daily_stats

    async def _save_daily_stats_to_db(self, daily_stats: Dict, sale_date: date):
        """
        Сохранение дневной статистики в БД
        ТАБЛИЦА: ozon_daily_stats (id, date, total_profit, total_revenue, products_updated, avg_margin)
        """
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:

                    # Расчет средней маржинальности в процентах
                    avg_margin = (
                        (daily_stats['total_profit'] / daily_stats['total_revenue'] * 100)
                        if daily_stats['total_revenue'] > 0 else 0
                    )

                    # Проверяем, есть ли запись за сегодня
                    await cursor.execute(f"""
                        SELECT id FROM {Config.OZON_DAILY_STATS_TABLE}
                        WHERE date = %s
                    """, (sale_date,))

                    existing = await cursor.fetchone()

                    if existing:
                        # Обновляем существующую запись
                        await cursor.execute(f"""
                            UPDATE {Config.OZON_DAILY_STATS_TABLE}
                            SET 
                                total_profit = %s,
                                total_revenue = %s,
                                products_updated = %s,
                                avg_margin = %s
                            WHERE date = %s
                        """, (
                            round(daily_stats['total_profit'], 2),
                            round(daily_stats['total_revenue'], 2),
                            daily_stats['products_count'],
                            round(avg_margin, 2),
                            sale_date
                        ))
                    else:
                        # Создаем новую запись
                        await cursor.execute(f"""
                            INSERT INTO {Config.OZON_DAILY_STATS_TABLE}
                            (date, total_profit, total_revenue, products_updated, avg_margin)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (
                            sale_date,
                            round(daily_stats['total_profit'], 2),
                            round(daily_stats['total_revenue'], 2),
                            daily_stats['products_count'],
                            round(avg_margin, 2)
                        ))

                    await conn.commit()

                    self.logger.info(f"✅ Дневная статистика сохранена за {sale_date}")

        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения дневной статистики: {e}")
            await self.db_logger.log_error(
                vendor_code=None,
                sku_ozon=None,
                error=f"Ошибка сохранения дневной статистики: {e}",
                details={"date": str(sale_date)}
            )

    def _log_daily_stats_summary(self, daily_stats: Dict):
        """
        Логирование итогов сбора дневной статистики
        """
        self._log_separator("ИТОГИ ДНЕВНОЙ СТАТИСТИКИ", "📈")
        self.logger.info(f"📊 ОБЩАЯ СТАТИСТИКА:")
        self.logger.info(f"   • Всего товаров: {daily_stats['products_count']}")
        self.logger.info(f"   • Всего заказов: {daily_stats['orders_count']}")
        self.logger.info(f"   • Общая выручка: {daily_stats['total_revenue']:,.2f}₽")
        self.logger.info(f"   • ОБЩАЯ ПРИБЫЛЬ (target_profit): {daily_stats['total_profit']:,.2f}₽")

        if daily_stats['total_revenue'] > 0:
            overall_margin = (daily_stats['total_profit'] / daily_stats['total_revenue']) * 100
            self.logger.info(f"   • Средняя маржинальность: {overall_margin:.2f}%")

    async def should_collect_daily_stats(self) -> bool:
        """
        Проверяет, нужно ли собирать дневную статистику
        Запускается 1 раз в 24 часа
        """
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:

                    # Получаем последнюю дату сбора статистики
                    await cursor.execute(f"""
                        SELECT MAX(date) as last_date 
                        FROM {Config.OZON_DAILY_STATS_TABLE}
                    """)

                    result = await cursor.fetchone()
                    last_date = result.get('last_date') if result else None

                    moscow_tz = pytz.timezone('Europe/Moscow')
                    now = datetime.now(moscow_tz)
                    today = now.date()

                    # Если сегодня еще не собирали
                    if last_date != today:
                        self.logger.info(f"✅ Время для сбора дневной статистики (последний сбор: {last_date})")
                        return True
                    else:
                        self.logger.info(f"ℹ️  Дневная статистика за {today} уже собрана")
                        return False

        except Exception as e:
            self.logger.error(f"❌ Ошибка проверки времени сбора статистики: {e}")
            return False

    async def calculate_logistics_with_cdek(self,
                                            product: ProductData,
                                            region: str,
                                            city: str = "") -> Tuple[float, Dict]:
        """
        Рассчитывает стоимость логистики через СДЭК API по весу товара
        и региону доставки
        """
        # Используем вес из базы данных
        if product.has_weight:
            weight_kg = product.weight
            weight_source = "из базы данных"
        else:
            # Запасной вариант: расчет веса по габаритам (0.3 кг/литр)
            volume_liters = (product.length * product.width * product.height) / 1000.0
            if volume_liters < 1.0:
                volume_liters = 1.0
            weight_kg = max(volume_liters * 0.3, 0.5)
            weight_source = "рассчитан по габаритам"
            self.logger.warning(
                f"⚠️ Для товара {product.vendor_code} не указан вес в БД. Используется расчетный вес: {weight_kg:.2f} кг")

        # Для расчета объема (если нужен для СДЭК API) все равно используем габариты
        volume_liters = (product.length * product.width * product.height) / 1000.0
        if volume_liters < 1.0:
            volume_liters = 1.0

        async with CdekAPI(Config.CDEK_CLIENT_ID, Config.CDEK_CLIENT_SECRET, self.logger) as cdek:
            delivery_cost, delivery_days, tariff_name = await cdek.calculate_delivery(
                weight_kg=weight_kg,
                length_cm=product.length,
                width_cm=product.width,
                height_cm=product.height,
                to_region=region,
                to_city=city
            )

        calculation_details = {
            'dimensions': f"{product.length}x{product.width}x{product.height} см",
            'volume_liters': volume_liters,
            'weight_kg': weight_kg,
            'weight_source': weight_source,
            'region': region,
            'city': city,
            'delivery_cost': delivery_cost,
            'delivery_days': delivery_days,
            'tariff_name': tariff_name,
            'warehouse_city_code': Config.CDEK_WAREHOUSE_CITY_CODE
        }

        self.logger.debug(
            f"📦 Логистика СДЭК для {product.vendor_code}: "
            f"вес={weight_kg:.2f}кг ({weight_source}), объем={volume_liters:.2f}л → {region} | "
            f"стоимость: {delivery_cost:.2f}₽, срок: {delivery_days}дн"
        )

        return delivery_cost, calculation_details

    async def process_product(self,
                              offer_id: str,
                              orders: List[OzonOrderData],
                              product: ProductData) -> PriceUpdate:
        """
        Основная логика расчёта цены для товара на Ozon с учетом веса и скидки продавца.
        """
        try:
            if Config.PROMOTIONS_ENABLED:
                if product.promotion_lock_until:
                    lock_until = product.promotion_lock_until
                    if lock_until.tzinfo is not None:
                        lock_until = lock_until.replace(tzinfo=None)
                    if datetime.now() < lock_until:
                        self.logger.info(f"🔒 Товар {offer_id} заблокирован до {product.promotion_lock_until} – пропускаем")
                        return PriceUpdate(
                            vendor_code=offer_id,
                            new_price_ozon=0,
                            new_real_price=0,
                            old_price_ozon=product.current_price_ozon,
                            old_real_price=product.current_real_price,
                            profit_correction=0,
                            status=ProcessingStatus.SKIPPED_PROMOTION,
                            error_msg="Товар заблокирован до окончания акции",
                            sku_ozon=product.sku_ozon,
                            logistics_cost=0,
                            current_profit=0,
                            target_profit=product.target_profit,
                            sales_count=len(orders),
                            purchase_price=product.purchase_price,
                            target_forpay=0,
                            action_type="skip_promotion_lock",
                            seller_discount=product.seller_discount
                        )

                if hasattr(product, 'promotion_active') and product.promotion_active and Config.PROMOTION_LOCK_PRICES:
                    self.logger.info(f"🔒 Товар {offer_id} УЧАСТВУЕТ В АКЦИИ - пропускаем изменение цены")

                    await self.db_logger.log_skip(
                        vendor_code=offer_id,
                        sku_ozon=product.sku_ozon,
                        reason="Товар участвует в акции",
                        details={
                            "promotion_active": True,
                            "promotion_lock_until": str(
                                product.promotion_lock_until) if product.promotion_lock_until else None
                        }
                    )

                    self.stats['products_in_promotion'] = self.stats.get('products_in_promotion', 0) + 1

                    return PriceUpdate(
                        vendor_code=offer_id,
                        new_price_ozon=0,
                        new_real_price=0,
                        old_price_ozon=product.current_price_ozon,
                        old_real_price=product.current_real_price,
                        profit_correction=0,
                        status=ProcessingStatus.SKIPPED_PROMOTION,
                        error_msg="Товар участвует в акции",
                        sku_ozon=product.sku_ozon,
                        logistics_cost=0,
                        current_profit=0,
                        target_profit=product.target_profit,
                        sales_count=len(orders),
                        purchase_price=product.purchase_price,
                        target_forpay=0,
                        action_type="skip_promotion",
                        seller_discount=product.seller_discount
                    )

            # ===== ШАГ 0: ИНФОРМАЦИЯ О НДС =====
            vat_percent = Config.VAT_PERCENT
            self.logger.info("🧾 ИНФОРМАЦИЯ О НДС:")
            self.logger.info(f"   Ставка НДС: {vat_percent}%")
            self.logger.info("   Товар собственного производства → НДС в закупке нет")
            self.logger.info(f"   Формула: НДС = цена × {vat_percent} / ({100 + vat_percent})")

            # ===== ШАГ 0.5: ИНФОРМАЦИЯ О СКИДКЕ ПРОДАВЦА =====
            if product.has_discount:
                self.logger.info("🏷️ ИНФОРМАЦИЯ О СКИДКЕ ПРОДАВЦА:")
                self.logger.info(f"   Скидка: {product.seller_discount}%")
                self.logger.info(f"   Базовая цена: {product.current_price_ozon:.0f}₽")
                self.logger.info(f"   Цена со скидкой: {product.current_price_with_discount:.0f}₽")
                self.logger.info(
                    f"   Размер скидки: {product.current_price_ozon - product.current_price_with_discount:.0f}₽")
            else:
                self.logger.info("🏷️ Скидка продавца отсутствует")

            # ===== ШАГ 1: Статистика по регионам =====
            region_stats = defaultdict(int)
            for order in orders:
                if order.region:
                    region_stats[order.region] += 1

            # ===== НОВЫЙ БЛОК: ПОЛУЧЕНИЕ СТОИМОСТИ ЛОГИСТИКИ =====
            # Сначала пробуем получить средневзвешенную из статистики
            avg_logistics = await self.get_weighted_logistics(offer_id)

            if avg_logistics is not None and avg_logistics > 0:
                logistics_cost = avg_logistics
                logistics_details = {
                    'source': 'weighted_stats',
                    'avg_logistics': avg_logistics,
                    'stats_days': Config.OZON_LOGISTICS_STATS_DAYS
                }
                self.logger.info(f"📊 Используем средневзвешенную логистику: {logistics_cost:.2f}₽")
            else:
                # Если статистики нет, собираем данные по текущим заказам
                region_costs = []
                for order in orders:
                    # Рассчитываем логистику для данного региона
                    cost, details = await self.calculate_logistics_with_cdek(
                        product=product,
                        region=order.region,
                        city=order.city
                    )
                    region_costs.append(cost)
                    # Обновляем статистику (is_returned = order.is_cancelled)
                    await self.update_logistics_stats(
                        vendor_code=offer_id,
                        region=order.region,
                        delivery_cost=cost,
                        is_returned=order.is_cancelled
                    )

                if region_costs:
                    logistics_cost = sum(region_costs) / len(region_costs)
                    logistics_details = {
                        'source': 'current_orders_avg',
                        'avg_logistics': logistics_cost,
                        'orders_processed': len(region_costs)
                    }
                else:
                    # Аварийный вариант (не должно происходить)
                    logistics_cost, logistics_details = await self.calculate_logistics_with_cdek(
                        product=product,
                        region='Москва',
                        city='Москва'
                    )
                    logistics_details['source'] = 'fallback_moscow'

            self.logger.debug(
                f"📦 Логистика для {offer_id}: {logistics_cost:.2f}₽ (источник: {logistics_details['source']})")
            # ===== КОНЕЦ НОВОГО БЛОКА =====

            # Далее идёт существующий код расчёта цены, который использует logistics_cost и logistics_details
            # (ниже приводится продолжение метода без изменений)

            # ===== ШАГ 3: ВЫВОД ИСХОДНЫХ ДАННЫХ =====
            self.logger.info("📊 ИСХОДНЫЕ ДАННЫЕ:")

            # Расчет НДС в текущей цене (с учетом скидки)
            if product.has_discount:
                current_price_for_calc = product.current_price_with_discount
                current_vat_in_price = product.current_vat_in_discounted_price
                current_price_without_vat = product.current_price_with_discount_without_vat
            else:
                current_price_for_calc = product.current_price_ozon
                current_vat_in_price = product.current_vat_in_price
                current_price_without_vat = product.current_price_without_vat

            product_info = {
                "Артикул (offer_id)": offer_id,
                "Себестоимость (без НДС)": f"{product.purchase_price:.2f}₽",
                "Целевая прибыль": f"{product.target_profit:.2f}₽",
                "Текущая базовая цена Ozon": f"{product.current_price_ozon:.0f}₽",
                "Скидка продавца": f"{product.seller_discount}%" if product.has_discount else "нет",
                "Текущая цена со скидкой": f"{current_price_for_calc:.0f}₽" if product.has_discount else "—",
                "Цена без НДС (со скидкой)": f"{current_price_without_vat:.0f}₽",
                "НДС в текущей цене": f"{current_vat_in_price:.0f}₽",
                "Ставка НДС": f"{vat_percent}%",
                "Габариты": logistics_details.get('dimensions', 'не указаны'),
                "Объем": f"{logistics_details.get('volume_liters', 0):.2f} л",
                "Вес": f"{logistics_details.get('weight_kg', 0):.2f} кг ({logistics_details.get('weight_source', 'неизвестно')})",
                "Логистика СДЭК": f"{logistics_cost:.2f}₽ (РАСХОД)",
                "Источник логистики": logistics_details['source'],
                "Срок доставки": f"{logistics_details.get('delivery_days', '?')} дней",
                "Тариф СДЭК": logistics_details.get('tariff_name', 'неизвестно'),
                "SKU Ozon": product.sku_ozon if product.sku_ozon else "не установлен"
            }
            self._log_table("ПАРАМЕТРЫ ТОВАРА", product_info)

            # ===== ШАГ 4: АНАЛИЗ ЗАКАЗОВ =====
            self.logger.info(f"📈 ДАННЫЕ О ЗАКАЗАХ:")
            self.logger.info(f"   Всего заказов: {len(orders)}")
            self.logger.info(f"   Требуется для расчета: {Config.MIN_SALES_FOR_CALC}")

            valid_orders = [order for order in orders if not order.is_cancelled]
            self.logger.info(f"✅ Валидных заказов: {len(valid_orders)}")

            if len(valid_orders) < Config.MIN_SALES_FOR_CALC:
                self._log_separator("РЕШЕНИЕ: ПРОПУСК", "!")
                self.logger.warning(f"⚠️ НЕДОСТАТОЧНО ДАННЫХ")
                self.logger.warning(f"   Требуется: {Config.MIN_SALES_FOR_CALC}, имеется: {len(valid_orders)}")

                await self.db_logger.log_skip(
                    vendor_code=offer_id,
                    sku_ozon=product.sku_ozon,
                    reason="Недостаточно данных для расчета",
                    details={
                        "valid_orders": len(valid_orders),
                        "required": Config.MIN_SALES_FOR_CALC,
                        "logistics_details": logistics_details,
                        "vat_percent": vat_percent,
                        "seller_discount": product.seller_discount,
                        "region_stats": dict(region_stats)
                    }
                )

                return PriceUpdate(
                    vendor_code=offer_id,
                    new_price_ozon=0,
                    new_real_price=0,
                    old_price_ozon=product.current_price_ozon,
                    old_real_price=product.current_real_price,
                    profit_correction=0,
                    status=ProcessingStatus.SKIPPED_NO_DATA,
                    error_msg=f"Недостаточно данных: {len(valid_orders)}",
                    sku_ozon=product.sku_ozon,
                    logistics_cost=logistics_cost,
                    region_stats=region_stats,
                    vat_percent=vat_percent,
                    seller_discount=product.seller_discount
                )

            # Статистика по заказам
            payout_list = [order.payout for order in valid_orders if order.payout > 0]
            price_list = [order.price for order in valid_orders if order.price > 0]
            payout_ratios = [order.payout_ratio for order in valid_orders if order.price > 0]

            if payout_list and price_list and payout_ratios:
                avg_payout = statistics.mean(payout_list)
                avg_price = statistics.mean(price_list)
                avg_payout_ratio = statistics.mean(payout_ratios)

                # Расчет НДС в средней цене
                avg_vat_in_price = avg_price * (vat_percent / (100 + vat_percent))
                avg_price_without_vat = avg_price - avg_vat_in_price

                self.logger.info("📊 АНАЛИЗ ЗАКАЗОВ С НДС:")
                self._log_table("СТАТИСТИКА ЗАКАЗОВ", {
                    "Средняя цена (с НДС)": f"{avg_price:.2f}₽",
                    "НДС в средней цене": f"{avg_vat_in_price:.2f}₽",
                    "Средняя цена (без НДС)": f"{avg_price_without_vat:.2f}₽",
                    "Средний payout": f"{avg_payout:.2f}₽",
                    "Среднее соотношение payout/price": f"{avg_payout_ratio:.3f}",
                    "Мин. payout": f"{min(payout_list):.2f}₽",
                    "Макс. payout": f"{max(payout_list):.2f}₽",
                    "Заказов использовано": len(valid_orders),
                    "Регионы доставки": f"{len(region_stats)} шт."
                })
            else:
                avg_payout = 0
                avg_price = current_price_for_calc
                avg_vat_in_price = current_vat_in_price
                avg_price_without_vat = current_price_without_vat

            # ===== ШАГ 5: РАСЧЁТ ТЕКУЩЕЙ ПРИБЫЛИ С НДС =====
            self.logger.info("🧮 РАСЧЕТ ТЕКУЩЕЙ ПРИБЫЛИ С УЧЕТОМ НДС:")
            self.logger.info("   ❗ Товар собственного производства → весь НДС с продажи = расход")
            self.logger.info("   ❗ Логистика СДЭК = РАСХОД")

            if avg_payout > 0:
                # Весь НДС с продажи = расход (собственное производство)
                vat_to_pay = avg_vat_in_price

                bank_commission_current = avg_payout * Config.BANK_COMMISSION
                current_profit = (
                        avg_payout -
                        bank_commission_current -
                        product.purchase_price -
                        vat_to_pay -
                        logistics_cost
                )

                profit_table = {
                    "Средний payout (доход)": f"{avg_payout:.2f}₽",
                    f"Банковская комиссия ({Config.BANK_COMMISSION * 100}%)": f"-{bank_commission_current:.2f}₽",
                    "Себестоимость (без НДС)": f"-{product.purchase_price:.2f}₽",
                    f"НДС с продажи ({vat_percent}%)": f"-{vat_to_pay:.2f}₽",
                    "Логистика СДЭК (расход)": f"-{logistics_cost:.2f}₽",
                    "ИТОГО ПРИБЫЛЬ": f"{current_profit:.2f}₽",
                    "Примечание": f"Собственное производство → НДС {vat_to_pay:.2f}₽ = полный расход"
                }
                self._log_table("РАСЧЕТ ТЕКУЩЕЙ ПРИБЫЛИ (с НДС)", profit_table)
            else:
                current_profit = 0
                avg_payout = 0
                vat_to_pay = 0

            # ===== ШАГ 6: РАСЧЁТ ЦЕЛЕВОЙ ЦЕНЫ СО СКИДКОЙ (реальная цена продажи) =====
            self.logger.info("🎯 РАСЧЁТ ЦЕЛЕВОЙ ЦЕНЫ СО СКИДКОЙ (реальная цена продажи):")
            self.logger.info("   ❗ Себестоимость производства = расход БЕЗ НДС")
            self.logger.info("   ❗ Весь НДС с продажи = расход")

            # Расчет требуемого payout для целевой прибыли
            # Общая комиссия = банк + эквайринг
            total_commission = Config.BANK_COMMISSION + Config.ACQUIRING_RATE

            # Предварительный расчёт НДС (приблизительно)
            estimated_vat = (product.target_profit + product.purchase_price + logistics_cost) * (
                    Config.VAT_PERCENT / 100)

            target_payout_with_all_costs = (
                                                   product.target_profit +
                                                   product.purchase_price +
                                                   logistics_cost +
                                                   estimated_vat  # 👈 Добавляем НДС
                                           ) / (1 - total_commission)  # 👈 Учитываем все комиссии

            # Рассчитываем цену БЕЗ НДС из payout
            if Config.OZON_FORPAY_TO_PRICEWITHDISC_RATIO > 0:
                target_price_without_vat = target_payout_with_all_costs / Config.OZON_FORPAY_TO_PRICEWITHDISC_RATIO
            else:
                target_price_without_vat = target_payout_with_all_costs / Config.OZON_FORPAY_TO_PRICEWITHDISC_RATIO

            # Добавляем НДС к цене → это ЦЕНА СО СКИДКОЙ (реальная цена продажи)
            target_discounted_price = target_price_without_vat * (1 + vat_percent / 100)
            target_discounted_price = round(target_discounted_price / 10) * 10  # Округление до 10₽

            # ===== ШАГ 6.5: УЧЕТ СКИДКИ ПРОДАВЦА — КОРРЕКТНЫЙ РАСЧЁТ БАЗОВОЙ ЦЕНЫ =====
            target_base_price = target_discounted_price  # по умолчанию (без скидки)

            if product.has_discount:
                # Формула: базовая_цена = цена_со_скидкой / (1 - скидка/100)
                target_base_price = target_discounted_price / (1 - product.seller_discount / 100)
                target_base_price = round(target_base_price / 10) * 10  # Округление до 10₽ для базовой цены

                # Пересчитываем ФАКТИЧЕСКУЮ цену со скидкой после округления базовой цены
                actual_discounted_price = target_base_price * (1 - product.seller_discount / 100)
                actual_discounted_price = round(actual_discounted_price / 10) * 10  # Округление до 10₽

                self.logger.info("🏷️ УЧЕТ СКИДКИ ПРОДАВЦА (КОРРЕКТНЫЙ РАСЧЁТ):")
                self.logger.info(f"   • Целевая цена со скидкой (до округления): {target_discounted_price:.0f}₽")
                self.logger.info(f"   • Скидка продавца: {product.seller_discount}%")
                self.logger.info(f"   • Рассчитанная базовая цена: {target_base_price:.0f}₽")
                self.logger.info(f"   • Фактическая цена со скидкой (после округления): {actual_discounted_price:.0f}₽")
                self.logger.info(
                    f"   • Проверка: {target_base_price:.0f}₽ × (1 - {product.seller_discount}%) = {actual_discounted_price:.0f}₽ ✓")

                # Используем фактическую цену со скидкой для дальнейших расчётов
                target_discounted_price = actual_discounted_price
            else:
                self.logger.info("🏷️ Скидка продавца отсутствует — базовая цена = цене продажи")

            # Пересчитываем НДС для цены со скидкой
            new_vat_in_discounted_price = target_discounted_price * (vat_percent / (100 + vat_percent))
            new_discounted_price_without_vat = target_discounted_price - new_vat_in_discounted_price
            new_vat_to_pay = new_vat_in_discounted_price  # Весь НДС с продажи = расход

            # Пересчитываем НДС для базовой цены (для логов)
            new_vat_in_base_price = target_base_price * (vat_percent / (100 + vat_percent))
            new_base_price_without_vat = target_base_price - new_vat_in_base_price

            total_commission = Config.BANK_COMMISSION + Config.ACQUIRING_RATE

            # Точный расчет требуемого payout с учетом НДС
            target_forpay = (
                                    product.target_profit +
                                    product.purchase_price +
                                    new_vat_to_pay +
                                    logistics_cost
                            ) / (1 - total_commission)

            self._log_table("РАСЧЁТ ЦЕЛЕВОЙ ЦЕНЫ", {
                "Себестоимость (без НДС)": f"{product.purchase_price:.2f}₽",
                "Целевая прибыль": f"{product.target_profit:.2f}₽",
                "Логистика СДЭК (расход)": f"{logistics_cost:.2f}₽",
                f"НДС к уплате ({vat_percent}%)": f"{new_vat_to_pay:.2f}₽",
                "Требуемый payout": f"{target_forpay:.2f}₽",
                f"Банковская комиссия ({Config.BANK_COMMISSION * 100}%)": f"{(target_forpay * Config.BANK_COMMISSION):.2f}₽",
                "Соотношение payout/price": f"{Config.OZON_FORPAY_TO_PRICEWITHDISC_RATIO:.3f}",
                "Целевая цена без НДС (со скидкой)": f"{new_discounted_price_without_vat:.0f}₽",
                f"НДС {vat_percent}% (в цене со скидкой)": f"{new_vat_in_discounted_price:.0f}₽",
                "Целевая цена со скидкой": f"{target_discounted_price:.0f}₽",
                "Базовая цена (на Ozon)": f"{target_base_price:.0f}₽" if product.has_discount else "—",
                "Текущая цена со скидкой": f"{current_price_for_calc:.0f}₽",
                "Изменение цены со скидкой": f"{target_discounted_price - current_price_for_calc:+.0f}₽",
            })

            # ===== ШАГ 7: ПРОВЕРКА МИНИМАЛЬНОЙ ЦЕНЫ =====
            min_profit = product.purchase_price * (Config.MIN_MARGIN_FACTOR - 1)

            # Расчет минимальной цены БЕЗ НДС
            min_price_without_vat_components = (
                                                       product.purchase_price +
                                                       logistics_cost +
                                                       min_profit
                                               ) / (1 - Config.BANK_COMMISSION)

            min_price_without_vat = min_price_without_vat_components / (
                Config.OZON_FORPAY_TO_PRICEWITHDISC_RATIO if Config.OZON_FORPAY_TO_PRICEWITHDISC_RATIO > 0 else 0.60
            )

            # Добавляем НДС к минимальной цене
            min_price_with_vat = min_price_without_vat * (1 + vat_percent / 100)
            min_price_with_vat = round(min_price_with_vat / 10) * 10

            self.logger.info(f"🛡️ ПРОВЕРКА МИНИМАЛЬНОЙ ЦЕНЫ (для цены со скидкой):")
            self.logger.info(f"   Минимальная цена со скидкой: {min_price_with_vat:.0f}₽")
            self.logger.info(f"   Рассчитанная цена со скидкой: {target_discounted_price:.0f}₽")

            if target_discounted_price < min_price_with_vat:
                self.logger.warning(f"⚠️ ЦЕНА СО СКИДКОЙ НИЖЕ МИНИМАЛЬНОЙ!")
                self.logger.warning(f"   {target_discounted_price:.0f}₽ < {min_price_with_vat:.0f}₽")

                # Корректируем цену со скидкой до минимальной
                target_discounted_price = min_price_with_vat

                # Пересчитываем базовую цену с учетом скидки
                if product.has_discount:
                    target_base_price = target_discounted_price / (1 - product.seller_discount / 100)
                    target_base_price = round(target_base_price / 10) * 10

                    # Пересчитываем фактическую цену со скидкой после коррекции
                    actual_discounted_price = target_base_price * (1 - product.seller_discount / 100)
                    actual_discounted_price = round(actual_discounted_price / 10) * 10
                    target_discounted_price = actual_discounted_price

                    self.logger.info(f"   • Новая базовая цена для Ozon: {target_base_price:.0f}₽")
                    self.logger.info(f"   • Новая цена со скидкой: {target_discounted_price:.0f}₽")
                    self.logger.info(
                        f"   • Проверка: {target_base_price:.0f}₽ × (1 - {product.seller_discount}%) = {target_discounted_price:.0f}₽ ✓")
                else:
                    target_base_price = target_discounted_price

                # Пересчитываем НДС
                new_vat_in_discounted_price = target_discounted_price * (vat_percent / (100 + vat_percent))
                new_discounted_price_without_vat = target_discounted_price - new_vat_in_discounted_price
                new_vat_to_pay = new_vat_in_discounted_price

            # ===== ШАГ 8: ВАЛИДАЦИЯ ИЗМЕНЕНИЯ ЦЕНЫ =====
            price_diff = target_discounted_price - current_price_for_calc
            price_diff_pct = (price_diff / current_price_for_calc * 100) if current_price_for_calc > 0 else 0

            if abs(price_diff) < Config.MIN_PRICE_CHANGE and abs(price_diff_pct) < 1.0:
                self.logger.info(f"ℹ️ Изменение слишком мало ({price_diff:+.0f}₽, {price_diff_pct:+.1f}%) — пропускаем")
                return PriceUpdate(
                    vendor_code=offer_id,
                    new_price_ozon=0,
                    new_real_price=0,
                    old_price_ozon=product.current_price_ozon,
                    old_real_price=product.current_real_price,
                    profit_correction=abs(target_forpay - avg_payout) if avg_payout else 0,
                    status=ProcessingStatus.SKIPPED_MIN_CHANGE,
                    error_msg=f"Изменение слишком мало: {price_diff:+.0f}₽ ({price_diff_pct:+.1f}%)",
                    sku_ozon=product.sku_ozon,
                    logistics_cost=logistics_cost,
                    current_profit=current_profit,
                    target_profit=product.target_profit,
                    sales_count=len(valid_orders),
                    purchase_price=product.purchase_price,
                    target_forpay=target_forpay,
                    action_type="skip_min_change",
                    region_stats=region_stats,
                    avg_payout_ratio=Config.OZON_FORPAY_TO_PRICEWITHDISC_RATIO,
                    vat_percent=vat_percent,
                    vat_amount=new_vat_in_discounted_price,
                    price_without_vat=new_discounted_price_without_vat,
                    vat_to_pay=new_vat_to_pay,
                    seller_discount=product.seller_discount
                )

            # Пересчитываем НДС после возможных корректировок
            new_vat_in_discounted_price = target_discounted_price * (vat_percent / (100 + vat_percent))
            new_discounted_price_without_vat = target_discounted_price - new_vat_in_discounted_price
            new_vat_to_pay = new_vat_in_discounted_price

            # ===== ШАГ 9: РАСЧЁТ ОЖИДАЕМОЙ ПРИБЫЛИ ПРИ НОВОЙ ЦЕНЕ =====
            expected_payout = target_discounted_price * (
                Config.OZON_FORPAY_TO_PRICEWITHDISC_RATIO if Config.OZON_FORPAY_TO_PRICEWITHDISC_RATIO > 0 else 0.60)
            expected_bank_fee = expected_payout * Config.BANK_COMMISSION
            expected_acquiring_fee = expected_payout * Config.ACQUIRING_RATE
            total_commission_fee = expected_payout * total_commission

            expected_profit = (
                    expected_payout -
                    expected_bank_fee -
                    expected_acquiring_fee -
                    product.purchase_price -
                    new_vat_to_pay -
                    logistics_cost
            )

            self._log_table("ОЖИДАЕМАЯ ПРИБЫЛЬ ПРИ НОВОЙ ЦЕНЕ", {
                "Цена продажи со скидкой": f"{target_discounted_price:.0f}₽",
                "Базовая цена (на Ozon)": f"{target_base_price:.0f}₽" if product.has_discount else "—",
                "НДС в цене со скидкой": f"{new_vat_in_discounted_price:.0f}₽",
                "Цена без НДС (со скидкой)": f"{new_discounted_price_without_vat:.0f}₽",
                "Ожидаемый payout": f"{expected_payout:.0f}₽",
                f"Банковская комиссия ({Config.BANK_COMMISSION * 100}%)": f"{expected_bank_fee:.0f}₽",
                f"Эквайринг ({Config.ACQUIRING_RATE * 100}%)": f"{expected_acquiring_fee:.0f}₽",
                f"Общая комиссия ({total_commission * 100}%)": f"{total_commission_fee:.0f}₽",
                "Себестоимость (без НДС)": f"{product.purchase_price:.0f}₽",
                "НДС к уплате": f"{new_vat_to_pay:.0f}₽",
                "Логистика СДЭК (расход)": f"{logistics_cost:.0f}₽",
                "Ожидаемая прибыль": f"{expected_profit:.0f}₽",
                "Маржинальность": f"{(expected_profit / target_discounted_price) * 100:.1f}%",
            })

            # ===== ШАГ 10: ФОРМИРОВАНИЕ РЕЗУЛЬТАТА =====
            update = PriceUpdate(
                vendor_code=offer_id,
                new_price_ozon=target_base_price,  # БАЗОВАЯ ЦЕНА ДЛЯ OZON (до скидки)
                new_real_price=target_discounted_price,  # РЕАЛЬНАЯ ЦЕНА ПРОДАЖИ (со скидкой)
                old_price_ozon=product.current_price_ozon,
                old_real_price=product.current_real_price,
                profit_correction=abs(target_forpay - avg_payout) if avg_payout else 0,
                status=ProcessingStatus.SUCCESS,
                error_msg="",
                discount=product.seller_discount if product.has_discount else None,
                sku_ozon=product.sku_ozon,
                logistics_cost=logistics_cost,
                current_profit=current_profit,
                target_profit=product.target_profit,
                sales_count=len(valid_orders),
                purchase_price=product.purchase_price,
                target_forpay=target_forpay,
                action_type="price_change",
                region_stats=region_stats,
                avg_payout_ratio=Config.OZON_FORPAY_TO_PRICEWITHDISC_RATIO,
                vat_percent=vat_percent,
                vat_amount=new_vat_in_discounted_price,
                price_without_vat=new_discounted_price_without_vat,
                vat_to_pay=new_vat_to_pay,
                seller_discount=product.seller_discount
            )

            self._log_separator("ИТОГОВОЕ РЕШЕНИЕ", "✅")
            self.logger.info(
                f"✅ {offer_id}: {current_price_for_calc:.0f}₽ → {target_discounted_price:.0f}₽ "
                f"({price_diff:+.0f}₽, {price_diff_pct:+.1f}%) | "
                f"ожидаемая прибыль: {expected_profit:.0f}₽"
            )
            if product.has_discount:
                self.logger.info(
                    f"   🏷️ Базовая цена на Ozon: {target_base_price:.0f}₽ (скидка {product.seller_discount}%) → "
                    f"реальная цена: {target_discounted_price:.0f}₽"
                )
            self.logger.info(
                f"   📦 Логистика СДЭК: {logistics_cost:.0f}₽ (вес: {logistics_details.get('weight_kg', 0):.2f}кг)")
            self.logger.info(f"   🧾 НДС {vat_percent}% в цене: {new_vat_in_discounted_price:.0f}₽ (весь НДС = расход)")
            self.logger.info(f"   💰 Цена без НДС: {new_discounted_price_without_vat:.0f}₽")
            self.logger.info(f"   ⚙️ Себестоимость: {product.purchase_price:.0f}₽ (без НДС)")

            await self.db_logger.log_price_calculation(
                vendor_code=offer_id,
                sku_ozon=product.sku_ozon,
                old_base_price=product.current_price_ozon,
                new_base_price=target_base_price,
                old_discounted_price=current_price_for_calc,
                new_discounted_price=target_discounted_price,
                seller_discount=product.seller_discount if product.has_discount else 0,
                current_profit=current_profit,
                target_profit=product.target_profit,
                logistics_cost=logistics_cost,
                sales_count=len(valid_orders),
                weight_kg=logistics_details.get('weight_kg', 0),
                region=next(iter(region_stats.keys())) if region_stats else '',
                details={
                    "logistics_details": logistics_details,
                    "region_stats": dict(region_stats),
                    "purchase_price": product.purchase_price,
                    "payout_ratio": Config.OZON_FORPAY_TO_PRICEWITHDISC_RATIO,
                    "target_forpay": target_forpay,
                    "expected_profit": expected_profit,
                    "vat_percent": vat_percent,
                    "vat_in_price": new_vat_in_discounted_price,
                    "vat_to_pay": new_vat_to_pay,
                    "price_without_vat": new_discounted_price_without_vat,
                    "is_own_production": True,
                    "weight_source": logistics_details.get('weight_source', 'unknown')
                }
            )

            if update.status == ProcessingStatus.SUCCESS:
                self.stats['total_processed'] = self.stats.get('total_processed', 0) + 1
                self.stats['success'] = self.stats.get('success', 0) + 1

                if update.action_type == 'price_change':
                    self.stats['price_changes'] = self.stats.get('price_changes', 0) + 1
                # Для Ozon отдельно считаем изменения с учетом скидки
                if update.has_discount:
                    self.stats['discount_changes'] = self.stats.get('discount_changes', 0) + 1

            return update

        except Exception as e:
            self.logger.error(f"❌ Ошибка обработки {offer_id}: {e}")
            self.logger.error(traceback.format_exc())
            return PriceUpdate(
                vendor_code=offer_id,
                new_price_ozon=0,
                new_real_price=0,
                old_price_ozon=product.current_price_ozon,
                old_real_price=product.current_real_price,
                profit_correction=0,
                status=ProcessingStatus.ERROR,
                error_msg=str(e),
                sku_ozon=product.sku_ozon,
                logistics_cost=0,
                current_profit=0,
                target_profit=product.target_profit,
                sales_count=len(orders),
                purchase_price=product.purchase_price,
                target_forpay=0,
                action_type="error",
                avg_payout_ratio=0,
                vat_percent=Config.VAT_PERCENT if hasattr(Config, 'VAT_PERCENT') else 20,
                seller_discount=product.seller_discount if hasattr(product, 'seller_discount') else 0
            )

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
                cursor.execute(f"SELECT COUNT(*) FROM {Config.OZON_SYSTEM_STATUS_TABLE} WHERE id = 1")
                count = cursor.fetchone()[0]

                if count == 0:
                    # Создаем запись
                    cursor.execute(f"""
                        INSERT INTO {Config.OZON_SYSTEM_STATUS_TABLE} 
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
                            UPDATE {Config.OZON_SYSTEM_STATUS_TABLE}
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
                            UPDATE {Config.OZON_SYSTEM_STATUS_TABLE}
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

    async def update_sales_flags(self, processed_vendor_codes: set):
        """Обновляет флаги наличия продаж для товаров"""
        try:
            self._log_separator("ОБНОВЛЕНИЕ ФЛАГОВ ПРОДАЖ", "🏷️")

            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # 1. Сначала все товары помечаем как "нет продаж" (по умолчанию)
                    await cursor.execute(f"""
                        UPDATE {Config.PPODUCT_TABLE} 
                        SET has_sales_last_period_ozon = 0
                        WHERE status = 1 AND target_profit_rub > 0
                    """)

                    # 2. Помечаем товары, которые были обработаны в этом цикле (есть продажи)
                    if processed_vendor_codes:
                        placeholders = ','.join(['%s'] * len(processed_vendor_codes))
                        await cursor.execute(f"""
                            UPDATE {Config.PPODUCT_TABLE} 
                            SET has_sales_last_period_ozon = 1,
                                last_sale_time = NOW()
                            WHERE model_ozon IN ({placeholders})
                        """, list(processed_vendor_codes))

                    # 3. Получаем статистику
                    await cursor.execute(f"""
                        SELECT 
                            COUNT(*) as total,
                            SUM(has_sales_last_period_ozon) as with_sales,
                            SUM(CASE WHEN has_sales_last_period_ozon = 0 THEN 1 ELSE 0 END) as without_sales
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

    async def save_price_update(self, update: PriceUpdate):
        """Сохраняет обновлённую цену в базу данных"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    current_time = datetime.now(pytz.timezone('Europe/Moscow'))
                    await cur.execute(f"""
                    UPDATE {Config.PPODUCT_TABLE}
                    SET price_ozon = %s,
                        ozon_real_price = %s,
                        sku_ozon = %s,
                        last_price_update = %s
                    WHERE model_ozon = %s
                    """, (
                        update.new_price_ozon,
                        update.new_real_price,
                        update.sku_ozon,
                        current_time,
                        update.vendor_code
                    ))

                    await cur.execute(f"""
                    INSERT INTO {Config.OZON_PRICE_HISTORY_TABLE}
                    (product_id, vendor_code, old_price_ozon, new_price_ozon,
                    old_real_price, new_real_price, profit_correction, avg_finished_price,
                    discount, change_reason, status, created_at,
                    current_margin, target_margin, sales_count,
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
                    %s
                    FROM {Config.PPODUCT_TABLE} p
                    WHERE p.model_ozon = %s
                    """, (
                        update.vendor_code,
                        update.old_price_ozon,
                        update.new_price_ozon,
                        update.old_real_price,
                        update.new_real_price,
                        update.profit_correction,
                        update.new_real_price,
                        update.discount or 0,
                        update.reason,
                        update.status.value,
                        current_time,
                        update.current_profit,
                        update.target_profit,
                        update.sales_count,
                        update.purchase_price,
                        update.logistics_cost,
                        update.vendor_code,
                    ))
                    self.stats['prices_updated'] += 1
                    self.logger.info(
                        f"💾 Цена сохранена для {update.vendor_code}: "
                        f"{update.old_price_ozon:.0f}₽ → {update.new_price_ozon:.0f}₽ "
                        f"({update.new_price_ozon - update.old_price_ozon:+.0f}₽)"
                    )
                    print(update.new_price_ozon, update.new_price_ozon, update.seller_discount)
                    discount = update.discount if update.discount is not None else 0.0
                    new_real_price = round(update.new_price_ozon * (1 - discount / 100), 2)
                    print(new_real_price)

                    await self.db_logger.log(
                        level="SUCCESS",
                        message=f"💾 Ozon {update.vendor_code}: цена сохранена",
                        vendor_code=update.vendor_code,
                        sku_ozon=update.sku_ozon,
                        mp="ozon",
                        details={
                            "event": "price_saved",
                            "old_base_price": update.old_price_ozon,
                            "new_base_price": update.new_price_ozon,
                            "old_discounted_price": update.old_real_price,
                            "new_discounted_price": update.new_real_price,
                            "price_change": update.new_price_ozon - update.old_price_ozon,
                            "seller_discount": update.seller_discount,
                            "profit_before": update.current_profit,
                            "target_profit": update.target_profit,
                            "logistics_cost": update.logistics_cost,
                            "sales_count": update.sales_count,
                            "payout_ratio": update.avg_payout_ratio,
                            "vat_percent": update.vat_percent,
                            "vat_amount": update.vat_amount,
                            "price_without_vat": update.price_without_vat
                        }
                    )
        except Exception as e:
            error_msg = f"❌ Ошибка сохранения цены для {update.vendor_code}: {e}"
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())
            await self.db_logger.log(
                level="ERROR",
                message=error_msg,
                vendor_code=update.vendor_code,
                details={"error": str(e)}
            )

    async def upload_prices_to_ozon(self, updates: List[PriceUpdate]):
        """Отправляет обновлённые цены на Ozon"""
        if not Config.LOAD_PRICE_TO_OZON:
            self.logger.info("ℹ️ Отправка цен на Ozon отключена (LOAD_PRICE_TO_OZON=False)")
            return

        price_updates = []
        for update in updates:
            new_price = update.new_real_price
            price_updates.append({
                "offer_id": update.vendor_code,
                # "old_price": str(old_price_for_api)
                "price": str(round(new_price, 2)),
                "currency_code": "RUB"
            })
            self.logger.debug(f"Подготовлена цена для {update.vendor_code}: {new_price}")


        if not price_updates:
            self.logger.info("ℹ️ Нет данных для отправки на Ozon")
            return

        self._log_separator("ОТПРАВКА ЦЕН НА OZON", "🚀")
        self.logger.info(f"📤 Отправка {len(price_updates)} цен на Ozon API...")

        self.logger.info("⏳ Пауза 65 секунд (ограничение Ozon API: 1 запрос/мин)...")
        await asyncio.sleep(65)

        async with OzonAPI(Config.OZON_CLIENT_ID, Config.OZON_API_KEY, self.logger) as ozon:
            success = await ozon.update_prices(price_updates)

            if success:
                self.stats['prices_uploaded_to_ozon'] = len(price_updates)
                self.logger.info(f"✅ Успешно отправлено {len(price_updates)} цен на Ozon")

                await self.db_logger.log(
                    level="SUCCESS",
                    message=f"🚀 Ozon: отправлено {len(price_updates)} цен",
                    mp="ozon",
                    details={
                        "event": "prices_uploaded",
                        "count": len(price_updates),
                        "cycle_id": self.current_cycle,
                        "sample_offers": [u.vendor_code for u in updates[:5]]
                    }
                )
            else:
                self.logger.error("❌ Ошибка отправки цен на Ozon")

    async def worker(self, worker_id: int):
        """Воркер для обработки товаров в фоновом режиме"""
        try:
            self.logger.info(f"👷 Воркер #{worker_id} запущен")

            while self.is_running:
                try:
                    offer_id, orders, product = await asyncio.wait_for(
                        self.queue.get(),
                        timeout=1.0
                    )

                    self.logger.info(f"👷 Воркер #{worker_id} обрабатывает: {offer_id}")

                    update = await self.process_product(offer_id, orders, product)

                    if update.status == ProcessingStatus.SUCCESS:
                        await self.save_price_update(update)
                        self.successful_updates.append(update)
                        self.logger.info(f"👷 Воркер #{worker_id}: {offer_id} - УСПЕХ")
                    else:
                        self.logger.info(f"👷 Воркер #{worker_id}: {offer_id} - {update.status.value}")

                    self.stats[update.status.value] += 1
                    self.queue.task_done()

                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    self.logger.error(f"❌ Ошибка в воркере {worker_id}: {e}")
                    self.stats['error'] = self.stats.get('error', 0) + 1
                    await self.db_logger.log_error(
                        vendor_code=offer_id if 'offer_id' in locals() else None,
                        sku_ozon=product.sku_ozon if 'product' in locals() else None,
                        error=f"Ошибка в воркере #{worker_id}: {e}",
                        details={
                            "worker_id": worker_id,
                            "cycle_id": self.current_cycle
                        }
                    )

        except asyncio.CancelledError:
            self.logger.info(f"👷 Воркер {worker_id} остановлен")

    async def run_cycle(self):
        """Основной цикл обработки"""
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

        # Флаг для отслеживания, была ли ошибка/прерывание
        cycle_interrupted = False
        error_message = None

        try:
            if Config.PROMOTIONS_ENABLED:
                if self.promotion_manager and self.current_cycle % Config.PROMOTION_SYNC_CYCLE == 0:
                    self._log_separator("ПОЛУЧЕНИЕ АКЦИЙ OZON", "🎯")
                    await self.promotion_manager.sync_promotions_from_ozon()
                    await self.promotion_manager.check_and_create_ramp_plans()
                    stats = await self.promotion_manager.get_promotion_stats()
                    self.stats['promotions_scheduled'] = stats.get('pending_products', 0) + stats.get('ramping_products', 0)

                if self.promotion_manager:
                    updated, locked = await self.promotion_manager.execute_daily_price_ramp()
                    if updated > 0 or locked > 0:
                        self.logger.info(f"📈 Акции: повышено {updated}, заблокировано {locked}")

                if self.promotion_manager:
                    locked = await self.promotion_manager.update_promotion_locks()
                    self.stats['products_in_promotion'] = locked

            # Получаем заказы с Ozon
            self.logger.info(f"📥 Получение заказов за последние {Config.SALES_HOURS_FILTER} часов...")
            async with OzonAPI(Config.OZON_CLIENT_ID, Config.OZON_API_KEY, self.logger) as ozon:
                orders_data = await ozon.get_orders(hours_back=Config.SALES_HOURS_FILTER)

            if not orders_data:
                self._log_separator("ЦИКЛ ПРЕРВАН", "⚠️")
                self.logger.warning("⚠️ Нет данных о заказах")
                cycle_interrupted = True
                return

            # Группируем заказы по артикулам
            orders_by_offer = defaultdict(list)
            for order in orders_data:
                if order.offer_id:
                    orders_by_offer[order.offer_id].append(order)

            offer_ids = list(orders_by_offer.keys())
            self.logger.info(f"📊 Найдено {len(offer_ids)} уникальных артикулов в заказах")

            # Загружаем данные о товарах из БД
            product_map = await self.fetch_products_batch(offer_ids)

            if not product_map:
                self._log_separator("ЦИКЛ ПРЕРВАН", "⚠️")
                self.logger.warning("⚠️ Нет товаров для обработки")
                cycle_interrupted = True
                return

            # Формируем очередь задач
            self._log_separator("ФОРМИРОВАНИЕ ОЧЕРЕДИ ЗАДАЧ", "📋")
            queue_tasks = 0
            skipped_tasks = 0
            processed_vendor_codes = set()

            for offer_id in offer_ids:
                if offer_id in product_map:
                    product = product_map[offer_id]
                    await self.queue.put((offer_id, orders_by_offer[offer_id], product))
                    processed_vendor_codes.add(offer_id)
                    queue_tasks += 1

                    if queue_tasks <= 3:
                        self.logger.info(
                            f"📥 Добавлен: {offer_id} "
                            f"({len(orders_by_offer[offer_id])} заказов)"
                        )
                else:
                    skipped_tasks += 1

            self.logger.info(f"📊 В очередь добавлено: {queue_tasks} задач")
            if skipped_tasks > 0:
                self.logger.info(f"📊 Пропущено (нет в БД): {skipped_tasks} артикулов")

            # Запуск воркеров
            self._log_separator("ЗАПУСК ОБРАБОТКИ", "⚡")
            self.logger.info(f"👥 Запускаем {Config.WORKERS_COUNT} воркеров...")

            workers = [
                asyncio.create_task(self.worker(i))
                for i in range(Config.WORKERS_COUNT)
            ]

            self.logger.info("⏳ Ожидание завершения обработки...")
            await self.queue.join()

            # Остановка воркеров
            for worker_task in workers:
                worker_task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

            self.logger.info("✅ Обработка завершена")

            successful_vendor_codes = {update.vendor_code for update in self.successful_updates}
            await self.update_sales_flags(successful_vendor_codes)

            # Отправка цен на Ozon
            if Config.LOAD_PRICE_TO_OZON and self.successful_updates:
                await self.upload_prices_to_ozon(self.successful_updates)

            # Сбор дневной статистики
            try:
                should_collect = await self.should_collect_daily_stats()
                if should_collect:
                    self._log_separator("ЗАПУСК СБОРА ДНЕВНОЙ СТАТИСТИКИ", "📅")
                    await self.collect_daily_profit_stats(
                        orders_by_offer=orders_by_offer,
                        product_map=product_map
                    )
            except Exception as e:
                self.logger.error(f"❌ Ошибка сбора дневной статистики: {e}")

        except Exception as e:
            error_message = str(e)
            self.logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА в цикле: {e}")
            self.logger.error(traceback.format_exc())
            cycle_interrupted = True
            raise  # Пробрасываем ошибку дальше для обработки в run()

        finally:
            # ВСЕГДА сохраняем статистику, даже при прерывании или ошибке
            cycle_end = datetime.now()
            duration = (cycle_end - cycle_start).total_seconds()

            # Если цикл был прерван, обновляем stats для корректного логирования
            if cycle_interrupted:
                self.stats['cycle_interrupted'] = 1
                if error_message:
                    self.stats['cycle_error'] = error_message

            # Итоговая статистика (для логов)
            self._log_separator("СТАТИСТИКА ЦИКЛА", "📊")
            stats_data = {
                "Время выполнения": f"{duration:.1f} сек",
                "Статус": "ПРЕРВАН" if cycle_interrupted else "ЗАВЕРШЕН",
                "Всего артикулов": len(offer_ids) if 'offer_ids' in locals() else 0,
                "Товаров в БД": len(product_map) if 'product_map' in locals() else 0,
                "✅ Успешно обработано": self.stats.get('success', 0),
                "✅ Обновлено цен": self.stats.get('prices_updated', 0),
                "✅ Отправлено на Ozon": self.stats.get('prices_uploaded_to_ozon', 0),
                "🔒 В акциях (пропущено)": self.stats.get('products_in_promotion', 0),
                "⚠️ Пропущено (мало данных)": self.stats.get('skipped_no_data', 0),
                "⚠️ Пропущено (низкая цена)": self.stats.get('skipped_min_price', 0),
                "⚠️ Пропущено (мало изменений)": self.stats.get('skipped_min_change', 0),
                "❌ Ошибок": self.stats.get('error', 0),
                "🎯 Акций в работе": self.stats.get('promotions_scheduled', 0)
            }
            self._log_table("РЕЗУЛЬТАТЫ ОБРАБОТКИ", stats_data)

            if self.successful_updates and not cycle_interrupted:
                self._log_separator("ПРИМЕРЫ ИЗМЕНЕНИЙ", "📈")
                for update in self.successful_updates[:3]:
                    change = update.new_price_ozon - update.old_price_ozon
                    percent = (change / update.old_price_ozon * 100) if update.old_price_ozon > 0 else 0
                    self.logger.info(
                        f"   📈 {update.vendor_code}: "
                        f"{update.old_price_ozon:.0f}₽ → {update.new_price_ozon:.0f}₽ "
                        f"({change:+.0f}₽, {percent:+.1f}%)"
                    )

            # СОХРАНЯЕМ СТАТИСТИКУ В БД (всегда)
            await self.save_cycle_stats()

            # Логируем завершение цикла в db_logger
            await self.db_logger.log_cycle_end(
                cycle_id=self.current_cycle,
                stats={
                    "duration_sec": duration,
                    "status": "interrupted" if cycle_interrupted else "completed",
                    "offer_ids": len(offer_ids) if 'offer_ids' in locals() else 0,
                    "products_in_db": len(product_map) if 'product_map' in locals() else 0,
                    "success": self.stats.get('success', 0),
                    "prices_updated": self.stats.get('prices_updated', 0),
                    "prices_uploaded": self.stats.get('prices_uploaded_to_ozon', 0),
                    "products_in_promotion": self.stats.get('products_in_promotion', 0),
                    "skipped_no_data": self.stats.get('skipped_no_data', 0),
                    "skipped_min_change": self.stats.get('skipped_min_change', 0),
                    "errors": self.stats.get('error', 0),
                    "promotions_scheduled": self.stats.get('promotions_scheduled', 0),
                    "cycle_interrupted": 1 if cycle_interrupted else 0,
                    "error_message": error_message if error_message else None
                }
            )

    async def run(self):
        """Основной цикл работы системы"""
        self.is_running = True
        self.sync_update_system_running_status(True)
        cycle_count = 0

        try:
            while self.is_running:
                cycle_count += 1

                try:
                    await self.run_cycle()

                    if not self.is_running:
                        break

                    hours = Config.OZON_CYCLE_INTERVAL // 3600
                    minutes = (Config.OZON_CYCLE_INTERVAL % 3600) // 60

                    self._log_separator("ОЖИДАНИЕ СЛЕДУЮЩЕГО ЦИКЛА", "⏰")
                    self.logger.info(f"🕒 Следующий цикл через: {hours}ч {minutes}мин")
                    next_run = datetime.now() + timedelta(seconds=Config.OZON_CYCLE_INTERVAL)
                    self.logger.info(f"📅 Время следующего запуска: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")

                    # ✅ Разбиваем сон на маленькие интервалы для проверки is_running
                    sleep_interval = 60  # проверяем каждую минуту
                    slept = 0
                    while slept < Config.OZON_CYCLE_INTERVAL and self.is_running:
                        await asyncio.sleep(min(sleep_interval, Config.OZON_CYCLE_INTERVAL - slept))
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
                        sku_ozon=None,
                        error=f"Фатальная ошибка: {e}",
                        trace=traceback.format_exc(),
                        details={
                            "cycle_id": self.current_cycle,
                            "phase": "main_loop",
                            "cycle_count": cycle_count
                        }
                    )

                    wait_time = min(300 * (2 ** (cycle_count % 5)), 3600)
                    self.logger.info(f"⏸️ Пауза {wait_time} сек перед повторной попыткой...")

                    # ✅ Разбиваем паузу на маленькие интервалы
                    sleep_interval = 60
                    slept = 0
                    while slept < wait_time and self.is_running:
                        await asyncio.sleep(min(sleep_interval, wait_time - slept))
                        slept += sleep_interval

        finally:
            # ✅ Гарантированно обновляем статус при любом выходе
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
            message="🔚 Ozon: завершение работы системы",
            mp="ozon",
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

        if self.promotion_manager:
            self.logger.info("✅ Менеджер акций остановлен")

        self.logger.info("✅ Очистка завершена")
        self._log_separator("РАБОТА ЗАВЕРШЕНА", "🏁")