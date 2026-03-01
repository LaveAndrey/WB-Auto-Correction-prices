#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Менеджер акций Ozon - повышение перед акцией + блокировка во время акции
Версия: 7.1 (ИСПРАВЛЕННАЯ РАБОЧАЯ)
"""
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import aiomysql
import pytz
from OZON_integration.structures.dataclass import OzonPromotion, PromotionProduct
from OZON_integration.api.ozon_api_client import OzonAPI
from config import Config
import asyncio

class OzonPromotionManager:
    """Управление акциями Ozon - повышение перед акцией + блокировка во время"""

    def __init__(self, db_pool, logger, db_logger=None):
        self.db_pool = db_pool
        self.logger = logger
        self.active_promotions: Dict[int, OzonPromotion] = {}
        self.ramp_products: Dict[str, PromotionProduct] = {}
        self.processed_today: set = set()
        self.db_logger = db_logger

    async def initialize(self):
        """Инициализация менеджера акций"""
        self.logger.debug("🎯 Инициализация менеджера акций Ozon...")
        await self._ensure_table_exists()
        await self._ensure_product_fields_exist()
        await self._load_active_promotions()
        await self._load_ramp_products()

    async def _ensure_table_exists(self):
        """Создаёт таблицу для акций"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {Config.OZON_PROMOTION_TABLE} (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        promotion_id INT NOT NULL,
                        vendor_code VARCHAR(100) NOT NULL DEFAULT '',
                        product_id INT,
                        sku_ozon INT,
                        promotion_title VARCHAR(255),
                        discount_value DECIMAL(5,2),
                        date_start DATETIME,
                        date_end DATETIME,
                        freeze_date DATETIME,
                        base_price DECIMAL(10,2),
                        target_price DECIMAL(10,2),
                        current_price DECIMAL(10,2),
                        action_price DECIMAL(10,2),
                        max_action_price DECIMAL(10,2),
                        daily_increase DECIMAL(10,2),
                        ramp_day INT DEFAULT 0,
                        ramp_start_date DATETIME,
                        status VARCHAR(20) DEFAULT 'pending',
                        active_promotion_id INT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY unique_product_promotion (vendor_code, promotion_id),
                        INDEX idx_promotion_id (promotion_id),
                        INDEX idx_vendor_code (vendor_code),
                        INDEX idx_status (status),
                        INDEX idx_date_start (date_start),
                        INDEX idx_date_end (date_end)
                    )
                    """)
            self.logger.debug(f"✅ Таблица {Config.OZON_PROMOTION_TABLE} создана/проверена")
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания таблицы: {e}")

    async def _ensure_product_fields_exist(self):
        """Проверяет наличие полей для блокировки в таблице products"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # Проверяем поле promotion_active
                    await cursor.execute("""
                        SELECT COUNT(*) 
                        FROM information_schema.columns 
                        WHERE table_name = %s 
                        AND column_name = 'promotion_active'
                    """, (Config.PPODUCT_TABLE,))

                    if (await cursor.fetchone())[0] == 0:
                        await cursor.execute(f"""
                            ALTER TABLE {Config.PPODUCT_TABLE} 
                            ADD COLUMN promotion_active TINYINT(1) DEFAULT 0
                        """)
                        self.logger.debug(f"✅ Добавлено поле promotion_active")

                    # Проверяем поле promotion_lock_until
                    await cursor.execute("""
                        SELECT COUNT(*) 
                        FROM information_schema.columns 
                        WHERE table_name = %s 
                        AND column_name = 'promotion_lock_until'
                    """, (Config.PPODUCT_TABLE,))

                    if (await cursor.fetchone())[0] == 0:
                        await cursor.execute(f"""
                            ALTER TABLE {Config.PPODUCT_TABLE} 
                            ADD COLUMN promotion_lock_until DATETIME NULL
                        """)
                        self.logger.debug(f"✅ Добавлено поле promotion_lock_until")
        except Exception as e:
            self.logger.error(f"❌ Ошибка проверки полей: {e}")

    async def _load_active_promotions(self):
        """Загружает акции из БД"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute(f"""
                    SELECT DISTINCT promotion_id, promotion_title, discount_value,
                           date_start, date_end, freeze_date
                    FROM {Config.OZON_PROMOTION_TABLE}
                    WHERE date_end > NOW() OR date_end IS NULL
                    """)
                    rows = await cursor.fetchall()
                    for row in rows:
                        self.active_promotions[row['promotion_id']] = OzonPromotion(
                            promotion_id=row['promotion_id'],
                            title=row['promotion_title'] or '',
                            action_type='',
                            date_start=row['date_start'],
                            date_end=row['date_end'],
                            freeze_date=row['freeze_date'],
                            discount_value=float(row['discount_value'] or 0)
                        )
            self.logger.debug(f"✅ Загружено {len(self.active_promotions)} акций из БД")
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки акций: {e}")

    async def _load_ramp_products(self):
        """Загружает товары в процессе повышения"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute(f"""
                    SELECT vendor_code, promotion_id, product_id, sku_ozon, base_price,
                           target_price, current_price, daily_increase, ramp_day, 
                           ramp_start_date, status, date_end as promotion_end_date
                    FROM {Config.OZON_PROMOTION_TABLE}
                    WHERE status IN ('ramping', 'pending', 'active')
                    """)
                    rows = await cursor.fetchall()
                    for row in rows:
                        if row['vendor_code']:
                            self.ramp_products[row['vendor_code']] = PromotionProduct(
                                vendor_code=row['vendor_code'],
                                promotion_id=row['promotion_id'],
                                product_id=row['product_id'] or 0,
                                sku_ozon=row['sku_ozon'] or 0,
                                base_price=float(row['base_price'] or 0),
                                target_price=float(row['target_price'] or 0),
                                current_price=float(row['current_price'] or 0),
                                action_price=0,
                                max_action_price=0,
                                daily_increase=float(row['daily_increase'] or 0),
                                ramp_day=int(row['ramp_day'] or 0),
                                ramp_start_date=row['ramp_start_date'],
                                status=row['status'],
                                promotion_end_date=row['promotion_end_date'],
                                active_promotion_id=row['promotion_id']
                            )
            self.logger.info(f"✅ Загружено {len(self.ramp_products)} товаров в работе")
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки товаров: {e}")

    # ============================================================
    # 1️⃣ ПОЛУЧЕНИЕ АКЦИЙ ИЗ OZON API
    # ============================================================

    async def sync_promotions_from_ozon(self):
        """Синхронизирует акции из Ozon API"""
        self.logger.debug("🔄 Получение акций с Ozon API...")

        async with OzonAPI(Config.OZON_CLIENT_ID, Config.OZON_API_KEY, self.logger) as ozon:
            promotions = await ozon.get_promotions()

            self.logger.info(f"✅ Получено {len(promotions)} акций от Ozon")

            saved_count = 0
            for promo in promotions:
                if promo.date_start:
                    self.active_promotions[promo.promotion_id] = promo
                    await self._save_promotion_to_db(promo)
                    saved_count += 1

                    days_until = promo.days_until_start
                    if days_until > 0:
                        self.logger.info(f"📅 Будущая акция: {promo.title}, через {days_until} дней")
                    elif promo.is_active:
                        self.logger.info(f"🔒 Активная акция: {promo.title} (идёт сейчас)")

    async def _save_promotion_to_db(self, promo: OzonPromotion):
        """Сохраняет акцию в БД"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    date_start = promo.date_start.replace(
                        tzinfo=None) if promo.date_start and promo.date_start.tzinfo else promo.date_start
                    date_end = promo.date_end.replace(
                        tzinfo=None) if promo.date_end and promo.date_end.tzinfo else promo.date_end
                    freeze_date = promo.freeze_date.replace(
                        tzinfo=None) if promo.freeze_date and promo.freeze_date.tzinfo else promo.freeze_date

                    await cursor.execute(f"""
                    INSERT INTO {Config.OZON_PROMOTION_TABLE} 
                    (promotion_id, vendor_code, promotion_title, discount_value,
                     date_start, date_end, freeze_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    promotion_title=VALUES(promotion_title),
                    discount_value=VALUES(discount_value),
                    date_start=VALUES(date_start),
                    date_end=VALUES(date_end),
                    freeze_date=VALUES(freeze_date),
                    updated_at=CURRENT_TIMESTAMP
                    """, (
                        promo.promotion_id,
                        '',
                        promo.title,
                        promo.discount_value,
                        date_start,
                        date_end,
                        freeze_date
                    ))
        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения акции {promo.promotion_id}: {e}")

    # ============================================================
    # 2️⃣ РАСЧЁТ БЕЗОПАСНОГО ПОВЫШЕНИЯ
    # ============================================================

    def _calculate_safe_increase(self, current_price: float, days_until_start: int) -> Dict:
        """
        Рассчитывает безопасное повышение цены перед акцией
        """
        if days_until_start <= 0:
            return {
                'can_increase': False,
                'reason': 'Акция уже началась',
                'target_price': current_price,
                'daily_increase': 0
            }

        # Параметры из конфига
        target_total_percent = Config.PROMOTION_DESIRED_TOTAL_PERCENT  # 10%
        target_daily_percent = Config.PROMOTION_DESIRED_DAILY_PERCENT  # 2%
        safe_daily_fixed = Config.PROMOTION_SAFE_DAILY_FIXED  # 50₽

        # Желаемое повышение за весь период
        desired_total_rub = current_price * (target_total_percent / 100)

        # Дневной лимит
        daily_limit = min(
            current_price * (target_daily_percent / 100),
            safe_daily_fixed
        )

        # Лимит по времени
        max_by_days = daily_limit * days_until_start
        total_increase = min(desired_total_rub, max_by_days)

        # Ежедневный шаг
        if days_until_start > 0 and total_increase > 0:
            daily_increase = total_increase / days_until_start
            daily_increase = min(daily_increase, daily_limit)
        else:
            daily_increase = 0

        return {
            'can_increase': True,
            'total_increase': round(total_increase, 2),
            'daily_increase': round(daily_increase, 2),
            'target_price': round(current_price + total_increase, 2),
            'total_percent': round((total_increase / current_price) * 100, 2) if current_price > 0 else 0,
            'daily_percent': round((daily_increase / current_price) * 100,
                                   2) if current_price > 0 and daily_increase > 0 else 0,
            'days_until': days_until_start
        }

    # ============================================================
    # 3️⃣ СОЗДАНИЕ ПЛАНОВ ПОВЫШЕНИЯ ДЛЯ БУДУЩИХ АКЦИЙ
    # ============================================================

    async def check_and_create_ramp_plans(self):
        """
        Проверяет БУДУЩИЕ акции и создаёт планы повышения
        """
        self.logger.debug("🔍 Проверка будущих акций для создания планов повышения...")

        now = datetime.now(pytz.timezone('Europe/Moscow'))
        now_naive = now.replace(tzinfo=None)
        created_count = 0

        for promo_id, promo in self.active_promotions.items():
            if not promo.date_start:
                continue

            promo_start = promo.date_start.replace(tzinfo=None) if promo.date_start.tzinfo else promo.date_start
            days_until = (promo_start - now_naive).days

            # 👇 ТОЛЬКО БУДУЩИЕ АКЦИИ (ещё не начались)
            if days_until <= 0:
                continue

            # Проверяем, не заморожена ли акция
            if promo.is_frozen:
                self.logger.info(f"❄️ Акция {promo.title} заморожена")
                continue

            self.logger.info(f"📅 Будущая акция: {promo.title}, через {days_until} дней")
            await self._process_promotion_candidates(promo_id, days_until)
            created_count += 1

        self.logger.info(f"✅ Создано планов для {created_count} будущих акций")

    async def _process_promotion_candidates(self, promotion_id: int, days_until_start: int):
        """
        Получает кандидатов из API и создаёт планы повышения
        """
        if promotion_id not in self.active_promotions:
            return

        promo = self.active_promotions[promotion_id]

        async with OzonAPI(Config.OZON_CLIENT_ID, Config.OZON_API_KEY, self.logger) as ozon:
            candidates = await ozon.get_promotion_candidates(promotion_id)

            if not candidates:
                self.logger.info(f"ℹ️ Нет кандидатов для акции {promo.title}")
                return

            self.logger.info(f"📦 Получено {len(candidates)} кандидатов")

            # Получаем список product_id кандидатов
            candidate_ids = [c['product_id'] for c in candidates if c.get('product_id')]

            if not candidate_ids:
                return

            # Проверяем, какие из этих товаров есть в нашей БД
            our_products = await self._get_products_from_db(candidate_ids)

            if not our_products:
                self.logger.info(f"ℹ️ Нет наших товаров среди кандидатов")
                return

            self.logger.info(f"📊 Найдено {len(our_products)} наших товаров среди кандидатов")

            # Создаём план для каждого нашего товара
            for product in our_products:
                await self._create_ramp_plan(
                    vendor_code=product['model_ozon'],
                    sku_ozon=product['sku_ozon'],
                    current_price=float(product['price_ozon'] or 0),
                    promo=promo,
                    days_until_start=days_until_start
                )

    async def _get_products_from_db(self, sku_list: List[int]) -> List[Dict]:
        """Получает товары из БД по списку sku_ozon"""
        if not sku_list:
            return []

        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    placeholders = ','.join(['%s'] * len(sku_list))
                    await cursor.execute(f"""
                    SELECT model_ozon, sku_ozon, price_ozon, ozon_real_price
                    FROM {Config.PPODUCT_TABLE}
                    WHERE sku_ozon IN ({placeholders})
                    AND status = 1
                    AND price_ozon > 0
                    """, sku_list)

                    return await cursor.fetchall()
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения товаров из БД: {e}")
            return []

    async def _create_ramp_plan(self, vendor_code: str, sku_ozon: int,
                                current_price: float, promo: OzonPromotion,
                                days_until_start: int):
        """Создаёт план повышения для одного товара"""


        if current_price < Config.PROMOTION_MIN_PRICE:
            return

        if vendor_code in self.ramp_products:
            return

        plan = self._calculate_safe_increase(current_price, days_until_start)

        if not plan['can_increase']:
            return

        self.logger.info(f"✅ {vendor_code}: план повышения")
        self.logger.info(f"   📊 {current_price:.0f}₽ → {plan['target_price']:.0f}₽")
        self.logger.info(f"   📈 +{plan['daily_increase']:.0f}₽/день ({plan['daily_percent']}%)")

        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"""
                    INSERT INTO {Config.OZON_PROMOTION_TABLE}
                    (promotion_id, vendor_code, product_id, sku_ozon, promotion_title,
                     date_start, date_end, base_price, target_price, current_price,
                     daily_increase, ramp_start_date, ramp_day, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    target_price = VALUES(target_price),
                    daily_increase = VALUES(daily_increase),
                    status = VALUES(status),
                    updated_at = CURRENT_TIMESTAMP
                    """, (
                        promo.promotion_id,
                        vendor_code,
                        sku_ozon,
                        sku_ozon,
                        promo.title,
                        promo.date_start.replace(tzinfo=None) if promo.date_start else None,
                        promo.date_end.replace(tzinfo=None) if promo.date_end else None,
                        current_price,
                        plan['target_price'],
                        current_price,
                        plan['daily_increase'],
                        datetime.now().replace(tzinfo=None),
                        0,
                        'pending'
                    ))

            if self.db_logger:
                await self.db_logger.log_promotion_ozon(
                    vendor_code=vendor_code,
                    sku_ozon=sku_ozon,
                    promotion_id=promo.promotion_id,
                    promotion_title=promo.title,
                    action='ramp_start',
                    old_price=current_price,
                    new_price=plan['target_price'],
                    daily_increase=plan['daily_increase'],
                    days_until_start=days_until_start,
                    details={
                        "base_price": current_price,
                        "target_price": plan['target_price'],
                        "daily_percent": plan.get('daily_percent', 0)
                    }
                )

            self.ramp_products[vendor_code] = PromotionProduct(
                vendor_code=vendor_code,
                promotion_id=promo.promotion_id,
                product_id=sku_ozon,
                sku_ozon=sku_ozon,
                base_price=current_price,
                target_price=plan['target_price'],
                current_price=current_price,
                action_price=0,
                max_action_price=0,
                daily_increase=plan['daily_increase'],
                ramp_day=0,
                ramp_start_date=datetime.now(),
                status='pending',
                promotion_end_date=promo.date_end,
                active_promotion_id=promo.promotion_id
            )

        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения плана {vendor_code}: {e}")

    # ============================================================
    # 4️⃣ ЕЖЕДНЕВНОЕ ПОВЫШЕНИЕ ЦЕН + БЛОКИРОВКА/РАЗБЛОКИРОВКА
    # ============================================================

    async def execute_daily_price_ramp(self):
        """
        ЕЖЕДНЕВНОЕ ПОВЫШЕНИЕ ЦЕН + БЛОКИРОВКА/РАЗБЛОКИРОВКА
        Запускается каждый цикл
        """
        now = datetime.now(pytz.timezone('Europe/Moscow'))
        now_naive = now.replace(tzinfo=None)

        self.logger.debug("📈 Выполнение ежедневных операций с акциями...")

        updated_count = 0
        locked_count = 0
        unlocked_count = 0
        self.processed_today = set()

        for vendor_code, plan in list(self.ramp_products.items()):
            if vendor_code in self.processed_today:
                continue

            promo = self.active_promotions.get(plan.promotion_id)
            if not promo:
                continue

            # ===== 1️⃣ ПРОВЕРЯЕМ ОКОНЧАНИЕ АКЦИИ =====
            if plan.promotion_end_date and now_naive > plan.promotion_end_date.replace(tzinfo=None):
                self.logger.info(f"🔓 {vendor_code}: акция завершена, разблокируем и возвращаем цену")
                await self._unlock_product(vendor_code)
                if Config.PROMOTION_PRICE_RESTORE:
                    await self._restore_product_price(vendor_code, plan.base_price)
                plan.status = 'completed'
                unlocked_count += 1
                continue

            # ===== 2️⃣ ПРОВЕРЯЕМ НАЧАЛО АКЦИИ =====
            if promo.is_active:
                if plan.status != 'active':
                    self.logger.info(f"🔒 {vendor_code}: акция началась, блокируем цену")
                    await self._lock_product(vendor_code, plan.promotion_end_date)
                    plan.status = 'active'
                    locked_count += 1
                continue

            # ===== 3️⃣ ЕСЛИ АКЦИЯ ЕЩЁ НЕ НАЧАЛАСЬ - ПОВЫШАЕМ ЦЕНУ =====
            if plan.status in ['pending', 'ramping']:
                old_price = plan.current_price
                new_price = round(old_price + plan.daily_increase, 2)

                if new_price > plan.target_price:
                    new_price = plan.target_price
                    plan.status = 'ramping'

                await self._update_product_price(vendor_code, new_price)

                if self.db_logger:
                    await self.db_logger.log_promotion_ozon(
                        vendor_code=vendor_code,
                        sku_ozon=plan.sku_ozon,
                        promotion_id=plan.promotion_id,
                        promotion_title=promo.title,
                        action='price_increased',
                        old_price=old_price,
                        new_price=new_price,
                        daily_increase=plan.daily_increase,
                        details={
                            "ramp_day": plan.ramp_day,
                            "target_price": plan.target_price
                        }
                    )

                plan.ramp_day += 1
                plan.current_price = new_price
                self.processed_today.add(vendor_code)

                if new_price >= plan.target_price:
                    plan.status = 'ramping'

                await self._update_ramp_plan(plan)

                self.logger.info(f"⬆️ {vendor_code}: {old_price:.0f}₽ → {new_price:.0f}₽")
                updated_count += 1

        self.logger.debug(
            f"✅ Повышено: {updated_count} | Заблокировано: {locked_count} | Разблокировано: {unlocked_count}")
        return updated_count, locked_count

    # ============================================================
    # 5️⃣ МЕТОДЫ БЛОКИРОВКИ/РАЗБЛОКИРОВКИ
    # ============================================================

    async def _lock_product(self, vendor_code: str, lock_until: Optional[datetime]):
        """Блокирует товар от изменений на время акции"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"""
                    UPDATE {Config.PPODUCT_TABLE}
                    SET promotion_active = 1,
                        promotion_lock_until = %s
                    WHERE model_ozon = %s
                    """, (lock_until.replace(tzinfo=None) if lock_until else None, vendor_code))

            if self.db_logger and vendor_code in self.ramp_products:
                plan = self.ramp_products[vendor_code]
                promo = self.active_promotions.get(plan.promotion_id)
                if promo:
                    await self.db_logger.log_promotion_ozon(
                        vendor_code=vendor_code,
                        sku_ozon=plan.sku_ozon,
                        promotion_id=plan.promotion_id,
                        promotion_title=promo.title,
                        action='locked',
                        old_price=plan.current_price,
                        details={"lock_until": str(lock_until) if lock_until else None}
                    )

            self.logger.debug(f"🔒 {vendor_code}: заблокирован до {lock_until}")
        except Exception as e:
            self.logger.error(f"❌ Ошибка блокировки {vendor_code}: {e}")

    async def _unlock_product(self, vendor_code: str):
        """Снимает блокировку с товара после акции"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"""
                    UPDATE {Config.PPODUCT_TABLE}
                    SET promotion_active = 0,
                        promotion_lock_until = NULL
                    WHERE model_ozon = %s
                    """, (vendor_code,))

            if self.db_logger and vendor_code in self.ramp_products:
                plan = self.ramp_products[vendor_code]
                promo = self.active_promotions.get(plan.promotion_id)
                if promo:
                    await self.db_logger.log_promotion_ozon(
                        vendor_code=vendor_code,
                        sku_ozon=plan.sku_ozon,
                        promotion_id=plan.promotion_id,
                        promotion_title=promo.title,
                        action='unlocked'
                    )

            self.logger.debug(f"🔓 {vendor_code}: разблокирован")
        except Exception as e:
            self.logger.error(f"❌ Ошибка разблокировки {vendor_code}: {e}")

    async def update_promotion_locks(self):
        """
        Обновляет блокировки для всех активных акций
        Вызывается отдельно для синхронизации
        """
        self.logger.debug("🔒 Синхронизация блокировок активных акций...")

        locked_count = 0
        now = datetime.now(pytz.timezone('Europe/Moscow'))
        now_naive = now.replace(tzinfo=None)

        for vendor_code, plan in self.ramp_products.items():
            if plan.status != 'active':
                continue

            promo = self.active_promotions.get(plan.promotion_id)
            if not promo:
                continue

            # Если акция всё ещё активна - обновляем блокировку
            if promo.is_active:
                await self._lock_product(vendor_code, plan.promotion_end_date)
                locked_count += 1
            # Если акция уже закончилась - разблокируем
            elif plan.promotion_end_date and now_naive > plan.promotion_end_date.replace(tzinfo=None):
                await self._unlock_product(vendor_code)
                plan.status = 'completed'

        self.logger.info(f"✅ Заблокировано: {locked_count}")
        return locked_count

    # ============================================================
    # 6️⃣ ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ
    # ============================================================

    async def _update_product_price(self, vendor_code: str, new_price: float):
        """Обновляет цену товара"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"""
                    UPDATE {Config.PPODUCT_TABLE}
                    SET price_ozon = %s,
                        ozon_real_price = %s,
                        last_price_update = NOW()
                    WHERE model_ozon = %s
                    """, (new_price, new_price, vendor_code))
        except Exception as e:
            self.logger.error(f"❌ Ошибка обновления цены {vendor_code}: {e}")

    async def _restore_product_price(self, vendor_code: str, base_price: float):
        """Возвращает цену после акции"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"""
                    UPDATE {Config.PPODUCT_TABLE}
                    SET price_ozon = %s,
                        ozon_real_price = %s,
                        last_price_update = NOW()
                    WHERE model_ozon = %s
                    """, (base_price, base_price, vendor_code))

            if self.db_logger and vendor_code in self.ramp_products:
                plan = self.ramp_products[vendor_code]
                promo = self.active_promotions.get(plan.promotion_id)
                if promo:
                    await self.db_logger.log_promotion_ozon(
                        vendor_code=vendor_code,
                        sku_ozon=plan.sku_ozon,
                        promotion_id=plan.promotion_id,
                        promotion_title=promo.title,
                        action='restored',
                        old_price=plan.current_price,
                        new_price=base_price
                    )
        except Exception as e:
            self.logger.error(f"❌ Ошибка возврата цены {vendor_code}: {e}")

    async def _update_ramp_plan(self, plan: PromotionProduct):
        """Обновляет план в БД"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"""
                    UPDATE {Config.OZON_PROMOTION_TABLE}
                    SET current_price = %s,
                        ramp_day = %s,
                        status = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE vendor_code = %s AND promotion_id = %s
                    """, (
                        plan.current_price,
                        plan.ramp_day,
                        plan.status,
                        plan.vendor_code,
                        plan.promotion_id
                    ))
        except Exception as e:
            self.logger.error(f"❌ Ошибка обновления плана {plan.vendor_code}: {e}")

    async def check_overlapping_promotions(self):
        """Заглушка"""
        pass

    async def get_promotion_stats(self) -> Dict:
        """Статистика по акциям"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute(f"""
                    SELECT 
                        COUNT(DISTINCT promotion_id) as total_promotions,
                        SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_products,
                        SUM(CASE WHEN status = 'ramping' THEN 1 ELSE 0 END) as ramping_products,
                        SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) as active_products,
                        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_products
                    FROM {Config.OZON_PROMOTION_TABLE}
                    WHERE status IN ('pending', 'ramping', 'active', 'completed')
                    """)
                    stats = await cursor.fetchone()

                    return {
                        'total_promotions': stats['total_promotions'] or 0,
                        'pending_products': stats['pending_products'] or 0,
                        'ramping_products': stats['ramping_products'] or 0,
                        'active_products': stats['active_products'] or 0,
                        'completed_products': stats['completed_products'] or 0
                    }
        except Exception as e:
            self.logger.error(f"❌ Ошибка статистики: {e}")
            return {}

    async def sync_promotion_status(self) -> int:
        """
        Синхронизирует статус участия товаров в акциях через API.
        Обновляет поле promotion_active в таблице products.
        Возвращает количество обновлённых товаров.
        """
        self.logger.debug("🔄 Синхронизация статуса участия товаров в акциях...")

        # Получаем все активные товары (status=1, target_profit>0)
        async with self.db_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(f"""
                    SELECT model_ozon
                    FROM {Config.PPODUCT_TABLE}
                    WHERE status = 1 AND target_profit_rub > 0
                """)
                products = await cursor.fetchall()

        if not products:
            return 0

        offer_ids = [p['model_ozon'] for p in products if p['model_ozon']]
        batch_size = 100
        total_updated = 0

        async with OzonAPI(Config.OZON_CLIENT_ID, Config.OZON_API_KEY, self.logger) as ozon:
            for i in range(0, len(offer_ids), batch_size):
                batch = offer_ids[i:i + batch_size]
                status_map = await ozon.get_products_promotion_status(batch)

                if not status_map:
                    continue

                # Формируем данные для массового обновления
                update_data = []
                for offer_id, status in status_map.items():
                    in_promotion = 1 if status['in_promotion'] else 0
                    update_data.append((in_promotion, offer_id))

                if update_data:
                    async with self.db_pool.acquire() as conn:
                        async with conn.cursor() as cursor:
                            # Обновляем только те товары, у которых нет активной блокировки или она истекла
                            await cursor.executemany(f"""
                                UPDATE {Config.PPODUCT_TABLE}
                                SET promotion_active = %s
                                WHERE model_ozon = %s
                                  AND (promotion_lock_until IS NULL OR promotion_lock_until < NOW())
                            """, update_data)
                            total_updated += cursor.rowcount

                    self.logger.debug(f"✅ Обработан батч {i // batch_size + 1}, обновлено {len(update_data)} товаров")
                    await asyncio.sleep(0.5)

        self.logger.info(f"✅ Статус акций обновлён для {total_updated} товаров")
        return total_updated