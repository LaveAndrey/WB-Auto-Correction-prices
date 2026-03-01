import asyncio
import json
import aiohttp
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple, Set
from collections import defaultdict
from dataclasses import dataclass, field
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from config import Config
from WB_integration.structurs.dataclass import ProductData
from WB_integration.utils.logging_utils import log_separator, log_table
import aiomysql
import pytz
from aiolimiter import AsyncLimiter


@dataclass
class WBPromotion:
    """Структура акции WB"""
    promotion_id: int
    title: str
    date_start: datetime
    date_end: datetime
    discount_value: float = 0.0
    type: str = 'auto'

    @property
    def is_active(self) -> bool:
        """Проверяет, активна ли акция сейчас"""
        now = datetime.now(pytz.UTC)
        return self.date_start <= now <= self.date_end

    @property
    def days_until_start(self) -> int:
        """Дней до начала акции"""
        now = datetime.now(pytz.UTC)
        if now < self.date_start:
            return (self.date_start - now).days
        return -1

    @property
    def is_frozen(self) -> bool:
        """Заморожена ли акция (нельзя менять цены)"""
        now = datetime.now(pytz.UTC)
        freeze_before_days = 1  # Заморозка за 1 день до акции
        freeze_start = self.date_start - timedelta(days=freeze_before_days)
        return freeze_start <= now <= self.date_end


@dataclass
class WBPromotionProduct:
    """Товар, участвующий в акции WB"""
    vendor_code: str
    nm_id: int
    promotion_id: int
    base_price: float  # Исходная цена до повышения
    target_price: float  # Целевая цена (до которой повышаем)
    current_price: float  # Текущая цена
    daily_increase: float  # Ежедневное повышение
    ramp_day: int = 0  # День повышения (счетчик)
    ramp_start_date: Optional[datetime] = None  # Дата начала повышения
    status: str = 'pending'  # pending, ramping, active, completed
    promotion_end_date: Optional[datetime] = None
    active_promotion_id: Optional[int] = None  # ID активной акции (для блокировки)


class WBPromotionsClient:
    """
    Клиент для работы с календарем акций Wildberries
    """

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.logger = None
        self.base_url = "https://dp-calendar-api.wildberries.ru/api/v1/calendar"

        # Rate limiting: 10 запросов за 6 секунд
        self.rate_limiter = AsyncLimiter(max_rate=10, time_period=6)

        # Кэш акций (TTL 1 час)
        self.promotions_cache = []
        self.cache_timestamp = None
        self.CACHE_TTL_SECONDS = 3600

    def set_logger(self, logger):
        self.logger = logger

    def _log_separator(self, title: str = " ", char: str = "= ", length: int = 80):
        if self.logger:
            log_separator(self.logger, title, char, length)

    def _log_table(self, title: str, data: dict):
        if self.logger:
            log_table(self.logger, title, data)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def get_promotions(
            self,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
            all_promo: bool = False,
            limit: int = 1000,
            offset: int = 0
    ) -> List[Dict]:
        """Получение ВСЕХ акций за период"""
        if start_date is None:
            start_date = datetime.now(timezone.utc)
        if end_date is None:
            end_date = start_date + timedelta(days=Config.PROMOTION_RAMP_DAYS)

        start_str = start_date.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str = end_date.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        url = f"{self.base_url}/promotions"
        headers = {"Authorization": Config.WB_PRICES_TOKEN}

        all_promotions = []
        current_offset = offset

        while True:
            params = {
                "startDateTime": start_str,
                "endDateTime": end_str,
                "allPromo": str(all_promo).lower(),
                "limit": limit,
                "offset": current_offset
            }

            if self.logger:
                period_days = (end_date - start_date).days
                self.logger.info(
                    f"📅 Запрос акций: {start_str} → {end_str} "
                    f"(период: {period_days} дней, allPromo={all_promo}, offset={current_offset})"
                )

            try:
                async with self.rate_limiter:
                    async with self.session.get(url, headers=headers, params=params) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            promotions = data.get("data", {}).get("promotions", [])
                            total = data.get("data", {}).get("total", len(promotions))

                            all_promotions.extend(promotions)

                            if self.logger:
                                self.logger.info(
                                    f"✅ Получено акций: {len(promotions)} (всего: {len(all_promotions)} из {total})"
                                )

                            if len(promotions) < limit or len(all_promotions) >= total:
                                break

                            current_offset += limit

                        elif resp.status == 401:
                            if self.logger:
                                self.logger.error("❌ 401: Неверный токен WB_PRICES_TOKEN")
                            return []
                        else:
                            text = await resp.text()
                            if self.logger:
                                self.logger.error(f"❌ API ошибка {resp.status}: {text[:300]}")
                            resp.raise_for_status()

            except aiohttp.ClientResponseError as e:
                if e.status == 401:
                    return []
                raise
            except Exception as e:
                if self.logger:
                    self.logger.error(f"❌ Исключение при запросе акций: {e}")
                raise

        return all_promotions

    async def get_upcoming_promotions(self) -> List[WBPromotion]:
        """
        Получает все предстоящие акции (включая автоакции)
        """
        # Проверка кэша
        now = datetime.now(pytz.UTC)
        if self.promotions_cache and self.cache_timestamp:
            cache_age = (now - self.cache_timestamp).total_seconds()
            if cache_age < self.CACHE_TTL_SECONDS:
                if self.logger:
                    self.logger.info(f"📋 Используем кэш акций (возраст: {cache_age:.0f} сек)")
                all_promotions = self.promotions_cache
            else:
                all_promotions = await self.get_promotions(
                    now,
                    now + timedelta(days=Config.PROMOTION_RAMP_DAYS),
                    all_promo=False  # 👈 Берем ВСЕ акции, не только auto
                )
                self.promotions_cache = all_promotions
                self.cache_timestamp = now
        else:
            all_promotions = await self.get_promotions(
                now,
                now + timedelta(days=Config.PROMOTION_RAMP_DAYS),
                all_promo=False
            )
            self.promotions_cache = all_promotions
            self.cache_timestamp = now

        upcoming = []
        for promo in all_promotions:
            try:
                promo_start = datetime.strptime(promo['startDateTime'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC)
                promo_end = datetime.strptime(promo['endDateTime'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC)
            except (KeyError, ValueError) as e:
                if self.logger:
                    self.logger.warning(f"⚠️ Пропущена акция с некорректной датой: {e}")
                continue

            # Только предстоящие
            if promo_start > now:
                upcoming.append(WBPromotion(
                    promotion_id=promo.get('id', 0),
                    title=promo.get('name', ''),
                    date_start=promo_start,
                    date_end=promo_end,
                    discount_value=float(promo.get('discount', 0)),
                    type=promo.get('type', 'auto')
                ))

        if self.logger and upcoming:
            self._log_separator("ПРЕДСТОЯЩИЕ АКЦИИ WB", "🎯")
            for i, promo in enumerate(upcoming[:10], 1):
                days_until = promo.days_until_start
                self.logger.info(
                    f"   {i:2d}. 🎯 {promo.title[:40]:40} | "
                    f"Тип: {promo.type} | "
                    f"Старт: {promo.date_start.strftime('%d.%m')} (через {days_until} дн) | "
                    f"Конец: {promo.date_end.strftime('%d.%m')}"
                )
            if len(upcoming) > 10:
                self.logger.info(f"   ... и еще {len(upcoming) - 10} акций")

        return upcoming


class WBPromotionManager:
    """
    Менеджер акций WB - повышение перед акцией + блокировка во время
    Полностью аналогичен OzonPromotionManager
    """

    def __init__(self, session: aiohttp.ClientSession, db_pool, logger=None, db_logger=None):
        self.session = session
        self.db_pool = db_pool
        self.db_logger = db_logger
        self.logger = logger
        self.promotions_client = WBPromotionsClient(session)

        # Хранилища
        self.active_promotions: Dict[int, WBPromotion] = {}  # Все акции
        self.ramp_products: Dict[str, WBPromotionProduct] = {}  # Товары в повышении
        self.processed_today: Set[str] = set()  # Обработанные сегодня товары

        if logger:
            self.promotions_client.set_logger(logger)

    def set_logger(self, logger):
        self.logger = logger
        self.promotions_client.set_logger(logger)

    def _log_separator(self, title: str = " ", char: str = "= ", length: int = 80):
        if self.logger:
            log_separator(self.logger, title, char, length)

    def _log_table(self, title: str, data: dict):
        if self.logger:
            log_table(self.logger, title, data)

    async def initialize(self):
        """Инициализация менеджера акций"""
        if self.logger:
            self.logger.info("🎯 Инициализация менеджера акций WB...")

        await self._ensure_tables_exist()
        await self._ensure_product_fields_exist()
        await self._load_active_promotions()
        await self._load_ramp_products()

    async def _ensure_tables_exist(self):
        """Создаёт таблицу для акций WB"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {Config.PRICE_SCHEDULE_TABLE} (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        promotion_id INT NOT NULL,
                        vendor_code VARCHAR(100) NOT NULL DEFAULT '',
                        nm_id INT,
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

            if self.logger:
                self.logger.info("✅ Таблица wb_promotions создана/проверена")

        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Ошибка создания таблицы: {e}")

    async def _ensure_product_fields_exist(self):
        """Проверяет наличие полей для блокировки в таблице products"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # Проверяем поле promotion_active_wb
                    await cursor.execute("""
                        SELECT COUNT(*) 
                        FROM information_schema.columns 
                        WHERE table_name = %s 
                        AND column_name = 'promotion_active_wb'
                    """, (Config.PPODUCT_TABLE,))

                    if (await cursor.fetchone())[0] == 0:
                        await cursor.execute(f"""
                            ALTER TABLE {Config.PPODUCT_TABLE} 
                            ADD COLUMN promotion_active_wb TINYINT(1) DEFAULT 0
                        """)
                        if self.logger:
                            self.logger.info("✅ Добавлено поле promotion_active_wb")

                    # Проверяем поле promotion_lock_until_wb
                    await cursor.execute("""
                        SELECT COUNT(*) 
                        FROM information_schema.columns 
                        WHERE table_name = %s 
                        AND column_name = 'promotion_lock_until_wb'
                    """, (Config.PPODUCT_TABLE,))

                    if (await cursor.fetchone())[0] == 0:
                        await cursor.execute(f"""
                            ALTER TABLE {Config.PPODUCT_TABLE} 
                            ADD COLUMN promotion_lock_until_wb DATETIME NULL
                        """)
                        if self.logger:
                            self.logger.info("✅ Добавлено поле promotion_lock_until_wb")

        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Ошибка проверки полей: {e}")

    async def _load_active_promotions(self):
        """Загружает акции из БД"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute(f"""
                    SELECT DISTINCT promotion_id, promotion_title, discount_value,
                           date_start, date_end, freeze_date
                    FROM {Config.PRICE_SCHEDULE_TABLE}
                    WHERE date_end > NOW() OR date_end IS NULL
                    """)
                    rows = await cursor.fetchall()

                    for row in rows:
                        self.active_promotions[row['promotion_id']] = WBPromotion(
                            promotion_id=row['promotion_id'],
                            title=row['promotion_title'] or '',
                            date_start=row['date_start'].replace(tzinfo=pytz.UTC) if row['date_start'] else None,
                            date_end=row['date_end'].replace(tzinfo=pytz.UTC) if row['date_end'] else None,
                            discount_value=float(row['discount_value'] or 0)
                        )

            if self.logger:
                self.logger.info(f"✅ Загружено {len(self.active_promotions)} акций из БД")

        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Ошибка загрузки акций: {e}")

    async def _load_ramp_products(self):
        """Загружает товары в процессе повышения"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute(f"""
                    SELECT vendor_code, promotion_id, nm_id, base_price,
                           target_price, current_price, daily_increase, ramp_day, 
                           ramp_start_date, status, date_end as promotion_end_date
                    FROM {Config.PRICE_SCHEDULE_TABLE}
                    WHERE status IN ('ramping', 'pending', 'active')
                    """)
                    rows = await cursor.fetchall()

                    for row in rows:
                        if row['vendor_code']:
                            self.ramp_products[row['vendor_code']] = WBPromotionProduct(
                                vendor_code=row['vendor_code'],
                                nm_id=row['nm_id'] or 0,
                                promotion_id=row['promotion_id'],
                                base_price=float(row['base_price'] or 0),
                                target_price=float(row['target_price'] or 0),
                                current_price=float(row['current_price'] or 0),
                                daily_increase=float(row['daily_increase'] or 0),
                                ramp_day=int(row['ramp_day'] or 0),
                                ramp_start_date=row['ramp_start_date'],
                                status=row['status'],
                                promotion_end_date=row['promotion_end_date'],
                                active_promotion_id=row['promotion_id']
                            )

            if self.logger:
                self.logger.info(f"✅ Загружено {len(self.ramp_products)} товаров в работе")

        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Ошибка загрузки товаров: {e}")

    # ============================================================
    # 1️⃣ ПОЛУЧЕНИЕ АКЦИЙ ИЗ WB API
    # ============================================================

    async def sync_promotions_from_wb(self):
        """Синхронизирует акции из WB API"""
        if self.logger:
            self.logger.info("🔄 Получение акций с WB API...")

        promotions = await self.promotions_client.get_upcoming_promotions()

        if self.logger:
            self.logger.info(f"✅ Получено {len(promotions)} акций от WB")

        saved_count = 0
        for promo in promotions:
            if promo.date_start:
                self.active_promotions[promo.promotion_id] = promo
                await self._save_promotion_to_db(promo)
                saved_count += 1

                days_until = promo.days_until_start
                if days_until > 0:
                    if self.logger:
                        self.logger.info(f"📅 Будущая акция: {promo.title}, через {days_until} дней")
                elif promo.is_active:
                    if self.logger:
                        self.logger.info(f"🔒 Активная акция: {promo.title} (идёт сейчас)")

    async def _save_promotion_to_db(self, promo: WBPromotion):
        """Сохраняет акцию в БД"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    date_start = promo.date_start.replace(tzinfo=None) if promo.date_start else None
                    date_end = promo.date_end.replace(tzinfo=None) if promo.date_end else None

                    await cursor.execute(f"""
                    INSERT INTO {Config.PRICE_SCHEDULE_TABLE} 
                    (promotion_id, vendor_code, promotion_title, discount_value,
                     date_start, date_end)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    promotion_title=VALUES(promotion_title),
                    discount_value=VALUES(discount_value),
                    date_start=VALUES(date_start),
                    date_end=VALUES(date_end),
                    updated_at=CURRENT_TIMESTAMP
                    """, (
                        promo.promotion_id,
                        '',
                        promo.title,
                        promo.discount_value,
                        date_start,
                        date_end
                    ))
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Ошибка сохранения акции {promo.promotion_id}: {e}")

    # ============================================================
    # 2️⃣ РАСЧЁТ БЕЗОПАСНОГО ПОВЫШЕНИЯ (как в Ozon)
    # ============================================================

    def _calculate_safe_increase(self, current_price: float, days_until_start: int) -> Dict:
        """
        Рассчитывает безопасное повышение цены перед акцией
        Аналогично OzonPromotionManager
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

    async def check_and_create_ramp_plans(self, products: List[ProductData]):
        """
        Проверяет БУДУЩИЕ акции и создаёт планы повышения
        Аналогично OzonPromotionManager
        """
        if self.logger:
            self.logger.info("🔍 Проверка будущих акций для создания планов повышения...")

        created_count = 0

        for promo_id, promo in self.active_promotions.items():
            if not promo.date_start:
                continue

            days_until = promo.days_until_start

            # 👇 ТОЛЬКО БУДУЩИЕ АКЦИИ (ещё не начались)
            if days_until <= 0:
                continue

            if self.logger:
                self.logger.info(f"📅 Будущая акция: {promo.title}, через {days_until} дней")

            await self._process_promotion_candidates(promo_id, days_until, products)
            created_count += 1

        if self.logger:
            self.logger.info(f"✅ Создано планов для {created_count} будущих акций")

    async def _process_promotion_candidates(self, promotion_id: int, days_until_start: int,
                                            products: List[ProductData]):
        """
        Проверяет товары и создаёт планы повышения ТОЛЬКО для товаров с продажами
        """
        if promotion_id not in self.active_promotions:
            return

        promo = self.active_promotions[promotion_id]

        if not products:
            return

        if self.logger:
            self.logger.info(f"📦 Проверяем {len(products)} товаров для акции {promo.title}")

        # Счётчик для статистики
        created_count = 0
        skipped_no_sales = 0
        skipped_no_vendor_code = 0

        # Создаём план только для товаров с продажами
        for product in products:
            # Пропускаем товары без артикула
            if not product.vendor_code:
                skipped_no_vendor_code += 1
                if self.logger:
                    self.logger.debug(f"⏭️ Пропущен товар без артикула (nmId: {product.sku_wb})")
                continue

            # Проверяем наличие продаж
            has_sales = getattr(product, 'has_sales_last_period_wb', False)

            if not has_sales:
                skipped_no_sales += 1
                if self.logger:
                    self.logger.debug(f"⏭️ {product.vendor_code}: нет продаж, пропускаем план повышения")
                continue

            # Создаём план для товара с продажами
            await self._create_ramp_plan(
                vendor_code=product.vendor_code,
                nm_id=product.sku_wb,
                current_price=product.current_price_wb,
                promo=promo,
                days_until_start=days_until_start,
                has_sales=True  # Явно передаём, что есть продажи
            )
            created_count += 1

        if self.logger and (skipped_no_sales > 0 or skipped_no_vendor_code > 0):
            self.logger.info(f"📊 Статистика для акции {promo.title}: создано {created_count}, "
                             f"пропущено (нет продаж): {skipped_no_sales}, "
                             f"пропущено (нет артикула): {skipped_no_vendor_code}")

    async def _create_ramp_plan(self, vendor_code: str, nm_id: int,
                                current_price: float, promo: WBPromotion,
                                days_until_start: int, has_sales: bool = False):
        """Создаёт план повышения для одного товара"""

        # 👇 ПРОВЕРКА: если нет продаж - не создаём план
        if not has_sales:
            if self.logger:
                self.logger.debug(f"⏭️ {vendor_code}: нет продаж, пропускаем план повышения")
            return

        if current_price < Config.PROMOTION_MIN_PRICE:
            return

        if vendor_code in self.ramp_products:
            return

        # 👇 ПРОВЕРКА КОНФЛИКТОВ С ДРУГИМИ АКЦИЯМИ
        has_conflict, conflict_reason = await self.check_promotion_conflicts(nm_id, promo)
        if has_conflict:
            if self.logger:
                self.logger.warning(f"⚠️ {vendor_code}: конфликт акций - {conflict_reason}, пропускаем")
            return

        plan = self._calculate_safe_increase(current_price, days_until_start)

        if not plan['can_increase']:
            return

        if self.logger:
            self.logger.info(f"✅ {vendor_code}: план повышения")
            self.logger.info(f"   📊 {current_price:.0f}₽ → {plan['target_price']:.0f}₽")
            self.logger.info(f"   📈 +{plan['daily_increase']:.0f}₽/день ({plan['daily_percent']}%)")

        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"""
                    INSERT INTO {Config.PRICE_SCHEDULE_TABLE}
                    (promotion_id, vendor_code, nm_id, promotion_title,
                     date_start, date_end, base_price, target_price, current_price,
                     daily_increase, ramp_start_date, ramp_day, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    target_price = VALUES(target_price),
                    daily_increase = VALUES(daily_increase),
                    status = VALUES(status),
                    updated_at = CURRENT_TIMESTAMP
                    """, (
                        promo.promotion_id,
                        vendor_code,
                        nm_id,
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

            # 👇 ПОСЛЕ СОЗДАНИЯ ПЛАНА - ЗАЩИЩАЕМ ТОВАР ДО НАЧАЛА АКЦИИ
            await self._protect_product(vendor_code, promo.date_start)

            if self.db_logger:
                await self.db_logger.log_promotion_wb(
                    vendor_code=vendor_code,
                    nm_id=nm_id,
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

            self.ramp_products[vendor_code] = WBPromotionProduct(
                vendor_code=vendor_code,
                nm_id=nm_id,
                promotion_id=promo.promotion_id,
                base_price=current_price,
                target_price=plan['target_price'],
                current_price=current_price,
                daily_increase=plan['daily_increase'],
                ramp_day=0,
                ramp_start_date=datetime.now(),
                status='pending',
                promotion_end_date=promo.date_end,
                active_promotion_id=promo.promotion_id
            )

        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Ошибка сохранения плана {vendor_code}: {e}")

    # ============================================================
    # 4️⃣ ЗАЩИТА ОТ ПЕРЕСЕЧЕНИЙ (как в Ozon)
    # ============================================================

    async def _protect_product(self, vendor_code: str, protect_until: datetime):
        """Защищает товар от изменений до указанной даты"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # Устанавливаем promotion_lock_until_wb, но НЕ promotion_active_wb
                    await cursor.execute(f"""
                        UPDATE {Config.PPODUCT_TABLE}
                        SET promotion_lock_until_wb = %s
                        WHERE model_wb = %s
                    """, (protect_until.replace(tzinfo=None), vendor_code))

            async def _protect_product(self, vendor_code: str, protect_until: datetime):
                try:
                    async with self.db_pool.acquire() as conn:
                        async with conn.cursor() as cursor:
                            await cursor.execute(f"""
                                UPDATE {Config.PPODUCT_TABLE}
                                SET promotion_lock_until_wb = %s
                                WHERE model_wb = %s
                            """, (protect_until.replace(tzinfo=None), vendor_code))

                    # Логируем защиту
                    if self.db_logger and vendor_code in self.ramp_products:
                        plan = self.ramp_products[vendor_code]
                        promo = self.active_promotions.get(plan.promotion_id)
                        if promo:
                            await self.db_logger.log_promotion_wb(
                                vendor_code=vendor_code,
                                nm_id=plan.nm_id,
                                promotion_id=plan.promotion_id,
                                promotion_title=promo.title,
                                action='protected',
                                details={"protect_until": str(protect_until)}
                            )

                    self.logger.debug(f"🛡️ {vendor_code}: защищён до {protect_until}")
                except Exception as e:
                    self.logger.error(f"❌ Ошибка защиты {vendor_code}: {e}")

            if self.logger:
                self.logger.debug(f"🛡️ {vendor_code}: защищён до {protect_until}")
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Ошибка защиты {vendor_code}: {e}")

    async def check_promotion_conflicts(self, nm_id: int, new_promotion: WBPromotion) -> Tuple[bool, str]:
        """
        Проверяет конфликты новой акции с уже запланированными
        """
        # Находим все акции для этого товара
        product_plans = []
        for plan in self.ramp_products.values():
            if plan.nm_id == nm_id and plan.promotion_id != new_promotion.promotion_id:
                promo = self.active_promotions.get(plan.promotion_id)
                if promo:
                    product_plans.append(promo)

        if not product_plans:
            return False, ""

        new_start = new_promotion.date_start
        new_end = new_promotion.date_end
        now = datetime.now(pytz.UTC)

        for existing in product_plans:
            existing_start = existing.date_start
            existing_end = existing.date_end

            # Проверка 1: Акция уже активна
            if existing_start <= now <= existing_end:
                return True, f"Акция {existing.title} уже активна"

            # Проверка 2: Прямое пересечение
            if not (new_end <= existing_start or new_start >= existing_end):
                overlap_days = (min(new_end, existing_end) - max(new_start, existing_start)).days
                return True, f"Пересечение с {existing.title} на {overlap_days} дн"

            # Проверка 3: Слишком маленький разрыв (меньше 1 дня)
            gap_before = (new_start - existing_end).days
            gap_after = (existing_start - new_end).days

            if 0 < gap_before < 1:
                return True, f"Малый разрыв после {existing.title}: {gap_before:.1f} дн"

            if 0 < gap_after < 1:
                return True, f"Малый разрыв до {existing.title}: {gap_after:.1f} дн"

        return False, ""

    # ============================================================
    # 5️⃣ ЕЖЕДНЕВНОЕ ПОВЫШЕНИЕ ЦЕН + БЛОКИРОВКА/РАЗБЛОКИРОВКА
    # ============================================================

    async def schedule_all_promotions(self, products: List[ProductData]) -> Dict:
        """
        Планирует все акции для товаров
        """
        if self.logger:
            self.logger.info(f"📅 Планирование акций для {len(products)} товаров...")

        # Получаем предстоящие акции
        upcoming = await self.promotions_client.get_upcoming_promotions()

        if not upcoming:
            if self.logger:
                self.logger.info("ℹ️ Нет предстоящих акций для планирования")
            return {'auto_scheduled': 0, 'manual_scheduled': 0}

        auto_scheduled = 0

        # Для каждой акции создаем планы
        for promo in upcoming:
            if promo.type == 'auto':
                days_until = promo.days_until_start
                if days_until > 0:
                    for product in products:
                        await self._create_ramp_plan(
                            vendor_code=product.vendor_code,
                            nm_id=product.sku_wb,
                            current_price=product.current_price_wb,
                            promo=promo,
                            days_until_start=days_until
                        )
                    auto_scheduled += 1

        return {
            'auto_scheduled': auto_scheduled,
            'manual_scheduled': 0
        }

    async def execute_scheduled_changes(self) -> List[str]:
        """
        Выполняет запланированные изменения цен для акций
        """
        if self.logger:
            self.logger.info("⚡ Выполнение запланированных изменений...")

        executed = []
        now = datetime.now(pytz.UTC)

        for vendor_code, plan in list(self.ramp_products.items()):
            promo = self.active_promotions.get(plan.promotion_id)
            if not promo:
                continue

            # Проверяем, нужно ли выполнить изменение сегодня
            if plan.status == 'pending' and promo.days_until_start > 0:
                # Начинаем повышение
                plan.status = 'ramping'
                plan.ramp_start_date = now
                executed.append(vendor_code)
                await self._update_ramp_plan(plan)

        return executed

    async def execute_daily_price_ramp(self):
        """
        ЕЖЕДНЕВНОЕ ПОВЫШЕНИЕ ЦЕН + БЛОКИРОВКА/РАЗБЛОКИРОВКА
        Запускается каждый цикл
        """
        now = datetime.now(pytz.UTC)
        now_naive = now.replace(tzinfo=None)

        if self.logger:
            self.logger.info("📈 Выполнение ежедневных операций с акциями WB...")

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
                if self.logger:
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
                    if self.logger:
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

                plan.ramp_day += 1
                plan.current_price = new_price
                self.processed_today.add(vendor_code)

                if new_price >= plan.target_price:
                    plan.status = 'ramping'

                await self._update_ramp_plan(plan)

                if self.logger:
                    self.logger.info(f"⬆️ {vendor_code}: {old_price:.0f}₽ → {new_price:.0f}₽")
                updated_count += 1

            # После повышения цены
                if self.db_logger:
                    await self.db_logger.log_promotion_wb(
                        vendor_code=vendor_code,
                        nm_id=plan.nm_id,
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

        if self.logger:
            self.logger.info(
                f"✅ Повышено: {updated_count} | Заблокировано: {locked_count} | Разблокировано: {unlocked_count}"
            )

        return updated_count, locked_count

    # ============================================================
    # 6️⃣ МЕТОДЫ БЛОКИРОВКИ/РАЗБЛОКИРОВКИ
    # ============================================================

    async def _lock_product(self, vendor_code: str, lock_until: Optional[datetime]):
        """Блокирует товар от изменений на время акции"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"""
                    UPDATE {Config.PPODUCT_TABLE}
                    SET promotion_active_wb = 1,
                        promotion_lock_until_wb = %s
                    WHERE model_wb = %s
                    """, (lock_until.replace(tzinfo=None) if lock_until else None, vendor_code))

            if self.db_logger and vendor_code in self.ramp_products:
                plan = self.ramp_products[vendor_code]
                promo = self.active_promotions.get(plan.promotion_id)
                if promo:
                    await self.db_logger.log_promotion_wb(
                        vendor_code=vendor_code,
                        nm_id=plan.nm_id,
                        promotion_id=plan.promotion_id,
                        promotion_title=promo.title,
                        action='locked',
                        old_price=plan.current_price,
                        details={"lock_until": str(lock_until) if lock_until else None}
                    )

            if self.logger:
                self.logger.debug(f"🔒 {vendor_code}: заблокирован до {lock_until}")

        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Ошибка блокировки {vendor_code}: {e}")

    async def _unlock_product(self, vendor_code: str):
        """Снимает блокировку с товара после акции"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"""
                    UPDATE {Config.PPODUCT_TABLE}
                    SET promotion_active_wb = 0,
                        promotion_lock_until_wb = NULL
                    WHERE model_wb = %s
                    """, (vendor_code,))

            if self.db_logger and vendor_code in self.ramp_products:
                plan = self.ramp_products[vendor_code]
                promo = self.active_promotions.get(plan.promotion_id)
                if promo:
                    await self.db_logger.log_promotion_wb(
                        vendor_code=vendor_code,
                        nm_id=plan.nm_id,
                        promotion_id=plan.promotion_id,
                        promotion_title=promo.title,
                        action='unlocked'
                    )

            if self.logger:
                self.logger.debug(f"🔓 {vendor_code}: разблокирован")

        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Ошибка разблокировки {vendor_code}: {e}")

    async def update_promotion_locks(self):
        """
        Обновляет блокировки для всех активных акций
        """
        if self.logger:
            self.logger.info("🔒 Синхронизация блокировок активных акций...")

        locked_count = 0
        now = datetime.now(pytz.UTC)
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

        if self.logger:
            self.logger.info(f"✅ Заблокировано: {locked_count}")

        return locked_count

    # ============================================================
    # 7️⃣ ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ
    # ============================================================

    async def _update_product_price(self, vendor_code: str, new_price: float):
        """Обновляет цену товара"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"""
                    UPDATE {Config.PPODUCT_TABLE}
                    SET price_wb = %s,
                        wb_real_price = %s,
                        last_price_update = NOW()
                    WHERE model_wb = %s
                    """, (new_price, new_price, vendor_code))
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Ошибка обновления цены {vendor_code}: {e}")

    async def _restore_product_price(self, vendor_code: str, base_price: float):
        """Возвращает цену после акции"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"""
                    UPDATE {Config.PPODUCT_TABLE}
                    SET price_wb = %s,
                        wb_real_price = %s,
                        last_price_update = NOW()
                    WHERE model_wb = %s
                    """, (base_price, base_price, vendor_code))

            if self.db_logger and vendor_code in self.ramp_products:
                plan = self.ramp_products[vendor_code]
                promo = self.active_promotions.get(plan.promotion_id)
                if promo:
                    await self.db_logger.log_promotion_wb(
                        vendor_code=vendor_code,
                        nm_id=plan.nm_id,
                        promotion_id=plan.promotion_id,
                        promotion_title=promo.title,
                        action='restored',
                        old_price=plan.current_price,
                        new_price=base_price
                    )
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Ошибка возврата цены {vendor_code}: {e}")

    async def _update_ramp_plan(self, plan: WBPromotionProduct):
        """Обновляет план в БД"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"""
                    UPDATE {Config.PRICE_SCHEDULE_TABLE}
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
            if self.logger:
                self.logger.error(f"❌ Ошибка обновления плана {plan.vendor_code}: {e}")

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
                    FROM {Config.PRICE_SCHEDULE_TABLE}
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
            if self.logger:
                self.logger.error(f"❌ Ошибка статистики: {e}")
            return {}