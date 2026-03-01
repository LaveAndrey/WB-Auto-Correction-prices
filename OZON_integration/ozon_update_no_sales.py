#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для расчёта и обновления цен товаров БЕЗ ПРОДАЖ на Ozon
Адаптирован под логику WB интеграции: интервал 15 минут, реальные габариты из БД
АСИНХРОННАЯ ВЕРСИЯ
Добавлена возможность упрощённого расчёта через множитель (OZON_NO_SALES_MULTIPLIER)
"""
import asyncio
import logging
import sys
import traceback
import math
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import aiomysql
import pytz

from WB_integration.utils.logging_utils import setup_logger, log_separator, log_table

# Импорты из вашей структуры
from OZON_integration.api.ozon_api_client import OzonAPI
from OZON_integration.api.cdek_api_client import CdekAPI
from config import Config


class OzonNoSalesPriceUpdater:
    """Система расчёта цен для товаров без продаж на Озон (адаптированная под логику WB)"""

    def __init__(self):
        self.logger = setup_logger('ozon_no_sales_updater')
        self.db_pool = None
        self.cdek_client = None
        self.delivery_region_cache = None
        self.delivery_region_updated = None


    def _log_separator(self, title: str = "", char: str = "=", length: int = 80):
        """Разделитель с заголовком"""
        log_separator(self.logger, title, char, length)

    def _log_table(self, title: str, data: dict):
        """Логирование таблицы данных"""
        log_table(self.logger, title, data)

    def _log_calculation_step(self, step_num: int, description: str, value: float, unit: str = "₽"):
        """Логирование шага расчета"""
        self.logger.info(f"   [{step_num}] {description:.<50} {value:>10.2f} {unit}")

    def _log_calculation_header(self, vendor_code: str, sku: str = ""):
        """Заголовок расчета для товара"""
        self.logger.info(f"\n{'=' * 80}")
        self.logger.info(f"🧮 РАСЧЕТ ЦЕНЫ ДЛЯ ТОВАРА: {vendor_code} (SKU: {sku})")
        self.logger.info(f"{'=' * 80}")

    def _log_calculation_footer(self, old_price: float, new_price: float, change_percent: float):
        """Итог расчета"""
        self.logger.info(f"{'─' * 80}")
        self.logger.info(f"📈 ИТОГ: {old_price:.2f}₽ → {new_price:.2f}₽ ({change_percent:+.2f}%)")
        self.logger.info(f"{'=' * 80}\n")

    async def initialize(self):
        """Инициализация подключения к БД с улучшенными настройками"""
        try:
            # Подключение к БД - убираем неподдерживаемые параметры
            self.db_pool = await aiomysql.create_pool(
                host=Config.DB_HOST,
                user=Config.DB_USER,
                password=Config.DB_PASSWORD,
                db=Config.DB_NAME,
                port=Config.DB_PORT,
                autocommit=True,
                charset='utf8mb4',
                minsize=2,
                maxsize=5,
                cursorclass=aiomysql.DictCursor,
                # Только поддерживаемые параметры
                pool_recycle=3600,  # Пересоздавать соединение каждый час
                echo=False  # Отключаем echo для чистоты логов
            )

            # Проверяем соединение
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT 1")

            self.logger.info("✅ Подключение к БД установлено")

            # Инициализация Cdek клиента
            self.cdek_client = CdekAPI(
                Config.CDEK_CLIENT_ID,
                Config.CDEK_CLIENT_SECRET,
                self.logger
            )
            await self.cdek_client.__aenter__()
            self.logger.info("✅ Подключение к CDEK API установлено")

        except Exception as e:
            self.logger.error(f"❌ Ошибка инициализации: {e}")
            raise

    async def close(self):
        """Закрытие соединений"""
        try:
            if self.db_pool:
                self.db_pool.close()
                await self.db_pool.wait_closed()
                self.logger.info("✅ Соединение с БД закрыто")

            if self.cdek_client:
                await self.cdek_client.__aexit__(None, None, None)
                self.logger.info("✅ Соединение с CDEK API закрыто")
        except Exception as e:
            self.logger.error(f"❌ Ошибка при закрытии соединений: {e}")

    async def get_delivery_region(self) -> Tuple[str, str]:
        """Получение региона доставки (кэширование на 24 часа)"""
        if (self.delivery_region_cache is not None and
                self.delivery_region_updated is not None and
                datetime.now() - self.delivery_region_updated < timedelta(hours=24)):
            return self.delivery_region_cache

        # Пока используем дефолтный регион
        # В будущем можно добавить логику определения популярного региона из заказов
        self.delivery_region_cache = (Config.OZON_DEFAULT_REGION, Config.OZON_DEFAULT_CITY)
        self.delivery_region_updated = datetime.now()

        self.logger.info(f"📦 Регион доставки: {Config.OZON_DEFAULT_REGION} ({Config.OZON_DEFAULT_CITY})")
        return self.delivery_region_cache

    async def load_products_without_sales(self) -> List[Dict]:
        """Загружает товары без продаж из БД (аналог WB)"""
        max_retries = 3
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                # Проверяем соединение перед запросом
                if not await self.ensure_db_connection():
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_delay * (attempt + 1))
                        continue
                    else:
                        return []

                async with self.db_pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cursor:
                        # Устанавливаем таймаут для этого конкретного запроса
                        await cursor.execute("SET SESSION wait_timeout = 28800")

                        await cursor.execute(f"""
                            SELECT 
                                product_id,
                                model_ozon as vendor_code,
                                purchase_price,
                                target_profit_rub,
                                price_ozon as current_price_ozon,
                                ozon_real_price as current_real_price,
                                length,
                                width,
                                height,
                                weight,
                                sku_ozon,
                                ozon_seller_discount,
                                has_sales_last_period_ozon,
                                last_price_update,
                                promotion_active,          -- ДОБАВЛЕНО
                                promotion_lock_until       -- ДОБАВЛЕНО
                            FROM {Config.PPODUCT_TABLE}
                            WHERE status = 1 
                              AND purchase_price > 0
                              AND target_profit_rub > 0
                              AND (has_sales_last_period_ozon = 0 OR has_sales_last_period_ozon IS NULL)
                              AND model_ozon IS NOT NULL
                              AND model_ozon != ''
                            ORDER BY IFNULL(last_price_update, '2000-01-01') ASC
                        """)
                        rows = await cursor.fetchall()
                        products = []

                        for row in rows:
                            # Конвертируем None в 0 для числовых полей
                            products.append({
                                'product_id': row['product_id'],
                                'vendor_code': row['vendor_code'],
                                'purchase_price': float(row['purchase_price'] or 0),
                                'target_profit': float(row['target_profit_rub'] or 0),
                                'current_price_ozon': float(row['current_price_ozon'] or 0),
                                'current_real_price': float(row['current_real_price'] or 0),
                                'length': float(row['length'] or 0),
                                'width': float(row['width'] or 0),
                                'height': float(row['height'] or 0),
                                'weight': float(row['weight'] or 0),
                                'sku_ozon': str(row['sku_ozon'] or ''),
                                'ozon_seller_discount': float(row['ozon_seller_discount'] or 0),
                                'has_sales': bool(row['has_sales_last_period_ozon']),
                                'promotion_active': bool(row.get('promotion_active', False)),
                                'promotion_lock_until': row.get('promotion_lock_until')
                            })

                        self.logger.info(f"📥 Загружено товаров без продаж: {len(products)}")
                        return products

            except aiomysql.OperationalError as e:
                if "Lost connection" in str(e) or "MySQL server has gone away" in str(e):
                    self.logger.warning(f"⚠️ Потеря соединения с БД (попытка {attempt + 1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_delay * (attempt + 1))
                        # Принудительно пересоздаем пул
                        if self.db_pool:
                            self.db_pool.close()
                            await self.db_pool.wait_closed()
                        await self.initialize()
                        continue
                self.logger.error(f"❌ Ошибка загрузки товаров без продаж: {e}")
                return []

            except Exception as e:
                self.logger.error(f"❌ Ошибка загрузки товаров без продаж: {e}")
                return []

        return []

    async def calculate_logistics(self, product: Dict) -> Tuple[float, Dict]:
        """
        Рассчитывает логистику для товара с использованием реальных габаритов из БД
        """
        try:
            # Используем реальные габариты из БД
            length = product['length']
            width = product['width']
            height = product['height']
            weight = product['weight']

            # Проверка наличия габаритов
            if length <= 0 or width <= 0 or height <= 0:
                self.logger.warning(
                    f"⚠️ Для товара {product['vendor_code']} некорректные габариты, используем дефолтные")
                length = max(length, 20.0)
                width = max(width, 15.0)
                height = max(height, 10.0)

            # Проверка веса
            if weight <= 0:
                # Расчет веса по объему если нет в БД
                volume_liters = (length * width * height) / 1000.0
                weight = max(volume_liters * 0.3, 0.5)  # 0.3 кг/литр, минимум 0.5 кг
                self.logger.warning(f"⚠️ Вес не указан, рассчитан по габаритам: {weight:.2f} кг")

            # Получаем регион доставки
            region, city = await self.get_delivery_region()

            # Рассчитываем логистику через CDEK
            delivery_cost, delivery_days, tariff_name = await self.cdek_client.calculate_delivery(
                weight_kg=weight,
                length_cm=length,
                width_cm=width,
                height_cm=height,
                to_region=region,
                to_city=city
            )

            # Небольшой буфер на непредвиденные расходы
            delivery_cost *= 1.05

            details = {
                'logistics_cost': delivery_cost,
                'delivery_days': delivery_days,
                'tariff_name': tariff_name,
                'region': region,
                'city': city,
                'weight_kg': weight,
                'dimensions': f"{length}x{width}x{height} см",
                'volume_liters': (length * width * height) / 1000.0
            }

            self.logger.info(
                f"📦 Логистика: {delivery_cost:.2f}₽ ({delivery_days} дн, {tariff_name})"
            )

            return delivery_cost, details

        except Exception as e:
            self.logger.error(f"❌ Ошибка расчета логистики для {product['vendor_code']}: {e}")

            # Fallback: формульный расчет
            volume_liters = (product['length'] * product['width'] * product['height']) / 1000.0
            volume_liters = max(volume_liters, 1.0)

            # Базовая формула для логистики
            logistics_cost = 250.0 + (volume_liters - 1.0) * 35.0
            logistics_cost *= 1.15  # Буфер 15%

            return logistics_cost, {
                'logistics_cost': logistics_cost,
                'source': 'formula_fallback',
                'volume_liters': volume_liters
            }

    def calculate_target_price(self, product: Dict, logistics_cost: float, logistics_details: Dict) -> Tuple[
        float, float, Dict]:
        """
        Рассчитывает целевую цену для товара без продаж (аналог WB логики)
        """
        try:
            vendor_code = product['vendor_code']
            purchase_price = product['purchase_price']
            target_profit = product['target_profit']
            target_discount = product['ozon_seller_discount']

            # Текущие цены
            current_price = product['current_price_ozon']

            # Если нет целевой скидки, используем 10% по умолчанию
            if target_discount <= 0:
                target_discount = 10.0
                self.logger.debug(f"ℹ️ Целевая скидка не задана, используем: {target_discount}%")

            # Заголовок расчета
            self._log_calculation_header(vendor_code, product['sku_ozon'])

            # Входные данные
            self.logger.info(f"📊 ВХОДНЫЕ ДАННЫЕ:")
            self.logger.info(f"   • Закупочная цена (purchase_price):       {purchase_price:>10.2f} ₽")
            self.logger.info(f"   • Целевая прибыль (target_profit):        {target_profit:>10.2f} ₽")
            self.logger.info(f"   • Логистика:                              {logistics_cost:>10.2f} ₽")
            self.logger.info(
                f"   • Габариты (Д×Ш×В):                       {product['length']}×{product['width']}×{product['height']} см")
            self.logger.info(f"   • Вес:                                    {product['weight']:.2f} кг")
            self.logger.info(f"   • Текущая цена на Ozon:                   {current_price:>10.2f} ₽")
            self.logger.info(f"   • Целевая скидка:                         {target_discount:>10.1f} %")
            self.logger.info(f"   • НДС:                                    {Config.VAT_PERCENT:>10.1f} %")
            self.logger.info(f"   • Банковская комиссия:                    {Config.BANK_COMMISSION:>10.1%}")
            self.logger.info(f"   • Эквайринг:                              {Config.ACQUIRING_RATE:>10.1%}")
            self.logger.info(f"   • Коэффициент forPay:                     {Config.OZON_FORPAY_TO_PRICEWITHDISC_RATIO:>10.3f}")
            self.logger.info(f"\n🧮 ПОШАГОВЫЙ РАСЧЕТ:")

            # ШАГ 1: Общая комиссия
            total_commission = Config.BANK_COMMISSION + Config.ACQUIRING_RATE
            self._log_calculation_step(1, "Общая комиссия (банк + эквайринг)", total_commission * 100, "%")

            # ШАГ 2: Необходимый forPay для целевой прибыли (с учетом НДС)
            profit_vat = target_profit * Config.VAT_PERCENT / 100
            target_forpay = (target_profit + profit_vat + logistics_cost + purchase_price) / (1 - total_commission)
            self._log_calculation_step(2, "Целевой forPay (с учетом комиссии и НДС)", target_forpay)

            # ШАГ 3: Расчет цены со скидкой через коэффициент forPay
            required_price_wd = target_forpay / Config.OZON_FORPAY_TO_PRICEWITHDISC_RATIO
            self._log_calculation_step(3, f"Целевая цена со скидкой (forPay / {Config.OZON_FORPAY_TO_PRICEWITHDISC_RATIO})", required_price_wd)

            # ШАГ 4: Проверка минимальной цены (+10% к закупке с НДС)
            min_price = purchase_price * 1.1 * (1 + Config.VAT_PERCENT / 100)
            if required_price_wd < min_price:
                self.logger.warning(
                    f"⚠️ Цена {required_price_wd:.2f}₽ ниже минимальной {min_price:.2f}₽ (+10% к закупке с НДС)")
                required_price_wd = min_price
                self._log_calculation_step(4, "Минимальная цена (+10% к закупке с НДС)", required_price_wd)
            else:
                self._log_calculation_step(4, "Минимальная цена (+10% к закупке с НДС)", min_price)
                self.logger.info(f"      [✓] Текущая цена {required_price_wd:.2f}₽ >= минимальной {min_price:.2f}₽")

            # ШАГ 5: Расчет полной цены с учетом скидки
            if target_discount >= 100:
                target_discount = 99.9
                self.logger.warning("⚠️ Скидка >= 100%, ограничена до 99.9%")

            discount_factor = 1 - target_discount / 100
            needed_full_price = required_price_wd / discount_factor
            self._log_calculation_step(5, f"Полная цена (цена_со_скидкой / (1 - {target_discount}/100))",
                                       needed_full_price)

            # ШАГ 6: Округление до 10 рублей вверх
            new_price_rounded = math.ceil(needed_full_price / 10) * 10
            self._log_calculation_step(6, "Округленная цена (до 10₽ вверх)", new_price_rounded)

            # ШАГ 7: Расчет отклонения от текущей цены
            if current_price > 0:
                price_change = new_price_rounded - current_price
                price_change_percent = (price_change / current_price * 100)
            else:
                price_change = new_price_rounded
                price_change_percent = 100.0

            direction = "↑" if price_change > 0 else "↓" if price_change < 0 else "="
            self.logger.info(
                f"\n   [7] Изменение цены: {direction} {abs(price_change):.2f}₽ ({price_change_percent:+.2f}%)")

            # ШАГ 8: Расчет цены для отправки в Ozon (real price)
            real_price = required_price_wd

            # Детали расчета
            calculation_details = {
                'vendor_code': vendor_code,
                'purchase_price': purchase_price,
                'target_profit': target_profit,
                'logistics': logistics_cost,
                'total_commission_percent': total_commission * 100,
                'target_forpay': target_forpay,
                'forpay_ratio': Config.OZON_FORPAY_TO_PRICEWITHDISC_RATIO,
                'required_price_wd': required_price_wd,
                'target_discount': target_discount,
                'needed_full_price': needed_full_price,
                'new_price_rounded': new_price_rounded,
                'current_price': current_price,
                'price_change': price_change,
                'price_change_percent': price_change_percent,
                'real_price': real_price,
                **logistics_details
            }

            # Итог расчета
            self._log_calculation_footer(current_price, new_price_rounded, price_change_percent)

            return new_price_rounded, real_price, calculation_details

        except Exception as e:
            self.logger.error(f"❌ Ошибка расчета цены для {product['vendor_code']}: {e}")
            self.logger.error(traceback.format_exc())
            return 0, 0, {}

    async def update_price_in_db(self, product_id: int, vendor_code: str,
                                 new_price: float, new_real_price: float,
                                 sku_ozon: str):
        """Обновление цены в БД"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    current_time = datetime.now(pytz.timezone('Europe/Moscow'))

                    await cursor.execute(f"""
                        UPDATE {Config.PPODUCT_TABLE}
                        SET price_ozon = %s, 
                            ozon_real_price = %s,
                            last_price_update = %s
                        WHERE product_id = %s
                    """, (new_price, new_real_price, current_time, product_id))

                    self.logger.info(
                        f"💾 Цена обновлена для {vendor_code}: {new_price:.2f}₽ (real: {new_real_price:.2f}₽)")
                    return True

        except Exception as e:
            self.logger.error(f"❌ Ошибка обновления цены в БД: {e}")
            return False

    async def upload_prices_to_ozon(self, price_updates: List[Dict]) -> bool:
        """Отправка цен на Ozon"""
        if not price_updates:
            return False

        try:
            self.logger.info(f"\n📤 ОТПРАВКА ЦЕН НА OZON ({len(price_updates)} товаров)")

            # Пауза 65 секунд из-за лимитов Ozon API
            self.logger.info("⏳ Пауза 65 секунд (ограничение Ozon API)...")
            await asyncio.sleep(65)

            async with OzonAPI(Config.OZON_CLIENT_ID, Config.OZON_API_KEY, self.logger) as ozon:
                # Форматируем данные для Ozon API
                ozon_payload = []
                for update in price_updates:
                    ozon_payload.append({
                        "offer_id": update['offer_id'],
                        "price": str(update['price']),
                        "currency_code": "RUB"
                    })

                success = await ozon.update_prices(ozon_payload)

                if success:
                    self.logger.info(f"✅ Успешно отправлено на Ozon: {len(price_updates)} товаров")
                else:
                    self.logger.error("❌ Ошибка отправки цен на Ozon")

                return success

        except Exception as e:
            self.logger.error(f"❌ Ошибка отправки цен на Ozon: {e}")
            return False

    async def ensure_db_connection(self):
        """Проверка и восстановление соединения с БД"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT 1")
            return True
        except Exception as e:
            self.logger.warning(f"⚠️ Потеряно соединение с БД: {e}. Переподключаемся...")
            try:
                # Закрываем старый пул
                if self.db_pool:
                    self.db_pool.close()
                    await self.db_pool.wait_closed()

                # Создаем новый пул
                await self.initialize()
                return True
            except Exception as e2:
                self.logger.error(f"❌ Не удалось переподключиться к БД: {e2}")
                return False

    async def process_cycle(self):
        """Один цикл обработки"""
        self._log_separator("ЦИКЛ ОБРАБОТКИ ТОВАРОВ БЕЗ ПРОДАЖ НА OZON", "🔄")
        cycle_start = datetime.now()

        # ========== НОВЫЙ БЛОК: получение множителя ==========
        multiplier = Config.OZON_NO_SALES_MULTIPLIER
        if multiplier > 0:
            self.logger.info(f"🔢 Используется упрощённый расчёт: цена = закупочная × {multiplier}")
        else:
            self.logger.info("🔢 Упрощённый расчёт отключён (множитель = 0), используется расчёт с логистикой")
        # =====================================================

        products = await self.load_products_without_sales()

        if not products:
            self.logger.info("ℹ️ Нет товаров без продаж для обработки")
            return

        successful_updates = []
        ozon_updates = []
        stats = {
            'total': len(products),
            'processed': 0,
            'price_changed': 0,
            'skipped_low_deviation': 0,
            'errors': 0,
            'simple_calc': 0,
            'full_calc': 0
        }

        for idx, product in enumerate(products, 1):
            try:
                vendor_code = product['vendor_code']
                self.logger.info(f"\n{'#' * 80}")
                self.logger.info(f"#{idx:3d}/{len(products):3d} 🔍 ОБРАБОТКА ТОВАРА: {vendor_code}")
                self.logger.info(f"{'#' * 80}\n")

                # Пропускаем товары в акциях (без изменений)
                if product.get('promotion_active') and Config.PROMOTION_LOCK_PRICES:
                    self.logger.info(f"🔒 Товар {vendor_code} участвует в акции – пропускаем")
                    continue

                lock_until = product.get('promotion_lock_until')
                if lock_until:
                    if isinstance(lock_until, datetime):
                        if datetime.now() < lock_until.replace(tzinfo=None):
                            self.logger.info(f"🔒 Товар {vendor_code} заблокирован до {lock_until} – пропускаем")
                            continue

                # ========== УПРОЩЁННЫЙ РАСЧЁТ (ЕСЛИ МНОЖИТЕЛЬ > 0) ==========
                if multiplier > 0:
                    purchase_price = product['purchase_price']
                    new_price = purchase_price * multiplier
                    new_price = math.ceil(new_price / 10) * 10  # округление до 10₽ вверх
                    new_real_price = new_price  # для упрощённого real_price = price

                    self.logger.info(f"✅ {vendor_code}: упрощённый расчёт (закуп × {multiplier}) = {new_price}₽")

                    # Обновляем в БД
                    await self.update_price_in_db(
                        product['product_id'],
                        vendor_code,
                        new_price,
                        new_real_price,
                        product['sku_ozon']
                    )

                    # Добавляем в список для отправки на Ozon
                    ozon_updates.append({
                        "offer_id": vendor_code,
                        "price": float(new_price),
                        "old_price": product['current_price_ozon']
                    })

                    successful_updates.append({
                        'vendor_code': vendor_code,
                        'old_price': product['current_price_ozon'],
                        'new_price': new_price,
                        'change_percent': ((new_price - product['current_price_ozon']) / product[
                            'current_price_ozon'] * 100) if product['current_price_ozon'] > 0 else 100,
                        'logistics': 0,
                        'method': 'simple'
                    })

                    stats['price_changed'] += 1
                    stats['processed'] += 1
                    stats['simple_calc'] += 1
                    continue  # переходим к следующему товару, минуя полный расчёт

                # ========== ПОЛНЫЙ РАСЧЁТ (ЕСЛИ МНОЖИТЕЛЬ = 0) ==========
                # (весь код, который был ранее: calculate_logistics, calculate_target_price и т.д.)
                logistics_cost, logistics_details = await self.calculate_logistics(product)
                new_price, new_real_price, details = self.calculate_target_price(
                    product, logistics_cost, logistics_details
                )
                if new_price <= 0:
                    stats['errors'] += 1
                    self.logger.error(f"❌ Расчет вернул некорректную цену: {new_price}")
                    continue

                price_change_percent = details.get('price_change_percent', 0)
                if abs(price_change_percent) < Config.MIN_DEVIATION_PERCENT and product['current_price_ozon'] > 0:
                    stats['skipped_low_deviation'] += 1
                    self.logger.info(
                        f"⏭️ {vendor_code}: отклонение {price_change_percent:.2f}% < {Config.MIN_DEVIATION_PERCENT}% - пропускаем")
                    continue

                await self.update_price_in_db(
                    product['product_id'],
                    vendor_code,
                    new_price,
                    new_real_price,
                    product['sku_ozon']
                )

                ozon_updates.append({
                    "offer_id": vendor_code,
                    "price": float(new_price),
                    "old_price": product['current_price_ozon']
                })

                successful_updates.append({
                    'vendor_code': vendor_code,
                    'old_price': product['current_price_ozon'],
                    'new_price': new_price,
                    'change_percent': price_change_percent,
                    'logistics': logistics_cost,
                    'method': 'full'
                })

                stats['price_changed'] += 1
                stats['processed'] += 1
                stats['full_calc'] += 1

                await asyncio.sleep(0.5)  # пауза между запросами к СДЭК

            except Exception as e:
                stats['errors'] += 1
                self.logger.error(f"❌ Ошибка обработки {product.get('vendor_code', 'unknown')}: {e}")
                self.logger.error(traceback.format_exc())

        # Отправка на Ozon (без изменений)
        if ozon_updates:
            if Config.LOAD_PRICE_TO_OZON:
                await self.upload_prices_to_ozon(ozon_updates)
            else:
                self.logger.info(f"\nℹ️ РЕЖИМ ТЕСТИРОВАНИЯ: отправка на Ozon отключена")
                self.logger.info(f"   Подготовлено к отправке: {len(ozon_updates)} товаров")
                for update in ozon_updates[:3]:
                    self.logger.info(f"   • {update['offer_id']}: {update['old_price']:.2f}₽ → {update['price']:.2f}₽")

        # Статистика цикла
        cycle_end = datetime.now()
        duration = (cycle_end - cycle_start).total_seconds()

        self._log_separator("СТАТИСТИКА ЦИКЛА", "📊")
        self._log_table("РЕЗУЛЬТАТЫ", {
            "Время выполнения": f"{duration:.1f} сек",
            "Всего товаров": stats['total'],
            "Обработано": stats['processed'],
            "Изменено цен": stats['price_changed'],
            f"Пропущено (<{Config.MIN_DEVIATION_PERCENT}%)": stats['skipped_low_deviation'],
            "Ошибок": stats['errors'],
            "Упрощённый расчёт": stats['simple_calc'],
            "Полный расчёт (СДЭК)": stats['full_calc']
        })

        if successful_updates:
            self.logger.info("\n📋 ПРИМЕРЫ ИЗМЕНЕНИЙ:")
            for update in successful_updates[:3]:
                arrow = "↑" if update['change_percent'] > 0 else "↓"
                method = "упрощённо" if update['method'] == 'simple' else "с логистикой"
                self.logger.info(
                    f"   • {update['vendor_code']}: {update['old_price']:.2f}₽ → "
                    f"{update['new_price']:.2f}₽ {arrow}({update['change_percent']:+.2f}%), "
                    f"метод: {method}, логистика: {update['logistics']:.2f}₽"
                )
    async def run(self):
        """Основной цикл работы"""
        self.logger.info("=" * 80)
        self.logger.info("🚀 ЗАПУСК СИСТЕМЫ ОБРАБОТКИ ТОВАРОВ БЕЗ ПРОДАЖ НА OZON".center(80))
        self.logger.info("=" * 80)

        # Инициализация
        await self.initialize()

        self.logger.info(f"\n📊 ПАРАМЕТРЫ РАБОТЫ:")
        self.logger.info(f"   • Интервал работы: 15 минут")
        self.logger.info(f"   • Товары без продаж: > {Config.SALES_HOURS_FILTER} часов")
        self.logger.info(f"   • Минимальное отклонение: {Config.MIN_DEVIATION_PERCENT}%")
        self.logger.info(f"   • Коэффициент forPay: {Config.OZON_FORPAY_TO_PRICEWITHDISC_RATIO}")
        self.logger.info(f"   • Регион доставки: {Config.OZON_DEFAULT_REGION}")
        self.logger.info(f"   • Множитель упрощённого расчёта: {Config.OZON_NO_SALES_MULTIPLIER} (0 = отключён)")
        self.logger.info(f"   • Отправка на Ozon: {'ВКЛЮЧЕНО' if Config.LOAD_PRICE_TO_OZON else 'ОТКЛЮЧЕНО (тестовый режим)'}")
        self.logger.info("=" * 80)

        try:
            while True:
                # Выполняем цикл обработки
                await self.process_cycle()

                # Ожидание следующего цикла (15 минут)
                self.logger.info(f"\n{'=' * 80}")
                self.logger.info(f"⏳ Следующий цикл через 15 минут...")
                self.logger.info(f"{'=' * 80}\n")
                await asyncio.sleep(900)  # 15 минут

        except KeyboardInterrupt:
            self.logger.info("🛑 Остановка по запросу пользователя")
        except Exception as e:
            self.logger.error(f"❌ Фатальная ошибка: {e}")
            self.logger.error(traceback.format_exc())
        finally:
            # Закрываем соединения
            await self.close()