#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import math
import asyncio
import aiomysql
import pytz
import requests
import json
import sys
import traceback
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple
from config import Config
from WB_integration.utils.logging_utils import setup_logger, log_separator, log_table

# ИСПОЛЬЗУЕМ ЕДИНЫЙ ЛОГГЕР ИЗ UTILS
logger = setup_logger('wb_price_calc')


class NoSalesPriceUpdater:
    """Класс для обновления цен товаров без продаж на Wildberries"""

    def __init__(self):
        self.warehouse_tariff = None
        self.db_pool = None
        self.headers = {
            "Authorization": Config.WB_SALES_TOKEN,
            "Content-Type": "application/json"
        }

        # Эндпоинты Wildberries API
        self.WB_TARIFF_URL = "https://common-api.wildberries.ru/api/tariffs/v1/acceptance/coefficients"
        self.ORDERS_API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
        self.WAREHOUSES_API_URL = "https://supplies-api.wildberries.ru/api/v1/warehouses"
        self.CARDS_SEARCH_URL = "https://content-api.wildberries.ru/content/v2/get/cards/list"
        self.DISCOUNTS_FILTER_URL = "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter"
        self.PRICES_UPLOAD_URL = "https://discounts-prices-api.wildberries.ru/api/v2/upload/task"

        # API ключи
        self.api_key = Config.WB_PRICES_TOKEN
        self.stats_api_key = Config.WB_SALES_TOKEN or self.api_key

        # Параметры из конфига
        self.default_warehouse_id = Config.WB_WAREHOUSE_ID
        self.bank_rate = Config.BANK_COMMISSION
        self.acquiring_rate = Config.ACQUIRING_RATE
        self.box_type_id = Config.WB_BOX_TYPE
        self.localization_index = Config.LOCALIZATION_INDEX
        self.forpay_ratio = Config.WB_FORPAY_TO_PRICEWITHDISC_RATIO
        self.vat_percent = Config.VAT_PERCENT
        self.hours_without_sales = Config.SALES_HOURS_FILTER
        self.min_deviation_percent = Config.MIN_DEVIATION_PERCENT
        self.max_products_per_cycle = Config.WB_NO_SALES_INTERVAL
        self.load_to_wb = Config.LOAD_PRICE_TO_WB

        # Конфигурация БД
        self.db_config = {
            'host': Config.DB_HOST,
            'user': Config.DB_USER,
            'password': Config.DB_PASSWORD,
            'db': Config.DB_NAME,
            'port': Config.DB_PORT,
            'autocommit': True,
            'charset': 'utf8mb4',
        }

    def _log_separator(self, title: str = "", char: str = "=", length: int = 80):
        """Логирование разделителя"""
        log_separator(logger, title, char, length)

    def _log_table(self, title: str, data: dict):
        """Логирование таблицы данных"""
        log_table(logger, title, data)

    def _log_calculation_step(self, step_num: int, description: str, value: float, unit: str = "₽"):
        """Логирование шага расчета"""
        logger.info(f"   [{step_num}] {description:.<50} {value:>10.2f} {unit}")

    def _log_calculation_header(self, vendor_code: str, nm_id: int):
        """Заголовок расчета для товара"""
        logger.info(f"\n{'=' * 80}")
        logger.info(f"🧮 РАСЧЕТ ЦЕНЫ ДЛЯ ТОВАРА: {vendor_code} (nmID: {nm_id})")
        logger.info(f"{'=' * 80}")

    def _log_calculation_footer(self, old_price: float, new_price: float, change_percent: float):
        """Итог расчета"""
        logger.info(f"{'─' * 80}")
        logger.info(f"📈 ИТОГ: {old_price:.2f}₽ → {new_price:.2f}₽ ({change_percent:+.2f}%)")
        logger.info(f"{'=' * 80}\n")

    async def initialize_db(self):
        """Инициализация подключения к БД"""
        try:
            self.db_pool = await aiomysql.create_pool(
                **self.db_config,
                cursorclass=aiomysql.DictCursor
            )
            logger.info("✅ Подключение к БД установлено")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к БД: {e}")
            return False

    def get_last_active_warehouse_id(self):
        """Определяет ID склада, с которого были последние отгрузки"""
        date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

        try:
            logger.info("🔄 Получение последнего активного склада")

            # Получаем последние заказы
            orders_resp = requests.get(
                self.ORDERS_API_URL,
                headers={"Authorization": self.stats_api_key},
                params={"dateFrom": date_from, "flag": 1},
                timeout=15
            )

            orders_resp.raise_for_status()
            orders = orders_resp.json()

            if not orders:
                logger.warning("Не найдено заказов за последнюю неделю.")
                return self.default_warehouse_id

            last_order = orders[0]
            last_warehouse_name = last_order.get('warehouseName')

            if not last_warehouse_name:
                return self.default_warehouse_id

            # Получаем список всех складов
            warehouses_resp = requests.get(
                self.WAREHOUSES_API_URL,
                headers={"Authorization": self.api_key},
                timeout=15
            )
            warehouses_resp.raise_for_status()
            warehouses_list = warehouses_resp.json()

            # Ищем ID склада по имени
            for wh in warehouses_list:
                if wh.get('name') == last_warehouse_name:
                    return wh.get('ID')

            return self.default_warehouse_id

        except Exception as e:
            logger.error(f"❌ Ошибка определения склада: {e}")
            return self.default_warehouse_id

    def get_warehouse_tariff(self):
        """Получение тарифов логистики для склада"""
        try:
            warehouse_id = self.get_last_active_warehouse_id()
            logger.info(f"📦 Работаем со складом ID: {warehouse_id}, тип поставки: {self.box_type_id}")

            param = {
                "warehouseIDs": str(warehouse_id)
            }

            resp = requests.get(
                self.WB_TARIFF_URL,
                headers=self.headers,
                params=param,
                timeout=30
            )

            if resp.status_code == 429:
                logger.error("❌ Превышен лимит запросов")
                return None

            resp.raise_for_status()
            all_tariffs = resp.json()

            if not all_tariffs:
                logger.error("❌ API вернуло пустой ответ")
                return None

            # Ищем доступную дату
            for days_offset in range(0, 15):
                check_date = (date.today() + timedelta(days=days_offset)).strftime('%Y-%m-%d')

                for tariff in all_tariffs:
                    if (tariff.get('warehouseID') == warehouse_id and
                            tariff.get('boxTypeID') == self.box_type_id):

                        api_date_str = tariff.get('date', '')
                        try:
                            if api_date_str.endswith('Z'):
                                api_date = datetime.fromisoformat(api_date_str.replace('Z', '+00:00'))
                            else:
                                api_date = datetime.fromisoformat(api_date_str)

                            api_date_simple = api_date.strftime('%Y-%m-%d')

                            if api_date_simple == check_date:
                                coefficient = tariff.get('coefficient')
                                allow_unload = tariff.get('allowUnload')

                                if coefficient in [0, 1, -1] and allow_unload is True:
                                    def parse_float(val):
                                        if val is None:
                                            return 0.0
                                        try:
                                            return float(str(val).replace(',', '.'))
                                        except:
                                            return 0.0

                                    self.warehouse_tariff = {
                                        'warehouse_id': warehouse_id,
                                        'warehouse_name': tariff.get('warehouseName'),
                                        'delivery_base': parse_float(tariff.get('deliveryBaseLiter')),
                                        'delivery_liter': parse_float(tariff.get('deliveryAdditionalLiter')),
                                        'storage_base': parse_float(tariff.get('storageBaseLiter')),
                                        'storage_liter': parse_float(tariff.get('storageAdditionalLiter'))
                                    }

                                    logger.info(f"✅ Найден тариф: {self.warehouse_tariff['warehouse_name']}")
                                    logger.info(
                                        f"   Доставка: {self.warehouse_tariff['delivery_base']} + {self.warehouse_tariff['delivery_liter']} ₽/л")
                                    return True
                        except Exception:
                            continue

            logger.error("❌ Не найдено доступных тарифов")
            return False

        except Exception as e:
            logger.error(f"❌ Ошибка получения тарифов: {e}")
            return False

    def get_nm_id_by_vendor_code(self, vendor_code: str) -> Optional[int]:
        """Получение nmId по артикулу поставщика"""
        try:
            headers = {
                "Authorization": self.api_key,
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

            resp = requests.post(self.CARDS_SEARCH_URL, headers=headers, json=body, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            cards = data.get('cards', [])
            if cards:
                for card in cards:
                    card_vendor_code = str(card.get("vendorCode", "")).strip()
                    if card_vendor_code == str(vendor_code).strip():
                        nm_id = card.get("nmID", 0)
                        if nm_id:
                            logger.debug(f"🔍 Найден nmID для {vendor_code}: {nm_id}")
                            return nm_id

                logger.warning(f"⚠️  Карточка с артикулом {vendor_code} найдена, но nmID отсутствует")
                return 0
            else:
                logger.warning(f"⚠️  Карточка с артикулом {vendor_code} не найдена")
                return 0

        except Exception as e:
            logger.error(f"❌ Ошибка поиска nmId для {vendor_code}: {e}")
            return None

    def get_wb_product_info(self, nm_id: int) -> Optional[Dict]:
        """Получение информации о товаре с ценами через discounts-prices API"""
        try:
            headers = {
                "Authorization": self.api_key,
                "Content-Type": "application/json"
            }

            body = {
                "nmList": [nm_id]
            }

            resp = requests.post(self.DISCOUNTS_FILTER_URL, headers=headers, json=body, timeout=30)

            if resp.status_code == 200:
                data = resp.json()

                if data.get("error", True):
                    logger.error(f"❌ Ошибка в ответе от WB API: {data.get('errorText', 'Неизвестная ошибка')}")
                    return None

                list_goods = data.get("data", {}).get("listGoods", [])

                if not list_goods:
                    logger.warning(f"⚠️  Товар с nmID {nm_id} не найден")
                    return None

                product = list_goods[0]

                vendor_code = str(product.get('vendorCode', '')).strip() or None
                discount = int(product.get('discount', 0))

                # Получаем размеры товара
                sizes = product.get('sizes', [])

                if not sizes:
                    logger.warning(f"⚠️  У товара {nm_id} нет размеров")
                    return None

                size = sizes[0]

                price = float(size.get('price', 0))
                discounted_price = float(size.get('discountedPrice', 0))

                logger.info(f"📦 nmID {nm_id} | Артикул: {vendor_code}")
                logger.info(
                    f"   💰 Полная цена: {price:.2f}₽ | Скидка: {discount}% | Цена со скидкой: {discounted_price:.2f}₽")

                return {
                    'nm_id': nm_id,
                    'vendorCode': vendor_code,
                    'price': price,
                    'discounted_price': discounted_price,
                    'discount': discount,
                }
            else:
                logger.error(f"❌ Ошибка запроса к WB API: {resp.status_code} - {resp.text[:200]}")
                return None

        except Exception as e:
            logger.error(f"❌ Ошибка получения информации о товаре {nm_id}: {e}")
            logger.error(traceback.format_exc())
            return None

    def upload_prices_to_wb(self, price_data: List[Dict]) -> bool:
        """Отправка цен на Wildberries"""
        if not price_data:
            return False

        try:
            headers = {
                "Authorization": self.api_key,
                "Content-Type": "application/json"
            }

            payload = {"data": price_data}
            resp = requests.post(self.PRICES_UPLOAD_URL, headers=headers,
                                 json=payload, timeout=30)

            if resp.status_code == 200:
                result = resp.json()
                if not result.get('error'):
                    logger.info(f"✅ Цены отправлены на WB: {len(price_data)} товаров")
                    return True
                else:
                    logger.error(f"❌ Ошибка WB API: {result}")
            else:
                logger.error(f"❌ Ошибка HTTP {resp.status_code}: {resp.text[:200]}")

            return False

        except Exception as e:
            logger.error(f"❌ Ошибка отправки цен: {e}")
            return False

    async def load_products_without_sales(self) -> List[Dict]:
        """Загрузка товаров без продаж"""
        products = []

        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    hours_ago = (datetime.now() - timedelta(hours=self.hours_without_sales)).strftime(
                        '%Y-%m-%d %H:%M:%S')

                    await cursor.execute(f"""
                        SELECT p.product_id, p.model_wb, p.purchase_price, p.target_profit_rub,
                               p.price_wb, p.wb_real_price, p.length, p.width, p.height,
                               p.sku_wb, p.wb_seller_discount, p.has_sales_last_period_wb,
                               p.last_price_update,
                               p.promotion_active_wb,          -- 👈 ДОБАВИТЬ
                               p.promotion_lock_until_wb       -- 👈 ДОБАВИТЬ
                        FROM oc_product p
                        WHERE p.status = 1 
                          AND p.purchase_price > 0
                          AND p.target_profit_rub > 0
                          AND (p.has_sales_last_period_wb = 0 OR p.has_sales_last_period_wb IS NULL)
                          AND p.model_wb > 0
                        ORDER BY IFNULL(p.last_price_update, '2000-01-01') ASC
                        LIMIT {self.max_products_per_cycle}
                    """)

                    rows = await cursor.fetchall()

                    for row in rows:
                        products.append({
                            'product_id': row['product_id'],
                            'vendor_code': row['model_wb'],
                            'purchase_price': float(row['purchase_price'] or 0),
                            'target_profit': float(row['target_profit_rub'] or 0),
                            'current_price_wb': float(row['price_wb'] or 0),
                            'current_real_price': float(row['wb_real_price'] or 0),
                            'length': float(row['length'] or 0),
                            'width': float(row['width'] or 0),
                            'height': float(row['height'] or 0),
                            'sku_wb': int(row['sku_wb'] or 0),
                            'wb_seller_discount': float(row['wb_seller_discount'] or 0),
                            'has_sales': bool(row['has_sales_last_period_wb']),
                            'promotion_active_wb': bool(row.get('promotion_active_wb', False)),
                            'promotion_lock_until_wb': row.get('promotion_lock_until_wb')
                        })

                    logger.info(f"📥 Загружено товаров без продаж: {len(products)}")
                    return products

        except Exception as e:
            logger.error(f"❌ Ошибка загрузки товаров: {e}")
            return []

    def calculate_logistics(self, length: float, width: float, height: float) -> float:
        """Расчет логистики по габаритам"""
        try:
            # Расчет объема
            if not all([length > 0, width > 0, height > 0]):
                logger.warning(
                    f"⚠️  Некорректные габариты ({length}x{width}x{height}), используем дефолтную логистику 50₽")
                return 50.0

            # Объем в литрах (см³ -> л)
            volume = (length * width * height) / 1000.0
            volume = max(volume, 1.0)

            if not self.warehouse_tariff:
                logger.warning("⚠️  Тариф не загружен, используем дефолтную логистику 50₽")
                return 50.0

            base = self.warehouse_tariff['delivery_base']
            liter = self.warehouse_tariff['delivery_liter']

            # Логистика: база + (объем - 1) * за литр
            logistics = base + max(volume - 1.0, 0.0) * liter
            logistics *= self.localization_index

            logger.debug(
                f"📦 Логистика: база {base}₽ + ({volume:.2f}л - 1) × {liter}₽/л = {logistics:.2f}₽ (с индексом {self.localization_index})")

            return logistics

        except Exception as e:
            logger.error(f"❌ Ошибка расчета логистики: {e}")
            return 50.0

    def calculate_target_price(self, product: Dict, wb_info: Dict) -> Tuple[float, float, Dict]:
        """Расчет целевой цены с подробным логированием"""
        try:
            vendor_code = product['vendor_code']
            nm_id = product['sku_wb']
            purchase_price = product['purchase_price']
            target_profit = product['target_profit']
            target_discount = product['wb_seller_discount']

            # Текущие данные с WB
            current_price = wb_info['price']
            current_discount = wb_info['discount']

            # Если в БД нет целевой скидки, используем текущую с WB или 20% по умолчанию
            if target_discount <= 0:
                target_discount = current_discount if current_discount > 0 else 20.0
                logger.debug(f"ℹ️  Целевая скидка не задана в БД, используем: {target_discount}%")

            # Расчет логистики
            logistics = self.calculate_logistics(
                product['length'],
                product['width'],
                product['height']
            )

            # Выводим заголовок расчета
            self._log_calculation_header(vendor_code, nm_id)

            logger.info(f"📊 ВХОДНЫЕ ДАННЫЕ:")
            logger.info(f"   • Закупочная цена:                       {purchase_price:>10.2f} ₽")
            logger.info(f"   • Целевая прибыль:                       {target_profit:>10.2f} ₽")
            logger.info(f"   • Логистика:                             {logistics:>10.2f} ₽")
            logger.info(
                f"   • Габариты:                                {product['length']}×{product['width']}×{product['height']} см")
            logger.info(f"   • Текущая цена на WB:                     {current_price:>10.2f} ₽")
            logger.info(f"   • Текущая скидка на WB:                   {current_discount:>10.1f} %")
            logger.info(f"   • Целевая скидка:                         {target_discount:>10.1f} %")
            logger.info(f"   • НДС:                                    {self.vat_percent:>10.1f} %")
            logger.info(f"   • Банковская комиссия:                    {self.bank_rate:>10.1%}")
            logger.info(f"   • Эквайринг:                              {self.acquiring_rate:>10.1%}")
            logger.info(f"   • Коэффициент forPay:                     {self.forpay_ratio:>10.3f}")
            logger.info(f"\n🧮 ПОШАГОВЫЙ РАСЧЕТ:")

            # ШАГ 1: Общая комиссия
            total_commission = self.bank_rate + self.acquiring_rate
            self._log_calculation_step(1, "Общая комиссия (банк + эквайринг)", total_commission * 100, "%")

            # ШАГ 2: Расчет НДС с прибыли
            profit_vat = target_profit * self.vat_percent / 100
            self._log_calculation_step(2, f"НДС с прибыли ({self.vat_percent}%)", profit_vat)

            # ШАГ 3: Необходимый forPay для достижения целевой прибыли
            target_forpay = (target_profit + profit_vat + logistics + purchase_price) / (1 - total_commission)
            self._log_calculation_step(3, "Целевой forPay (с учетом комиссии и НДС)", target_forpay)

            # ШАГ 4: Расчет priceWithDisc через коэффициент
            required_price_wd = target_forpay / self.forpay_ratio
            self._log_calculation_step(4, f"Целевая цена со скидкой (forPay / {self.forpay_ratio})", required_price_wd)

            # ШАГ 5: Проверка минимальной цены
            min_price = purchase_price * 1.1
            if required_price_wd < min_price:
                logger.warning(
                    f"⚠️  Цена {required_price_wd:.2f}₽ ниже минимальной {min_price:.2f}₽ (+10% к закупке), корректируем")
                required_price_wd = min_price
                self._log_calculation_step(5, "Минимальная цена (+10% к закупке)", required_price_wd)
            else:
                self._log_calculation_step(5, "Минимальная цена (+10% к закупке)", min_price)
                logger.info(f"      [✓] Текущая цена >= минимальной")

            # ШАГ 6: Расчет полной цены с учетом скидки
            if target_discount >= 100:
                target_discount = 99.9
                logger.warning("⚠️  Скидка >= 100%, ограничена до 99.9%")

            discount_factor = 1 - target_discount / 100
            needed_full_price = required_price_wd / discount_factor
            self._log_calculation_step(6, f"Полная цена (цена_со_скидкой / (1 - {target_discount}/100))",
                                       needed_full_price)

            # ШАГ 7: Округление до целого (вверх)
            new_price_rounded = math.ceil(needed_full_price)
            self._log_calculation_step(7, "Округленная цена (вверх)", new_price_rounded)

            # ШАГ 8: Расчет отклонения от текущей цены
            if current_price > 0:
                price_change = new_price_rounded - current_price
                price_change_percent = (price_change / current_price * 100)
            else:
                price_change = new_price_rounded
                price_change_percent = 100.0

            direction = "↑" if price_change > 0 else "↓" if price_change < 0 else "="
            logger.info(f"\n   [8] Изменение цены: {direction} {abs(price_change):.2f}₽ ({price_change_percent:+.2f}%)")

            # Детали расчета
            calculation_details = {
                'vendor_code': vendor_code,
                'nm_id': nm_id,
                'purchase_price': purchase_price,
                'target_profit': target_profit,
                'logistics': logistics,
                'profit_vat': profit_vat,
                'total_commission_percent': total_commission * 100,
                'target_forpay': target_forpay,
                'forpay_ratio': self.forpay_ratio,
                'required_price_wd': required_price_wd,
                'target_discount': target_discount,
                'needed_full_price': needed_full_price,
                'new_price_rounded': new_price_rounded,
                'current_price': current_price,
                'price_change': price_change,
                'price_change_percent': price_change_percent,
                'current_discount': current_discount
            }

            # Итог расчета
            self._log_calculation_footer(current_price, new_price_rounded, price_change_percent)

            return new_price_rounded, target_discount, calculation_details

        except Exception as e:
            logger.error(f"❌ Ошибка расчета цены для {product['vendor_code']}: {e}")
            logger.error(traceback.format_exc())
            return 0, 0, {}

    async def update_product_price(self, product_id: int, vendor_code: str,
                                   new_price: float, new_real_price: float,
                                   nm_id: int, discount: float):
        """Обновление цены в БД"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    current_time = datetime.now(pytz.timezone('Europe/Moscow'))

                    await cursor.execute("""
                        UPDATE oc_product 
                        SET price_wb = %s, 
                            wb_real_price = %s,
                            sku_wb = %s,
                            last_price_update = %s
                        WHERE product_id = %s
                    """, (new_price, new_real_price, nm_id, current_time, product_id))

                    logger.info(f"💾 Цена обновлена для {vendor_code}: {new_price:.2f}₽ (real: {new_real_price:.2f}₽)")
                    return True

        except Exception as e:
            logger.error(f"❌ Ошибка обновления цены в БД: {e}")
            return False

    async def process_cycle(self):
        """Один цикл обработки"""
        self._log_separator("ЦИКЛ ОБРАБОТКИ ТОВАРОВ БЕЗ ПРОДАЖ", "🔄")
        cycle_start = datetime.now()

        # Инициализация
        if not await self.initialize_db():
            return

        if not self.get_warehouse_tariff():
            logger.warning("⚠️  Используем дефолтные тарифы логистики")
            self.warehouse_tariff = {
                'warehouse_name': 'По умолчанию',
                'delivery_base': 50.0,
                'delivery_liter': 12.5
            }

        # Загрузка товаров
        products = await self.load_products_without_sales()

        if not products:
            logger.info("ℹ️  Нет товаров без продаж для обработки")
            return

        successful_updates = []
        wb_updates = []
        stats = {
            'total': len(products),
            'found_on_wb': 0,
            'price_changed': 0,
            'skipped_low_deviation': 0,
            'skipped_promotion': 0,
            'errors': 0
        }

        # Обработка каждого товара
        for idx, product in enumerate(products, 1):
            try:

                vendor_code = product['vendor_code']

                # ===== ПРОВЕРКА НА УЧАСТИЕ В АКЦИИ =====
                if product.get('promotion_active_wb') and Config.PROMOTION_LOCK_PRICES:
                    logger.info(f"🔒 Товар {vendor_code} участвует в акции – пропускаем")
                    stats['skipped_promotion'] = stats.get('skipped_promotion', 0) + 1
                    continue

                # ===== ПРОВЕРКА НА РУЧНУЮ БЛОКИРОВКУ =====
                lock_until = product.get('promotion_lock_until_wb')
                if lock_until:
                    # Приводим к naive для сравнения
                    if isinstance(lock_until, datetime):
                        if lock_until.tzinfo is not None:
                            lock_until = lock_until.replace(tzinfo=None)
                        if datetime.now() < lock_until:
                            logger.info(f"🔒 Товар {vendor_code} заблокирован до {lock_until} – пропускаем")
                            stats['skipped_promotion'] = stats.get('skipped_promotion', 0) + 1
                            continue

                logger.info(f"\n{'#' * 80}")
                logger.info(f"#{idx:3d}/{len(products):3d} 🔍 ОБРАБОТКА ТОВАРА: {vendor_code}")
                logger.info(f"{'#' * 80}\n")

                # Получаем nmId
                nm_id = product['sku_wb']
                if not nm_id or nm_id <= 0:
                    logger.info(f"🔍 Поиск nmID по артикулу поставщика: {vendor_code}")
                    nm_id = self.get_nm_id_by_vendor_code(vendor_code)
                    if not nm_id:
                        stats['errors'] += 1
                        logger.warning(f"⚠️  Пропускаем {vendor_code} - не найден на WB")
                        continue
                    product['sku_wb'] = nm_id

                logger.info(f"🆔 Найден nmID: {nm_id}")
                stats['found_on_wb'] += 1

                # Получаем информацию с WB
                logger.info(f"📡 Запрос информации о товаре с WB API...")
                wb_info = self.get_wb_product_info(nm_id)
                if not wb_info:
                    stats['errors'] += 1
                    logger.warning(f"⚠️  Не удалось получить информацию с WB для {vendor_code}")
                    continue

                logger.info(f"✅ Получена информация: {wb_info['vendorCode'][:50]}...")
                logger.info(f"   Текущая цена: {wb_info['price']:.2f}₽ | Скидка: {wb_info['discount']:.1f}%")

                # Рассчитываем целевую цену
                logger.info(f"\n{'─' * 80}")
                logger.info(f"🧮 ЗАПУСК РАСЧЕТА ЦЕЛЕВОЙ ЦЕНЫ")
                logger.info(f"{'─' * 80}")
                new_price, new_discount, details = self.calculate_target_price(product, wb_info)

                if new_price <= 0:
                    stats['errors'] += 1
                    logger.error(f"❌ Расчет вернул некорректную цену: {new_price}")
                    continue

                # Проверяем минимальное отклонение
                price_change_percent = details['price_change_percent']
                if abs(price_change_percent) < self.min_deviation_percent:
                    stats['skipped_low_deviation'] += 1
                    logger.info(
                        f"⏭️  {vendor_code}: отклонение {price_change_percent:.2f}% < {self.min_deviation_percent}% - пропускаем")
                    continue

                # Обновляем в БД
                new_real_price = details['required_price_wd']
                await self.update_product_price(
                    product['product_id'], vendor_code,
                    new_price, new_real_price, nm_id, new_discount
                )

                # Добавляем в список для отправки на WB
                wb_updates.append({
                    "nmID": nm_id,
                    "price": int(new_price),
                    "discount": int(new_discount)
                })

                successful_updates.append({
                    'vendor_code': vendor_code,
                    'old_price': wb_info['price'],
                    'new_price': new_price,
                    'discount': new_discount,
                    'change_percent': price_change_percent
                })

                stats['price_changed'] += 1

                # Пауза между запросами
                await asyncio.sleep(0.5)

            except Exception as e:
                stats['errors'] += 1
                logger.error(f"❌ Ошибка обработки {product.get('vendor_code', 'unknown')}: {e}")
                logger.error(traceback.format_exc())

        # Отправка на WB
        if wb_updates:
            if self.load_to_wb:
                logger.info(f"\n📤 ОТПРАВКА ЦЕН НА WILDBERRIES ({len(wb_updates)} товаров)")
                success = self.upload_prices_to_wb(wb_updates)
                if success:
                    logger.info(f"✅ Успешно отправлено на WB: {len(wb_updates)} товаров")
            else:
                logger.info(f"\nℹ️  РЕЖИМ ТЕСТИРОВАНИЯ: отправка на WB отключена")
                logger.info(f"   Подготовлено к отправке: {len(wb_updates)} товаров")

        # Статистика
        cycle_end = datetime.now()
        duration = (cycle_end - cycle_start).total_seconds()

        self._log_separator("СТАТИСТИКА ЦИКЛА", "📊")
        self._log_table("РЕЗУЛЬТАТЫ", {
            "Время выполнения": f"{duration:.1f} сек",
            "Всего товаров": stats['total'],
            "Найдено на WB": stats['found_on_wb'],
            "Изменено цен": stats['price_changed'],
            f"Пропущено (<{self.min_deviation_percent}%)": stats['skipped_low_deviation'],
            "Пропущено (акции)": stats['skipped_promotion'],
            "Ошибок": stats['errors']
        })

        # Примеры изменений
        if successful_updates:
            logger.info("\n📋 ПРИМЕРЫ ИЗМЕНЕНИЙ:")
            for update in successful_updates[:3]:
                arrow = "↑" if update['change_percent'] > 0 else "↓"
                logger.info(f"   • {update['vendor_code']}: {update['old_price']:.2f}₽ → "
                            f"{update['new_price']:.2f}₽ {arrow}({update['change_percent']:+.2f}%), "
                            f"скидка: {update['discount']:.1f}%")

        # Закрываем соединение
        if self.db_pool:
            self.db_pool.close()
            await self.db_pool.wait_closed()

    async def run(self):
        """Основной цикл работы"""
        logger.info("🚀 ЗАПУСК СИСТЕМЫ ОБРАБОТКИ ТОВАРОВ БЕЗ ПРОДАЖ")
        logger.info(f"⏰ Интервал работы: {Config.WB_NO_SALES_INTERVAL / 60} минут")
        logger.info(f"📦 Товары без продаж: > {self.hours_without_sales} часов")
        logger.info(f"📊 Минимальное отклонение: {self.min_deviation_percent}%")
        logger.info(f"💰 Коэффициент forPay: {self.forpay_ratio}")
        logger.info(f"🔄 LOAD_TO_WB: {'ВКЛЮЧЕНО' if self.load_to_wb else 'ОТКЛЮЧЕНО (тестовый режим)'}")

        while True:
            try:
                await self.process_cycle()

                # Ожидание следующего цикла
                logger.info(f"\n{'=' * 80}")
                logger.info(f"⏳ Следующий цикл через {Config.WB_NO_SALES_INTERVAL / 60} минут...")
                logger.info(f"{'=' * 80}\n")
                await asyncio.sleep(Config.WB_NO_SALES_INTERVAL)

            except KeyboardInterrupt:
                logger.info("🛑 Остановка по запросу пользователя")
                break
            except Exception as e:
                logger.error(f"❌ Фатальная ошибка: {e}")
                logger.error(traceback.format_exc())
                await asyncio.sleep(300)


async def main():
    """Точка входа"""
    updater = NoSalesPriceUpdater()
    await updater.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Программа остановлена пользователем")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        logger.error(traceback.format_exc())