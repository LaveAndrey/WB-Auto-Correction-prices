import asyncio
import json
import aiohttp
from collections import defaultdict
from datetime import datetime, timedelta
import pytz
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from typing import Dict, List
from config import Config
from WB_integration.structurs.dataclass import OrderData


class WBApiClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.logger = None  # Логгер будет установлен из PriceUpdater

    def set_logger(self, logger):
        """Устанавливает логгер из PriceUpdater"""
        self.logger = logger

    def _log_separator(self, title: str = "", char: str = "=", length: int = 80):
        if self.logger:
            if title:
                padding = (length - len(title) - 4) // 2
                self.logger.info(f"\n{char * padding} {title} {char * padding}")
            else:
                self.logger.info(f"\n{char * length}")

    def _log_table(self, title: str, data: dict):
        if not self.logger:
            return

        self._log_separator(title, "-")
        max_key_len = max(len(str(k)) for k in data.keys())

        for key, value in data.items():
            if isinstance(value, float):
                value_str = f"{value:,.2f}" if value >= 1000 else f"{value:.2f}"
            else:
                value_str = str(value)

            self.logger.info(f"  {key:<{max_key_len}} : {value_str}")
        self._log_separator("-", "-")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def fetch_wb_orders(self) -> Dict:
        """Получает заказы с WB и группирует по nmId"""
        if self.logger:
            self._log_separator("ПОЛУЧЕНИЕ ЗАКАЗОВ С WB", "-")

        moscow_tz = pytz.timezone('Europe/Moscow')
        period_start = datetime.now(moscow_tz) - timedelta(hours=Config.SALES_HOURS_FILTER)
        date_from = period_start.strftime("%Y-%m-%dT%H:%M:%S")

        if self.logger:
            self.logger.info(f"📊 Запрашиваем заказы за последние {Config.SALES_HOURS_FILTER} часов")
            self.logger.info(f"📅 Период: с {date_from}")
            self.logger.info(f"🔍 Группировка по nmId")

        headers = {
            "Authorization": Config.WB_SALES_TOKEN,
            "Accept": "application/json"
        }
        params = {
            "dateFrom": date_from,
            "flag": 0
        }

        try:
            async with self.session.get(Config.ORDERS_API_URL, headers=headers, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()

                    #Для отладки можно раскомментировать
                    #print(json.dumps(data, indent=2, ensure_ascii=False))

                    # Группируем по nmId
                    orders_by_nm_id = defaultdict(list)
                    nm_id_to_vendor_code = {}  # Маппинг nmId -> vendor_code

                    total_orders = 0
                    valid_orders = 0
                    orders_without_nm_id = 0

                    for item in data:
                        total_orders += 1

                        # Пропускаем отмененные заказы
                        if item.get("isCancel", False):
                            continue

                        order = OrderData.from_api_dict(item)
                        if not order:
                            continue

                        valid_orders += 1

                        nm_id = order.nm_id
                        if nm_id > 0:
                            orders_by_nm_id[nm_id].append(order)
                            # Сохраняем маппинг nmId -> vendor_code
                            if nm_id not in nm_id_to_vendor_code:
                                nm_id_to_vendor_code[nm_id] = order.vendor_code
                        else:
                            orders_without_nm_id += 1

                    if self.logger:
                        self._log_table("СТАТИСТИКА ЗАКАЗОВ", {
                            "Всего заказов": total_orders,
                            "Валидных (не отмененных)": valid_orders,
                            "С nmId > 0": valid_orders - orders_without_nm_id,
                            "Без nmId": orders_without_nm_id,
                            "Уникальных nmId": len(orders_by_nm_id)
                        })

                        # Логируем примеры найденных nmId
                        if orders_by_nm_id:
                            self.logger.info("📊 Примеры nmId в заказах:")
                            for nm_id, orders in list(orders_by_nm_id.items())[:5]:
                                vendor_code = orders[0].vendor_code if orders else "N/A"
                                self.logger.info(
                                    f"   • nmId: {nm_id} -> Артикул: {vendor_code}, заказов: {len(orders)}")
                        else:
                            self.logger.warning("⚠️  В заказах не найдено nmId")
                            # Выводим структуру первого заказа для отладки
                            if data and len(data) > 0:
                                self.logger.info("📋 Структура первого заказа для отладки:")
                                first_item = data[0]
                                for key, value in first_item.items():
                                    if 'nm' in key.lower() or 'id' in key.lower():
                                        self.logger.info(f"   {key}: {value}")

                    return {
                        'by_nm_id': orders_by_nm_id,
                        'nm_id_to_vendor_code': nm_id_to_vendor_code
                    }
                else:
                    text = await resp.text()
                    if self.logger:
                        self.logger.error(f"❌ Ошибка API WB: {resp.status}")
                        self.logger.error(f"   Ответ: {text[:200]}")
                    return {'by_nm_id': defaultdict(list), 'nm_id_to_vendor_code': {}}

        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Ошибка при запросе к WB API: {e}")
                import traceback
                self.logger.error(f"   Трейсбэк: {traceback.format_exc()}")
            return {'by_nm_id': defaultdict(list), 'nm_id_to_vendor_code': {}}

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def fetch_nm_id(self, vendor_code: str) -> int:
        """Поиск nmId по артикулу"""
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
                if resp.status == 200:
                    data = await resp.json()
                    cards = data.get("cards", [])

                    if not cards:
                        return 0

                    for card in cards:
                        card_vendor_code = str(card.get("vendorCode", "")).strip()
                        if card_vendor_code == str(vendor_code).strip():
                            nm_id = card.get("nmID", 0)
                            if nm_id:
                                return nm_id

                    return 0
                else:
                    return 0

        except Exception:
            return 0

    async def get_warehouse_orders_stats(self, days: int = 14) -> Dict[str, int]:
        """Получает статистику заказов по складам"""
        try:
            from datetime import datetime, timedelta
            date_from = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

            headers = {"Authorization": Config.WB_PRICES_TOKEN}
            params = {"dateFrom": date_from, "flag": 0}

            async with self.session.get(Config.ORDERS_API_URL, headers=headers, params=params, timeout=30) as resp:
                if resp.status != 200:
                    return {}

                orders = await resp.json()
                warehouse_stats = {}

                for order in orders:
                    warehouse_name = order.get('warehouseName')
                    if warehouse_name and warehouse_name.strip():
                        warehouse_stats[warehouse_name] = warehouse_stats.get(warehouse_name, 0) + 1

                return warehouse_stats

        except Exception:
            return {}

    async def get_all_warehouses(self) -> List[Dict]:
        """Получает список всех складов"""
        try:
            headers = {"Authorization": Config.WB_PRICES_TOKEN}

            async with self.session.get(Config.WAREHOUSES_API_URL, headers=headers, timeout=15) as resp:
                if resp.status != 200:
                    return []

                return await resp.json()

        except Exception:
            return []

    async def get_warehouse_tariffs(self, warehouse_ids: List[int]) -> List[Dict]:
        """Получает тарифы логистики для складов"""
        try:
            headers = {"Authorization": Config.WB_PRICES_TOKEN}
            all_tariffs = []

            for i in range(0, len(warehouse_ids), 10):
                batch = warehouse_ids[i:i + 10]
                params = {"warehouseIDs": ",".join(str(id) for id in batch)}

                async with self.session.get(Config.WB_TARIFF_URL, headers=headers, params=params, timeout=30) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(10)
                        continue

                    if resp.status != 200:
                        continue

                    batch_tariffs = await resp.json()
                    all_tariffs.extend(batch_tariffs)

                    if i + 10 < len(warehouse_ids):
                        await asyncio.sleep(1)

            return all_tariffs

        except Exception:
            return []

