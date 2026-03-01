"""
Клиент для работы с СДЭК API (расчёт логистики)
"""

import logging
import traceback
import json

from datetime import datetime, timedelta
from typing import Tuple
from aiohttp import ClientTimeout, ClientSession
from config import Config



class CdekAPI:
    """Клиент для работы с СДЭК API (расчёт логистики)"""

    def __init__(self, client_id: str, client_secret: str, logger: logging.Logger):
        self.client_id = client_id
        self.client_secret = client_secret
        self.logger = logger
        self.session = None
        self.access_token = None
        self.token_expires_at = None


    async def __aenter__(self):
        timeout = ClientTimeout(total=30)
        self.session = ClientSession(timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _get_token(self) -> str:
        """Получает или обновляет токен доступа к СДЭК API"""
        if self.access_token and self.token_expires_at and datetime.now() < self.token_expires_at:
            return self.access_token

        url = f"{Config.CDEK_API_URL}/oauth/token"
        payload = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }

        try:
            async with self.session.post(url, data=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.access_token = data['access_token']
                    self.token_expires_at = datetime.now() + timedelta(seconds=data.get('expires_in', 3600) - 300)
                    self.logger.debug(f"✅ Получен новый токен СДЭК (действителен до {self.token_expires_at})")
                    return self.access_token
                else:
                    text = await resp.text()
                    raise Exception(f"Ошибка получения токена СДЭК: {resp.status} - {text[:200]}")
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения токена СДЭК: {e}")
            raise

    def _map_region_to_cdek_city(self, region: str, city: str = "") -> str:
        """
        Маппинг региона/города Ozon → код города СДЭК
        """
        city_lower = city.lower() if city else ""
        region_lower = region.lower() if region else ""

        city_mapping = {
            "москва": "44",
            "санкт-петербург": "137",
            "новосибирск": "114",
            "екатеринбург": "444",
            "казань": "551",
            "нижний новгород": "504",
            "челябинск": "549",
            "самара": "514",
            "омск": "232",
            "ростов-на-дону": "66",
            "уфа": "69",
            "красноярск": "107",
            "воронеж": "65",
            "пермь": "535"
        }

        for ozon_name, cdek_code in city_mapping.items():
            if ozon_name in city_lower:
                return cdek_code

        region_mapping = {
            "московская": "44",
            "ленинградская": "137",
            "свердловская": "444",
            "республика татарстан": "551",
            "ростовская": "66",
            "воронежская": "65"
        }

        for ozon_region, cdek_code in region_mapping.items():
            if ozon_region in region_lower:
                return cdek_code

        return "44"

    async def calculate_delivery(self,
                                 weight_kg: float,
                                 length_cm: float,
                                 width_cm: float,
                                 height_cm: float,
                                 to_region: str,
                                 to_city: str = "") -> Tuple[float, int, str]:
        """
        Рассчитывает стоимость и сроки доставки СДЭК с детальным логированием запроса/ответа
        """
        try:
            token = await self._get_token()
            to_city_code = self._map_region_to_cdek_city(to_region, to_city)

            weight_g = int(weight_kg * 1000)
            length = int(length_cm)
            width = int(width_cm)
            height = int(height_cm)

            url = f"{Config.CDEK_API_URL}/calculator/tariff"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            payload = {
                "type": 1,
                "currency": 643,
                "tariff_code": 136,
                "from_location": {
                    "code": Config.CDEK_WAREHOUSE_CITY_CODE
                },
                "to_location": {
                    "code": to_city_code
                },
                "packages": [
                    {
                        "weight": weight_g,
                        "length": length,
                        "width": width,
                        "height": height
                    }
                ]
            }

            # ===== ЛОГИРОВАНИЕ ЗАПРОСА =====
            self.logger.debug(f"📤 СДЭК ЗАПРОС к {url}:")
            self.logger.debug(f"   Тариф: 136 (Стандарт)")
            self.logger.debug(f"   Откуда: код {Config.CDEK_WAREHOUSE_CITY_CODE}")
            self.logger.debug(f"   Куда: {to_region} ({to_city}) → код {to_city_code}")
            self.logger.debug(f"   Параметры посылки:")
            self.logger.debug(f"     • Вес: {weight_kg:.2f} кг ({weight_g} г)")
            self.logger.debug(f"     • Габариты: {length}×{width}×{height} см")
            self.logger.debug(f"     • Сумма габаритов: {length + width + height} см")

            async with self.session.post(url, headers=headers, json=payload) as resp:
                response_text = await resp.text()

                # ===== ЛОГИРОВАНИЕ ОТВЕТА =====
                self.logger.debug(f"📥 СДЭК ОТВЕТ (статус {resp.status}):")
                try:
                    response_json = json.loads(response_text)
                    self.logger.debug(f"   {json.dumps(response_json, indent=2, ensure_ascii=False)}")
                except:
                    self.logger.debug(f"   {response_text[:500]}")

                if resp.status == 200:
                    data = json.loads(response_text)
                    delivery_cost = float(data.get('delivery_sum', data.get('total_sum', 250.0)))
                    delivery_days = int(data.get('period_max', data.get('calendar_max', 5)))
                    tariff_name = "СДЭК Стандарт 136"

                    self.logger.debug(
                        f"✅ СДЭК: {to_region} | "
                        f"{weight_kg}кг {length}x{width}x{height}см | "
                        f"стоимость: {delivery_cost:.2f}₽, срок: {delivery_days}дн"
                    )
                    return delivery_cost, delivery_days, tariff_name



                else:
                    # Детальный анализ ошибки
                    error_data = {}
                    try:
                        error_data = json.loads(response_text)
                    except:
                        pass

                    if error_data and 'errors' in error_data:
                        for err in error_data['errors']:
                            self.logger.warning(
                                f"⚠️ СДЭК ошибка {resp.status}: "
                                f"code={err.get('code')}, "
                                f"additional_code={err.get('additional_code')}, "
                                f"message={err.get('message')}"
                            )

                    self.logger.warning(
                        f"⚠️ СДЭК API ошибка {resp.status} для региона '{to_region}': {response_text[:300]}"
                    )
                    return 250.0, 5, "fallback"
        except Exception as e:
            self.logger.error(f"❌ Ошибка расчёта СДЭК для региона '{to_region}': {e}")
            self.logger.debug(f"Стек вызовов: {traceback.format_exc()}")
            return 250.0, 5, "error_fallback"