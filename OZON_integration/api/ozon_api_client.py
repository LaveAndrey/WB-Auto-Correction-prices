#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Клиент для работы с Ozon Seller API
"""
import json
import logging
import traceback
from typing import Dict, List
from datetime import datetime, timedelta

import aiohttp
import pytz
from aiohttp import ClientTimeout, ClientSession
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from OZON_integration.structures.dataclass import OzonOrderData, OzonPromotion
import asyncio
from config import Config


class OzonAPI:
    """Клиент для работы с Ozon Seller API"""

    def __init__(self, client_id: str, api_key: str, logger: logging.Logger):
        self.client_id = client_id
        self.api_key = api_key
        self.logger = logger
        self.session = None

    async def __aenter__(self):
        timeout = ClientTimeout(total=60)
        self.session = ClientSession(timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json"
        }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def get_orders(self, hours_back: int = 24) -> List[OzonOrderData]:
        """
        Получает ВСЕ неотмененные заказы за последние N часов.
        Важно: для анализа продаж нужны все активные заказы, включая новые (awaiting_packaging)!
        """
        moscow_tz = pytz.timezone('Europe/Moscow')
        now_msk = datetime.now(moscow_tz)
        since_msk = now_msk - timedelta(hours=hours_back)


        since_utc = since_msk.astimezone(pytz.UTC).strftime('%Y-%m-%dT%H:%M:%SZ')
        to_utc = now_msk.astimezone(pytz.UTC).strftime('%Y-%m-%dT%H:%M:%SZ')

        url = f"{Config.OZON_API_URL}/v3/posting/fbs/list"


        payload = {
            "dir": "asc",
            "filter": {
                "since": since_utc,
                "to": to_utc,
            },
            "limit": 1000,
            "offset": 0,
            "with": {
                "analytics_data": True,
                "financial_data": True
            }
        }

        try:
            async with self.session.post(url, headers=self._get_headers(), json=payload) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    self.logger.error(f"❌ Ozon orders API error {resp.status}: {text[:500]}")
                    return []

                data = await resp.json()
                #print(json.dumps(data, indent=2, ensure_ascii=False))
                postings = data.get('result', {}).get('postings', [])

                self.logger.debug(f"📥 Получено {len(postings)} постингов от Ozon API (все статусы)")

                status_counts = {}
                for p in postings:
                    status = p.get('status', 'unknown')
                    status_counts[status] = status_counts.get(status, 0) + 1
                self.logger.debug(f"📊 Статусы заказов: {status_counts}")

                orders = []
                for posting in postings:
                    status = posting.get('status', '')

                    if status == 'cancelled' or posting.get('is_cancelled') or posting.get('cancellation_type'):
                        self.logger.debug(
                            f"⏭️  Пропущен ОТМЕНЕННЫЙ заказ {posting.get('posting_number', 'N/A')} | Статус: {status}")
                        continue

                    order = OzonOrderData.from_api_dict(posting)
                    if not order:
                        self.logger.debug(
                            f"⏭️  Пропущен заказ {posting.get('posting_number', 'N/A')} — не удалось распарсить данные товара")
                        continue

                    orders.append(order)

                    self.logger.debug(
                        f"✅ Захвачен заказ {order.posting_number} | "
                        f"Статус: {order.status} | "
                        f"Артикул: {order.offer_id} | "
                        f"Кол-во: {order.quantity} шт | "
                        f"Цена: {order.price:.2f}₽ | "
                        f"Выплата: {order.payout:.2f}₽"
                    )

                self.logger.debug(
                    f"✅ Получено {len(orders)} НЕОТМЕНЕННЫХ заказов с Ozon за {hours_back}ч "
                    f"(включая {status_counts.get('awaiting_packaging', 0)} новых в статусе 'awaiting_packaging')"
                )
                return orders

        except Exception as e:
            self.logger.error(f"❌ Ошибка получения заказов Ozon: {e}")
            self.logger.error(traceback.format_exc())
            return []

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def get_current_prices(self, offer_ids: List[str]) -> Dict[str, float]:
        """Получает текущие цены товаров по артикулам (offer_id)"""
        url = f"{Config.OZON_API_URL}/v4/product/info/prices"
        payload = {
            "filter": {
                "offer_id": offer_ids,
                "visibility": "ALL"
            },
            "last_id": "",
            "limit": 1000
        }

        try:
            async with self.session.post(url, headers=self._get_headers(), json=payload) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    self.logger.error(f"❌ Ozon prices API error {resp.status}: {text[:500]}")
                    return {}

                data = await resp.json()
                prices = {}
                for item in data.get('result', {}).get('items', []):
                    offer_id = item.get('offer_id')
                    price_data = item.get('price', {})
                    price_str = price_data.get('price', '0').replace(' ', '').replace(',', '.')
                    try:
                        price = float(price_str)
                        prices[offer_id] = price
                    except (ValueError, TypeError):
                        self.logger.warning(f"⚠️ Невалидная цена для offer_id {offer_id}: {price_str}")

                return prices

        except Exception as e:
            self.logger.error(f"❌ Ошибка получения цен Ozon: {e}")
            return {}

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def update_prices(self, price_updates: List[Dict]) -> bool:
        """
        Обновляет цены на товары.
        ВАЖНО: Ozon разрешает только 1 запрос в минуту на этот эндпоинт!
        """
        url = f"{Config.OZON_API_URL}/v1/product/import/prices"
        payload = {"prices": price_updates}

        try:
            self.logger.debug(f"📤 Отправка цен на Ozon: {json.dumps(payload, ensure_ascii=False)[:500]}")

            async with self.session.post(url, headers=self._get_headers(), json=payload) as resp:
                response_text = await resp.text()
                self.logger.debug(f"📥 Ответ Ozon (статус {resp.status}): {response_text[:500]}")

                if resp.status == 200:
                    try:
                        result = json.loads(response_text)
                        # Проверяем тип ответа и безопасно извлекаем task_id
                        if isinstance(result, dict):
                            task_id = None
                            if 'result' in result:
                                if isinstance(result['result'], dict):
                                    task_id = result['result'].get('task_id')
                                elif isinstance(result['result'], list) and len(result['result']) > 0:
                                    # Если result - это список, берем первый элемент
                                    task_id = result['result'][0].get('task_id') if isinstance(result['result'][0],
                                                                                               dict) else None

                            self.logger.debug(f"✅ Цены отправлены на обработку Ozon (task_id: {task_id})")
                        elif isinstance(result, list):
                            # Если ответ - это список
                            self.logger.info(
                                f"✅ Цены отправлены на обработку Ozon (ответ - список из {len(result)} элементов)")
                        else:
                            self.logger.info(f"✅ Цены отправлены на обработку Ozon (ответ: {type(result)}")

                        return True
                    except json.JSONDecodeError as e:
                        self.logger.error(f"❌ Ошибка парсинга JSON ответа Ozon: {e}")
                        return False
                else:
                    self.logger.error(f"❌ Ozon price update error {resp.status}: {response_text[:500]}")
                    return False

        except Exception as e:
            self.logger.error(f"❌ Ошибка обновления цен Ozon: {e}")
            self.logger.error(traceback.format_exc())
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def get_promotions(self) -> List['OzonPromotion']:
        """Получает список доступных акций Ozon (API: GET /v1/actions)"""
        url = f"{Config.OZON_API_URL}/v1/actions"

        try:
            async with self.session.get(url, headers=self._get_headers()) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    #print(json.dumps(data, indent=2, ensure_ascii=False))
                    promotions = []
                    for promo in data.get('result', []):
                        date_start = None
                        date_end = None
                        freeze_date = None

                        if promo.get('date_start'):
                            date_start = datetime.fromisoformat(promo.get('date_start').replace('Z', '+00:00'))
                        if promo.get('date_end'):
                            date_end = datetime.fromisoformat(promo.get('date_end').replace('Z', '+00:00'))
                        if promo.get('freeze_date'):
                            freeze_date = datetime.fromisoformat(promo.get('freeze_date').replace('Z', '+00:00'))

                        promotions.append(OzonPromotion(
                            promotion_id=promo.get('id', 0),
                            title=promo.get('title', ''),
                            action_type=promo.get('action_type', ''),
                            date_start=date_start,
                            date_end=date_end,
                            freeze_date=freeze_date,
                            discount_type=promo.get('discount_type', ''),
                            discount_value=float(promo.get('discount_value', 0)),
                            is_participating=promo.get('is_participating', False),
                            is_voucher_action=promo.get('is_voucher_action', False),
                            potential_products_count=int(promo.get('potential_products_count', 0)),
                            participating_products_count=int(promo.get('participating_products_count', 0)),
                            banned_products_count=int(promo.get('banned_products_count', 0))
                        ))

                    self.logger.info(f"✅ Получено {len(promotions)} акций от Ozon")
                    return promotions
                else:
                    text = await resp.text()
                    self.logger.error(f"❌ Ozon promotions API error {resp.status}: {text[:500]}")
                    return []
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения акций: {e}")
            return []

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def get_promotion_candidates(self, action_id: int, limit: int = 1000) -> List[Dict]:
        """Получает список товаров, которые МОГУТ участвовать в акции (API: POST /v1/actions/candidates)"""
        url = f"{Config.OZON_API_URL}/v1/actions/candidates"
        payload = {
            "action_id": action_id,
            "limit": limit,
            "offset": 0,
            "last_id": ""
        }

        try:
            async with self.session.post(url, headers=self._get_headers(), json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    print(json.dumps(data, indent=2, ensure_ascii=False))
                    products = data.get('result', {}).get('products', [])
                    self.logger.info(f"✅ Получено {len(products)} товаров-кандидатов для акции {action_id}")
                    return products
                else:
                    text = await resp.text()
                    self.logger.error(f"❌ Ozon candidates API error {resp.status}: {text[:500]}")
                    return []
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения кандидатов акции: {e}")
            return []

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def get_promotion_products(self, action_id: int, limit: int = 1000) -> List[Dict]:
        """Получает список товаров, которые УЖЕ участвуют в акции (API: POST /v1/actions/products)"""
        url = f"{Config.OZON_API_URL}/v1/actions/products"
        payload = {
            "action_id": action_id,
            "limit": limit,
            "offset": 0,
            "last_id": ""
        }

        try:
            async with self.session.post(url, headers=self._get_headers(), json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    #print(json.dumps(data, indent=2, ensure_ascii=False))
                    products = data.get('result', {}).get('products', [])
                    self.logger.info(f"✅ Получено {len(products)} участвующих товаров для акции {action_id}")
                    return products
                else:
                    text = await resp.text()
                    self.logger.error(f"❌ Ozon products API error {resp.status}: {text[:500]}")
                    return []
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения товаров акции: {e}")
            return []

    async def get_products_promotion_status(self, offer_ids: List[str]) -> Dict[str, Dict]:
        """
        Получает информацию об участии товаров в акциях через /v5/product/info/prices.
        Возвращает {offer_id: {"in_promotion": bool, "action_title": str, ...}}
        """
        url = f"{Config.OZON_API_URL}/v5/product/info/prices"

        # Важно: filter должен быть объектом, даже если пустой [citation:1]
        payload = {
            "filter": {
                "offer_id": offer_ids,
                "visibility": "ALL"
            },
            "limit": 1000
        }

        try:
            async with self.session.post(url, headers=self._get_headers(), json=payload) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    self.logger.error(f"Ozon API error {resp.status}: {text[:500]}")
                    return {}

                data = await resp.json()
                result = {}

                for item in data.get('items', []):
                    offer_id = item.get('offer_id')
                    marketing_actions = item.get('marketing_actions', {})
                    actions = marketing_actions.get('actions', [])
                    price_data = item.get('price', {})
                    auto_add_enabled = price_data.get('auto_add_to_ozon_actions_list_enabled', True)

                    # Товар в акции, если есть хотя бы одна запись в actions
                    in_promotion = len(actions) > 0

                    # Можно сохранить первую акцию для информации
                    first_action = actions[0] if actions else {}

                    result[offer_id] = {
                        "in_promotion": in_promotion,
                        "action_title": first_action.get('title', ''),
                        "action_value": first_action.get('value', 0),
                        "date_from": first_action.get('date_from'),
                        "date_to": first_action.get('date_to'),
                        "actions_count": len(actions),
                        "auto_add_enabled": auto_add_enabled  # новое поле
                    }

                self.logger.debug(f"Получен статус для {len(result)} товаров")
                return result

        except Exception as e:
            self.logger.error(f"Ошибка в get_products_promotion_status: {e}")
            return {}