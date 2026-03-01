import asyncio
import json
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from config import Config
from typing import List, Dict


class PromoClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def get_wb_discounts(self, nm_ids: List[int]) -> Dict[int, Dict[str, float]]:
        """Получение актуальных скидок с WB API v2"""
        if not nm_ids:
            return {}

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
                            results[nm_id] = {"discount": 0}
                            error_count += 1
                            continue

                        list_goods = data.get("data", {}).get("listGoods", [])

                        if not list_goods:
                            results[nm_id] = {"discount": 0}
                            no_data_count += 1
                            continue

                        goods_info = list_goods[0]
                        discount = float(goods_info.get("discount", 0))

                        results[nm_id] = {
                            "discount": discount,
                            "effective_discount": discount
                        }
                        success_count += 1

                    elif resp.status in [401, 403]:
                        results[nm_id] = {"discount": 0}
                        error_count += 1
                    else:
                        results[nm_id] = {"discount": 0}
                        error_count += 1

                await asyncio.sleep(0.6)

            except asyncio.TimeoutError:
                results[nm_id] = {"discount": 0}
                error_count += 1
                await asyncio.sleep(1.0)
            except Exception:
                results[nm_id] = {"discount": 0}
                error_count += 1
                await asyncio.sleep(0.5)

        return results

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def upload_prices_to_wb(self, price_data: List[Dict]) -> Dict:
        """Отправка цен на Wildberries"""
        if not price_data:
            return {}

        url = "https://discounts-prices-api.wildberries.ru/api/v2/upload/task"
        headers = {
            "Authorization": Config.WB_PRICES_TOKEN,
            "Content-Type": "application/json"
        }

        try:
            async with self.session.post(url, headers=headers, json={"data": price_data}) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    text = await resp.text()
                    return {"error": True, "status": resp.status, "message": text[:500]}
        except Exception as e:
            return {"error": True, "exception": str(e)}