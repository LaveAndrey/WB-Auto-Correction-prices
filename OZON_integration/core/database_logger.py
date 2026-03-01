#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Логирование событий в базу данных
"""

import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any

import pytz

from config import Config


class DatabaseLogger:
    """Логирование событий в базу данных"""

    def __init__(self, db_pool):
        self.db_pool = db_pool
        self.cycle_id = 0
        self.error_logger = logging.getLogger('ozon_price_updater.db')

    async def set_cycle_id(self, cycle_id: int):
        """Устанавливает ID текущего цикла"""
        self.cycle_id = cycle_id

    async def log(self,
                  level: str,
                  message: str,
                  vendor_code: str = None,
                  mp: str = 'ozon',  # ✅ По умолчанию 'ozon' для Ozon
                  details: dict = None,
                  sku_ozon: int = None,  # ✅ SKU Ozon
                  action_type: str = None,
                  old_value: float = None,
                  new_value: float = None,
                  profit_before: float = None,
                  profit_after: float = None,
                  **extra):
        """Универсальный метод логирования"""
        try:
            enriched_details = details.copy() if details else {}
            enriched_details.update({
                'timestamp': datetime.now(pytz.timezone('Europe/Moscow')).isoformat(),
                'cycle_id': self.cycle_id,
                'mp': mp,
                **extra
            })

            if sku_ozon:
                enriched_details['sku_ozon'] = sku_ozon
            if action_type:
                enriched_details['action_type'] = action_type
            if old_value is not None:
                enriched_details['old_value'] = old_value
            if new_value is not None:
                enriched_details['new_value'] = new_value
            if profit_before is not None:
                enriched_details['profit_before'] = round(profit_before, 2)
            if profit_after is not None:
                enriched_details['profit_after'] = round(profit_after, 2)

            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"""
                        INSERT INTO {Config.LOGS_TABLE}
                        (level, vendor_code, message, details, cycle_id, created_at, mp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        level,
                        vendor_code,
                        message,
                        json.dumps(enriched_details, ensure_ascii=False, default=str) if enriched_details else None,
                        self.cycle_id,
                        datetime.now(pytz.timezone('Europe/Moscow')),
                        mp
                    ))
        except Exception as e:
            self.error_logger.error(f"❌ Ошибка записи в лог БД: {e}")

    # ✅ СПЕЦИАЛИЗИРОВАННЫЕ МЕТОДЫ ДЛЯ OZON

    async def log_cycle_start(self, cycle_id: int):
        """Лог начала цикла"""
        await self.log(
            level="INFO",
            message=f"🚀 Начат цикл #{cycle_id} (Ozon)",
            details={
                "event": "cycle_start",
                "cycle_number": cycle_id
            }
        )

    async def log_cycle_end(self, cycle_id: int, stats: Dict[str, Any]):
        """Лог окончания цикла"""
        await self.log(
            level="INFO",
            message=f"✅ Завершен цикл #{cycle_id} (Ozon)",
            details={
                "event": "cycle_end",
                "cycle_number": cycle_id,
                "statistics": stats
            }
        )

    async def log_promotion_ozon(self,
                                 vendor_code: str,
                                 sku_ozon: int,
                                 promotion_id: int,
                                 promotion_title: str,
                                 action: str,  # 'ramp_start', 'price_increased', 'locked', 'unlocked', 'restored'
                                 old_price: float = None,
                                 new_price: float = None,
                                 daily_increase: float = None,
                                 days_until_start: int = None,
                                 details: Dict[str, Any] = None):
        """
        Логирование действий с акциями Ozon

        action может быть:
        - 'ramp_start' - начало повышения цены перед акцией
        - 'price_increased' - ежедневное повышение цены
        - 'locked' - блокировка цены на время акции
        - 'unlocked' - разблокировка после акции
        - 'restored' - восстановление исходной цены
        - 'skipped_promotion' - пропуск товара из-за акции
        """

        action_messages = {
            'ramp_start': '🚀 Начало повышения цены перед акцией',
            'price_increased': '📈 Повышение цены перед акцией',
            'locked': '🔒 Блокировка цены на время акции',
            'unlocked': '🔓 Разблокировка цены после акции',
            'restored': '🔄 Восстановление исходной цены после акции',
            'skipped_promotion': '⏭️ Пропуск товара (участвует в акции)'
        }

        message = action_messages.get(action, f"Акция: {action}")

        log_details = {
            "event": "ozon_promotion",
            "promotion_id": promotion_id,
            "promotion_title": promotion_title,
            "action": action,
            "vendor_code": vendor_code,
            "sku_ozon": sku_ozon
        }

        if old_price is not None:
            log_details["old_price"] = round(old_price, 2)
        if new_price is not None:
            log_details["new_price"] = round(new_price, 2)
        if daily_increase is not None:
            log_details["daily_increase"] = round(daily_increase, 2)
        if days_until_start is not None:
            log_details["days_until_start"] = days_until_start
        if details:
            log_details.update(details)

        await self.log(
            level="INFO",
            message=f"{message} для {vendor_code}: {promotion_title}",
            vendor_code=vendor_code,
            sku_ozon=sku_ozon,
            action_type="promotion",
            details=log_details
        )

    async def log_price_calculation(self,
                                    vendor_code: str,
                                    sku_ozon: int,
                                    old_base_price: float,
                                    new_base_price: float,
                                    old_discounted_price: float,
                                    new_discounted_price: float,
                                    seller_discount: float,
                                    current_profit: float,
                                    target_profit: float,
                                    logistics_cost: float,
                                    sales_count: int,
                                    weight_kg: float,
                                    region: str,
                                    details: Dict[str, Any]):
        """Лог расчета цены для Ozon с учетом скидки"""

        price_diff = new_discounted_price - old_discounted_price
        profit_diff = target_profit - current_profit

        message = (f"💰 Ozon {vendor_code}: {old_discounted_price:.0f}₽ → {new_discounted_price:.0f}₽ "
                   f"({price_diff:+.0f}₽, {price_diff / old_discounted_price * 100:+.1f}%) | "
                   f"прибыль: {current_profit:.0f}₽ → {target_profit:.0f}₽ ({profit_diff:+.0f}₽)")

        await self.log(
            level="SUCCESS",
            message=message,
            vendor_code=vendor_code,
            sku_ozon=sku_ozon,
            action_type="price_change",
            old_value=old_discounted_price,
            new_value=new_discounted_price,
            profit_before=current_profit,
            profit_after=target_profit,
            details={
                "event": "price_calculation",
                "old_base_price": round(old_base_price, 2),
                "new_base_price": round(new_base_price, 2),
                "old_discounted_price": round(old_discounted_price, 2),
                "new_discounted_price": round(new_discounted_price, 2),
                "price_change": round(price_diff, 2),
                "price_change_percent": round(price_diff / old_discounted_price * 100 if old_discounted_price else 0,
                                              2),
                "seller_discount": seller_discount,
                "current_profit": round(current_profit, 2),
                "target_profit": round(target_profit, 2),
                "profit_diff": round(profit_diff, 2),
                "logistics_cost": round(logistics_cost, 2),
                "sales_used": sales_count,
                "weight_kg": weight_kg,
                "region": region,
                **details
            }
        )

    async def log_skip(self,
                       vendor_code: str,
                       sku_ozon: int,
                       reason: str,
                       details: Dict[str, Any]):
        """Лог пропуска товара"""
        await self.log(
            level="WARNING",
            message=f"⏭️ Ozon {vendor_code}: {reason}",
            vendor_code=vendor_code,
            sku_ozon=sku_ozon,
            details={
                "event": "skip",
                "skip_reason": reason,
                **details
            }
        )

    async def log_error(self,
                        vendor_code: str,
                        sku_ozon: int,
                        error: str,
                        trace: str = None,
                        details: Dict[str, Any] = None):
        """Лог ошибки"""
        await self.log(
            level="ERROR",
            message=f"❌ Ozon {vendor_code}: {error[:200]}",
            vendor_code=vendor_code,
            sku_ozon=sku_ozon,
            details={
                "event": "error",
                "error": error,
                "traceback": trace[-500:] if trace else None,
                **(details or {})
            }
        )

    async def log_cdek_calculation(self,
                                   vendor_code: str,
                                   sku_ozon: int,
                                   weight_kg: float,
                                   dimensions: Dict[str, float],
                                   region: str,
                                   city: str,
                                   delivery_cost: float,
                                   delivery_days: int,
                                   tariff_name: str,
                                   details: Dict[str, Any]):
        """Лог расчета логистики через СДЭК"""
        await self.log(
            level="INFO",
            message=f"📦 Ozon {vendor_code}: логистика СДЭК {delivery_cost:.2f}₽ (вес: {weight_kg:.2f}кг, {region})",
            vendor_code=vendor_code,
            sku_ozon=sku_ozon,
            details={
                "event": "cdek_calculation",
                "weight_kg": weight_kg,
                "dimensions": dimensions,
                "region": region,
                "city": city,
                "delivery_cost": round(delivery_cost, 2),
                "delivery_days": delivery_days,
                "tariff_name": tariff_name,
                "volume_liters": details.get('volume_liters'),
                "weight_source": details.get('weight_source')
            }
        )

    async def log_ozon_api_call(self,
                                method: str,
                                endpoint: str,
                                status_code: int,
                                response_time: float,
                                request_data: Dict = None,
                                response_data: Dict = None,
                                error: str = None):
        """Лог вызовов API Ozon"""
        level = "ERROR" if status_code >= 400 or error else "INFO"

        await self.log(
            level=level,
            message=f"📡 Ozon API {method}: {status_code} ({response_time:.2f}с)",
            details={
                "event": "ozon_api_call",
                "method": method,
                "endpoint": endpoint,
                "status_code": status_code,
                "response_time": round(response_time, 3),
                "request": request_data,
                "response": response_data,
                "error": error
            }
        )

    async def log_discount_info(self,
                                vendor_code: str,
                                sku_ozon: int,
                                seller_discount: float,
                                base_price: float,
                                discounted_price: float,
                                action: str):
        """Лог информации о скидке продавца"""
        await self.log(
            level="INFO",
            message=f"🏷️ Ozon {vendor_code}: скидка {seller_discount}%",
            vendor_code=vendor_code,
            sku_ozon=sku_ozon,
            details={
                "event": "discount_info",
                "seller_discount": seller_discount,
                "base_price": round(base_price, 2),
                "discounted_price": round(discounted_price, 2),
                "discount_amount": round(base_price - discounted_price, 2),
                "action": action
            }
        )

    async def log_vat_info(self,
                           vendor_code: str,
                           sku_ozon: int,
                           vat_percent: float,
                           price_with_vat: float,
                           price_without_vat: float,
                           vat_amount: float,
                           is_own_production: bool):
        """Лог информации о НДС"""
        await self.log(
            level="INFO",
            message=f"🧾 Ozon {vendor_code}: НДС {vat_percent}% = {vat_amount:.2f}₽",
            vendor_code=vendor_code,
            sku_ozon=sku_ozon,
            details={
                "event": "vat_info",
                "vat_percent": vat_percent,
                "price_with_vat": round(price_with_vat, 2),
                "price_without_vat": round(price_without_vat, 2),
                "vat_amount": round(vat_amount, 2),
                "is_own_production": is_own_production
            }
        )