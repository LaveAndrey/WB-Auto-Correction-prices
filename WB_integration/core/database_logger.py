import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any
import pytz


class DatabaseLogger:
    def __init__(self, db_pool):
        self.db_pool = db_pool
        self.cycle_id = 0
        # Добавляем отдельный логгер для ошибок БД
        self.error_logger = logging.getLogger('price_updater.db')

    async def set_cycle_id(self, cycle_id: int):
        """Устанавливает ID текущего цикла"""
        self.cycle_id = cycle_id

    async def log(self,
                  level: str,
                  message: str,
                  vendor_code: str = None,
                  mp: str = 'wb',  # ✅ Добавлен столбец mp с значением по умолчанию 'wb'
                  details: dict = None,
                  nm_id: int = None,  # ✅ Добавлен nm_id
                  action_type: str = None,  # ✅ Тип действия (price_change/discount_change)
                  old_value: float = None,  # ✅ Старое значение
                  new_value: float = None,  # ✅ Новое значение
                  profit_before: float = None,  # ✅ Прибыль до
                  profit_after: float = None,  # ✅ Прибыль после
                  **extra):  # ✅ Дополнительные параметры

        """Универсальный метод логирования с поддержкой всех полей"""
        try:
            # Обогащаем details дополнительной информацией
            enriched_details = details.copy() if details else {}

            # Добавляем всегда полезную информацию
            enriched_details.update({
                'timestamp': datetime.now(pytz.timezone('Europe/Moscow')).isoformat(),
                'cycle_id': self.cycle_id,
                'mp': mp,
                **extra
            })

            # Добавляем опциональные поля если они есть
            if nm_id:
                enriched_details['nm_id'] = nm_id
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
                    await cursor.execute("""
                        INSERT INTO oc_price_updater_logs 
                        (level, vendor_code, message, details, cycle_id, created_at, mp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        level,
                        vendor_code,
                        message,
                        json.dumps(enriched_details, ensure_ascii=False, default=str) if enriched_details else None,
                        self.cycle_id,
                        datetime.now(pytz.timezone('Europe/Moscow')),
                        mp  # ✅ Обязательно сохраняем mp
                    ))

        except Exception as e:
            self.error_logger.error(f"❌ Ошибка записи в лог БД: {e}")
            self.error_logger.error(f"   Параметры: level={level}, vendor={vendor_code}, mp={mp}")

    # ✅ Специализированные методы для разных типов событий

    async def log_cycle_start(self, cycle_id: int):
        """Лог начала цикла"""
        await self.log(
            level="INFO",
            message=f"🚀 Начат цикл #{cycle_id}",
            details={
                "event": "cycle_start",
                "cycle_number": cycle_id
            }
        )

    async def log_cycle_end(self, cycle_id: int, stats: Dict[str, Any]):
        """Лог окончания цикла со статистикой"""
        await self.log(
            level="INFO",
            message=f"✅ Завершен цикл #{cycle_id}",
            details={
                "event": "cycle_end",
                "cycle_number": cycle_id,
                "statistics": stats
            }
        )

    async def log_price_calculation(self,
                                    vendor_code: str,
                                    nm_id: int,
                                    old_price: float,
                                    new_price: float,
                                    old_discount: float,
                                    new_discount: float,
                                    current_profit: float,
                                    target_profit: float,
                                    action_type: str,
                                    source: str,
                                    logistics_cost: float,
                                    sales_count: int,
                                    details: Dict[str, Any]):
        """Лог расчета цены для товара"""

        profit_diff = target_profit - current_profit
        price_diff = new_price - old_price
        discount_diff = new_discount - old_discount

        # Формируем читаемое сообщение
        if action_type == "price_change":
            message = (f"💰 Изменение цены: {old_price:.0f}₽ → {new_price:.0f}₽ "
                       f"({price_diff:+.0f}₽, {price_diff / old_price * 100:+.1f}%)")
        elif action_type == "discount_change":
            message = (f"🎯 Изменение скидки: {old_discount:.1f}% → {new_discount:.1f}% "
                       f"({discount_diff:+.1f}%)")
        else:
            message = f"📊 Расчет цены: {new_price:.0f}₽ со скидкой {new_discount:.1f}%"

        await self.log(
            level="SUCCESS" if abs(profit_diff) < 100 else "WARNING",
            message=message,
            vendor_code=vendor_code,
            nm_id=nm_id,
            action_type=action_type,
            old_value=old_price,
            new_value=new_price,
            profit_before=current_profit,
            profit_after=target_profit,
            details={
                "event": "price_calculation",
                "old_price": round(old_price, 2),
                "new_price": round(new_price, 2),
                "price_change": round(price_diff, 2),
                "price_change_percent": round(price_diff / old_price * 100 if old_price else 0, 2),
                "old_discount": round(old_discount, 2),
                "new_discount": round(new_discount, 2),
                "discount_change": round(discount_diff, 2),
                "current_profit": round(current_profit, 2),
                "target_profit": round(target_profit, 2),
                "profit_diff": round(profit_diff, 2),
                "action_type": action_type,
                "source": source,
                "logistics_cost": round(logistics_cost, 2),
                "sales_used": sales_count,
                "nm_id": nm_id,
                **details
            }
        )

    async def log_skip(self,
                       vendor_code: str,
                       nm_id: int,
                       reason: str,
                       details: Dict[str, Any]):
        """Лог пропуска товара"""
        await self.log(
            level="WARNING",
            message=f"⏭️ Пропущен: {reason}",
            vendor_code=vendor_code,
            nm_id=nm_id,
            details={
                "event": "skip",
                "skip_reason": reason,
                **details
            }
        )

    async def log_error(self,
                        vendor_code: str,
                        nm_id: int,
                        error: str,
                        trace: str = None,
                        details: Dict[str, Any] = None):
        """Лог ошибки"""
        await self.log(
            level="ERROR",
            message=f"❌ Ошибка: {error[:200]}",  # Обрезаем длинные сообщения
            vendor_code=vendor_code,
            nm_id=nm_id,
            details={
                "event": "error",
                "error": error,
                "traceback": trace[-500:] if trace else None,
                **(details or {})
            }
        )

    async def log_wb_api_call(self,
                              method: str,
                              url: str,
                              status_code: int,
                              response_time: float,
                              request_data: Dict = None,
                              response_data: Dict = None,
                              error: str = None):
        """Лог вызовов API Wildberries"""
        level = "ERROR" if status_code >= 400 or error else "INFO"

        await self.log(
            level=level,
            message=f"📡 WB API {method}: {status_code} ({response_time:.2f}с)",
            details={
                "event": "wb_api_call",
                "method": method,
                "url": url,
                "status_code": status_code,
                "response_time": round(response_time, 3),
                "request": request_data,
                "response": response_data,
                "error": error
            }
        )

    async def log_logistics_calculation(self,
                                        vendor_code: str,
                                        nm_id: int,
                                        dimensions: Dict[str, float],
                                        volume: float,
                                        logistics_cost: float,
                                        warehouses_used: int,
                                        orders_used: int,
                                        details: Dict[str, Any]):
        """Лог расчета логистики"""
        await self.log(
            level="INFO",
            message=f"📦 Логистика: {logistics_cost:.2f}₽ (объем: {volume:.2f}л)",
            vendor_code=vendor_code,
            nm_id=nm_id,
            details={
                "event": "logistics_calculation",
                "dimensions": dimensions,
                "volume_liters": round(volume, 3),
                "logistics_cost": round(logistics_cost, 2),
                "warehouses_used": warehouses_used,
                "orders_used": orders_used,
                "weighted_base": details.get('weighted_base'),
                "weighted_liter": details.get('weighted_liter'),
                "localization_index": details.get('localization_index'),
                "calculation_formula": details.get('calculation_formula')
            }
        )

    async def log_promotion(self,
                            vendor_code: str,
                            nm_id: int,
                            promotion_type: str,
                            action: str,
                            details: Dict[str, Any]):
        """Лог работы с акциями"""
        await self.log(
            level="INFO",
            message=f"🤖 Акция {promotion_type}: {action}",
            vendor_code=vendor_code,
            nm_id=nm_id,
            details={
                "event": "promotion",
                "promotion_type": promotion_type,
                "action": action,
                **details
            }
        )