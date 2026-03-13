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

    def _format_details_for_display(self, details: dict) -> dict:
        """
        Преобразует детали события в читаемый вид для сохранения в JSON:
        - переводит ключи на русский
        - форматирует числа (добавляет ₽, %, л, кг)
        - рекурсивно обрабатывает вложенные словари
        """
        if not isinstance(details, dict):
            return details

        # Словарь соответствия английских ключей русским
        mapping = {
            # Основные события
            'event': 'Событие',
            'timestamp': 'Время',
            'cycle_id': 'Цикл ID',
            'mp': 'МП',

            # Цены и скидки
            'old_price': 'Старая цена',
            'new_price': 'Новая цена',
            'price_change': 'Изменение цены',
            'price_change_percent': 'Изменение, %',
            'old_discount': 'Старая скидка',
            'new_discount': 'Новая скидка',
            'discount_change': 'Изменение скидки',
            'seller_discount': 'Скидка продавца',
            'target_discount_from_db': 'Целевая скидка из БД',

            # Цены со скидкой (для WB)
            'old_price_wd': 'Старая цена со скидкой',
            'new_price_wd': 'Новая цена со скидкой',
            'price_wd_diff': 'Изменение цены со скидкой',
            'old_discounted_price': 'Старая цена со скидкой',
            'new_discounted_price': 'Новая цена со скидкой',
            'discounted_price_change': 'Изменение цены со скидкой',

            # Прибыль
            'current_profit': 'Текущая прибыль',
            'target_profit': 'Целевая прибыль',
            'profit_diff': 'Разница прибыли',
            'expected_profit': 'Ожидаемая прибыль',
            'profit_before': 'Прибыль до',
            'profit_after': 'Прибыль после',

            # Логистика (общая)
            'logistics_cost': 'Логистика',
            'logistics_details': 'Детали логистики',
            'weight_kg': 'Вес',
            'weight_source': 'Источник веса',
            'volume_liters': 'Объём',
            'dimensions': 'Габариты',
            'region': 'Регион',
            'city': 'Город',
            'delivery_days': 'Срок доставки',
            'delivery_cost': 'Стоимость доставки',
            'tariff_name': 'Тариф',
            'warehouse_city_code': 'Код города склада',

            # Логистика WB (специфичная)
            'weighted_base': 'Ср. баз. ставка',
            'weighted_liter': 'Ср. за литр',
            'warehouses_count': 'Кол-во складов',
            'warehouses_used': 'Складов учтено',
            'orders_used': 'Заказов учтено',
            'localization_index': 'Индекс локализации',
            'calculation_formula': 'Формула расчёта',
            'delivery_base': 'Базовая ставка',
            'delivery_liter': 'Ставка за литр',
            'warehouse_name': 'Склад',
            'base_logistics': 'Базовая логистика',
            'total_logistics': 'Итого логистика',

            # Данные товара
            'vendor_code': 'Артикул',
            'sku_ozon': 'SKU Ozon',
            'nm_id': 'NM ID',
            'purchase_price': 'Себестоимость',
            'sales_used': 'Продаж учтено',
            'total_orders': 'Всего заказов',
            'region_stats': 'Статистика по регионам',
            'valid_orders': 'Валидных заказов',
            'required': 'Требуется заказов',
            'spp_used': 'СПП, %',
            'finished_price': 'Цена со скидкой СПП',

            # НДС
            'vat_percent': 'НДС',
            'vat_amount': 'Сумма НДС',
            'price_without_vat': 'Цена без НДС',
            'vat_to_pay': 'НДС к уплате',
            'vat_in_price': 'НДС в цене',
            'is_own_production': 'Собственное производство',

            # Комиссии и коэффициенты
            'forpay_ratio': 'Коэф. forPay',
            'target_forpay': 'Целевой forPay',
            'avg_payout_ratio': 'Ср. коэф. выплаты',
            'payout_ratio': 'Коэф. выплаты',

            # Акции
            'promotion_id': 'ID акции',
            'promotion_title': 'Название акции',
            'action': 'Действие',
            'daily_increase': 'Ежедневное повышение',
            'days_until_start': 'Дней до старта',
            'ramp_day': 'День повышения',
            'lock_until': 'Заблокировано до',
            'skip_reason': 'Причина пропуска',
            'promotion_active': 'Участвует в акции',
            'promotion_lock_until': 'Заблокировано до',
            'promotion_active_wb': 'Участвует в акции WB',
            'promotion_lock_until_wb': 'Заблокировано до WB',

            # Ошибки
            'error': 'Ошибка',
            'traceback': 'Стек вызовов',

            # Прочее
            'action_type': 'Тип действия',
            'source': 'Источник',
            'old_value': 'Старое значение',
            'new_value': 'Новое значение',
            'discount_amount': 'Сумма скидки',
            'base_price': 'Базовая цена',
            'discounted_price': 'Цена со скидкой',
            'volume': 'Объём',

            # API-логи
            'method': 'Метод',
            'url': 'URL',
            'status_code': 'Код ответа',
            'response_time': 'Время ответа, с',
            'request': 'Запрос',
            'response': 'Ответ',
        }

        result = {}
        for eng_key, value in details.items():
            rus_key = mapping.get(eng_key, eng_key)  # если нет в словаре, оставляем как есть

            # Рекурсивная обработка вложенных словарей
            if isinstance(value, dict):
                result[rus_key] = self._format_details_for_display(value)
            # Форматирование чисел
            elif isinstance(value, (int, float)):
                key_lower = rus_key.lower()
                if any(word in key_lower for word in
                       ['цена', 'прибыль', 'логистика', 'forpay', 'стоимость', 'комиссия', 'выплата', 'себестоимость',
                        'повышение', 'сумма', 'ставка']):
                    result[rus_key] = f"{value:.2f} ₽"
                elif any(word in key_lower for word in ['скидка', 'ндс', 'коэф', 'процент', 'изменение, %']):
                    result[rus_key] = f"{value:.2f}%"
                elif 'объём' in key_lower or 'объем' in key_lower:
                    result[rus_key] = f"{value:.3f} л"
                elif 'вес' in key_lower:
                    result[rus_key] = f"{value:.2f} кг"
                else:
                    result[rus_key] = value
            # Списки чисел – оставляем как есть, но можно рекурсивно обработать каждый элемент, если это словари
            elif isinstance(value, list):
                if value and isinstance(value[0], dict):
                    result[rus_key] = [self._format_details_for_display(item) for item in value]
                else:
                    result[rus_key] = value
            else:
                result[rus_key] = value

        return result

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
                        json.dumps(
                            self._format_details_for_display(enriched_details),
                            ensure_ascii=False,
                            indent=2,  # Добавим отступы для красоты
                            default=str
                        ) if enriched_details else None,
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

    async def log_promotion_wb(self,
                               vendor_code: str,
                               nm_id: int,
                               promotion_id: int,
                               promotion_title: str,
                               action: str,
                               # 'ramp_start', 'price_increased', 'locked', 'unlocked', 'restored', 'protected'
                               old_price: float = None,
                               new_price: float = None,
                               daily_increase: float = None,
                               days_until_start: int = None,
                               details: Dict[str, Any] = None):
        """
        Логирование действий с акциями Wildberries

        action может быть:
        - 'ramp_start' - начало повышения цены перед акцией
        - 'price_increased' - ежедневное повышение цены
        - 'locked' - блокировка цены на время акции
        - 'unlocked' - разблокировка после акции
        - 'restored' - восстановление исходной цены
        - 'protected' - защита товара до начала акции
        - 'skipped_promotion' - пропуск товара из-за акции
        """

        action_messages = {
            'ramp_start': '🚀 Начало повышения цены перед акцией (WB)',
            'price_increased': '📈 Повышение цены перед акцией (WB)',
            'locked': '🔒 Блокировка цены на время акции (WB)',
            'unlocked': '🔓 Разблокировка цены после акции (WB)',
            'restored': '🔄 Восстановление исходной цены после акции (WB)',
            'protected': '🛡️ Защита товара до начала акции (WB)',
            'skipped_promotion': '⏭️ Пропуск товара (участвует в акции WB)'
        }

        message = action_messages.get(action, f"Акция WB: {action}")

        log_details = {
            "event": "wb_promotion",
            "promotion_id": promotion_id,
            "promotion_title": promotion_title,
            "action": action,
            "vendor_code": vendor_code,
            "nm_id": nm_id
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
            message=f"{message} для {vendor_code}",
            vendor_code=vendor_code,
            nm_id=nm_id,
            action_type="promotion",
            details=log_details
        )