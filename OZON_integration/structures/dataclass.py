#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data classes для Ozon Price Updater
"""

from config import Config
from dataclasses import dataclass, field
from typing import Dict, Optional
from enum import Enum
from datetime import datetime


class ProcessingStatus(Enum):
    """Статусы обработки товара"""
    SUCCESS = "success"
    SKIPPED_NO_DATA = "skipped_no_data"
    SKIPPED_MIN_PRICE = "skipped_min_price"
    SKIPPED_MIN_CHANGE = "skipped_min_change"
    SKIPPED_INVALID = "skipped_invalid"
    SKIPPED_PROMOTION = "skipped_promotion"
    ERROR = "error"


@dataclass
class OzonOrderData:
    """Структура данных заказа с Ozon (rFBS/FBS)"""
    offer_id: str
    posting_number: str
    product_id: int
    price: float  # Цена С НДС
    quantity: int
    commission_amount: float
    payout: float  # Выплата (уже без комиссий)
    delivery_cost: float
    region: str
    city: str
    created_at: str
    status: str

    @property
    def is_cancelled(self) -> bool:
        """Проверка, отменён ли заказ"""
        return self.status in ['cancelled', 'canceled']

    @property
    def for_pay(self) -> float:
        """Для Ozon payout уже содержит чистую сумму к выплате"""
        return self.payout

    @property
    def payout_ratio(self) -> float:
        """Соотношение payout к цене (payout / price)"""
        if self.price > 0:
            return self.payout / self.price
        return 0.0

    @property
    def vat_in_price(self) -> float:
        """НДС в цене заказа (20%) - цена НА OZON ВКЛЮЧАЕТ НДС"""
        vat_percent = Config.VAT_PERCENT
        # Формула: НДС = цена × (ставка_НДС / (100 + ставка_НДС))
        return self.price * (vat_percent / (100 + vat_percent))

    @property
    def price_without_vat(self) -> float:
        """Цена заказа без НДС"""
        return self.price - self.vat_in_price

    @classmethod
    def from_api_dict(cls, data: Dict) -> Optional['OzonOrderData']:
        """Создаёт объект из ответа Ozon API"""
        try:
            product = data.get('products', [{}])[0]
            offer_id = product.get('offer_id', '').strip()
            if not offer_id:
                return None

            financial_products = data.get('financial_data', {}).get('products', [{}])[0]
            posting_services = data.get('financial_data', {}).get('posting_services', {})
            analytics = data.get('analytics_data', {})

            return cls(
                offer_id=offer_id,
                posting_number=data.get('posting_number', ''),
                product_id=financial_products.get('product_id', 0),
                price=float(financial_products.get('price', 0)),
                quantity=product.get('quantity', 1),
                commission_amount=float(financial_products.get('commission_amount', 0)),
                payout=float(financial_products.get('payout', 0)),
                delivery_cost=(
                        float(posting_services.get('delivery_to_customer', 0)) +
                        float(posting_services.get('basic_fee', 0))
                ),
                region=analytics.get('region', ''),
                city=analytics.get('city', ''),
                created_at=data.get('in_process_at', ''),
                status=data.get('status', '')
            )
        except (ValueError, TypeError, IndexError, KeyError):
            return None


@dataclass
class ProductData:
    """Данные о товаре из базы"""
    vendor_code: str
    purchase_price: float  # Себестоимость производства БЕЗ НДС
    target_profit: float  # Целевая прибыль
    current_price_ozon: float  # Текущая цена на Ozon С НДС
    current_real_price: float
    length: float = 0.0
    width: float = 0.0
    seller_discount: float = 0.0
    height: float = 0.0
    weight: float = 0.0  # Вес товара в кг (добавлено)
    sku_ozon: int = 0
    status: int = 1
    vat_included_in_price: bool = True  # Цена на Ozon всегда включает НДС
    promotion_active: bool = False  # 👈 НОВОЕ ПОЛЕ
    promotion_lock_until: Optional[datetime] = None  # 👈 НОВОЕ ПОЛЕ


    @classmethod
    def from_db_row(cls, row: Dict) -> Optional['ProductData']:
        """Создаёт объект из строки БД"""
        try:
            return cls(
                vendor_code=str(row['model_ozon']),
                purchase_price=float(row['purchase_price']),  # Себестоимость БЕЗ НДС
                target_profit=float(row['target_profit_rub']),
                current_price_ozon=float(row.get('price_ozon', 0) or 0),  # Цена С НДС
                current_real_price=float(row.get('ozon_real_price', 0) or 0),
                seller_discount=float(row.get('ozon_seller_discount', 0) or 0),
                length=float(row.get('length', 0) or 0),
                width=float(row.get('width', 0) or 0),
                height=float(row.get('height', 0) or 0),
                weight=float(row.get('weight', 0) or 0),  # Добавлено поле weight
                sku_ozon=int(row.get('sku_ozon', 0) or 0),
                status=int(row.get('status', 1)),
                vat_included_in_price=True, # Цена на Ozon всегда включает НДС
                promotion_active=bool(row.get('promotion_active', False)),  # 👈
                promotion_lock_until=row.get('promotion_lock_until')  # 👈
            )
        except (ValueError, TypeError, KeyError) as e:
            return None

    @property
    def has_discount(self) -> bool:
        """Проверяет, есть ли скидка у товара"""
        return self.seller_discount > 0

    @property
    def current_price_with_discount(self) -> float:
        """Текущая цена с учетом скидки (реальная цена продажи)"""
        if self.has_discount:
            return self.current_price_ozon * (1 - self.seller_discount / 100)
        return self.current_price_ozon

    @property
    def current_price_without_vat(self) -> float:
        """Текущая цена на Ozon без НДС (базовая цена)"""
        vat_percent = Config.VAT_PERCENT
        if self.vat_included_in_price:
            return self.current_price_ozon / (1 + vat_percent / 100)
        return self.current_price_ozon

    @property
    def current_price_with_discount_without_vat(self) -> float:
        """Цена со скидкой без НДС"""
        vat_percent = Config.VAT_PERCENT
        price_with_discount = self.current_price_with_discount
        if self.vat_included_in_price:
            return price_with_discount / (1 + vat_percent / 100)
        return price_with_discount

    @property
    def current_vat_in_price(self) -> float:
        """НДС в текущей цене на Ozon (базовой цене)"""
        vat_percent = Config.VAT_PERCENT
        if self.vat_included_in_price:
            return self.current_price_ozon * (vat_percent / (100 + vat_percent))
        return 0

    @property
    def current_vat_in_discounted_price(self) -> float:
        """НДС в цене со скидкой"""
        vat_percent = Config.VAT_PERCENT
        price_with_discount = self.current_price_with_discount
        if self.vat_included_in_price:
            return price_with_discount * (vat_percent / (100 + vat_percent))
        return 0

    @property
    def current_vat_to_pay(self) -> float:
        """НДС к уплате при текущей цене (учитывая скидку)"""
        # Если товар собственного производства, НДС к уплате = весь НДС с продажи
        return self.current_vat_in_discounted_price

    @property
    def has_weight(self) -> bool:
        """Проверяет, указан ли вес товара"""
        return self.weight > 0


@dataclass
class PriceUpdate:
    """Результат расчёта новой цены"""
    vendor_code: str
    new_price_ozon: float  # Новая базовая цена С НДС (отправляется на Ozon)
    new_real_price: float  # Новая реальная цена (с учетом скидки)
    old_price_ozon: float  # Старая базовая цена С НДС
    old_real_price: float  # Старая реальная цена
    profit_correction: float  # Корректировка прибыли
    status: ProcessingStatus  # Статус обработки
    error_msg: str = ""  # Сообщение об ошибке
    discount: Optional[float] = None  # Скидка продавца в процентах
    sku_ozon: int = 0  # SKU на Ozon
    logistics_cost: float = 0.0  # Стоимость логистики
    current_profit: float = 0.0  # Текущая прибыль
    target_profit: float = 0.0  # Целевая прибыль
    sales_count: int = 0  # Количество продаж для расчета
    purchase_price: float = 0.0  # Себестоимость БЕЗ НДС
    target_forpay: float = 0.0  # Целевой payout
    action_type: str = ""  # Тип действия
    region_stats: Dict = field(default_factory=dict)  # Статистика по регионам
    avg_payout_ratio: float = 0.0  # Среднее соотношение payout/price
    vat_percent: float = 20.0  # Ставка НДС
    vat_amount: float = 0.0  # НДС в новой цене (в цене со скидкой)
    price_without_vat: float = 0.0  # Новая цена без НДС (цена со скидкой без НДС)
    vat_to_pay: float = 0.0  # НДС к уплате (весь НДС с продажи)
    seller_discount: float = 0.0  # Скидка продавца из БД

    def __post_init__(self):
        if self.region_stats is None:
            self.region_stats = {}

    @property
    def has_discount(self) -> bool:
        """Проверяет, есть ли скидка у товара"""
        return self.seller_discount > 0 or (self.discount is not None and self.discount > 0)

    @property
    def effective_discount(self) -> float:
        """Эффективная скидка (приоритет у seller_discount)"""
        if self.seller_discount > 0:
            return self.seller_discount
        return self.discount or 0.0

    @property
    def discounted_price(self) -> float:
        """Цена со скидкой (реальная цена продажи)"""
        if self.has_discount and self.new_price_ozon > 0:
            return self.new_price_ozon * (1 - self.effective_discount / 100)
        return self.new_price_ozon

    @property
    def old_discounted_price(self) -> float:
        """Старая цена со скидкой"""
        if self.has_discount and self.old_price_ozon > 0:
            return self.old_price_ozon * (1 - self.effective_discount / 100)
        return self.old_price_ozon

    @property
    def reason(self) -> str:
        """Человекочитаемая причина результата"""
        if self.error_msg:
            return self.error_msg
        if self.status == ProcessingStatus.SUCCESS:
            direction = "↑" if self.new_price_ozon > self.old_price_ozon else "↓"
            change = abs(self.new_price_ozon - self.old_price_ozon)
            discount_info = f", скидка {self.effective_discount}%" if self.has_discount else ""
            return f"Цена {direction} на {change:.0f} ₽ (НДС {self.vat_percent}% учтен{discount_info})"
        status_reasons = {
            ProcessingStatus.SKIPPED_NO_DATA: "Недостаточно данных о заказах",
            ProcessingStatus.SKIPPED_MIN_PRICE: "Цена ниже минимальной с учетом НДС",
            ProcessingStatus.SKIPPED_MIN_CHANGE: "Изменение меньше порога",
            ProcessingStatus.SKIPPED_INVALID: "Некорректное значение",
            ProcessingStatus.ERROR: "Ошибка обработки"
        }
        return status_reasons.get(self.status, str(self.status.value))

    @property
    def profit_change(self) -> float:
        """Изменение прибыли"""
        return self.target_profit - self.current_profit

    @property
    def price_change(self) -> float:
        """Абсолютное изменение базовой цены"""
        return self.new_price_ozon - self.old_price_ozon

    @property
    def discounted_price_change(self) -> float:
        """Абсолютное изменение цены со скидкой"""
        return self.discounted_price - self.old_discounted_price

    @property
    def price_change_percent(self) -> float:
        """Процентное изменение базовой цены"""
        if self.old_price_ozon > 0:
            return (self.price_change / self.old_price_ozon) * 100
        return 0.0

    @property
    def discounted_price_change_percent(self) -> float:
        """Процентное изменение цены со скидкой"""
        if self.old_discounted_price > 0:
            return (self.discounted_price_change / self.old_discounted_price) * 100
        return 0.0

    @property
    def vat_change(self) -> float:
        """Изменение НДС в цене"""
        old_vat = self.old_price_ozon * (self.vat_percent / (100 + self.vat_percent))
        return self.vat_amount - old_vat

    @property
    def expected_payout(self) -> float:
        """Ожидаемый payout с новой ценой (от цены со скидкой)"""
        if self.avg_payout_ratio > 0:
            return self.discounted_price * self.avg_payout_ratio
        return self.discounted_price * 0.60  # fallback

    @property
    def expected_bank_fee(self) -> float:
        """Ожидаемая банковская комиссия"""
        return self.expected_payout * Config.BANK_COMMISSION

    @property
    def profit_with_new_price(self) -> float:
        """Ожидаемая прибыль с новой ценой"""
        # Прибыль = payout - банк.комиссия - себестоимость - НДС - логистика
        return (
                self.expected_payout -
                self.expected_bank_fee -
                self.purchase_price -
                self.vat_to_pay -
                self.logistics_cost
        )

    @property
    def margin_percent(self) -> float:
        """Маржинальность в процентах (от цены со скидкой)"""
        if self.discounted_price > 0:
            return (self.profit_with_new_price / self.discounted_price) * 100
        return 0.0

    @property
    def vat_in_discounted_price(self) -> float:
        """НДС в цене со скидкой"""
        if self.has_discount:
            return self.discounted_price * (self.vat_percent / (100 + self.vat_percent))
        return self.vat_amount

    @property
    def discounted_price_without_vat(self) -> float:
        """Цена со скидкой без НДС"""
        if self.has_discount:
            return self.discounted_price / (1 + self.vat_percent / 100)
        return self.price_without_vat

    def to_dict(self) -> Dict:
        """Конвертация в словарь для логирования"""
        return {
            "vendor_code": self.vendor_code,
            "old_base_price": self.old_price_ozon,
            "new_base_price": self.new_price_ozon,
            "base_price_change": self.price_change,
            "base_price_change_percent": self.price_change_percent,
            "old_discounted_price": self.old_discounted_price,
            "new_discounted_price": self.discounted_price,
            "discounted_price_change": self.discounted_price_change,
            "discounted_price_change_percent": self.discounted_price_change_percent,
            "seller_discount": self.seller_discount,
            "effective_discount": self.effective_discount,
            "old_real_price": self.old_real_price,
            "new_real_price": self.new_real_price,
            "current_profit": self.current_profit,
            "target_profit": self.target_profit,
            "profit_change": self.profit_change,
            "expected_profit": self.profit_with_new_price,
            "expected_payout": self.expected_payout,
            "expected_bank_fee": self.expected_bank_fee,
            "margin_percent": self.margin_percent,
            "vat_percent": self.vat_percent,
            "vat_in_base_price": self.vat_amount,
            "vat_in_discounted_price": self.vat_in_discounted_price,
            "price_without_vat_base": self.price_without_vat,
            "price_without_vat_discounted": self.discounted_price_without_vat,
            "vat_to_pay": self.vat_to_pay,
            "logistics_cost": self.logistics_cost,
            "purchase_price": self.purchase_price,
            "sales_count": self.sales_count,
            "payout_ratio": self.avg_payout_ratio,
            "action_type": self.action_type,
            "status": self.status.value,
            "sku_ozon": self.sku_ozon,
            "region_stats": self.region_stats
        }

    def to_simple_dict(self) -> Dict:
        """Упрощенный словарь для быстрого просмотра"""
        return {
            "vendor_code": self.vendor_code,
            "old_price": f"{self.old_price_ozon:.0f}₽",
            "new_price": f"{self.new_price_ozon:.0f}₽",
            "old_discounted": f"{self.old_discounted_price:.0f}₽" if self.has_discount else None,
            "new_discounted": f"{self.discounted_price:.0f}₽" if self.has_discount else None,
            "discount": f"{self.effective_discount}%" if self.has_discount else None,
            "change": f"{self.price_change:+.0f}₽ ({self.price_change_percent:+.1f}%)",
            "profit": f"{self.profit_with_new_price:.0f}₽",
            "margin": f"{self.margin_percent:.1f}%",
            "status": self.status.value
        }


# ===== КЛАССЫ ДЛЯ АКЦИЙ OZON =====
from datetime import datetime
from typing import Optional


@dataclass
class OzonPromotion:
    """Данные об акции Ozon (из API /v1/actions)"""
    promotion_id: int
    title: str
    action_type: str
    date_start: Optional[datetime] = None
    date_end: Optional[datetime] = None
    freeze_date: Optional[datetime] = None
    discount_type: str = ""
    discount_value: float = 0.0
    is_participating: bool = False
    is_voucher_action: bool = False
    potential_products_count: int = 0
    participating_products_count: int = 0
    banned_products_count: int = 0

    @property
    def days_until_start(self) -> int:
        if not self.date_start:
            return 0
        # Приводим к timezone-naive для сравнения
        date_start_naive = self.date_start.replace(tzinfo=None) if self.date_start.tzinfo else self.date_start
        now_naive = datetime.now()
        if date_start_naive > now_naive:
            return (date_start_naive - now_naive).days
        return 0

    @property
    def days_until_end(self) -> int:
        if not self.date_end:
            return 0
        date_end_naive = self.date_end.replace(tzinfo=None) if self.date_end.tzinfo else self.date_end
        now_naive = datetime.now()
        if date_end_naive > now_naive:
            return (date_end_naive - now_naive).days
        return 0

    @property
    def is_ramp_period(self) -> bool:
        """Период повышения цены (за 14 дней до начала)"""
        return 0 < self.days_until_start <= 14

    @property
    def is_active(self) -> bool:
        """Акция активна прямо сейчас"""
        if not self.date_start or not self.date_end:
            return False
        now = datetime.now()
        date_start_naive = self.date_start.replace(tzinfo=None) if self.date_start.tzinfo else self.date_start
        date_end_naive = self.date_end.replace(tzinfo=None) if self.date_end.tzinfo else self.date_end
        return date_start_naive <= now <= date_end_naive

    @property
    def is_completed(self) -> bool:
        """Акция завершена"""
        if not self.date_end:
            return False
        date_end_naive = self.date_end.replace(tzinfo=None) if self.date_end.tzinfo else self.date_end
        return datetime.now() > date_end_naive

    @property
    def is_frozen(self) -> bool:
        """Акция заморожена (нельзя повышать цены)"""
        if not self.freeze_date:
            return False
        freeze_naive = self.freeze_date.replace(tzinfo=None) if self.freeze_date.tzinfo else self.freeze_date
        return datetime.now() > freeze_naive


@dataclass
class PromotionProduct:
    """Товар в акции с планом повышения"""
    vendor_code: str
    promotion_id: int
    product_id: int
    sku_ozon: int
    base_price: float  # Исходная цена (до всех акций)
    target_price: float  # Целевая цена (с повышением)
    current_price: float  # Текущая цена
    action_price: float  # Цена в акции
    max_action_price: float  # Максимальная цена для акции
    daily_increase: float  # Повышение в день
    ramp_day: int  # Текущий день повышения
    ramp_start_date: datetime  # Когда начали повышать
    status: str = "pending"  # pending, ramping, active, completed, restoring
    promotion_end_date: Optional[datetime] = None
    active_promotion_id: Optional[int] = None