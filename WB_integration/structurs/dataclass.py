from dataclasses import dataclass, field
from typing import Dict, Optional
from config import Config
from WB_integration.structurs.enums import ProcessingStatus
from datetime import datetime


@dataclass
class WarehouseLogistics:
    """Структура для хранения тарифов логистики по складам"""
    warehouse_id: int
    warehouse_name: str
    delivery_base: float
    delivery_liter: float
    storage_base: float
    storage_liter: float
    coefficient: int
    is_sorting_center: bool
    orders_count: int = 0  # Количество заказов с этого склада
    weight: float = 0.0  # Вес в расчете средневзвешенной логистики

    @property
    def is_available(self) -> bool:
        """Проверяет, доступен ли склад для приемки"""
        return self.coefficient in [0, 1] and self.orders_count > 0


@dataclass
class OrderData:
    nm_id: int  # nmId из заказа
    vendor_code: str  # Артикул поставщика из заказа
    total_price: float
    price_with_desc: float
    discount_percent: float
    spp_percent: float
    finished_price: float
    date: str
    warehouse_name: str = ""
    quantity: int = 1
    srid: str = ""
    is_cancel: bool = False

    @property
    def for_pay(self) -> float:
        """Расчет forPay на основе соотношения"""
        if self.price_with_desc > 0:
            return self.price_with_desc * Config.WB_FORPAY_TO_PRICEWITHDISC_RATIO
        elif self.finished_price > 0:
            return self.finished_price * Config.WB_FORPAY_TO_PRICEWITHDISC_RATIO
        return self.total_price * Config.WB_FORPAY_TO_PRICEWITHDISC_RATIO  # fallback

    @property
    def vat_amount(self) -> float:
        """Расчет НДС в цене для покупателя"""
        if self.price_with_desc > 0:
            return self.price_with_desc * (Config.VAT_PERCENT / (100 + Config.VAT_PERCENT))
        return 0.0

    @classmethod
    def from_api_dict(cls, data: Dict) -> Optional['OrderData']:
        try:
            return cls(
                nm_id=int(data.get('nmId', 0)),  # nmId из заказа
                vendor_code=str(data.get('supplierArticle', '')).strip(),
                total_price=float(data.get('totalPrice', 0)),
                price_with_desc=float(data.get('priceWithDisc', 0)),
                discount_percent=float(data.get('discountPercent', 0)),
                spp_percent=float(data.get('spp', 0)),
                finished_price=float(data.get('finishedPrice', 0)),
                date=data.get('date', ''),
                warehouse_name=data.get('warehouseName', ''),
                srid=data.get('srid', ''),
                is_cancel=data.get('isCancel', False)
            )
        except (ValueError, TypeError) as e:
            return None


@dataclass
class ProductData:
    vendor_code: str
    purchase_price: float
    target_profit: float
    current_price_wb: float
    current_real_price: float
    length: float = 0.0
    width: float = 0.0
    height: float = 0.0
    sku_wb: int = 0  # Это nmId из БД
    status: int = 1
    wb_seller_discount: float = 0.0
    # Дополнительные поля для НДС
    vat_rate: float = Config.VAT_PERCENT  # Ставка НДС по умолчанию из конфига
    is_manufacturer: bool = True  # Флаг производителя
    production_cost: float = 0.0  # Себестоимость производства
    material_cost_with_vat: float = 0.0  # Материальные затраты с НДС
    purchase_price_without_vat: float = 0.0  # Закупка без НДС
    promotion_active_wb: bool = False
    promotion_lock_until_wb: Optional[datetime] = None
    has_sales_last_period_wb: bool = False

    @classmethod
    def from_db_row(cls, row: Dict) -> Optional['ProductData']:
        try:
            # Создаем объект
            product = cls(
                vendor_code=str(row['model_wb']),
                purchase_price=float(row['purchase_price']),
                target_profit=float(row['target_profit_rub']),
                current_price_wb=float(row.get('price_wb', 0) or 0),
                current_real_price=float(row.get('wb_real_price', 0) or 0),
                length=float(row.get('length', 0) or 0),
                width=float(row.get('width', 0) or 0),
                height=float(row.get('height', 0) or 0),
                sku_wb=int(row.get('sku_wb', 0) or 0),  # nmId из БД
                status=int(row.get('status', 1)),
                wb_seller_discount=float(row.get('wb_seller_discount', 0) or 0),
                has_sales_last_period_wb=bool(row.get('has_sales_last_period_wb', False))
            )

            product.promotion_active_wb = bool(row.get('promotion_active_wb', False))
            product.promotion_lock_until_wb = row.get('promotion_lock_until_wb')

            # Дополнительные поля из БД (если есть)
            product.vat_rate = float(row.get('vat_rate', Config.VAT_PERCENT) or Config.VAT_PERCENT)
            product.production_cost = float(
                row.get('production_cost', product.purchase_price) or product.purchase_price)
            product.material_cost_with_vat = float(row.get('material_cost', 0) or 0)

            # Рассчитываем закупочную цену без НДС
            if product.purchase_price > 0 and product.vat_rate > 0:
                product.purchase_price_without_vat = product.purchase_price / (1 + product.vat_rate / 100)
            else:
                product.purchase_price_without_vat = product.purchase_price

            return product

        except (ValueError, TypeError, KeyError):
            return None

    @property
    def current_vat_amount(self) -> float:
        """Расчет НДС в текущей цене WB"""
        if self.current_price_wb > 0:
            return self.current_price_wb * (self.vat_rate / (100 + self.vat_rate))
        return 0.0

    @property
    def current_price_without_vat(self) -> float:
        """Текущая цена без НДС"""
        return self.current_price_wb - self.current_vat_amount

    @property
    def production_cost_without_vat(self) -> float:
        """Себестоимость производства без НДС"""
        if self.material_cost_with_vat > 0 and self.vat_rate > 0:
            material_cost_without_vat = self.material_cost_with_vat / (1 + self.vat_rate / 100)
            # Предполагаем, что остальные затраты не включают НДС
            return material_cost_without_vat + (self.production_cost - self.material_cost_with_vat)
        return self.production_cost


@dataclass
class PriceUpdate:
    vendor_code: str
    new_price_wb: float
    new_real_price: float
    old_price_wb: float
    profit_correction: float
    finished_price: float
    status: ProcessingStatus
    error_msg: str = ""
    discount: Optional[float] = None
    sku_wb: int = 0
    logistics_cost: float = 0
    current_profit: float = 0
    target_profit: float = 0
    sales_count: int = 0
    spp_used: float = 0
    purchase_price: float = 0
    old_real_price: float = 0
    target_forpay: float = 0
    action_type: str = ""
    source: str = "unknown"  # 'nm_id' или 'vendor_code'
    # Новые поля для НДС
    vat_amount: float = 0.0  # Сумма НДС в новой цене
    vat_percent: float = Config.VAT_PERCENT  # Ставка НДС
    price_without_vat: float = 0.0  # Цена без НДС
    old_vat_amount: float = 0.0  # Сумма НДС в старой цене
    old_price_without_vat: float = 0.0  # Старая цена без НДС
    # Дополнительные поля для производителя
    production_cost: float = 0.0
    material_cost_with_vat: float = 0.0
    material_cost_without_vat: float = 0.0
    material_vat: float = 0.0
    vat_to_pay: float = 0.0  # НДС к уплате в бюджет

    @property
    def reason(self) -> str:
        if self.error_msg:
            return self.error_msg

        if self.status == ProcessingStatus.SUCCESS:
            if self.action_type == "discount_change":
                return f"Изменена скидка: {self.discount:.1f}% (НДС: {self.vat_amount:.0f}₽)"
            else:
                direction = "↑" if self.new_price_wb > self.old_price_wb else "↓"
                return f"Цена {direction} на {abs(self.new_price_wb - self.old_price_wb):.0f} ₽ (скидка: {self.discount:.1f}%, НДС: {self.vat_amount:.0f}₽)"

        status_reasons = {
            ProcessingStatus.SKIPPED_NO_DATA: "Недостаточно данных о заказах",
            ProcessingStatus.SKIPPED_MIN_PRICE: "Цена ниже минимальной",
            ProcessingStatus.SKIPPED_MIN_CHANGE: "Изменение меньше порога",
            ProcessingStatus.SKIPPED_INVALID: "Некорректное значение",
            ProcessingStatus.ERROR: "Ошибка обработки"
        }
        return status_reasons.get(self.status, str(self.status.value))

    @property
    def new_vat_details(self) -> Dict[str, float]:
        """Детали расчета НДС для новой цены"""
        return {
            'new_price_with_vat': self.new_price_wb,
            'new_price_without_vat': self.price_without_vat,
            'vat_amount': self.vat_amount,
            'vat_percent': self.vat_percent,
            'vat_share': (self.vat_amount / self.new_price_wb * 100) if self.new_price_wb > 0 else 0
        }

    @property
    def old_vat_details(self) -> Dict[str, float]:
        """Детали расчета НДС для старой цены"""
        old_vat = self.old_price_wb * (self.vat_percent / (100 + self.vat_percent))
        old_price_without_vat = self.old_price_wb - old_vat

        return {
            'old_price_with_vat': self.old_price_wb,
            'old_price_without_vat': old_price_without_vat,
            'old_vat_amount': old_vat,
            'vat_percent': self.vat_percent,
            'vat_share': (old_vat / self.old_price_wb * 100) if self.old_price_wb > 0 else 0
        }

    @property
    def vat_change(self) -> Dict[str, float]:
        """Изменение НДС"""
        old_vat = self.old_vat_details['old_vat_amount']
        return {
            'vat_change_absolute': self.vat_amount - old_vat,
            'vat_change_percent': ((self.vat_amount - old_vat) / old_vat * 100) if old_vat > 0 else 0,
            'old_vat': old_vat,
            'new_vat': self.vat_amount
        }