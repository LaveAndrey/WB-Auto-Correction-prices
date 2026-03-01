import statistics
from typing import List
from WB_integration.structurs.dataclass import OrderData


def _get_last_non_zero_spp(orders: List[OrderData]) -> float:
    """Расчет среднего СПП"""
    try:
        spp_orders = [order for order in orders if order.spp_percent > 0]

        if not spp_orders:
            return 0.0

        avg_spp = sum(order.spp_percent for order in spp_orders) / len(spp_orders)
        return avg_spp

    except Exception:
        return 0.0


def calculate_discount_statistics(discount_list: List[float]):
    """Расчет статистики по скидкам"""
    if not discount_list:
        return None

    return {
        "median": statistics.median(discount_list),
        "min": min(discount_list),
        "max": max(discount_list),
        "mean": statistics.mean(discount_list),
        "count": len(discount_list)
    }


def calculate_price_statistics(orders: List[OrderData]):
    """Расчет статистики по ценам"""
    if not orders:
        return None

    price_wd_list = [o.price_with_desc for o in orders]
    forpay_list = [o.for_pay for o in orders]

    return {
        "avg_price_wd": statistics.mean(price_wd_list),
        "avg_forpay": statistics.mean(forpay_list),
        "min_price_wd": min(price_wd_list),
        "max_price_wd": max(price_wd_list)
    }


def calculate_volume(length: float, width: float, height: float) -> float:
    """Расчет объема в литрах"""
    try:
        l = float(length) if length is not None else 0.0
        w = float(width) if width is not None else 0.0
        h = float(height) if height is not None else 0.0
    except (TypeError, ValueError):
        l = w = h = 0.0

    if not all([l, w, h]):
        return 1.0

    vol = (l * w * h) / 1000.0
    return max(vol, 1.0)