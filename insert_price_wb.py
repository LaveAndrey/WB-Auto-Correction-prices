import math
import os
import asyncio
import aiomysql
import pytz
import requests
import json
import logging
import sys
from datetime import datetime, timedelta, date
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    print("Предупреждение: python-dotenv не установлен. Используются переменные окружения.")

OC_DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'db': os.getenv('DB_NAME'),
    'port': int(os.getenv('DB_PORT')),
    'autocommit': True,
    'charset': 'utf8mb4',
}

API_KEY = os.getenv('WB_PRICES_TOKEN', 'your_default_token_here')
STATS_API_KEY = os.getenv('WB_STATS_TOKEN', API_KEY)  # Для заказов может понадобиться отдельный ключ
WB_TARIFF_URL = "https://common-api.wildberries.ru/api/tariffs/v1/acceptance/coefficients"
ORDERS_API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
WAREHOUSES_API_URL = "https://supplies-api.wildberries.ru/api/v1/warehouses"

# Запасной ID склада из настроек
DEFAULT_WAREHOUSE_ID = int(os.getenv('WB_WAREHOUSE_ID'))
WB_COMMISSION = float(os.getenv('WB_COMMISSION'))
BANK_RATE = float(os.getenv('BANK_COMMISSION'))
BUFFER_COEFF = float(os.getenv('BUFFER_COEFF'))
BOX_TYPE_ID = int(os.getenv('WB_BOX_TYPE'))

scheduler = AsyncIOScheduler()

LOG_LEVEL = logging.INFO
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("wb_price_calc")


# -----------------------------------------------

def log_json_response(name, data, max_items=5):
    """
    Логирует JSON ответ в удобочитаемом формате
    """
    logger.info(f"📄 {name}:")

    if not data:
        logger.info("  Пустой ответ")
        return

    if isinstance(data, list):
        logger.info(f"  Кол-во элементов: {len(data)}")
        if data:
            # Логируем первые несколько элементов
            for i, item in enumerate(data[:max_items]):
                logger.info(f"  Элемент {i + 1}: {json.dumps(item, ensure_ascii=False, indent=2)}")
            if len(data) > max_items:
                logger.info(f"  ... и ещё {len(data) - max_items} элементов")
    else:
        logger.info(f"  {json.dumps(data, ensure_ascii=False, indent=2)}")


def get_last_active_warehouse_id():
    """
    Определяет ID склада, с которого были последние отгрузки.
    Возвращает ID или запасной DEFAULT_WAREHOUSE_ID.
    """
    date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    date_now = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d')

    try:
        logger.info("=" * 60)
        logger.info("🔄 Получение последнего активного склада")
        logger.info(f"Запрашиваем заказы с {date_from} по {date_now}")

        # 1. Получаем последние заказы
        orders_resp = requests.get(
            ORDERS_API_URL,
            headers={"Authorization": STATS_API_KEY},
            params={"dateFrom": date_from, "flag": 1},
            timeout=15
        )

        logger.info(f"Статус Orders API: {orders_resp.status_code}")

        orders_resp.raise_for_status()
        orders = orders_resp.json()

        # Логируем ответ от API заказов
        log_json_response("Ответ от Orders API", orders, max_items=3)

        if not orders:
            logger.warning("Не найдено заказов за последнюю неделю. Использую склад по умолчанию.")
            return DEFAULT_WAREHOUSE_ID

        last_order = orders[0]
        last_warehouse_name = last_order.get('warehouseName')

        logger.info(f"Последний заказ: ID={last_order.get('gNumber')}, Склад={last_warehouse_name}")

        if not last_warehouse_name:
            logger.warning("В последнем заказе не указан склад. Использую склад по умолчанию.")
            return DEFAULT_WAREHOUSE_ID

        # 2. Получаем список всех складов для сопоставления
        logger.info("Запрашиваем список всех складов...")
        warehouses_resp = requests.get(
            WAREHOUSES_API_URL,
            headers={"Authorization": API_KEY},
            timeout=15
        )

        logger.info(f"Статус Warehouses API: {warehouses_resp.status_code}")
        warehouses_resp.raise_for_status()
        warehouses_list = warehouses_resp.json()

        # Логируем ответ от API складов
        log_json_response("Ответ от Warehouses API", warehouses_list, max_items=3)

        # 3. Ищем ID склада по имени
        for wh in warehouses_list:
            if wh.get('name') == last_warehouse_name:
                found_id = wh.get('ID')
                logger.info(f"✅ Найден активный склад: '{last_warehouse_name}' (ID: {found_id})")
                return found_id

        logger.warning(f"Склад '{last_warehouse_name}' не найден. Использую склад по умолчанию.")
        return DEFAULT_WAREHOUSE_ID

    except requests.exceptions.RequestException as e:
        logger.exception(f"❌ Ошибка при определении активного склада: {e}")
        return DEFAULT_WAREHOUSE_ID
    except Exception as e:
        logger.exception(f"❌ Неожиданная ошибка в get_last_active_warehouse_id: {e}")
        return DEFAULT_WAREHOUSE_ID


def get_acceptance_tariff():
    """
    Получить тарифы на приемку для активного склада.
    Один запрос -> все данные за 14 дней -> фильтрация локально.
    """
    warehouse_id = get_last_active_warehouse_id()
    logger.info("=" * 60)
    logger.info(f"📦 Работаем со складом ID: {warehouse_id}, тип поставки: {BOX_TYPE_ID}")

    try:
        # ОДИН запрос получает ВСЕ данные на 14 дней
        logger.info(f"Запрашиваем тарифы для склада {warehouse_id}...")
        resp = requests.get(
            WB_TARIFF_URL,
            headers={"Authorization": API_KEY},
            params={"warehouseIDs": str(warehouse_id)},
            timeout=30
        )

        logger.info(f"Статус Tariffs API: {resp.status_code}")

        # Обработка 429 ошибки
        if resp.status_code == 429:
            logger.error("❌ Превышен лимит запросов (6 в минуту). Подождите 60+ секунд.")
            logger.error(f"Response headers: {resp.headers}")
            return None

        resp.raise_for_status()
        all_tariffs = resp.json()

        # Логируем полный ответ от API тарифов
        #log_json_response("Ответ от Tariffs API", all_tariffs)


        if not all_tariffs:
            logger.error(f"❌ API вернуло пустой ответ для склада {warehouse_id}")
            return None

        logger.info(f"✅ Получено {len(all_tariffs)} записей о тарифах")

        # Группируем по датам для анализа
        tariffs_by_date = {}
        for tariff in all_tariffs:
            if tariff.get('warehouseID') == warehouse_id and tariff.get('boxTypeID') == BOX_TYPE_ID:
                date_key = tariff.get('date')
                if date_key not in tariffs_by_date:
                    tariffs_by_date[date_key] = []
                tariffs_by_date[date_key].append(tariff)

        # Преобразуем ключи дат для логирования (убираем время для читаемости)
        simple_dates = []
        for date_key in tariffs_by_date.keys():
            try:
                # Преобразуем "2025-12-26T00:00:00Z" -> "2025-12-26"
                dt = datetime.fromisoformat(date_key.replace('Z', '+00:00'))
                simple_dates.append(dt.strftime('%Y-%m-%d'))
            except:
                simple_dates.append(date_key)

        logger.info(f"Доступные даты для склада {warehouse_id}: {simple_dates}")

        # ФИЛЬТРАЦИЯ в памяти: ищем доступную дату
        for days_offset in range(0, 15):
            check_date_simple = (date.today() + timedelta(days=days_offset)).strftime('%Y-%m-%d')

            logger.info(f"🔍 Проверяем дату {check_date_simple}...")

            # Ищем подходящую запись - сравниваем даты БЕЗ учета времени
            matching_tariffs = []
            for tariff in all_tariffs:
                if (tariff.get('warehouseID') == warehouse_id and
                        tariff.get('boxTypeID') == BOX_TYPE_ID):

                    # Преобразуем дату из API в простой формат для сравнения
                    api_date_str = tariff.get('date', '')
                    try:
                        # Обрабатываем формат с Z или без
                        if api_date_str.endswith('Z'):
                            api_date = datetime.fromisoformat(api_date_str.replace('Z', '+00:00'))
                        else:
                            api_date = datetime.fromisoformat(api_date_str)

                        api_date_simple = api_date.strftime('%Y-%m-%d')

                        if api_date_simple == check_date_simple:
                            matching_tariffs.append(tariff)
                    except Exception as e:
                        logger.debug(f"Ошибка парсинга даты {api_date_str}: {e}")
                        continue

            if not matching_tariffs:
                logger.debug(f"Нет данных на дату {check_date_simple}")
                continue

            tariff_data = matching_tariffs[0]  # Берём первую найденную
            coefficient = tariff_data.get('coefficient')
            allow_unload = tariff_data.get('allowUnload')

            logger.info(f"Найден тариф: coefficient={coefficient}, allowUnload={allow_unload}")

            # Проверяем условия доступности
            if coefficient in [0, 1] and allow_unload is True:
                warehouse_name = tariff_data.get('warehouseName')
                logger.info(f"✅ Найдена доступная дата: {check_date_simple} ({warehouse_name})")

                # Парсим стоимости (помощник для удобства)
                def parse_float(val):
                    if val is None:
                        return 0.0
                    try:
                        return float(str(val).replace(',', '.'))
                    except:
                        return 0.0

                result = {
                    'warehouse_id': warehouse_id,
                    'warehouse_name': warehouse_name,
                    'date': check_date_simple,  # Сохраняем простой формат
                    'coefficient': coefficient,
                    'delivery_base': parse_float(tariff_data.get('deliveryBaseLiter')),
                    'delivery_liter': parse_float(tariff_data.get('deliveryAdditionalLiter')),
                    'storage_base': parse_float(tariff_data.get('storageBaseLiter')),
                    'storage_liter': parse_float(tariff_data.get('storageAdditionalLiter')),
                    'is_sorting_center': tariff_data.get('isSortingCenter', False),
                    'is_future_date': True,
                    'raw_data': tariff_data  # Добавляем сырые данные для отладки
                }

                logger.info(f"📊 Парсированные данные тарифа: {json.dumps(result, ensure_ascii=False, indent=2)}")
                return result
            else:
                # Дата есть, но недоступна для приемки
                logger.debug(
                    f"Дата {check_date_simple} недоступна: coefficient={coefficient}, allowUnload={allow_unload}")
                continue

        # Если дошли сюда — доступных дат нет
        logger.error("❌ Нет доступных дат для приемки в ближайшие 14 дней!")

        # Логируем ВСЕ тарифы для отладки
        logger.info("📋 Все тарифы для анализа:")
        for tariff in all_tariffs:
            if tariff.get('warehouseID') == warehouse_id and tariff.get('boxTypeID') == BOX_TYPE_ID:
                logger.info(
                    f"  {tariff.get('date')}: coeff={tariff.get('coefficient')}, unload={tariff.get('allowUnload')}")

        return None

    except requests.exceptions.RequestException as e:
        logger.exception(f"❌ Ошибка сети/API: {e}")
        logger.error(f"URL: {WB_TARIFF_URL}")
        logger.error(f"Params: warehouseIDs={warehouse_id}")
        return None
    except Exception as e:
        logger.exception(f"❌ Неожиданная ошибка в get_acceptance_tariff: {e}")
        return None


def calc_volume(length, width, height):
    """Возвращает литры. Менее 1 л считается как 1."""
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


def calc_logistics(volume, base, liter):
    """Логистика: первый литр = base, остальные = liter * (V-1)."""
    first = base
    extra_vol = max(volume - 1.0, 0.0)
    extra_cost = extra_vol * liter
    return first + extra_cost


def calc_price_breakdown(cost, profit, logistics, buffer_coeff=1.0,
                         bank_rate=BANK_RATE, wb_commission=WB_COMMISSION):
    """Возвращает детальную схему расчёта и финальную цену."""
    cost = float(cost)
    profit = float(profit)
    logistics = float(logistics)

    base_needed = cost + profit
    after_bank = base_needed / (1.0 - bank_rate)
    with_logistics = after_bank + logistics
    before_buffer = with_logistics / (1.0 - wb_commission)
    after_buffer = before_buffer * buffer_coeff
    final_rounded = round(after_buffer)

    return {
        "cost": cost,
        "profit": profit,
        "base_needed": base_needed,
        "after_bank": after_bank,
        "logistics": logistics,
        "with_logistics": with_logistics,
        "wb_commission_pct": wb_commission,
        "before_buffer": before_buffer,
        "buffer_coeff": buffer_coeff,
        "after_buffer": after_buffer,
        "final_rounded": final_rounded
    }


async def load_products(pool):
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("""
                SELECT product_id, model, purchase_price, target_profit_rub,
                       length, width, height
                FROM oc_product
                WHERE status = 1 AND target_profit_rub > 0 AND wb_real_price IS NULL
            """)
            return await cur.fetchall()


async def save_price_wb(pool, product_id, price_wb):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE oc_product
                SET price_wb = %s
                WHERE product_id = %s
                """,
                (price_wb, product_id)
            )


async def main():
    logger.info("=" * 60)
    logger.info("🚀 ЗАПУСК РАСЧЕТА ЦЕН WILDBERRIES")
    logger.info("=" * 60)

    tariff_data = get_acceptance_tariff()

    if not tariff_data:
        logger.error("❌ Не удалось получить доступные тарифы. Расчет невозможен.")
        return

    if tariff_data.get('is_future_date'):
        logger.warning(f"⚠️ Приемка доступна только с {tariff_data['date']}!")

    delivery_base = tariff_data['delivery_base']
    delivery_liter = tariff_data['delivery_liter']
    warehouse_name = tariff_data['warehouse_name']
    coefficient = tariff_data['coefficient']

    logger.info("=" * 60)
    logger.info(
        f"📊 Используем тарифы для '{warehouse_name}' (ID: {tariff_data['warehouse_id']}) на {tariff_data['date']}:")
    logger.info(f"  Коэффициент приемки: {coefficient}")
    logger.info(f"  Доставка: первый литр = {delivery_base} руб, доп. литр = {delivery_liter} руб")
    logger.info(
        f"  Хранение: базовое = {tariff_data['storage_base']} руб, доп. литр = {tariff_data['storage_liter']} руб")
    logger.info(
        f"Параметры: эквайринг={BANK_RATE * 100:.2f}%, комиссия_WB={WB_COMMISSION * 100:.2f}%, буфер={BUFFER_COEFF * 100 - 100:.1f}%")
    logger.info("=" * 60)

    pool = await aiomysql.create_pool(**OC_DB_CONFIG, cursorclass=aiomysql.DictCursor)

    try:
        rows = await load_products(pool)
    except Exception as e:
        logger.exception(f"Ошибка загрузки товаров из БД: {e}")
        pool.close()
        await pool.wait_closed()
        return

    results = []
    logger.info(f"Найдено товаров: {len(rows)}\n")

    for row in rows:
        pid = row.get("product_id")
        model = row.get("model")
        try:
            vol = calc_volume(row.get("length"), row.get("width"), row.get("height"))
            logistics = calc_logistics(vol, delivery_base, delivery_liter)

            breakdown = calc_price_breakdown(
                cost=float(row["purchase_price"]),
                profit=float(row.get("target_profit_rub") or 0.0),
                logistics=logistics,
                buffer_coeff=BUFFER_COEFF
            )
            price_wb = breakdown["final_rounded"]

            await save_price_wb(pool, pid, price_wb)
            logger.info(f"  → Товар {pid} ('{model}'): price_wb = {price_wb} руб")

            results.append({
                "product_id": pid,
                "model": model,
                "volume_l": round(vol, 2),
                "logistics": round(logistics, 2),
                "price": price_wb,
            })

        except Exception as e:
            logger.exception(f"Ошибка расчёта для product_id={pid}: {e}")

    if results:
        logger.info("\n📋 Сводка по рассчитанным товарам:")
        for r in results[:10]:
            logger.info(
                f"{r['product_id']} {r['model']} | объем {r['volume_l']} л | логистика {r['logistics']} руб | цена {r['price']} руб")
        if len(results) > 10:
            logger.info(f"... и еще {len(results) - 10} товаров")

    pool.close()
    await pool.wait_closed()
    return results


async def scheduled_job():
    logger.info("=" * 60)
    logger.info("⏰ Запуск плановой задачи WB FBS")
    logger.info("=" * 60)
    await main()


async def start():
    await main()

    scheduler.add_job(
        scheduled_job,
        IntervalTrigger(hours=2),
        id="wb_fbs_interval",
        replace_existing=True,
        max_instances=1
    )

    scheduler.start()
    logger.info(f"Scheduler запущен. Задачи будут выполняться каждые 2 часа")

    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Остановка планировщика...")
        scheduler.shutdown()
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(start())