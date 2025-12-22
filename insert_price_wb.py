import math
import os
import asyncio
import aiomysql
import requests
import json
import logging
import sys
from datetime import date, timedelta
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
WB_TARIFF_URL = "https://common-api.wildberries.ru/api/v1/tariffs/box"
WAREHOUSE_NAME = os.getenv('WB_WAREHOUSE_NAME')

WB_COMMISSION = float(os.getenv('WB_COMMISSION'))
BANK_RATE = float(os.getenv('BANK_COMMISSION'))
BUFFER_COEFF = float(os.getenv('BUFFER_COEFF'))

# Интервал запуска (в секундах)
INTERVAL_SECONDS = int(os.getenv('WB_FBS_INTERVAL'))

scheduler = AsyncIOScheduler()

# Логи
LOG_LEVEL = logging.INFO
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("wb_price_calc")
# -----------------------------------------------


def get_today_tariff():
    for days_back in range(0, 5):
        check_date = (date.today() - timedelta(days=days_back)).isoformat()
        logger.info(f"Пробуем тарифы WB на дату {check_date}")

        try:
            resp = requests.get(
                WB_TARIFF_URL,
                headers={
                    "Authorization": API_KEY,
                    "Accept": "application/json"
                },
                params={"date": check_date},
                timeout=30
            )
            resp.raise_for_status()
        except Exception:
            continue

        raw = resp.json()
        warehouse_list = raw.get("response", {}).get("data", {}).get("warehouseList")

        if not warehouse_list:
            continue

        for wh in warehouse_list:
            if wh.get("warehouseName") == WAREHOUSE_NAME:
                base = float(str(wh["boxDeliveryMarketplaceBase"]).replace(",", "."))
                liter = float(str(wh["boxDeliveryMarketplaceLiter"]).replace(",", "."))
                logger.info(f"Тарифы WB взяты за дату {check_date}")
                return base, liter

    raise RuntimeError("Не удалось получить тарифы WB ни за один из последних дней")

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
    """Возвращает детальную схему расчёта и финальную цену (до и после округления)."""
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
    try:
        base_price, liter_price = get_today_tariff()
    except Exception as e:
        logger.exception(f"Не удалось получить тарифы WB: {e}")
        return

    logger.info(f"Тарифы WB для '{WAREHOUSE_NAME}': первый литр = {base_price} руб, доп. литр = {liter_price} руб")
    logger.info(f"Параметры: эквайринг={BANK_RATE*100:.2f}%, комиссия_WB={WB_COMMISSION*100:.2f}%, буфер={BUFFER_COEFF*100-100:.1f}%")

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
            logistics = calc_logistics(vol, base_price, liter_price)

            breakdown = calc_price_breakdown(
                cost=float(row["purchase_price"]),
                profit=float(row.get("target_profit_rub") or 0.0),
                logistics=logistics,
                buffer_coeff=BUFFER_COEFF
            )
            price_wb = breakdown["final_rounded"]

            await save_price_wb(pool, pid, price_wb)

            logger.info(f"  → Записано в БД: price_wb = {price_wb} руб")

            # Логируем подробную развертку
            logger.info("----")
            logger.info(f"nm product_id={pid} model='{model}'")
            logger.info(f"  Габариты (LxWxH): {row.get('length')} x {row.get('width')} x {row.get('height')} см → объем: {vol:.2f} л")
            logger.info(f"  Тарифы WB использованы: base={base_price} руб, liter={liter_price} руб")
            logger.info(f"  Себестоимость: {breakdown['cost']:.2f} руб")
            logger.info(f"  Цель прибыли: {breakdown['profit']:.2f} руб")
            logger.info(f"  Нужна на руки (закуп+прибыль): {breakdown['base_needed']:.2f} руб")
            logger.info(f"  После эквайринга ({BANK_RATE*100:.2f}%): {breakdown['after_bank']:.2f} руб")
            logger.info(f"  Логистика (WB): {breakdown['logistics']:.2f} руб")
            logger.info(f"  Сумма до комиссии WB: {breakdown['with_logistics']:.2f} руб")
            logger.info(f"  Комиссия WB: {breakdown['wb_commission_pct']*100:.2f}% → нужно выставить до комиссии: {breakdown['before_buffer']:.2f} руб")
            logger.info(f"  Буфер СПП: x{breakdown['buffer_coeff']:.3f} → после буфера: {breakdown['after_buffer']:.2f} руб")
            logger.info(f"  Итог (округлённо): {breakdown['final_rounded']} руб")
            logger.info("----\n")

            results.append({
                "product_id": pid,
                "model": model,
                "volume_l": round(vol, 2),
                "logistics": round(logistics, 2),
                "price": breakdown["final_rounded"],
                "breakdown": breakdown
            })

        except Exception as e:
            logger.exception(f"Ошибка расчёта для product_id={pid} model='{model}': {e}")

    # Вывод таблицы итогов
    logger.info("Результаты расчёта (кратко):")
    for r in results:
        logger.info(f"{r['product_id']} {r['model']} | объем {r['volume_l']} л | логистика {r['logistics']} руб | цена {r['price']} руб")

    pool.close()
    await pool.wait_closed()
    return results

async def scheduled_job():
    logger.info("Запуск плановой задачи WB FBS")
    await main()   # твоя основная логика

async def start():

    await main()


    scheduler.add_job(
        scheduled_job,
        IntervalTrigger(hours=2),  # Запуск каждые 2 часа
        id="wb_fbs_interval",
        replace_existing=True,
        max_instances=1
    )

    scheduler.start()
    logger.info(f"Scheduler запущен. Задачи будут выполняться каждые 2 часа")

    # Держим loop живым
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Остановка планировщика...")
        scheduler.shutdown()
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(start())