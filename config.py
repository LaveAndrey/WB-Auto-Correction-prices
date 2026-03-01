#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Конфигурация системы Ozon Price Updater
"""

import os
import aiomysql
import asyncio
from dotenv import load_dotenv

load_dotenv()


# Функция для загрузки настроек из БД
def load_db_settings():
    """Загружает настройки из таблицы oc_auto_price_settings"""
    settings = {}

    async def _load():
        pool = None
        try:
            pool = await aiomysql.create_pool(
                host=os.getenv('DB_HOST'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                db=os.getenv('DB_NAME'),
                port=int(os.getenv('DB_PORT', 3306)),
                autocommit=True,
                charset='utf8mb4',
                minsize=1,
                maxsize=1
            )

            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    table = os.getenv('AUTO_PRICE_SETTING_TABLE', 'oc_auto_price_settings')
                    await cursor.execute(f"""
                        SELECT setting_key, setting_value, setting_type 
                        FROM {table} 
                        WHERE setting_value IS NOT NULL
                    """)

                    rows = await cursor.fetchall()

                    for row in rows:
                        key = row['setting_key']
                        value = row['setting_value']
                        value_type = row['setting_type']

                        # Преобразуем значение
                        try:
                            if value_type == 'boolean':
                                settings[key] = str(value).lower() in ('true', '1', 'yes', 'on')
                            elif value_type == 'number':
                                if '.' in str(value):
                                    settings[key] = float(value)
                                else:
                                    settings[key] = int(value)
                            else:
                                settings[key] = value
                        except:
                            settings[key] = value

            print(f"✅ Загружено {len(settings)} настроек из БД")
            return settings

        except Exception as e:
            print(f"⚠️ Не удалось загрузить настройки из БД: {e}")
            return {}
        finally:
            if pool:
                pool.close()
                await pool.wait_closed()

    # Запускаем асинхронную загрузку синхронно
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        db_config = loop.run_until_complete(_load())
        loop.close()
        return db_config
    except:
        return {}


# Загружаем настройки из БД
DB_SETTINGS = load_db_settings()


class Config:
    """Конфигурация системы"""
    # База данных
    DB_HOST = os.getenv('DB_HOST')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_NAME = os.getenv('DB_NAME')
    DB_PORT = int(os.getenv('DB_PORT'))

    # Ozon API (только из .env, безопасно)
    OZON_CLIENT_ID = os.getenv('OZON_CLIENT_ID')
    OZON_API_KEY = os.getenv('OZON_API_KEY')
    CDEK_CLIENT_ID = os.getenv('CDEK_CLIENT_ID')
    CDEK_CLIENT_SECRET = os.getenv('CDEK_CLIENT_SECRET')
    WB_SALES_TOKEN = os.getenv('WB_SALES_TOKEN')
    WB_PRICES_TOKEN = os.getenv('WB_PRICES_TOKEN')
    WB_CONTENT_TOKEN = os.getenv('WB_CONTENT_TOKEN')

    # Параметры СДЭК
    CDEK_API_URL = "https://api.cdek.ru/v2"
    CDEK_WAREHOUSE_CITY_CODE = os.getenv('CDEK_WAREHOUSE_CITY_CODE', '66')

    # Параметры OZON (с приоритетом из БД)
    OZON_API_URL = "https://api-seller.ozon.ru"
    LOAD_PRICE_TO_OZON = DB_SETTINGS.get('LOAD_PRICE_TO_OZON',
                                         os.getenv('LOAD_PRICE_TO_OZON', 'False').lower() == 'true')
    OZON_FORPAY_TO_PRICEWITHDISC_RATIO = float(
        DB_SETTINGS.get('OZON_FORPAY_TO_PRICEWITHDISC_RATIO', os.getenv('OZON_FORPAY_TO_PRICEWITHDISC_RATIO', 1.0)))
    OZON_CYCLE_INFO_TABLE = str(
        DB_SETTINGS.get('OZON_CYCLE_INFO_TABLE', os.getenv('OZON_CYCLE_INFO_TABLE', 'ozon_cycle_info')))
    OZON_SYSTEM_STATUS_TABLE = str(
        DB_SETTINGS.get('OZON_SYSTEM_STATUS_TABLE', os.getenv('OZON_SYSTEM_STATUS_TABLE', 'ozon_system_status')))
    OZON_DAILY_STATS_TABLE = str(
        DB_SETTINGS.get('OZON_DAILY_STATS_TABLE', os.getenv('OZON_DAILY_STATS_TABLE', 'ozon_daily_stats')))
    OZON_PRICE_HISTORY_TABLE = str(
        DB_SETTINGS.get('OZON_PRICE_HISTORY_TABLE', os.getenv('OZON_PRICE_HISTORY_TABLE', 'ozon_price_history')))
    OZON_CYCLE_INTERVAL = int(DB_SETTINGS.get('OZON_CYCLE_INTERVAL', os.getenv('OZON_CYCLE_INTERVAL', 7200)))
    OZON_NO_SALES_INTERVAL = int(DB_SETTINGS.get('OZON_NO_SALES_INTERVAL', os.getenv('OZON_NO_SALES_INTERVAL', 3600)))
    OZON_LOGISTIC_TABLE = str(os.getenv('OZON_LOGISTIC_TABLE'))
    WB_LOGIDTIC_TABLE = str(os.getenv('WB_LOGIDTIC_TABLE'))
    OZON_DEFAULT_REGION = 'Москва'
    OZON_DEFAULT_CITY = 'Москва'
    MIN_DEVIATION_PERCENT = float(DB_SETTINGS.get('MIN_DEVIATION_PERCENT', os.getenv('MIN_DEVIATION_PERCENT', 1.0)))

    # Параметры WB (с приоритетом из БД)
    WB_INTERVAL = int(DB_SETTINGS.get('WB_INTERVAL', os.getenv('WB_CYCLE_INTERVAL', 7200)))
    WB_NO_SALES_INTERVAL = int(DB_SETTINGS.get('WB_NO_SALES_INTERVAL', os.getenv('WB_NO_SALES_INTERVAL', 3600)))
    WB_CYCLE_INFO_TABLE = str(DB_SETTINGS.get('WB_CYCLE_INFO_TABLE', os.getenv('WB_CYCLE_INFO_TABLE', 'wb_cycle_info')))
    WB_SYSTEM_STATUS_TABLE = str(
        DB_SETTINGS.get('WB_SYSTEM_STATUS_TABLE', os.getenv('WB_SYSTEM_STATUS_TABLE', 'wb_system_status')))
    WB_DAILY_STATS_TABLE = str(
        DB_SETTINGS.get('WB_DAILY_STATS_TABLE', os.getenv('WB_DAILY_STATS_TABLE', 'wb_daily_stats')))
    WB_PRICE_HISTORY_TABLE = str(
        DB_SETTINGS.get('WB_PRICE_HISTORY_TABLE', os.getenv('WB_PRICE_HISTORY_TABLE', 'wb_price_history')))
    LOAD_PRICE_TO_WB = DB_SETTINGS.get('LOAD_PRICE_TO_WB', os.getenv('LOAD_PRICE_TO_WB', 'False').lower() == 'true')
    WB_BOX_TYPE = int(DB_SETTINGS.get('WB_BOX_TYPE', os.getenv('WB_BOX_TYPE', 1)))
    WB_FORPAY_TO_PRICEWITHDISC_RATIO = float(
        DB_SETTINGS.get('WB_FORPAY_TO_PRICEWITHDISC_RATIO', os.getenv('WB_FORPAY_TO_PRICEWITHDISC_RATIO', 1.0)))
    WB_TARIFF_URL = os.getenv('WB_TARIFF_URL')
    ORDERS_API_URL = os.getenv('ORDERS_API_URL')
    WAREHOUSES_API_URL = os.getenv('WAREHOUSES_API_URL')
    PROMO_CALENDAR_URL = os.getenv('PROMO_CALENDAR_URL')
    PROMO_DETAILS_URL = os.getenv('PROMO_DETAILS_URL')
    WB_WAREHOUSE_ID = os.getenv('WB_WAREHOUSE_ID')

    # Финансовые параметры (с приоритетом из БД)
    VAT_PERCENT = float(DB_SETTINGS.get('VAT_PERCENT', os.getenv('VAT_PERCENT', 20)))
    BANK_COMMISSION = float(DB_SETTINGS.get('BANK_COMMISSION', os.getenv('BANK_COMMISSION', 0.025)))
    MIN_MARGIN_FACTOR = float(DB_SETTINGS.get('MIN_MARGIN_FACTOR', os.getenv('MIN_MARGIN_FACTOR', 1.2)))
    MIN_PRICE_CHANGE = float(DB_SETTINGS.get('MIN_PRICE_CHANGE', os.getenv('MIN_PRICE_CHANGE', 10)))
    ACQUIRING_RATE = float(DB_SETTINGS.get('ACQUIRING_RATE', os.getenv('ACQUIRING_RATE', 0.03)))
    LOCALIZATION_INDEX = float(DB_SETTINGS.get('LOCALIZATION_INDEX', os.getenv('LOCALIZATION_INDEX', 1.0)))

    # Параметры работы (с приоритетом из БД)
    SALES_HOURS_FILTER = int(DB_SETTINGS.get('SALES_HOURS_FILTER', os.getenv('SALES_HOURS_FILTER', 24)))
    MIN_SALES_FOR_CALC = int(DB_SETTINGS.get('MIN_SALES_FOR_CALC', os.getenv('MIN_SALES_FOR_CALC', 2)))
    MAX_PRICE_CHANGE_PERCENT = float(
        DB_SETTINGS.get('MAX_PRICE_CHANGE_PERCENT', os.getenv('MAX_PRICE_CHANGE_PERCENT', 30)))
    BATCH_SIZE = int(DB_SETTINGS.get('BATCH_SIZE', os.getenv('BATCH_SIZE', 100)))
    WORKERS_COUNT = int(DB_SETTINGS.get('WORKERS_COUNT', os.getenv('WORKERS_COUNT', 5)))
    MAX_QUEUE_SIZE = int(DB_SETTINGS.get('MAX_QUEUE_SIZE', os.getenv('MAX_QUEUE_SIZE', 1000)))
    LOG_LEVEL = str(DB_SETTINGS.get('LOG_LEVEL', os.getenv('LOG_LEVEL', 'INFO')))
    LOG_FORMAT = os.getenv('LOG_FORMAT', '%(asctime)s | %(levelname)-7s | %(message)s')

    # Включение модулей (с приоритетом из БД)
    WB_ENABLED = DB_SETTINGS.get('WB_ENABLED', os.getenv('WB_ENABLED', 'False').lower() == 'true')
    WB_WITH_SALES_ENABLED = DB_SETTINGS.get('WB_WITH_SALES_ENABLED',
                                            os.getenv('WB_WITH_SALES_ENABLED', 'False').lower() == 'true')
    WB_NO_SALES_ENABLED = DB_SETTINGS.get('WB_NO_SALES_ENABLED',
                                          os.getenv('WB_NO_SALES_ENABLED', 'False').lower() == 'true')
    OZON_ENABLED = DB_SETTINGS.get('OZON_ENABLED', os.getenv('OZON_ENABLED', 'False').lower() == 'true')
    OZON_WITH_SALES_ENABLED = DB_SETTINGS.get('OZON_WITH_SALES_ENABLED',
                                              os.getenv('OZON_WITH_SALES_ENABLED', 'False').lower() == 'true')
    OZON_NO_SALES_ENABLED = DB_SETTINGS.get('OZON_NO_SALES_ENABLED',
                                            os.getenv('OZON_NO_SALES_ENABLED', 'False').lower() == 'true')

    # Таблицы БД (с приоритетом из БД)
    LOGS_TABLE = str(DB_SETTINGS.get('LOGS_TABLE', os.getenv('LOGS_TABLE', 'system_logs')))
    PPODUCT_TABLE = str(DB_SETTINGS.get('PPODUCT_TABLE', os.getenv('PPODUCT_TABLE', 'products')))
    PRICE_SCHEDULE_TABLE = str(
        DB_SETTINGS.get('PRICE_SCHEDULE_TABLE', os.getenv('PRICE_SCHEDULE_TABLE', 'oc_price_schedule')))
    AUTO_PRICE_SETTING_TABLE = os.getenv('AUTO_PRICE_SETTING_TABLE', 'oc_auto_price_settings')

    # SALES (ПРОМО) (с приоритетом из БД)
    MAX_PROMO_PRICE_INCREASE = float(
        DB_SETTINGS.get('MAX_PROMO_PRICE_INCREASE', os.getenv('MAX_PROMO_PRICE_INCREASE', 0.15)))
    PROMO_MIN_MARGIN_FACTOR = float(
        DB_SETTINGS.get('PROMO_MIN_MARGIN_FACTOR', os.getenv('PROMO_MIN_MARGIN_FACTOR', 1.3)))
    PROMO_CHECK_INTERVAL = int(DB_SETTINGS.get('PROMO_CHECK_INTERVAL', os.getenv('PROMO_CHECK_INTERVAL', 6)))

    # НАСТРОЙКИ АВТООЧИСТКИ БД (с приоритетом из БД)
    DB_AUTO_CLEANUP_ENABLED = DB_SETTINGS.get('DB_AUTO_CLEANUP_ENABLED', True)
    DB_CLEANUP_INTERVAL_DAYS = int(DB_SETTINGS.get('DB_CLEANUP_INTERVAL_DAYS', 3))
    DB_CLEANUP_HOUR = int(DB_SETTINGS.get('DB_CLEANUP_HOUR', 3))

    # Таблицы для очистки (из БД или по умолчанию)
    _cleanup_tables = DB_SETTINGS.get('DB_CLEANUP_TABLES')
    if _cleanup_tables:
        if isinstance(_cleanup_tables, str):
            import json
            try:
                DB_CLEANUP_TABLES = json.loads(_cleanup_tables)
            except:
                DB_CLEANUP_TABLES = [t.strip() for t in _cleanup_tables.split(',')]
        else:
            DB_CLEANUP_TABLES = _cleanup_tables
    else:
        DB_CLEANUP_TABLES = [
            WB_PRICE_HISTORY_TABLE,
            OZON_PRICE_HISTORY_TABLE,
            LOGS_TABLE,
            OZON_LOGISTIC_TABLE,
            WB_LOGIDTIC_TABLE
        ]

    DB_KEEP_LAST_STATUS_RECORDS = int(DB_SETTINGS.get('DB_KEEP_LAST_STATUS_RECORDS', 50))
    DB_CLEANUP_BATCH_SIZE = int(DB_SETTINGS.get('DB_CLEANUP_BATCH_SIZE', 1000))
    DB_CLEANUP_TIMEOUT = int(DB_SETTINGS.get('DB_CLEANUP_TIMEOUT', 300))

    PROMOTION_DESIRED_TOTAL_PERCENT = float(DB_SETTINGS.get('PROMOTION_DESIRED_TOTAL_PERCENT',
                                                            os.getenv('PROMOTION_DESIRED_TOTAL_PERCENT', 10)))
    PROMOTION_DESIRED_DAILY_PERCENT = float(DB_SETTINGS.get('PROMOTION_DESIRED_DAILY_PERCENT',
                                                            os.getenv('PROMOTION_DESIRED_DAILY_PERCENT', 2)))
    PROMOTION_RAMP_DAYS = int(DB_SETTINGS.get('PROMOTION_RAMP_DAYS',
                                              os.getenv('PROMOTION_RAMP_DAYS', 14)))
    PROMOTION_SYNC_CYCLE = int(DB_SETTINGS.get('PROMOTION_SYNC_CYCLE',
                                               os.getenv('PROMOTION_SYNC_CYCLE', 24)))


    PROMOTION_LOCK_PRICES = True

    PROMOTION_STATUS_SYNC_ENABLED = True
    PROMOTION_STATUS_SYNC_INTERVAL = 3600  # раз в час

    WB_PROMOTION_SYNC_ENABLED = True  # Включить фоновую синхронизацию акций WB
    WB_PROMOTION_SYNC_INTERVAL = 3600  # Интервал в секундах (6 часов)


    PROMOTION_SAFE_DAILY_FIXED = 50  # Не больше 50₽ в день
    PROMOTION_MIN_PRICE = 300  # Только товары дороже 300₽

    # --- СРОКИ И ФИЛЬТРЫ ---
    OZON_PROMOTION_TABLE = str(os.getenv('oc_ozon_promotions')) # Таблица для хранения акций
    PROMOTIONS_ENABLED = False

    PROMOTION_PRICE_RESTORE = True  # Возвращать цену после акции

    DAILY_STATS_ENABLED = True
    DAILY_STATS_HOUR = 2  # Во сколько часов запускать сбор статистики (0-23)

    OZON_NO_SALES_MULTIPLIER = float(DB_SETTINGS.get('OZON_NO_SALES_MULTIPLIER', os.getenv('OZON_NO_SALES_MULTIPLIER', 4.0)))
    OZON_RETURN_COST_PERCENT = float(DB_SETTINGS.get('OZON_RETURN_COST_PERCENT', os.getenv('OZON_RETURN_COST_PERCENT', 80)))
    OZON_LOGISTICS_STATS_DAYS = int(DB_SETTINGS.get('OZON_LOGISTICS_STATS_DAYS', os.getenv('OZON_LOGISTICS_STATS_DAYS', 30)))
    WB_RETURN_COST = float(DB_SETTINGS.get('WB_RETURN_COST', os.getenv('WB_RETURN_COST', 50)))
