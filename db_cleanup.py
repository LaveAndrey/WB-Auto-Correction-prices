#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ПОЛНОСТЬЮ ГОТОВЫЙ МОДУЛЬ ДЛЯ АВТОМАТИЧЕСКОЙ ОЧИСТКИ БАЗЫ ДАННЫХ
Использует настройки из config.py:
- DB_AUTO_CLEANUP_ENABLED = True/False
- DB_CLEANUP_INTERVAL_DAYS = 3
- DB_CLEANUP_HOUR = 3
- DB_KEEP_LAST_STATUS_RECORDS = 10  # Сколько записей статуса оставлять
- DB_CLEANUP_TABLES = список таблиц для очистки
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, Optional
import traceback

import aiomysql
import pytz

from config import Config
from WB_integration.utils.logging_utils import setup_logger, log_separator


class DatabaseCleanup:
    """
    Класс для автоматической очистки устаревших данных из БД.
    Удаляет записи старше DB_CLEANUP_INTERVAL_DAYS дней.
    Для таблиц статуса оставляет только последние DB_KEEP_LAST_STATUS_RECORDS записей.
    """

    def __init__(self, db_pool):
        self.db_pool = db_pool
        self.logger = setup_logger('db_cleanup')
        self.last_cleanup_time = None  # Время последней очистки

    async def check_and_cleanup(self) -> Dict[str, int]:
        """
        Проверяет, нужно ли выполнить очистку (по интервалу и времени суток),
        и если да - запускает её.
        Возвращает словарь с результатами очистки.
        """
        # Проверяем, включена ли автоочистка
        if not Config.DB_AUTO_CLEANUP_ENABLED:
            self.logger.debug("Автоочистка БД отключена в конфигурации")
            return {}

        # Текущее время в МСК
        moscow_tz = pytz.timezone('Europe/Moscow')
        now = datetime.now(moscow_tz)

        # Проверяем интервал с последней очистки
        if self.last_cleanup_time:
            days_passed = (now - self.last_cleanup_time).days
            if days_passed < Config.DB_CLEANUP_INTERVAL_DAYS:
                self.logger.debug(f"С последней очистки прошло {days_passed} дней, "
                                  f"интервал {Config.DB_CLEANUP_INTERVAL_DAYS} дней. Очистка не требуется.")
                return {}

        # Проверяем, подходит ли текущий час для очистки
        if now.hour != Config.DB_CLEANUP_HOUR:
            self.logger.debug(f"Текущий час {now.hour}, время очистки {Config.DB_CLEANUP_HOUR}:00. Очистка отложена.")
            return {}

        # Всё совпало - запускаем очистку
        self.logger.info(
            f"✅ Наступило время плановой очистки (интервал {Config.DB_CLEANUP_INTERVAL_DAYS} дней, час {Config.DB_CLEANUP_HOUR}:00)")
        results = await self._perform_cleanup()
        self.last_cleanup_time = now
        return results

    async def force_cleanup(self) -> Dict[str, int]:
        """
        Принудительная очистка (игнорирует интервал и время суток).
        """
        self.logger.info("🔄 Запуск принудительной очистки БД")
        results = await self._perform_cleanup()
        self.last_cleanup_time = datetime.now(pytz.timezone('Europe/Moscow'))
        return results

    async def _perform_cleanup(self) -> Dict[str, int]:
        """
        Выполняет очистку всех таблиц из списка DB_CLEANUP_TABLES.
        Для таблиц статуса (system_status) применяет особую логику - оставляет только последние N записей.
        """
        log_separator(self.logger, "АВТОМАТИЧЕСКАЯ ОЧИСТКА БАЗЫ ДАННЫХ", "🧹")

        # Дата, старше которой удаляем записи (для обычных таблиц)
        cutoff_date = datetime.now() - timedelta(days=Config.DB_CLEANUP_INTERVAL_DAYS)
        cutoff_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')

        self.logger.info(f"📅 Удаляем записи старше: {cutoff_str} (интервал {Config.DB_CLEANUP_INTERVAL_DAYS} дней)")
        self.logger.info(f"📋 Таблицы для очистки: {', '.join(Config.DB_CLEANUP_TABLES)}")
        self.logger.info(f"📊 Для таблиц статуса оставляем последние {Config.DB_KEEP_LAST_STATUS_RECORDS} записей")

        results = {}

        for table_name in Config.DB_CLEANUP_TABLES:
            try:
                # Проверяем, является ли таблица таблицей статуса
                if 'system_status' in table_name.lower():
                    # Для таблиц статуса - особая логика
                    deleted = await self._clean_status_table(table_name)
                else:
                    # Для обычных таблиц - удаление по дате
                    deleted = await self._clean_table_by_date(table_name, cutoff_date)

                results[table_name] = deleted
            except Exception as e:
                self.logger.error(f"❌ Ошибка при очистке {table_name}: {e}")
                results[table_name] = -1  # -1 означает ошибку

        # Итоги
        total_deleted = sum(v for v in results.values() if v > 0)
        tables_with_errors = sum(1 for v in results.values() if v == -1)

        self.logger.info("📊 ИТОГИ ОЧИСТКИ:")
        self.logger.info(f"   • Всего удалено записей: {total_deleted}")

        # Детали по таблицам
        for table, count in results.items():
            if count > 0:
                self.logger.info(f"   ✅ {table}: удалено {count} записей")
            elif count == 0:
                self.logger.info(f"   ℹ️ {table}: нет записей для удаления")
            elif count == -1:
                self.logger.info(f"   ❌ {table}: ошибка при очистке")

        if tables_with_errors > 0:
            self.logger.warning(f"⚠️ Очистка завершена с {tables_with_errors} ошибками")
        else:
            log_separator(self.logger, "ОЧИСТКА УСПЕШНО ЗАВЕРШЕНА", "✅")

        return results

    async def _clean_table_by_date(self, table_name: str, cutoff_date: datetime) -> int:
        """
        Удаляет старые записи из таблицы по дате.
        Возвращает количество удаленных записей.
        """
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # Проверяем существование таблицы
                    await cursor.execute("SHOW TABLES LIKE %s", (table_name,))
                    if not await cursor.fetchone():
                        self.logger.warning(f"⚠️ Таблица {table_name} не существует, пропускаем")
                        return 0

                    # Определяем поле с датой для этой таблицы
                    date_field = self._get_date_field(table_name)
                    if not date_field:
                        self.logger.warning(f"⚠️ Для таблицы {table_name} не определено поле с датой, пропускаем")
                        return 0

                    # Проверяем существование поля
                    await cursor.execute(f"SHOW COLUMNS FROM {table_name} LIKE %s", (date_field,))
                    if not await cursor.fetchone():
                        self.logger.warning(f"⚠️ В таблице {table_name} нет поля {date_field}, пропускаем")
                        return 0

                    # Считаем, сколько записей будет удалено
                    await cursor.execute(f"""
                        SELECT COUNT(*) FROM {table_name} 
                        WHERE {date_field} < %s
                    """, (cutoff_date,))

                    count = (await cursor.fetchone())[0]

                    if count == 0:
                        return 0

                    # Удаляем записи пачками по 1000
                    deleted_total = 0
                    batch_size = 1000

                    while True:
                        await cursor.execute(f"""
                            DELETE FROM {table_name} 
                            WHERE {date_field} < %s
                            LIMIT %s
                        """, (cutoff_date, batch_size))

                        deleted = cursor.rowcount
                        deleted_total += deleted

                        if deleted < batch_size:
                            break

                        await asyncio.sleep(0.1)

                    return deleted_total

        except Exception as e:
            self.logger.error(f"   ❌ Ошибка при очистке {table_name}: {e}")
            raise

    async def _clean_status_table(self, table_name: str) -> int:
        """
        Специальная очистка для таблиц статуса.
        Оставляет только последние DB_KEEP_LAST_STATUS_RECORDS записей.
        Возвращает количество удаленных записей.
        """
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # Проверяем существование таблицы
                    await cursor.execute("SHOW TABLES LIKE %s", (table_name,))
                    if not await cursor.fetchone():
                        self.logger.warning(f"⚠️ Таблица статуса {table_name} не существует, пропускаем")
                        return 0

                    # Получаем общее количество записей
                    await cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    total = (await cursor.fetchone())[0]

                    keep_count = Config.DB_KEEP_LAST_STATUS_RECORDS

                    if total <= keep_count:
                        self.logger.info(
                            f"   ℹ️ {table_name}: записей меньше лимита ({total} ≤ {keep_count}), ничего не удаляем")
                        return 0

                    # Находим ID последней записи, которую нужно оставить
                    # (сортировка по убыванию ID, пропускаем keep_count записей)
                    await cursor.execute(f"""
                        SELECT id FROM {table_name} 
                        ORDER BY id DESC 
                        LIMIT 1 OFFSET %s
                    """, (keep_count - 1,))

                    result = await cursor.fetchone()
                    if not result:
                        return 0

                    keep_up_to_id = result[0]

                    # Удаляем все записи с ID меньше этого (старые записи)
                    await cursor.execute(f"DELETE FROM {table_name} WHERE id < %s", (keep_up_to_id,))

                    deleted = cursor.rowcount
                    self.logger.info(
                        f"   ✅ {table_name}: удалено {deleted} старых записей, оставлено {keep_count} последних (ID >= {keep_up_to_id})")
                    return deleted

        except Exception as e:
            self.logger.error(f"   ❌ Ошибка при очистке таблицы статуса {table_name}: {e}")
            return 0

    def _get_date_field(self, table_name: str) -> Optional[str]:
        """
        Определяет, какое поле с датой использовать для каждой таблицы.
        """
        # Словарь соответствия таблиц и полей с датой
        date_fields = {
            # Таблицы Wildberries
            Config.WB_PRICE_HISTORY_TABLE: 'created_at',
            Config.WB_CYCLE_INFO_TABLE: 'start_time',
            Config.WB_SYSTEM_STATUS_TABLE: 'updated_at',

            # Таблицы Ozon
            Config.OZON_PRICE_HISTORY_TABLE: 'created_at',
            Config.OZON_CYCLE_INFO_TABLE: 'start_time',
            Config.OZON_SYSTEM_STATUS_TABLE: 'updated_at',

            # Таблица логов
            Config.LOGS_TABLE: 'created_at',
        }

        return date_fields.get(table_name)