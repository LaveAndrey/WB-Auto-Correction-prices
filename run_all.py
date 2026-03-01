#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Главный файл для управления запуском всех систем (Wildberries и Ozon).
Запускает оркестраторы для WB и Ozon одновременно с защитой от конфликтов.
Добавлена автоматическая очистка БД по интервалу из конфига.
Добавлена фоновая синхронизация статуса акций Ozon.
"""

import asyncio
import logging
import sys
import traceback
from datetime import datetime, timedelta
from typing import Optional

# Импорты модулей Wildberries и Ozon
from WB_integration.insert_wb import NoSalesPriceUpdater
from OZON_integration.ozon_update_no_sales import OzonNoSalesPriceUpdater
from OZON_integration.core.price_updater import OzonPriceUpdater
from OZON_integration.api.promotions_client import OzonPromotionManager
from WB_integration.core.price_updater import PriceUpdater

from config import Config
from db_cleanup import DatabaseCleanup

# Импорт для работы с БД в фоновых задачах
import aiomysql

# Настройка формата логов с именем модуля
LOG_FORMAT = '%(asctime)s | %(levelname)-7s | %(name)-20s | %(message)s'

# Настройка логгера для главного оркестратора
logger = logging.getLogger("main_orchestrator")
logger.setLevel(getattr(logging, Config.LOG_LEVEL))
logger.handlers.clear()
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter(LOG_FORMAT))
logger.addHandler(handler)
logger.propagate = False


class CustomLoggerAdapter(logging.LoggerAdapter):
    """Адаптер для добавления контекста в логи модулей"""

    def __init__(self, logger, module_name):
        super().__init__(logger, {})
        self.module_name = module_name

    def process(self, msg, kwargs):
        # Добавляем имя модуля в начало сообщения
        return f"[{self.module_name}] {msg}", kwargs


class WBSalesModule:
    """Обёртка для модуля с продажами Wildberries"""

    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.name = "WB_WithSales"
        self.display_name = "WB.SALES"
        self.running = False
        self.logger = CustomLoggerAdapter(logger, self.display_name)
        self.updater = None   # будет создан в start()

    async def start(self):
        """Инициализация и установка статуса"""
        self.updater = PriceUpdater()
        await self.updater.initialize()
        # Устанавливаем глобальный статус True
        self.updater.sync_update_system_running_status(True)
        self.running = True
        self.logger.info("✅ Модуль инициализирован, статус RUNNING")

    async def run_cycle(self):
        """Выполнить один цикл обработки"""
        if not self.updater:
            raise RuntimeError("Module not started")
        await self.updater.run_cycle()

    async def stop(self):
        if self.updater:
            try:
                # Защищаем от отмены – статус обязательно обновится
                await asyncio.shield(self.updater.update_system_running_status(False))
            except asyncio.CancelledError:
                self.logger.warning("⚠️ Обновление статуса было отменено, но shield обычно предотвращает это")
            await self.updater.cleanup()
            self.updater = None
        self.running = False
        self.logger.info("🛑 Модуль остановлен, статус STOPPED")
        sys.stdout.flush()


class WBNoSalesModule:
    """Обёртка для модуля без продаж Wildberries"""

    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.name = "WB_NoSales"
        self.display_name = "WB.NOSALES"
        self.running = False
        self.logger = CustomLoggerAdapter(logger, self.display_name)
        self.updater = None

    async def start(self):
        """Инициализация и установка статуса"""
        self.updater = NoSalesPriceUpdater()
        self.running = True
        self.logger.info("✅ Модуль инициализирован")

    async def run_cycle(self):
        """Выполнить один цикл обработки"""
        if not self.updater:
            raise RuntimeError("Module not started")
        await self.updater.process_cycle()

    async def stop(self):
        """Остановка модуля и сброс статуса"""
        if self.updater:
            # Если есть cleanup - вызываем
            # await self.updater.cleanup()
            self.updater = None
        self.running = False
        self.logger.info("🛑 Модуль остановлен")


class OzonSalesModule:
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.name = "Ozon_WithSales"
        self.display_name = "OZON.SALES"
        self.running = False
        self.logger = CustomLoggerAdapter(logger, self.display_name)
        self.updater = None

    async def start(self):
        """Инициализация и установка статуса"""
        self.updater = OzonPriceUpdater()
        await self.updater.initialize()
        self.updater.sync_update_system_running_status(True)
        self.running = True
        self.logger.info("✅ Модуль инициализирован, статус RUNNING")

    async def run_cycle(self):
        """Выполнить один цикл обработки"""
        if not self.updater:
            raise RuntimeError("Module not started")
        await self.updater.run_cycle()

    async def stop(self):
        if self.updater:
            try:
                await asyncio.shield(self.updater.update_system_running_status(False))
            except asyncio.CancelledError:
                self.logger.warning("⚠️ Обновление статуса было отменено")
            await self.updater.cleanup()
            self.updater = None
        self.running = False
        self.logger.info("🛑 Модуль остановлен, статус STOPPED")
        sys.stdout.flush()


class OzonNoSalesModule:
    """Обёртка для модуля без продаж Ozon"""

    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.name = "Ozon_NoSales"
        self.display_name = "OZON.NOSALES"
        self.running = False
        self.logger = CustomLoggerAdapter(logger, self.display_name)
        self.updater = None

    async def start(self):
        """Инициализация и установка статуса"""
        self.updater = OzonNoSalesPriceUpdater()
        await self.updater.initialize()
        self.running = True
        self.logger.info("✅ Модуль инициализирован")

    async def run_cycle(self):
        """Выполнить один цикл обработки"""
        if not self.updater:
            raise RuntimeError("Module not started")
        await self.updater.process_cycle()

    async def stop(self):
        """Остановка модуля и сброс статуса"""
        if self.updater:
            await self.updater.close()
            self.updater = None
        self.running = False
        self.logger.info("🛑 Модуль остановлен")


class ModuleOrchestrator:
    """
    Оркестратор для одного типа модулей (с продажами или без продаж)
    Управляет расписанием и защитой от конфликтов
    """

    def __init__(self, name: str, module_class, interval: int,
                 market: str, module_type: str, parent):
        self.name = name
        self.module_class = module_class
        self.interval = interval
        self.market = market
        self.module_type = module_type
        self.parent = parent
        self.is_running = False
        self.current_task: Optional[asyncio.Task] = None
        self.last_run = datetime.min
        self.module_instance = module_class(self)
        self.wb_promotion_sync_task = None

        # Определяем отображаемое имя для логов
        if market == "WB":
            market_display = "WB"
        else:
            market_display = "OZON"

        if module_type == "with_sales":
            type_display = "SALES"
        else:
            type_display = "NOSALES"

        self.display_name = f"{market_display}.{type_display}"
        self.logger = CustomLoggerAdapter(logger, self.display_name)

    async def _check_can_run(self) -> bool:
        """
        Проверяет, можно ли запустить модуль сейчас
        Учитывает:
        1. Не запущен ли уже этот модуль
        2. Не запущен ли модуль с продажами (для модулей без продаж)
        3. Глобальный лок выполнения
        """
        if self.is_running:
            self.logger.debug(f"⏳ {self.name} уже выполняется, пропускаем")
            return False

        if self.module_type == 'no_sales':
            sales_modules = self.parent.get_sales_modules(self.market)
            for module in sales_modules:
                if module.is_running:
                    self.logger.info(f"⏳ Модуль с продажами {self.market} выполняется, откладываем запуск")
                    return False

        async with self.parent.global_lock:
            if self.is_running:
                return False
            return True

    async def _run_with_lock(self):
        """Запуск модуля с блокировкой"""
        async with self.parent.global_lock:
            try:
                self.is_running = True
                self.last_run = datetime.now()

                self.logger.info("=" * 80)
                self.logger.info(f"ЗАПУСК ЦИКЛА {self.name}".center(80))
                self.logger.info("=" * 80)

                await self.module_instance.run_cycle()

                self.logger.info(f"✅ Цикл {self.name} успешно завершён")
                self.logger.info("=" * 80)

            except Exception as e:
                self.logger.error(f"❌ Ошибка в цикле {self.name}: {e}")
                self.logger.error(traceback.format_exc())
            finally:
                self.is_running = False

    async def run_cycle(self):
        """Выполнение одного цикла с проверками"""
        if not await self._check_can_run():
            return

        if self.module_type == 'no_sales':
            now = datetime.now()
            if now.minute == 0 and now.second < 30:
                delay = 10 + (hash(f"{self.market}{now}") % 20)
                self.logger.info(f"🔄 Начало часа, добавляем задержку {delay}сек")
                await asyncio.sleep(delay)

        await self._run_with_lock()

    async def run_forever(self):
        """Бесконечный цикл запуска по расписанию"""
        self.logger.info(f"🚀 Запущен вечный цикл для {self.name}")

        await self.module_instance.start()

        try:
            while self.parent.is_running:
                try:
                    await self.run_cycle()
                    next_run = datetime.now() + timedelta(seconds=self.interval)
                    self.logger.info(f"⏱️ Следующий запуск {self.name} через: {self.interval // 60} мин")

                    for _ in range(self.interval):
                        if not self.parent.is_running:
                            break
                        await asyncio.sleep(1)
                except asyncio.CancelledError:
                    self.logger.info(f"🛑 {self.name} получил CancelledError")
                    break
                except Exception as e:
                    self.logger.error(f"❌ Ошибка в цикле {self.name}: {e}")
                    await asyncio.sleep(300)
        finally:
            self.logger.info(f"🔄 finally блок run_forever для {self.name}")
            sys.stdout.flush()
            try:
                await self.module_instance.stop()
                self.logger.info(f"✅ Модуль {self.name} остановлен в finally")
                sys.stdout.flush()
            except Exception as e:
                self.logger.error(f"❌ Ошибка в finally {self.name}: {e}")
                sys.stdout.flush()


class MainOrchestrator:
    """
    Главный оркестратор, управляющий всеми модулями
    """

    def __init__(self):
        self.is_running = False
        self.global_lock = asyncio.Lock()
        self.modules: list[ModuleOrchestrator] = []
        self.tasks: list[asyncio.Task] = []

        # Для очистки БД
        self.db_cleaner = None
        self.db_cleanup_pool = None
        self.db_cleanup_task = None

        # Для синхронизации статуса акций Ozon
        self.promotion_sync_task = None

        logger.info("=" * 80)
        logger.info("ИНИЦИАЛИЗАЦИЯ ГЛАВНОГО ОРКЕСТРАТОРА".center(80))
        logger.info("=" * 80)

        self._init_modules()
        self._log_config()
        logger.info("=" * 80)

    def _init_modules(self):
        """Инициализация всех модулей"""
        if Config.WB_WITH_SALES_ENABLED:
            self.modules.append(ModuleOrchestrator(
                name="WB_WithSales",
                module_class=WBSalesModule,
                interval=Config.WB_INTERVAL,
                market="WB",
                module_type="with_sales",
                parent=self
            ))

        if Config.WB_NO_SALES_ENABLED:
            self.modules.append(ModuleOrchestrator(
                name="WB_NoSales",
                module_class=WBNoSalesModule,
                interval=Config.WB_NO_SALES_INTERVAL,
                market="WB",
                module_type="no_sales",
                parent=self
            ))

        if Config.OZON_WITH_SALES_ENABLED:
            self.modules.append(ModuleOrchestrator(
                name="Ozon_WithSales",
                module_class=OzonSalesModule,
                interval=Config.OZON_CYCLE_INTERVAL,
                market="Ozon",
                module_type="with_sales",
                parent=self
            ))

        if Config.OZON_NO_SALES_ENABLED:
            self.modules.append(ModuleOrchestrator(
                name="Ozon_NoSales",
                module_class=OzonNoSalesModule,
                interval=Config.OZON_NO_SALES_INTERVAL,
                market="Ozon",
                module_type="no_sales",
                parent=self
            ))

    def get_sales_modules(self, market: str) -> list[ModuleOrchestrator]:
        return [
            m for m in self.modules
            if m.market == market and m.module_type == 'with_sales'
        ]

    def _log_config(self):
        logger.info("📊 КОНФИГУРАЦИЯ МОДУЛЕЙ:")

        wb_sales = next((m for m in self.modules if m.name == "WB_WithSales"), None)
        wb_nosales = next((m for m in self.modules if m.name == "WB_NoSales"), None)
        ozon_sales = next((m for m in self.modules if m.name == "Ozon_WithSales"), None)
        ozon_nosales = next((m for m in self.modules if m.name == "Ozon_NoSales"), None)

        logger.info("")
        logger.info("   🟦 WILDBERRIES:")
        if wb_sales:
            logger.info(f"      • С продажами: КАЖДЫЙ ЧАС ({wb_sales.interval // 3600}ч)")
        else:
            logger.info("      • С продажами: ОТКЛЮЧЕН")

        if wb_nosales:
            logger.info(f"      • Без продаж:   КАЖДЫЕ {wb_nosales.interval // 60} МИН")
        else:
            logger.info("      • Без продаж:   ОТКЛЮЧЕН")

        logger.info("")
        logger.info("   🟢 OZON:")
        if ozon_sales:
            logger.info(f"      • С продажами: КАЖДЫЙ ЧАС ({ozon_sales.interval // 3600}ч)")
        else:
            logger.info("      • С продажами: ОТКЛЮЧЕН")

        if ozon_nosales:
            logger.info(f"      • Без продаж:   КАЖДЫЕ {ozon_nosales.interval // 60} МИН")
        else:
            logger.info("      • Без продаж:   ОТКЛЮЧЕН")

        logger.info("")
        logger.info("   🧹 ОЧИСТКА БАЗЫ ДАННЫХ:")
        if Config.DB_AUTO_CLEANUP_ENABLED:
            logger.info(f"      • Статус: ВКЛЮЧЕНА")
            logger.info(f"      • Интервал: КАЖДЫЕ {Config.DB_CLEANUP_INTERVAL_DAYS} ДНЯ")
            logger.info(f"      • Время: {Config.DB_CLEANUP_HOUR}:00 (проверка каждый час)")
            logger.info(f"      • Таблицы: {', '.join(Config.DB_CLEANUP_TABLES)}")
        else:
            logger.info("      • Статус: ОТКЛЮЧЕНА")

        logger.info("")
        logger.info("   🔄 СИНХРОНИЗАЦИЯ СТАТУСА АКЦИЙ OZON:")
        if Config.PROMOTION_STATUS_SYNC_ENABLED:
            logger.info(f"      • Статус: ВКЛЮЧЕНА")
            logger.info(f"      • Интервал: КАЖДЫЕ {Config.PROMOTION_STATUS_SYNC_INTERVAL // 3600} ЧАС")
        else:
            logger.info("      • Статус: ОТКЛЮЧЕНА")

        logger.info("")
        logger.info("   🔄 СИНХРОНИЗАЦИЯ АКЦИЙ WILDBERRIES:")
        if Config.WB_PROMOTION_SYNC_ENABLED:
            logger.info(f"      • Статус: ВКЛЮЧЕНА")
            logger.info(f"      • Интервал: КАЖДЫЕ {Config.WB_PROMOTION_SYNC_INTERVAL // 3600} ЧАС")
        else:
            logger.info("      • Статус: ОТКЛЮЧЕНА")

    async def _init_db_cleaner(self):
        try:
            self.db_cleanup_pool = await aiomysql.create_pool(
                host=Config.DB_HOST,
                user=Config.DB_USER,
                password=Config.DB_PASSWORD,
                db=Config.DB_NAME,
                port=Config.DB_PORT,
                autocommit=True,
                charset='utf8mb4',
                minsize=1,
                maxsize=2
            )
            self.db_cleaner = DatabaseCleanup(self.db_cleanup_pool)
            logger.info("✅ Модуль очистки БД инициализирован")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации очистки БД: {e}")
            return False

    async def _db_cleanup_worker(self):
        if not Config.DB_AUTO_CLEANUP_ENABLED:
            logger.info("ℹ️ Автоочистка БД отключена в конфигурации")
            return

        logger.info("🧹 Запущен фоновый рабочий очистки БД (проверка каждый час)")

        try:
            while self.is_running:
                try:
                    results = await self.db_cleaner.check_and_cleanup()
                    if results:
                        total = sum(v for v in results.values() if v > 0)
                        logger.info(f"📊 Очистка БД завершена. Удалено записей: {total}")
                except Exception as e:
                    logger.error(f"❌ Ошибка в фоновой очистке БД: {e}")

                for _ in range(3600):
                    if not self.is_running:
                        break
                    await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info("🛑 Фоновая задача очистки БД остановлена")
        finally:
            if self.db_cleanup_pool:
                self.db_cleanup_pool.close()
                await self.db_cleanup_pool.wait_closed()
                logger.info("✅ Подключение к БД для очистки закрыто")

    async def _wb_promotion_sync_worker(self):
        """
        Фоновая задача для синхронизации акций Wildberries.
        Запускается с интервалом WB_PROMOTION_SYNC_INTERVAL.
        """
        if not Config.WB_PROMOTION_SYNC_ENABLED:
            logger.info("ℹ️ Синхронизация акций Wildberries отключена в конфигурации")
            return

        logger.info("🔄 Запущен фоновый рабочий синхронизации акций Wildberries")

        # Создаём отдельный пул для этой задачи
        import aiomysql
        pool = await aiomysql.create_pool(
            host=Config.DB_HOST,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            db=Config.DB_NAME,
            port=Config.DB_PORT,
            autocommit=True,
            charset='utf8mb4',
            minsize=1,
            maxsize=2
        )
        try:
            # Создаём экземпляр менеджера акций WB
            from WB_integration.api.promotions_client_wb import WBPromotionManager
            import aiohttp
            session = aiohttp.ClientSession()
            promo_manager = WBPromotionManager(session, pool, logger)

            while self.is_running:
                try:
                    logger.info("🔄 Запуск синхронизации акций Wildberries...")
                    await promo_manager.sync_promotions_from_wb()
                    logger.info("✅ Акции Wildberries синхронизированы")
                except Exception as e:
                    logger.error(f"❌ Ошибка синхронизации акций Wildberries: {e}")
                    logger.error(traceback.format_exc())

                # Ожидание интервала с проверкой флага is_running
                for _ in range(Config.WB_PROMOTION_SYNC_INTERVAL):
                    if not self.is_running:
                        break
                    await asyncio.sleep(1)
        finally:
            await session.close()
            pool.close()
            await pool.wait_closed()
            logger.info("✅ Подключение к БД для синхронизации акций WB закрыто")

    async def _promotion_status_sync_worker(self):
        """
        Фоновая задача для синхронизации статуса акций Ozon.
        Запускается с интервалом PROMOTION_STATUS_SYNC_INTERVAL.
        """
        if not Config.PROMOTION_STATUS_SYNC_ENABLED:
            logger.info("ℹ️ Синхронизация статуса акций Ozon отключена в конфигурации")
            return

        logger.info("🔄 Запущен фоновый рабочий синхронизации статуса акций Ozon")

        # Создаём отдельный пул для этой задачи
        import aiomysql
        pool = await aiomysql.create_pool(
            host=Config.DB_HOST,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            db=Config.DB_NAME,
            port=Config.DB_PORT,
            autocommit=True,
            charset='utf8mb4',
            minsize=1,
            maxsize=2
        )
        try:
            # Создаём экземпляр менеджера акций (ему нужен только пул и логгер)
            promo_manager = OzonPromotionManager(pool, logger)

            while self.is_running:
                try:
                    logger.info("🔄 Запуск синхронизации статуса акций Ozon...")
                    updated = await promo_manager.sync_promotion_status()
                    logger.info(f"✅ Статус акций Ozon синхронизирован, обновлено {updated} товаров")
                except Exception as e:
                    logger.error(f"❌ Ошибка синхронизации статуса акций: {e}")
                    logger.error(traceback.format_exc())

                # Ожидание интервала с проверкой флага is_running
                for _ in range(Config.PROMOTION_STATUS_SYNC_INTERVAL):
                    if not self.is_running:
                        break
                    await asyncio.sleep(1)
        finally:
            pool.close()
            await pool.wait_closed()
            logger.info("✅ Подключение к БД для синхронизации акций закрыто")

    async def run(self):
        """Запуск всех модулей"""
        self.is_running = True

        logger.info("=" * 80)
        logger.info("ЗАПУСК ГЛАВНОГО ОРКЕСТРАТОРА".center(80))
        logger.info("=" * 80)

        try:
            # Запуск очистки БД по расписанию
            if Config.DB_AUTO_CLEANUP_ENABLED:
                if await self._init_db_cleaner():
                    self.db_cleanup_task = asyncio.create_task(
                        self._db_cleanup_worker(),
                        name="DB_Cleanup"
                    )
                    logger.info("🚀 Запущена фоновая задача очистки БД (проверка каждый час)")
                    await asyncio.sleep(1)

            if Config.PROMOTIONS_ENABLED:
                # Запуск синхронизации статуса акций Ozon
                if Config.PROMOTION_STATUS_SYNC_ENABLED:
                    self.promotion_sync_task = asyncio.create_task(
                        self._promotion_status_sync_worker(),
                        name="PromotionStatusSync"
                    )
                    logger.info("🚀 Запущена фоновая задача синхронизации статуса акций Ozon")
                    await asyncio.sleep(1)

                if Config.WB_PROMOTION_SYNC_ENABLED:
                    self.wb_promotion_sync_task = asyncio.create_task(
                        self._wb_promotion_sync_worker(),
                        name="WBPromotionSync"
                    )
                    logger.info("🚀 Запущена фоновая задача синхронизации акций Wildberries")
                    await asyncio.sleep(1)

            # Запуск всех модулей
            for module in self.modules:
                logger.info(f"🚀 Запуск модуля: {module.name}")
                task = asyncio.create_task(
                    module.run_forever(),
                    name=module.name
                )
                self.tasks.append(task)
                await asyncio.sleep(2)

            if not self.tasks:
                logger.warning("⚠️ Нет активных модулей для запуска!")
                return

            logger.info(f"✅ Запущено модулей: {len(self.tasks)}")
            if self.db_cleanup_task:
                logger.info(f"✅ Запущена фоновая очистка БД")
            if self.promotion_sync_task:
                logger.info(f"✅ Запущена фоновая синхронизация акций")

            # Ожидаем завершения задач
            await asyncio.gather(*self.tasks)

        except asyncio.CancelledError:
            logger.info("🛑 Главный оркестратор остановлен")
        except Exception as e:
            logger.error(f"❌ Критическая ошибка: {e}")
            logger.error(traceback.format_exc())
        finally:
            await self.cleanup()

    async def cleanup(self):
        logger.info("🧹 Очистка ресурсов...")
        self.is_running = False
        sys.stdout.flush()

        # Остановка модулей
        for module in self.modules:
            try:
                await asyncio.shield(module.module_instance.stop())
                logger.info(f"✅ Модуль {module.name} остановлен")
                sys.stdout.flush()
            except asyncio.CancelledError:
                logger.warning(f"⚠️ Остановка модуля {module.name} была прервана")
            except Exception as e:
                logger.error(f"❌ Ошибка остановки {module.name}: {e}")

        # Отмена задач модулей
        for task in self.tasks:
            if not task.done():
                task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)

        # Отмена фоновых задач
        if self.db_cleanup_task and not self.db_cleanup_task.done():
            self.db_cleanup_task.cancel()
            try:
                await self.db_cleanup_task
            except asyncio.CancelledError:
                pass

        if self.promotion_sync_task and not self.promotion_sync_task.done():
            self.promotion_sync_task.cancel()
            try:
                await self.promotion_sync_task
            except asyncio.CancelledError:
                pass

        if self.wb_promotion_sync_task and not self.wb_promotion_sync_task.done():
            self.wb_promotion_sync_task.cancel()
            try:
                await self.wb_promotion_sync_task
            except asyncio.CancelledError:
                pass

        # Закрытие пулов БД
        if self.db_cleanup_pool and not self.db_cleanup_pool.closed:
            self.db_cleanup_pool.close()
            await self.db_cleanup_pool.wait_closed()
            logger.info("✅ Подключение к БД для очистки закрыто")

        logger.info("✅ Очистка завершена")
        sys.stdout.flush()


async def main():
    """Точка входа"""
    orchestrator = MainOrchestrator()
    try:
        await orchestrator.run()
    except KeyboardInterrupt:
        logger.info("🛑 Получен сигнал KeyboardInterrupt")
        await orchestrator.cleanup()
    except Exception as e:
        logger.error(f"❌ Необработанная ошибка: {e}")
        logger.error(traceback.format_exc())
        await orchestrator.cleanup()
    finally:
        if orchestrator.is_running:
            await asyncio.shield(orchestrator.cleanup())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Программа остановлена пользователем")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        logger.error(traceback.format_exc())