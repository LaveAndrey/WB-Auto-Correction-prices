#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Оркестратор для управления запуском скриптов Ozon.
Запускает price_updater.py (с продажами) каждый час,
и ozon_update_no_sales.py (без продаж) каждые 15 минут с защитой от одновременного запуска.
"""

import asyncio
import logging
import sys
import traceback
from datetime import datetime, timedelta
from typing import Optional

from config import Config

# Настройка логирования
logger = logging.getLogger("ozon_orchestrator")
logger.setLevel(getattr(logging, Config.LOG_LEVEL))
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter(Config.LOG_FORMAT))
logger.addHandler(handler)
logger.propagate = False


class OzonOrchestrator:
    """Оркестратор для управления запуском скриптов Ozon"""

    def __init__(self):
        self.is_running = False
        self.with_sales_task: Optional[asyncio.Task] = None
        self.no_sales_task: Optional[asyncio.Task] = None

        # Интервалы запуска из конфига
        self.with_sales_interval = Config.OZON_CYCLE_INTERVAL
        self.no_sales_interval = Config.OZON_NO_SALES_INTERVAL

        # Флаги включения модулей
        self.with_sales_enabled = Config.OZON_WITH_SALES_ENABLED
        self.no_sales_enabled = Config.OZON_NO_SALES_ENABLED

        # Флаг для контроля выполнения модуля с продажами
        self.with_sales_running = False

        # Блокировка для гарантии, что модули не выполняются одновременно
        self.execution_lock = asyncio.Lock()

        logger.info("=" * 80)
        logger.info("ИНИЦИАЛИЗАЦИЯ ОРКЕСТРАТОРА OZON".center(80))
        logger.info("=" * 80)
        logger.info(f"📊 Модуль с продажами (OzonPriceUpdater): {'ВКЛЮЧЕН' if self.with_sales_enabled else 'ОТКЛЮЧЕН'}")
        logger.info(
            f"   Интервал запуска: {self.with_sales_interval // 3600}ч {(self.with_sales_interval % 3600) // 60}м")
        logger.info(f"📦 Модуль без продаж (ozon_update_no_sales): {'ВКЛЮЧЕН' if self.no_sales_enabled else 'ОТКЛЮЧЕН'}")
        logger.info(f"   Интервал запуска: {self.no_sales_interval // 60} мин")
        logger.info("=" * 80)

    async def run_with_sales(self):
        """Запуск OzonPriceUpdater (с продажами)"""
        if self.with_sales_running:
            logger.warning("⚠️ Модуль с продажами уже запущен, пропускаем")
            return

        async with self.execution_lock:  # Блокируем выполнение других модулей
            self.with_sales_running = True
            try:
                logger.info("-" * 80)
                logger.info("ЗАПУСК МОДУЛЯ С ПРОДАЖАМИ (OzonPriceUpdater)".center(80))
                logger.info("-" * 80)

                from OZON_integration.core.price_updater import OzonPriceUpdater

                updater = OzonPriceUpdater()
                await updater.initialize()

                # Запускаем с таймаутом
                try:
                    await asyncio.wait_for(updater.run_cycle(), timeout=3600)
                except asyncio.TimeoutError:
                    logger.error("❌ Цикл с продажами превысил таймаут 1 час")
                finally:
                    await updater.cleanup()

                logger.info("✅ Модуль с продажами успешно завершил работу")

            except Exception as e:
                logger.error(f"❌ Ошибка в модуле с продажами: {e}")
                logger.error(traceback.format_exc())
            finally:
                self.with_sales_running = False

    async def run_no_sales(self):
        """Запуск ozon_update_no_sales.py (без продаж) с защитой от одновременного запуска"""

        # Проверка 1: Не запущен ли модуль с продажами прямо сейчас
        if self.with_sales_running:
            logger.info("⏳ Модуль с продажами выполняется, откладываем запуск без продаж")

            # Ждем завершения модуля с продажами (максимум 30 минут)
            wait_start = datetime.now()
            while self.with_sales_running:
                elapsed = (datetime.now() - wait_start).total_seconds()
                if elapsed > 1800:  # 30 минут
                    logger.warning("⚠️ Таймаут ожидания модуля с продажами (30 мин), принудительный запуск")
                    break
                await asyncio.sleep(5)

        # Проверка 2: Если сейчас начало часа (00 минут) - возможна коллизия
        now = datetime.now()
        if now.minute == 0 and now.second < 30:
            logger.info("🔄 Обнаружено начало часа, добавляем случайную задержку 10-30 сек для избежания коллизий")
            delay = 10 + (hash(str(now)) % 20)  # Случайная задержка 10-30 сек
            await asyncio.sleep(delay)

        # Основной запуск с блокировкой
        async with self.execution_lock:
            try:
                logger.info("-" * 80)
                logger.info("ЗАПУСК МОДУЛЯ БЕЗ ПРОДАЖ (ozon_update_no_sales)".center(80))
                logger.info("-" * 80)

                from ozon_update_no_sales import OzonNoSalesPriceUpdater

                updater = OzonNoSalesPriceUpdater()
                await updater.initialize()

                try:
                    await asyncio.wait_for(updater.process_cycle(), timeout=1800)
                except asyncio.TimeoutError:
                    logger.error("❌ Цикл без продаж превысил таймаут 30 минут")

                logger.info("✅ Модуль без продаж успешно завершил работу")

            except Exception as e:
                logger.error(f"❌ Ошибка в модуле без продаж: {e}")
                logger.error(traceback.format_exc())
            finally:
                await updater.close()

    async def with_sales_loop(self):
        """Цикл запуска модуля с продажами"""
        if not self.with_sales_enabled:
            logger.info("ℹ️ Модуль с продажами отключен в конфигурации")
            return

        while self.is_running:
            try:
                await self.run_with_sales()

                next_run = datetime.now() + timedelta(seconds=self.with_sales_interval)

                logger.info("=" * 80)
                logger.info("ОЖИДАНИЕ СЛЕДУЮЩЕГО ЦИКЛА".center(80))
                logger.info(f"🕒 Следующий запуск модуля с продажами через: "
                            f"{self.with_sales_interval // 3600}ч {(self.with_sales_interval % 3600) // 60}м")
                logger.info(f"📅 Время следующего запуска: {next_run.strftime('%H:%M:%S')}")
                logger.info("=" * 80)

                # Поэтапное ожидание с проверкой флага
                for _ in range(self.with_sales_interval):
                    if not self.is_running:
                        break
                    await asyncio.sleep(1)

            except asyncio.CancelledError:
                logger.info("🛑 Цикл с продажами остановлен")
                break
            except Exception as e:
                logger.error(f"❌ Ошибка в цикле с продажами: {e}")
                logger.error(traceback.format_exc())
                await asyncio.sleep(300)

    async def no_sales_loop(self):
        """Цикл запуска модуля без продаж с защитой от коллизий"""
        if not self.no_sales_enabled:
            logger.info("ℹ️ Модуль без продаж отключен в конфигурации")
            return

        # Даем фору модулю с продажами при первом запуске
        logger.info("⏳ Ожидание 60 секунд перед первым запуском модуля без продаж...")
        await asyncio.sleep(60)

        while self.is_running:
            try:
                await self.run_no_sales()

                next_run = datetime.now() + timedelta(seconds=self.no_sales_interval)

                logger.info(f"⏱️ Следующий запуск модуля без продаж через: {self.no_sales_interval // 60} мин")
                logger.info(f"📅 Время следующего запуска: {next_run.strftime('%H:%M:%S')}")

                # Поэтапное ожидание с проверкой
                for _ in range(self.no_sales_interval):
                    if not self.is_running:
                        break
                    await asyncio.sleep(1)

            except asyncio.CancelledError:
                logger.info("🛑 Цикл без продаж остановлен")
                break
            except Exception as e:
                logger.error(f"❌ Ошибка в цикле без продаж: {e}")
                logger.error(traceback.format_exc())
                await asyncio.sleep(300)

    async def run(self):
        """Основной метод запуска оркестратора"""
        self.is_running = True

        logger.info("=" * 80)
        logger.info("ЗАПУСК ОРКЕСТРАТОРА OZON".center(80))
        logger.info("=" * 80)

        try:
            tasks = []

            if self.with_sales_enabled:
                self.with_sales_task = asyncio.create_task(
                    self.with_sales_loop(),
                    name="with_sales_loop"
                )
                tasks.append(self.with_sales_task)

            if self.no_sales_enabled:
                # Небольшая задержка перед запуском цикла без продаж
                await asyncio.sleep(10)
                self.no_sales_task = asyncio.create_task(
                    self.no_sales_loop(),
                    name="no_sales_loop"
                )
                tasks.append(self.no_sales_task)

            if not tasks:
                logger.warning("⚠️ Ни один модуль не включен в конфигурации!")
                return

            await asyncio.gather(*tasks)

        except asyncio.CancelledError:
            logger.info("🛑 Оркестратор остановлен")
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в оркестраторе: {e}")
            logger.error(traceback.format_exc())
        finally:
            await self.cleanup()

    async def cleanup(self):
        """Очистка ресурсов при завершении"""
        logger.info("🧹 Очистка ресурсов...")
        self.is_running = False

        for task in [self.with_sales_task, self.no_sales_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        logger.info("✅ Очистка завершена")
        logger.info("=" * 80)
        logger.info("ОРКЕСТРАТОР ОСТАНОВЛЕН".center(80))
        logger.info("=" * 80)


async def main():
    """Точка входа"""
    orchestrator = OzonOrchestrator()

    try:
        await orchestrator.run()
    except KeyboardInterrupt:
        logger.info("\n🛑 Получен сигнал KeyboardInterrupt")
    except Exception as e:
        logger.error(f"❌ Необработанная ошибка: {e}")
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Программа остановлена пользователем")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        logger.error(traceback.format_exc())