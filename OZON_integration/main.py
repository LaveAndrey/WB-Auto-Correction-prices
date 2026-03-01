#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Точка входа в систему Ozon Price Updater
"""

import asyncio
import os
import sys

from OZON_integration.core.price_updater import OzonPriceUpdater


async def main():
    """Точка входа в программу"""
    updater = OzonPriceUpdater()

    try:
        await updater.initialize()
        await updater.run()
    except Exception as e:
        updater.logger.critical(f"❌ ФАТАЛЬНАЯ ОШИБКА: {e}")
        import traceback
        updater.logger.critical(traceback.format_exc())
        sys.exit(1)
    finally:
        await updater.cleanup()


if __name__ == "__main__":
    # Проверка обязательных переменных окружения
    required_vars = [
        'DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME',
        'OZON_CLIENT_ID', 'OZON_API_KEY',
        'CDEK_CLIENT_ID', 'CDEK_CLIENT_SECRET'
    ]

    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        print(f"❌ ОШИБКА: Не установлены переменные окружения: {', '.join(missing)}")
        print("\nПример .env файла для Ozon:")
        sys.exit(1)

    # Запуск системы
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Программа остановлена пользователем")
        sys.exit(0)