import asyncio
import sys
from core.price_updater import PriceUpdater
from config import Config


async def main():
    updater = PriceUpdater()

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

    missing = []
    if not Config.WB_SALES_TOKEN:
        missing.append("WB_SALES_TOKEN")
    if not Config.WB_PRICES_TOKEN:
        missing.append("WB_PRICES_TOKEN")
    if not Config.WB_CONTENT_TOKEN:
        missing.append("WB_CONTENT_TOKEN")
    if missing:
        print(f"❌ ОШИБКА: Не установлены переменные: {', '.join(missing)}")
        sys.exit(1)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Программа остановлена пользователем")
        sys.exit(0)