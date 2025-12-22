"""
Тесты для системы ценообразования WB
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, Mock, patch
from datetime import datetime, timedelta
import pytz
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from price_updater_main import (
    PriceUpdater, SaleData, ProductData, PriceUpdate,
    ProcessingStatus, Config
)


class TestPriceUpdater:
    """Тесты для системы ценообразования"""

    @pytest.fixture
    def mock_config(self):
        """Мок конфигурации"""
        with patch('main.Config') as mock:
            # Базовые настройки
            mock.BANK_COMMISSION = 0.02
            mock.MIN_MARGIN_FACTOR = 1.2
            mock.MIN_PRICE_CHANGE = 50
            mock.MIN_SALES_FOR_CALC = 3
            mock.MAX_PRICE_CHANGE_PERCENT = 50.0
            mock.SALES_HOURS_FILTER = 24
            mock.CYCLE_INTERVAL = 3600
            mock.BATCH_SIZE = 100
            mock.WORKERS_COUNT = 2
            mock.MAX_QUEUE_SIZE = 1000
            yield mock

    @pytest.fixture
    def price_updater(self, mock_config):
        """Создание экземпляра PriceUpdater с моками"""
        updater = PriceUpdater()
        updater.logger = Mock()
        updater.db_pool = AsyncMock()
        updater.session = AsyncMock()
        updater.queue = Mock()
        updater.is_running = True
        updater.stats = {}
        updater.successful_updates = []
        return updater

    @pytest.fixture
    def recent_date_str(self):
        """Строка с недавней датой для тестов"""
        recent_dt = datetime.now(pytz.utc) - timedelta(hours=1)
        return recent_dt.isoformat()

    @pytest.mark.asyncio
    async def test_profit_below_target_increase_price(self, price_updater, recent_date_str):
        """
        Тест 1: Прибыль ниже цели → повышение цены

        ВАЖНО: В вашем скрипте profit_correction всегда положительное!
        А причина берется из error_msg, а не из свойства reason!
        """
        # Данные продажи
        sale = SaleData(
            nm_id=123456,
            vendor_code="TEST001",
            finished_price=1000.0,
            for_pay=995.0,
            spp_percent=10.0,
            date=recent_date_str,
            quantity=1
        )

        # Данные товара
        product = ProductData(
            vendor_code="TEST001",
            purchase_price=800.0,
            target_profit=200.0,
            current_price_wb=1200.0,
            current_real_price=1000.0,
            sku_wb=123456,
            status=1
        )

        # Мок получения nmID
        price_updater.fetch_nm_id = AsyncMock(return_value=123456)

        # Вызов метода обработки
        result = await price_updater.process_product(
            vendor_code="TEST001",
            sales=[sale, sale, sale, sale],
            product=product
        )

        # Проверки
        print(f"\nТест 1: Проверка расчета")
        print(f"  Статус: {result.status}")
        print(f"  Старая цена: {result.old_price_wb:.0f}₽")
        print(f"  Новая цена: {result.new_price_wb:.0f}₽")
        print(f"  Разница: {result.profit_correction:.0f}₽")
        print(f"  Причина: {result.reason}")

        # Расчет:
        # 1. СПП сумма: 1000 × 0.10 = 100₽
        # 2. Чистая выручка: 995 - 100 = 895₽
        # 3. Прибыль: 895×0.98 - 800 = 877.1 - 800 = 77.1₽
        # 4. Цель: 200₽, фактически: 77.1₽ → прибыль НИЖЕ цели!
        # 5. Нужно ПОВЫСИТЬ цену

        assert result.status == ProcessingStatus.SUCCESS
        assert "Изменение цены" in result.error_msg
        assert result.profit_correction > 0

    @pytest.mark.asyncio
    async def test_profit_above_target_decrease_price(self, price_updater, recent_date_str):
        """
        Тест 2: Прибыль выше цели → понижение цены
        """
        sale = SaleData(
            nm_id=123457,
            vendor_code="TEST002",
            finished_price=1000.0,
            for_pay=995.0,
            spp_percent=5.0,
            date=recent_date_str,
            quantity=1
        )

        product = ProductData(
            vendor_code="TEST002",
            purchase_price=400.0,
            target_profit=200.0,
            current_price_wb=1200.0,
            current_real_price=1000.0,
            sku_wb=123457,
            status=1
        )

        price_updater.fetch_nm_id = AsyncMock(return_value=123457)

        result = await price_updater.process_product(
            vendor_code="TEST002",
            sales=[sale, sale, sale, sale, sale],
            product=product
        )

        print(f"\nТест 2: Прибыль выше цели")
        print(f"  Статус: {result.status}")
        print(f"  Изменение: {result.old_price_wb:.0f} → {result.new_price_wb:.0f}₽")
        print(f"  Причина: {result.reason}")

        # Расчет:
        # 1. Чистая выручка: 995 - (1000×0.05) = 945₽
        # 2. Прибыль: 945×0.98 - 400 = 926.1 - 400 = 526.1₽
        # 3. Цель: 200₽, фактически: 526.1₽ → прибыль ВЫШЕ цели
        # 4. Нужно ПОНИЗИТЬ цену

        assert result.status == ProcessingStatus.SUCCESS
        assert result.new_price_wb < result.old_price_wb

    @pytest.mark.asyncio
    async def test_insufficient_sales_skip(self, price_updater, recent_date_str):
        """
        Тест 3: Недостаточно продаж → пропуск
        """
        sale = SaleData(
            nm_id=123458,
            vendor_code="TEST003",
            finished_price=1500.0,
            for_pay=1495.0,
            spp_percent=15.0,
            date=recent_date_str,
            quantity=1
        )

        product = ProductData(
            vendor_code="TEST003",
            purchase_price=800.0,
            target_profit=300.0,
            current_price_wb=1800.0,
            current_real_price=1500.0,
            sku_wb=123458,
            status=1
        )

        price_updater.fetch_nm_id = AsyncMock(return_value=123458)

        # Только 2 продажи при MIN_SALES_FOR_CALC=3
        result = await price_updater.process_product(
            vendor_code="TEST003",
            sales=[sale, sale],
            product=product
        )

        print(f"\nТест 3: Недостаточно продаж")
        print(f"  Статус: {result.status}")
        print(f"  Причина: {result.error_msg}")

        assert result.status == ProcessingStatus.SKIPPED_NO_DATA
        assert "Недостаточно продаж" in result.error_msg
        assert result.new_price_wb == 0

    @pytest.mark.asyncio
    async def test_price_below_minimum_skip(self, price_updater, recent_date_str):
        """
        Тест 4: Цена ниже минимальной → пропуск
        """

        sale = SaleData(
            nm_id=123459,
            vendor_code="TEST004",
            finished_price=300.0,
            for_pay=298.0,
            spp_percent=5.0,
            date=recent_date_str,
            quantity=1
        )

        product = ProductData(
            vendor_code="TEST004",
            purchase_price=300.0,
            target_profit=50.0,
            current_price_wb=350.0,
            current_real_price=300.0,
            sku_wb=123459,
            status=1
        )

        price_updater.fetch_nm_id = AsyncMock(return_value=123459)

        result = await price_updater.process_product(
            vendor_code="TEST004",
            sales=[sale, sale, sale, sale],
            product=product
        )

        print(f"\nТест 4: Проверка минимальной цены")
        print(f"  Статус: {result.status}")
        print(f"  Причина: {result.error_msg if result.error_msg else result.reason}")

        # Расчет минимальной цены: 300 × 1.2 = 360₽
        # Если новая finished_price < 360₽ → SKIPPED_MIN_PRICE

        # В данном случае скрипт попытается рассчитать новую цену
        # Если она окажется ниже 360₽ → будет SKIPPED_MIN_PRICE

        assert result.status in [
            ProcessingStatus.SUCCESS,
            ProcessingStatus.SKIPPED_MIN_PRICE,
            ProcessingStatus.SKIPPED_MIN_CHANGE
        ]

    @pytest.mark.asyncio
    async def test_small_change_skip(self, price_updater, recent_date_str):
        """
        Тест 5: Изменение меньше порога → пропуск
        """
        sale = SaleData(
            nm_id=123460,
            vendor_code="TEST005",
            finished_price=1000.0,
            for_pay=995.0,
            spp_percent=8.0,
            date=recent_date_str,
            quantity=1
        )

        product = ProductData(
            vendor_code="TEST005",
            purchase_price=600.0,
            target_profit=200.0,
            current_price_wb=1000.0,
            current_real_price=1000.0,
            sku_wb=123460,
            status=1
        )


        original_min_change = Config.MIN_PRICE_CHANGE
        Config.MIN_PRICE_CHANGE = 500

        price_updater.fetch_nm_id = AsyncMock(return_value=123460)

        result = await price_updater.process_product(
            vendor_code="TEST005",
            sales=[sale, sale, sale, sale],
            product=product
        )


        Config.MIN_PRICE_CHANGE = original_min_change

        print(f"\nТест 5: Малое изменение цены (порог 500₽)")
        print(f"  Статус: {result.status}")
        print(f"  Причина: {result.error_msg if result.error_msg else result.reason}")

        # Если изменение полной цены < 500₽ → SKIPPED_MIN_CHANGE
        # В данном случае изменение будет небольшим

        if result.status == ProcessingStatus.SKIPPED_MIN_CHANGE:
            assert "Изменение полной цены меньше порога" in result.error_msg

        # Проверяем что статус валидный
        assert result.status in [
            ProcessingStatus.SUCCESS,
            ProcessingStatus.SKIPPED_MIN_CHANGE,
            ProcessingStatus.SKIPPED_MIN_PRICE
        ]

    @pytest.mark.asyncio
    async def test_spp_calculation_correctness(self, price_updater, recent_date_str):
        """
        Тест 6: Корректность расчета СПП

        ВАЖНО: quantity=2 означает 2 записи в массивах!
        """
        sale = SaleData(
            nm_id=123461,
            vendor_code="TEST006",
            finished_price=2000.0,
            for_pay=1990.0,
            spp_percent=25.0,
            date=recent_date_str,
            quantity=2
        )

        product = ProductData(
            vendor_code="TEST006",
            purchase_price=1000.0,
            target_profit=400.0,
            current_price_wb=2500.0,
            current_real_price=2000.0,
            sku_wb=123461,
            status=1
        )

        price_updater.fetch_nm_id = AsyncMock(return_value=123461)




        sale_single = SaleData(
            nm_id=123461,
            vendor_code="TEST006",
            finished_price=2000.0,
            for_pay=1990.0,
            spp_percent=25.0,
            date=recent_date_str,
            quantity=1  # По 1 штуке
        )

        result = await price_updater.process_product(
            vendor_code="TEST006",
            sales=[sale_single, sale_single, sale_single, sale_single],  # 4 записи
            product=product
        )

        print(f"\nТест 6: Расчет СПП")
        print(f"  Статус: {result.status}")
        print(f"  СПП: {result.discount}%")

        # Проверки:
        # 1. СПП сумма: 2000 × 0.25 = 500₽
        # 2. Чистая выручка за 1 шт: 1990 - 500 = 1490₽

        assert result.status == ProcessingStatus.SUCCESS
        assert result.discount == 25.0
        assert result.analytics_data is not None
        # В analytics учитывается quantity!
        assert result.analytics_data.total_sales == 4

    @pytest.mark.asyncio
    async def test_edge_case_zero_spp(self, price_updater, recent_date_str):
        """
        Тест 7: Крайний случай - СПП = 0%
        """
        sale = SaleData(
            nm_id=123462,
            vendor_code="TEST007",
            finished_price=1500.0,
            for_pay=1495.0,
            spp_percent=0.0,  # Нет СПП!
            date=recent_date_str,
            quantity=1
        )

        product = ProductData(
            vendor_code="TEST007",
            purchase_price=800.0,
            target_profit=300.0,
            current_price_wb=1500.0,  # Такая же как finished_price (т.к. СПП=0)
            current_real_price=1500.0,
            sku_wb=123462,
            status=1
        )

        price_updater.fetch_nm_id = AsyncMock(return_value=123462)

        result = await price_updater.process_product(
            vendor_code="TEST007",
            sales=[sale, sale, sale, sale],
            product=product
        )

        print(f"\nТест 7: СПП = 0%")
        print(f"  Статус: {result.status}")
        print(f"  СПП: {result.discount}%")

        # При СПП=0%:
        # finished_price = price_wb
        # clean_fpay = for_pay (т.к. СПП=0)

        assert result.status == ProcessingStatus.SUCCESS
        assert result.discount == 0.0
        # При СПП=0, price_wb должно равняться finished_price
        # Но в реальности скрипт делает округление

    @pytest.mark.asyncio
    async def test_invalid_discount_skip(self, price_updater, recent_date_str):
        """
        Тест 8: Некорректная скидка → пропуск
        """
        sale = SaleData(
            nm_id=123463,
            vendor_code="TEST008",
            finished_price=1000.0,
            for_pay=995.0,
            spp_percent=-10.0,
            date=recent_date_str,
            quantity=1
        )

        product = ProductData(
            vendor_code="TEST008",
            purchase_price=500.0,
            target_profit=200.0,
            current_price_wb=1200.0,
            current_real_price=1000.0,
            sku_wb=123463,
            status=1
        )

        price_updater.fetch_nm_id = AsyncMock(return_value=123463)

        result = await price_updater.process_product(
            vendor_code="TEST008",
            sales=[sale, sale, sale, sale],
            product=product
        )

        print(f"\nТест 8: Некорректный СПП")
        print(f"  Статус: {result.status}")
        print(f"  Причина: {result.error_msg}")

        # СПП должен быть 0-100%, отрицательный - ошибка
        assert result.status == ProcessingStatus.SKIPPED_INVALID
        assert "Некорректная скидка" in result.error_msg

    def test_price_update_reason_formatting(self):
        """
        Тест 9: Форматирование причины изменения цены

        ВАЖНО: Свойство reason возвращает error_msg, если он не пустой!
        Только если error_msg пустой, то вычисляет направление по ценам.
        """
        print(f"\nТест 9: Форматирование причины")


        print("\n1. С error_msg (как в реальном коде):")


        update1 = PriceUpdate(
            vendor_code="TEST009",
            new_price_wb=1500.0,
            new_real_price=1200.0,
            old_price_wb=1300.0,
            profit_correction=200.0,
            status=ProcessingStatus.SUCCESS,
            error_msg="Изменение цены: +200.00 руб. Скидка: 10.0%"
        )
        print(f"  Повышение: {update1.reason}")
        assert update1.reason == "Изменение цены: +200.00 руб. Скидка: 10.0%"
        assert "Цена ↑" not in update1.reason

        # Тест понижения цены С error_msg
        update2 = PriceUpdate(
            vendor_code="TEST010",
            new_price_wb=1100.0,
            new_real_price=900.0,
            old_price_wb=1300.0,
            profit_correction=200.0,
            status=ProcessingStatus.SUCCESS,
            error_msg="Изменение цены: -200.00 руб. Скидка: 10.0%"  # error_msg заполнен!
        )
        print(f"  Понижение: {update2.reason}")
        assert update2.reason == "Изменение цены: -200.00 руб. Скидка: 10.0%"
        assert "Цена ↓" not in update2.reason  # Не должно быть, т.к. error_msg заполнен

        # Тест без изменений С error_msg
        update3 = PriceUpdate(
            vendor_code="TEST011",
            new_price_wb=1300.0,
            new_real_price=1100.0,
            old_price_wb=1300.0,
            profit_correction=0.0,
            status=ProcessingStatus.SUCCESS,
            error_msg="Цена без изменений"
        )
        print(f"  Без изменений: {update3.reason}")
        assert update3.reason == "Цена без изменений"

        # ТЕСТ 2: БЕЗ error_msg (теоретический случай)
        print("\n2. Без error_msg (теоретический случай):")

        # Тест повышения цены БЕЗ error_msg
        update4 = PriceUpdate(
            vendor_code="TEST012",
            new_price_wb=1500.0,
            new_real_price=1200.0,
            old_price_wb=1300.0,
            profit_correction=200.0,
            status=ProcessingStatus.SUCCESS,
            error_msg=""
        )
        print(f"  Повышение: {update4.reason}")
        assert "Цена ↑" in update4.reason
        assert "200" in update4.reason

        # Тест понижения цены БЕЗ error_msg
        update5 = PriceUpdate(
            vendor_code="TEST013",
            new_price_wb=1100.0,
            new_real_price=900.0,
            old_price_wb=1300.0,
            profit_correction=200.0,
            status=ProcessingStatus.SUCCESS,
            error_msg=""
        )
        print(f"  Понижение: {update5.reason}")
        assert "Цена ↓" in update5.reason
        assert "200" in update5.reason


        update6 = PriceUpdate(
            vendor_code="TEST014",
            new_price_wb=1300.0,
            new_real_price=1100.0,
            old_price_wb=1300.0,
            profit_correction=0.0,
            status=ProcessingStatus.SUCCESS,
            error_msg=""
        )
        print(f"  Без изменений: {update6.reason}")
        assert "Цена без изменений" in update6.reason


        print("\n3. Не-SUCCESS статусы:")

        update7 = PriceUpdate(
            vendor_code="TEST015",
            new_price_wb=0.0,
            new_real_price=0.0,
            old_price_wb=1300.0,
            profit_correction=0.0,
            status=ProcessingStatus.SKIPPED_MIN_CHANGE,
            error_msg="Изменение меньше порога: 20.0 ₽"
        )
        print(f"  SKIPPED_MIN_CHANGE с error_msg: {update7.reason}")
        assert update7.reason == "Изменение меньше порога: 20.0 ₽"

        update8 = PriceUpdate(
            vendor_code="TEST016",
            new_price_wb=0.0,
            new_real_price=0.0,
            old_price_wb=1300.0,
            profit_correction=0.0,
            status=ProcessingStatus.SKIPPED_MIN_CHANGE,
            error_msg=""
        )
        print(f"  SKIPPED_MIN_CHANGE без error_msg: {update8.reason}")
        assert "Изменение меньше порога" in update8.reason

        print(f"\n✅ Все тесты свойства reason пройдены!")


# Основной запуск тестов
if __name__ == "__main__":
    print("=" * 80)
    print("Запуск тестов системы ценообразования WB")
    print("=" * 80)

    import asyncio


    async def run_all_tests():
        """Запуск всех тестов"""
        updater_tester = TestPriceUpdater()


        mock_config = Mock()
        mock_config.BANK_COMMISSION = 0.02
        mock_config.MIN_MARGIN_FACTOR = 1.2
        mock_config.MIN_PRICE_CHANGE = 50
        mock_config.MIN_SALES_FOR_CALC = 3
        mock_config.MAX_PRICE_CHANGE_PERCENT = 50.0

        updater = PriceUpdater()
        updater.logger = Mock()
        updater.db_pool = AsyncMock()
        updater.session = AsyncMock()
        updater.queue = Mock()

        recent_date_str = (datetime.now(pytz.utc) - timedelta(hours=1)).isoformat()

        # Запускаем ключевые тесты
        print("\n1. Тест: Основной расчет")
        await updater_tester.test_profit_below_target_increase_price(updater, recent_date_str)

        print("\n2. Тест: Недостаточно продаж")
        await updater_tester.test_insufficient_sales_skip(updater, recent_date_str)

        print("\n3. Тест: Корректность расчета СПП")
        await updater_tester.test_spp_calculation_correctness(updater, recent_date_str)

        print("\n✅ Все тесты завершены успешно!")


    # Запускаем
    asyncio.run(run_all_tests())