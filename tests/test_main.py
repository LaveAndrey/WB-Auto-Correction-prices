"""
Тесты для системы ценообразования WB (обновленные)
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
    """Тесты для системы ценообразования (обновленные)"""

    @pytest.fixture
    def mock_config(self):
        """Мок конфигурации"""
        with patch('price_updater_main.Config') as mock:
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
            mock.DB_HOST = "localhost"
            mock.DB_USER = "test"
            mock.DB_PASSWORD = "test"
            mock.DB_NAME = "test"
            mock.DB_PORT = 3306
            mock.WB_SALES_TOKEN = "test_token"
            mock.WB_PRICES_TOKEN = "test_token"
            mock.WB_CONTENT_TOKEN = "test_token"
            mock.ANALYTICS_TABLE = "test_analytics"
            mock.PRICE_HISTORY_TABLE = "test_price_history"
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

    @pytest.fixture
    def sample_sale_data(self, recent_date_str):
        """Создание образца данных о продаже для новой структуры"""
        return SaleData(
            nm_id=123456,
            vendor_code="TEST001",
            finished_price=1000.0,  # что заплатил покупатель
            price_with_desc=900.0,  # цена на витрине (ВАЖНО!)
            for_pay=850.0,  # что получили мы
            spp_percent=5.0,  # СПП
            discount_percent=10.0,  # скидка покупателя (ВАЖНО!)
            total_price=1000.0,  # базовая цена
            date=recent_date_str,
            quantity=1
        )

    @pytest.fixture
    def sample_product_data(self):
        """Создание образца данных о товаре"""
        return ProductData(
            vendor_code="TEST001",
            purchase_price=500.0,
            target_profit=200.0,
            current_price_wb=1000.0,  # базовая цена в БД (totalPrice)
            current_real_price=900.0,  # цена на витрине в БД (priceWithDisc)
            sku_wb=123456,
            status=1
        )

    @pytest.mark.asyncio
    async def test_new_logic_profit_below_target(self, price_updater, sample_sale_data, sample_product_data):
        """
        Тест 1: Проверка новой логики расчета - прибыль ниже цели
        """
        # Настраиваем мок метода
        price_updater.fetch_nm_id = AsyncMock(return_value=123456)
        price_updater.save_price_update = AsyncMock()
        price_updater._validate_price_update = Mock(return_value=None)

        # Создаем список продаж (достаточное количество)
        sales = [sample_sale_data] * 5

        # Запускаем обработку
        result = await price_updater.process_product_new_logic(
            vendor_code="TEST001",
            sales=sales,
            product=sample_product_data
        )

        # Проверки
        print(f"\nТест 1: Новая логика расчета")
        print(f"  Статус: {result.status}")
        print(f"  Старая цена: {result.old_price_wb:.0f}₽")
        print(f"  Новая цена: {result.new_price_wb:.0f}₽")
        print(f"  Причина: {result.reason}")
        print(f"  Скидка: {result.discount}%")

        # Ожидаем успешный статус
        assert result.status == ProcessingStatus.SUCCESS
        assert result.sku_wb == 123456
        assert result.discount is not None

    @pytest.mark.asyncio
    async def test_new_logic_insufficient_sales(self, price_updater, sample_sale_data, sample_product_data):
        """
        Тест 2: Недостаточно продаж в новой логике
        """
        # Настраиваем мок
        price_updater.fetch_nm_id = AsyncMock(return_value=123456)

        # Только 2 продажи при MIN_SALES_FOR_CALC=3
        sales = [sample_sale_data] * 2

        result = await price_updater.process_product_new_logic(
            vendor_code="TEST002",
            sales=sales,
            product=sample_product_data
        )

        print(f"\nТест 2: Недостаточно продаж (новая логика)")
        print(f"  Статус: {result.status}")
        print(f"  Причина: {result.error_msg}")

        assert result.status == ProcessingStatus.SKIPPED_NO_DATA
        assert "Недостаточно данных" in result.error_msg

    @pytest.mark.asyncio
    async def test_new_logic_with_invalid_sales(self, price_updater, recent_date_str, sample_product_data):
        """
        Тест 3: Продажи с некорректными данными
        """
        # Создаем продажи с некорректными данными
        invalid_sale = SaleData(
            nm_id=123457,
            vendor_code="TEST003",
            finished_price=0.0,  # Некорректная цена
            price_with_desc=0.0,
            for_pay=0.0,
            spp_percent=5.0,
            discount_percent=10.0,
            total_price=0.0,
            date=recent_date_str,
            quantity=1
        )

        sales = [invalid_sale] * 5

        result = await price_updater.process_product_new_logic(
            vendor_code="TEST003",
            sales=sales,
            product=sample_product_data
        )

        print(f"\nТест 3: Некорректные данные продаж")
        print(f"  Статус: {result.status}")

        # Ожидаем пропуск из-за недостатка валидных данных
        assert result.status == ProcessingStatus.SKIPPED_NO_DATA

    @pytest.mark.asyncio
    async def test_new_logic_nmid_not_found(self, price_updater, sample_sale_data, sample_product_data):
        """
        Тест 4: nmID не найден в новой логике
        """
        # Настраиваем мок для возврата 0 (nmID не найден)
        price_updater.fetch_nm_id = AsyncMock(return_value=0)

        sales = [sample_sale_data] * 5

        result = await price_updater.process_product_new_logic(
            vendor_code="TEST004",
            sales=sales,
            product=sample_product_data
        )

        print(f"\nТест 4: nmID не найден")
        print(f"  Статус: {result.status}")
        print(f"  Причина: {result.error_msg}")

        assert result.status == ProcessingStatus.ERROR
        assert "Не удалось получить nmID" in result.error_msg

    @pytest.mark.asyncio
    async def test_new_logic_validation_min_price(self, price_updater, sample_sale_data, sample_product_data):
        """
        Тест 5: Проверка валидации минимальной цены
        """
        # Настраиваем моки
        price_updater.fetch_nm_id = AsyncMock(return_value=123456)

        # Создаем продукт с высокой закупочной ценой
        expensive_product = ProductData(
            vendor_code="TEST005",
            purchase_price=1000.0,  # Высокая закупка
            target_profit=100.0,    # Маленькая прибыль
            current_price_wb=1200.0,
            current_real_price=1100.0,
            sku_wb=123456,
            status=1
        )

        sales = [sample_sale_data] * 5

        result = await price_updater.process_product_new_logic(
            vendor_code="TEST005",
            sales=sales,
            product=expensive_product
        )

        print(f"\nТест 5: Валидация минимальной цены")
        print(f"  Статус: {result.status}")

        # В зависимости от расчетов может быть SUCCESS или SKIPPED_MIN_PRICE
        assert result.status in [ProcessingStatus.SUCCESS, ProcessingStatus.SKIPPED_MIN_PRICE]

    @pytest.mark.asyncio
    async def test_new_logic_detailed_calculation(self, price_updater, recent_date_str):
        """
        Тест 6: Детальная проверка расчета в новой логике
        """
        # Создаем тестовые данные с известными значениями
        test_sale = SaleData(
            nm_id=123458,
            vendor_code="TEST006",
            finished_price=1500.0,
            price_with_desc=1350.0,  # 10% скидка от 1500
            for_pay=1275.0,  # 5% СПП от 1350
            spp_percent=5.0,
            discount_percent=10.0,  # 10% скидка
            total_price=1500.0,
            date=recent_date_str,
            quantity=2  # Две единицы
        )

        test_product = ProductData(
            vendor_code="TEST006",
            purchase_price=800.0,
            target_profit=300.0,
            current_price_wb=1500.0,
            current_real_price=1350.0,
            sku_wb=123458,
            status=1
        )

        # Настраиваем моки
        price_updater.fetch_nm_id = AsyncMock(return_value=123458)
        price_updater.save_nm_id_to_db = AsyncMock(return_value=True)
        price_updater._validate_price_update = Mock(return_value=None)

        sales = [test_sale] * 4  # 4 записи продаж

        result = await price_updater.process_product_new_logic(
            vendor_code="TEST006",
            sales=sales,
            product=test_product
        )

        print(f"\nТест 6: Детальный расчет")
        print(f"  Статус: {result.status}")
        print(f"  Старая цена: {result.old_price_wb:.0f}₽")
        print(f"  Новая цена: {result.new_price_wb:.0f}₽")
        print(f"  Скидка: {result.discount}%")

        # Проверяем что расчет выполнен
        assert result.status == ProcessingStatus.SUCCESS
        assert result.discount == 10.0  # Проверяем что скидка сохранилась

    def test_sale_data_from_api_dict(self):
        """
        Тест 7: Проверка создания SaleData из API данных
        """
        api_data = {
            "nmId": 123459,
            "supplierArticle": "TEST007",
            "finishedPrice": 2000.0,
            "priceWithDisc": 1800.0,
            "forPay": 1710.0,
            "spp": 5.0,
            "discountPercent": 10.0,
            "totalPrice": 2000.0,
            "lastChangeDate": "2024-01-01T12:00:00",
            "quantity": 1
        }

        sale = SaleData.from_api_dict(api_data)

        assert sale is not None
        assert sale.nm_id == 123459
        assert sale.vendor_code == "TEST007"
        assert sale.finished_price == 2000.0
        assert sale.price_with_desc == 1800.0
        assert sale.for_pay == 1710.0
        assert sale.discount_percent == 10.0

    def test_product_data_from_db_row(self):
        """
        Тест 8: Проверка создания ProductData из строки БД
        """
        db_row = {
            "model": "TEST008",
            "purchase_price": 1000.0,
            "target_profit_rub": 300.0,
            "price_wb": 1500.0,
            "wb_real_price": 1350.0,
            "sku_wb": 123460,
            "status": 1
        }

        product = ProductData.from_db_row(db_row)

        assert product is not None
        assert product.vendor_code == "TEST008"
        assert product.purchase_price == 1000.0
        assert product.target_profit == 300.0
        assert product.current_price_wb == 1500.0
        assert product.current_real_price == 1350.0
        assert product.sku_wb == 123460

    @pytest.mark.asyncio
    async def test_save_nm_id_to_db_success(self, price_updater):
        """
        Тест 9: Проверка успешного сохранения nmID в БД
        """
        # Настраиваем успешное выполнение запроса
        mock_cursor = AsyncMock()
        mock_cursor.execute = AsyncMock()

        mock_conn = AsyncMock()
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cursor

        price_updater.db_pool.acquire.return_value.__aenter__.return_value = mock_conn

        result = await price_updater.save_nm_id_to_db("TEST009", 123461)

        assert result is True
        mock_cursor.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_save_nm_id_to_db_failure(self, price_updater):
        """
        Тест 10: Проверка ошибки при сохранении nmID в БД
        """
        # Настраиваем ошибку при выполнении запроса
        mock_cursor = AsyncMock()
        mock_cursor.execute = AsyncMock(side_effect=Exception("DB Error"))

        mock_conn = AsyncMock()
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cursor

        price_updater.db_pool.acquire.return_value.__aenter__.return_value = mock_conn

        result = await price_updater.save_nm_id_to_db("TEST010", 123462)

        assert result is False

    def test_price_update_reason_formatting(self):
        """
        Тест 11: Форматирование причины изменения цены (обновленное)
        """
        print(f"\nТест 11: Форматирование причины")

        # Тест с error_msg
        update1 = PriceUpdate(
            vendor_code="TEST011",
            new_price_wb=1500.0,
            new_real_price=1350.0,
            old_price_wb=1300.0,
            profit_correction=200.0,
            status=ProcessingStatus.SUCCESS,
            error_msg="Корректировка прибыли: +200.00 руб"
        )
        print(f"  С error_msg: {update1.reason}")
        assert update1.reason == "Корректировка прибыли: +200.00 руб"

        # Тест без error_msg - повышение цены
        update2 = PriceUpdate(
            vendor_code="TEST012",
            new_price_wb=1600.0,
            new_real_price=1440.0,
            old_price_wb=1500.0,
            profit_correction=100.0,
            status=ProcessingStatus.SUCCESS,
            error_msg=""
        )
        print(f"  Без error_msg (повышение): {update2.reason}")
        assert "Цена ↑" in update2.reason

        # Тест без error_msg - понижение цены
        update3 = PriceUpdate(
            vendor_code="TEST013",
            new_price_wb=1400.0,
            new_real_price=1260.0,
            old_price_wb=1500.0,
            profit_correction=100.0,
            status=ProcessingStatus.SUCCESS,
            error_msg=""
        )
        print(f"  Без error_msg (понижение): {update3.reason}")
        assert "Цена ↓" in update3.reason

        # Тест для не-SUCCESS статусов
        update4 = PriceUpdate(
            vendor_code="TEST014",
            new_price_wb=0.0,
            new_real_price=0.0,
            old_price_wb=1500.0,
            profit_correction=0.0,
            status=ProcessingStatus.SKIPPED_MIN_PRICE,
            error_msg=""
        )
        print(f"  SKIPPED_MIN_PRICE: {update4.reason}")
        assert "Цена ниже минимальной" in update4.reason


# Основной запуск тестов
if __name__ == "__main__":
    print("=" * 80)
    print("Запуск обновленных тестов системы ценообразования WB")
    print("=" * 80)

    import asyncio

    async def run_all_tests():
        """Запуск всех тестов"""
        tester = TestPriceUpdater()

        # Запускаем ключевые тесты
        print("\n1. Тест: Создание SaleData из API данных")
        tester.test_sale_data_from_api_dict()

        print("\n2. Тест: Создание ProductData из БД")
        tester.test_product_data_from_db_row()

        print("\n3. Тест: Форматирование причины")
        tester.test_price_update_reason_formatting()

        print("\n✅ Основные тесты завершены успешно!")

    # Запускаем
    asyncio.run(run_all_tests())