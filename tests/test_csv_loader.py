from pathlib import Path
from unittest.mock import AsyncMock

import pytest
import tempfile


class TestCSVLoaderTransformFixed:
    """Тесты после исправления _transform"""

    def test_transform_integer_now_works(self, csv_loader):
        """Тест, что целые числа теперь преобразуются"""
        result = csv_loader._transform("1235")
        assert result == 1235
        assert isinstance(result, int)

    def test_transform_float_with_dot(self, csv_loader):
        """Тест дробных чисел с точкой"""
        result = csv_loader._transform("123.45")
        assert result == 123.45
        assert isinstance(result, float)

    def test_transform_float_starts_with_dot(self, csv_loader):
        """Тест дробных чисел начинающихся с точки"""
        result = csv_loader._transform(".75")
        assert result == 0.75
        assert isinstance(result, float)

    def test_transform_currency_code_remains_string(self, csv_loader):
        """Тест, что коды валют остаются строками"""
        result = csv_loader._transform("643")
        assert result == "643"
        assert isinstance(result, str)

    def test_transform_account_number_unchanged(self, csv_loader):
        """Тест, что номера счетов остаются строками"""
        account_num = "30114840700000770002"
        result = csv_loader._transform(account_num)
        assert result == account_num
        assert isinstance(result, str)

    def test_transform_dates_work(self, csv_loader):
        """Тест преобразования дат"""
        from datetime import date

        result = csv_loader._transform("31.12.2017")
        assert result == date(2017, 12, 31)
        assert isinstance(result, date)


class TestDataValidator:
    """Тесты DataValidator"""

    def test_validator_initialization(self):
        """Тест инициализации валидатора"""
        from validation import DataValidator

        validator_strict = DataValidator(stop_on_error=True)
        assert validator_strict.stop_on_error == True

        validator_relaxed = DataValidator(stop_on_error=False)
        assert validator_relaxed.stop_on_error == False

    @pytest.mark.asyncio
    async def test_validate_all_methods_called(self):
        """Тест, что все методы валидации вызываются"""
        from validation import DataValidator

        validator = DataValidator()
        mock_conn = AsyncMock()

        validator.validate_referential_integrity = AsyncMock()
        validator.validate_business_rules = AsyncMock()
        validator.validate_data_quality = AsyncMock()

        await validator.validate_all(mock_conn)

        validator.validate_referential_integrity.assert_called_once_with(mock_conn)
        validator.validate_business_rules.assert_called_once_with(mock_conn)
        validator.validate_data_quality.assert_called_once_with(mock_conn)


class TestCSVLoaderExtract:
    """Тесты метода _extract"""

    @pytest.mark.asyncio
    async def test_extract_simple_csv(self, csv_loader, monkeypatch):
        """Тест чтения простого CSV файла"""

        with tempfile.TemporaryDirectory() as temp_dir:
            files_dir = Path(temp_dir) / "files"
            files_dir.mkdir()

            monkeypatch.setattr("etl_process.BASE_DIR", Path(temp_dir))

            csv_content = """COL1;COL2;COL3
    value1;123;45.67
    value2;456;89.01"""

            test_file = files_dir / "test_file.csv"
            test_file.write_text(csv_content, encoding="utf-8")

            result = await csv_loader._extract("test_file.csv")

            headers, data_rows = result

            assert headers == ["COL1", "COL2", "COL3"]
            assert len(data_rows) == 2

            assert data_rows[0][0] == "value1"
            assert data_rows[0][1] == "123"
            assert data_rows[0][2] == 45.67

            assert data_rows[1][0] == "value2"
            assert data_rows[1][1] == "456"
            assert data_rows[1][2] == 89.01

    @pytest.mark.asyncio
    async def test_extract_empty_csv(self, csv_loader, monkeypatch):
        """Тест чтения пустого CSV файла (только заголовки)"""

        with tempfile.TemporaryDirectory() as temp_dir:
            files_dir = Path(temp_dir) / "files"
            files_dir.mkdir()

            monkeypatch.setattr("etl_process.BASE_DIR", Path(temp_dir))

            csv_content = """COL1;COL2;COL3"""

            test_file = files_dir / "test_only_headers.csv"
            test_file.write_text(csv_content, encoding="utf-8")

            result = await csv_loader._extract("test_only_headers.csv")

            headers, data_rows = result

            assert headers == ["COL1", "COL2", "COL3"]
            assert len(data_rows) == 0
            assert data_rows == []
