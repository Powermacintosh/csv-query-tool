import pytest
from pathlib import Path
from typing import Dict, List

from core.csv_processor import DataProcessor
from tests.utils.csv_analyzer import analyze_csv_file, ColumnInfo, get_test_values_for_column


class CSVTestBase:
    """Базовый класс для тестирования CSV-файлов."""
    
    @pytest.fixture(scope='class')
    def csv_file(self, request) -> Path:
        """Фикстура, возвращающая путь к тестируемому CSV-файлу."""
        return request.param
    
    @pytest.fixture(scope='class')
    def csv_data(self, csv_file: Path) -> Dict[str, ColumnInfo]:
        """Фикстура, возвращающая проанализированные данные CSV-файла."""
        return analyze_csv_file(csv_file)
    
    @pytest.fixture(scope='class')
    def processor(self, csv_file: Path) -> DataProcessor:
        """Фикстура, создающая и инициализирующая процессор CSV."""
        processor = DataProcessor(csv_file)
        processor.load_data()
        return processor
    
    @pytest.fixture(scope='class')
    def numeric_columns(self, csv_data: Dict[str, ColumnInfo]) -> List[str]:
        """Фикстура, возвращающая список числовых колонок."""
        return [col for col, info in csv_data.items() if info.is_numeric]
    
    @pytest.fixture(scope='class')
    def text_columns(self, csv_data: Dict[str, ColumnInfo]) -> List[str]:
        """Фикстура, возвращающая список текстовых колонок."""
        return [col for col, info in csv_data.items() 
                if not info.is_numeric and info.type.name not in ('BOOLEAN', 'UNKNOWN')]
    
    @pytest.fixture(scope='class')
    def test_values(self, csv_data: Dict[str, ColumnInfo]) -> Dict[str, Dict[str, str]]:
        """Фикстура, возвращающая тестовые значения для каждой колонки."""
        return {
            col: get_test_values_for_column(info) 
            for col, info in csv_data.items()
            if info.sample_values
        }
