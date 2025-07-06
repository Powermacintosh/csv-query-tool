import pytest
from pathlib import Path
from typing import Dict, List

from core.csv_processor import DataProcessor, FilterCondition
from tests.base_test import CSVTestBase


class TestCSVFilter(CSVTestBase):
    """Тесты для функциональности фильтрации."""
    
    @pytest.mark.parametrize('csv_file', 
                          [pytest.param(f, id=f.name) 
                           for f in (Path(__file__).parent.parent / 'data').glob('*.csv')],
                          indirect=True)
    def test_filter_equals(self, processor: DataProcessor, test_values: Dict[str, Dict[str, str]]):
        """Тестирование фильтрации с оператором равенства."""
        for column, values in test_values.items():
            if 'eq' not in values:
                continue
            condition = f'{column}={values["eq"]}'
            result = processor.filter_data(condition)
            for row in result:
                assert str(row[column]) == values['eq']
    
    @pytest.mark.parametrize('csv_file', 
                          [pytest.param(f, id=f.name) 
                           for f in (Path(__file__).parent.parent / 'data').glob('*.csv')],
                          indirect=True)
    def test_filter_greater_than(self, processor: DataProcessor, numeric_columns: List[str], 
                               test_values: Dict[str, Dict[str, str]]):
        """Тестирование фильтрации с оператором 'больше'."""
        for column in numeric_columns:
            if 'gt' not in test_values.get(column, {}):
                continue
            condition = f'{column}>{test_values[column]["gt"]}'
            result = processor.filter_data(condition)
            for row in result:
                # Пропускаем пустые или нечисловые значения
                if not row[column] or not str(row[column]).strip():
                    continue
                try:
                    assert float(row[column]) > float(test_values[column]['gt'])
                except (ValueError, TypeError):
                    # Пропускаем нечисловые значения
                    continue
    
    @pytest.mark.parametrize('csv_file', 
                          [pytest.param(f, id=f.name) 
                           for f in (Path(__file__).parent.parent / 'data').glob('*.csv')],
                          indirect=True)
    def test_filter_less_than(self, processor: DataProcessor, numeric_columns: List[str],
                            test_values: Dict[str, Dict[str, str]]):
        """Тестирование фильтрации с оператором 'меньше'."""
        for column in numeric_columns:
            if 'lt' not in test_values.get(column, {}):
                continue
            condition = f'{column}<{test_values[column]["lt"]}'
            result = processor.filter_data(condition)
            for row in result:
                # Пропускаем пустые или нечисловые значения
                if not row[column] or not str(row[column]).strip():
                    continue
                try:
                    assert float(row[column]) < float(test_values[column]['lt'])
                except (ValueError, TypeError):
                    # Пропускаем нечисловые значения
                    continue
    
    @pytest.mark.parametrize('csv_file',
                          [pytest.param(f, id=f.name) 
                           for f in (Path(__file__).parent.parent / 'data').glob('*.csv')],
                          indirect=True)
    def test_filter_invalid_condition(self, processor: DataProcessor):
        """Тестирование фильтрации с невалидными условиями"""
        with pytest.raises(ValueError, match='Колонка .* не найдена'):
            processor.filter_data('nonexistent>10')
        with pytest.raises(ValueError, match='Неподдерживаемый оператор'):
            processor.filter_data('age<>25')
    
    @pytest.mark.parametrize('csv_file',
                          [pytest.param(f, id=f.name) 
                           for f in (Path(__file__).parent.parent / 'data').glob('*.csv')],
                          indirect=True)
    def test_filter_missing_column(self, processor: DataProcessor):
        """Тестирование фильтрации по несуществующей колонке."""
        with pytest.raises(ValueError, match='Колонка .* не найдена'):
            processor.filter_data('nonexistent_column_123=test')
    
    @pytest.mark.parametrize('invalid_condition, expected_error', [
        ('>100', r'Не указано имя колонки для фильтрации'),
        ('=100', r'Не указано имя колонки для фильтрации'),
        (
            'column~100', 
            r'Не найден поддерживаемый оператор в условии: [\"\']?column~100[\"\']?[^\n]*Используйте один из: =, >, <'
        ),
        ('column=', r'Не указано значение для сравнения'),
    ])
    def test_filter_condition_validation(self, invalid_condition: str, expected_error: str):
        """Тестирование валидации условий фильтрации."""
        with pytest.raises(ValueError, match=expected_error):
            FilterCondition.from_string(invalid_condition)
            
    @pytest.mark.parametrize('empty_condition', ['', '   '])
    def test_empty_filter_condition(self, empty_condition: str):
        """Тестирование пустого условия фильтрации."""
        # Пустые условия должны возвращать None, а не вызывать исключение
        assert FilterCondition.from_string(empty_condition) is None
    
    @pytest.mark.parametrize('csv_file',
                          [pytest.param(f, id=f.name) 
                           for f in (Path(__file__).parent.parent / 'data').glob('*.csv')],
                          indirect=True)
    def test_filter_with_empty_condition(self, processor: DataProcessor):
        """Тестирование фильтрации с пустым условием."""
        # Пустое условие должно вернуть все данные
        result = processor.filter_data('')
        assert len(result) == len(processor._data)
