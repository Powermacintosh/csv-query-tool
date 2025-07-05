import pytest
from pathlib import Path
from typing import List, Dict

from core.csv_processor import DataProcessor
from tests.base_test import CSVTestBase

class TestIntegration(CSVTestBase):
    """Интеграционные тесты для полного цикла работы с данными."""
    
    @pytest.mark.parametrize('csv_file', 
                          [pytest.param(f, id=f.name) 
                           for f in (Path(__file__).parent.parent / 'data').glob('*.csv')],
                          indirect=True)
    def test_full_workflow(self, processor: DataProcessor, numeric_columns: List[str], 
                          test_values: Dict[str, Dict[str, str]]):
        """Тестирование полного цикла: загрузка -> фильтрация -> агрегация."""
        if not numeric_columns:
            pytest.skip('Нет числовых колонок для тестирования')
            
        # Выбираем первую числовую колонку
        column = numeric_columns[0]
        
        # Получаем тестовые значения для фильтрации
        column_values = test_values.get(column, {})
        if not column_values.get('gt') or not column_values.get('lt'):
            pytest.skip(f'Недостаточно тестовых значений для колонки {column}')
        
        # 1. Фильтрация: значения больше определенного
        gt_value = column_values['gt']
        filtered = processor.filter_data(f'{column}>{gt_value}')
        assert isinstance(filtered, list)
        
        # Проверяем, что все значения соответствуют условию
        for row in filtered:
            assert float(row[column]) > float(gt_value)
        
        # 2. Агрегация: среднее значение
        if filtered:  # если есть отфильтрованные данные
            result = processor.aggregate_data(filtered, f'{column}=avg')
            assert result is not None
            assert 'value' in result
            assert isinstance(result['value'], (int, float))
            
            # Проверяем, что среднее значение в пределах разумного диапазона
            values = [float(row[column]) for row in filtered]
            assert min(values) <= result['value'] <= max(values)
    
    @pytest.mark.parametrize('csv_file', 
                          [pytest.param(f, id=f.name) 
                           for f in (Path(__file__).parent.parent / 'data').glob('*.csv')],
                          indirect=True)
    def test_multiple_operations(self, processor: DataProcessor, numeric_columns: List[str], text_columns: List[str]):
        """
        Тестирование последовательного применения нескольких операций.
        Если числовых колонок недостаточно, используем текстовые колонки для фильтрации.
        """
        if not numeric_columns:
            pytest.skip('Нет числовых колонок для тестирования')
            
        # Используем первую числовую колонку для агрегации
        num_col = numeric_columns[0]
        
        # Если есть вторая числовая колонка, используем её для фильтрации
        # Иначе используем первую текстовую колонку
        filter_col = numeric_columns[1] if len(numeric_columns) > 1 else (text_columns[0] if text_columns else None)
        
        if not filter_col:
            pytest.skip('Нет подходящих колонок для фильтрации')
            
        # 1. Первая фильтрация по числовой колонке
        filtered1 = processor.filter_data(f'{num_col}>0')
        if not filtered1:
            pytest.skip(f'Нет данных, удовлетворяющих условию {num_col}>0')
        
        # 2. Получаем уникальные значения для фильтрации
        filter_values = {row[filter_col] for row in filtered1 if row.get(filter_col) is not None}
        
        if not filter_values:
            pytest.skip(f'Нет данных в колонке {filter_col} для фильтрации')
            
        # Берем первое значение для фильтрации
        filter_value = next(iter(filter_values))
        
        # 3. Вторая фильтрация по выбранной колонке
        filtered2 = [row for row in filtered1 if row.get(filter_col) == filter_value]
        
        if not filtered2:
            pytest.skip(f'Нет данных, удовлетворяющих условию {filter_col}="{filter_value}"')
            
        # 4. Агрегация по числовой колонке
        agg_result = processor.aggregate_data(filtered2, f'{num_col}=avg')
        assert agg_result is not None
        assert 'value' in agg_result
        
        # Проверяем, что результат агрегации корректен
        values = [float(row[num_col]) for row in filtered2 if row.get(num_col) is not None]
        if values:  # На случай, если все значения None
            assert min(values) <= agg_result['value'] <= max(values)
