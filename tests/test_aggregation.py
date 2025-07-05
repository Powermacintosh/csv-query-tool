import pytest
from pathlib import Path
from typing import List

from core.csv_processor import DataProcessor, Aggregation
from tests.base_test import CSVTestBase


class TestCSVAggregation(CSVTestBase):
    """Тесты для функциональности агрегации."""
    
    @pytest.mark.parametrize('csv_file', 
                          [pytest.param(f, id=f.name) 
                           for f in (Path(__file__).parent.parent / 'data').glob('*.csv')],
                          indirect=True)
    def test_aggregate_avg(self, processor: DataProcessor, numeric_columns: List[str]):
        """Тестирование агрегации со средним значением."""
        for column in numeric_columns:
            agg_condition = f'{column}=avg'
            result = processor.aggregate_data(processor._data, agg_condition)
            assert result is not None
            assert 'operation' in result
            assert 'value' in result
            assert result['operation'] == 'avg'
            assert isinstance(result['value'], (int, float))
    
    @pytest.mark.parametrize('csv_file', 
                          [pytest.param(f, id=f.name) 
                           for f in (Path(__file__).parent.parent / 'data').glob('*.csv')],
                          indirect=True)
    def test_aggregate_min(self, processor: DataProcessor, numeric_columns: List[str]):
        """Тестирование агрегации с минимальным значением."""
        for column in numeric_columns:
            agg_condition = f'{column}=min'
            result = processor.aggregate_data(processor._data, agg_condition)
            assert result is not None
            assert 'operation' in result
            assert 'value' in result
            assert result['operation'] == 'min'
            assert isinstance(result['value'], (int, float))
            values = [float(row[column]) for row in processor._data
                if row[column] is not None and str(row[column]).strip()]
            if values:
                assert result['value'] == min(values)
    
    @pytest.mark.parametrize('csv_file', 
                          [pytest.param(f, id=f.name) 
                           for f in (Path(__file__).parent.parent / 'data').glob('*.csv')],
                          indirect=True)
    def test_aggregate_max(self, processor: DataProcessor, numeric_columns: List[str]):
        """Тестирование агрегации с максимальным значением."""
        for column in numeric_columns:
            agg_condition = f'{column}=max'
            result = processor.aggregate_data(processor._data, agg_condition)
            assert result is not None
            assert 'operation' in result
            assert 'value' in result
            assert result['operation'] == 'max'
            assert isinstance(result['value'], (int, float))
            values = [float(row[column]) for row in processor._data
                if row[column] is not None and str(row[column]).strip()]
            if values:
                assert result['value'] == max(values)
    
    @pytest.mark.parametrize('csv_file', 
                          [pytest.param(f, id=f.name) 
                           for f in (Path(__file__).parent.parent / 'data').glob('*.csv')],
                          indirect=True)
    def test_aggregate_invalid_column(self, processor: DataProcessor):
        """Тестирование агрегации с несуществующей колонкой."""
        result = processor.aggregate_data(processor._data, 'nonexistent_column=avg')
        assert result is None
        result = processor.aggregate_data(processor._data, 'invalid=avg')
        assert result is None
    
    @pytest.mark.parametrize('invalid_agg', [
        'column',
        'column=',
        '=avg',
        'column=invalid_op',
        '',
        '   ',
    ])
    def test_aggregation_validation(self, invalid_agg: str):
        """Тестирование валидации условий агрегации."""
        with pytest.raises(ValueError, match='Неверный формат агрегации'):
            Aggregation.from_string(invalid_agg)
    
    @pytest.mark.parametrize('csv_file',
                          [pytest.param(f, id=f.name) 
                           for f in (Path(__file__).parent.parent / 'data').glob('*.csv')],
                          indirect=True)
    def test_aggregate_empty_data(self, processor: DataProcessor, numeric_columns: List[str]):
        """Тестирование агрегации с пустыми данными."""
        if not numeric_columns:
            pytest.skip('Нет числовых колонок для тестирования')
        column = numeric_columns[0]
        empty_data = []
        result = processor.aggregate_data(empty_data, f'{column}=avg')
        assert result is None or result['value'] is None
    
    @pytest.mark.parametrize('csv_file', 
                          [pytest.param(f, id=f.name) 
                           for f in (Path(__file__).parent.parent / 'data').glob('*.csv')],
                          indirect=True)
    def test_aggregate_invalid_operation(self, processor: DataProcessor, 
                                       numeric_columns: List[str]):
        """Тестирование агрегации с недопустимой операцией."""
        if not numeric_columns:
            pytest.skip('Нет числовых колонок для тестирования')
        with pytest.raises(ValueError):
            processor.aggregate_data(processor._data, f'{numeric_columns[0]}=invalid_operation')
