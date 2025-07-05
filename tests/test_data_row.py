import pytest
from core.csv_processor import DataRow

class TestDataRow:
    """Тесты для класса DataRow."""
    
    # Тестовые данные для параметризации
    TEST_DATA = [
        # Простые скалярные значения
        ('string_value', 'test_string'),
        ('int_value', 42),
        ('float_value', 3.14),
        ('bool_value', True),
        ('none_value', None),
        # Составные типы
        ('list_value', [1, 2, 3]),
        ('dict_value', {'key': 'value'}),
        ('tuple_value', (1, 2, 3)),
    ]
    
    @pytest.mark.parametrize('key,value', TEST_DATA)
    def test_getitem(self, key, value):
        """Проверка доступа к данным по ключу для разных типов данных."""
        row = DataRow(**{key: value})
        assert row[key] == value
        
    def test_getitem_multiple_values(self):
        """Проверка доступа к данным при нескольких значениях."""
        test_data = dict(self.TEST_DATA[:3])
        row = DataRow(**test_data)
        for key, expected_value in test_data.items():
            assert row[key] == expected_value
    
    @pytest.mark.parametrize('key,value', TEST_DATA)
    def test_contains(self, key, value):
        """Проверка проверки существования ключа."""
        row = DataRow(**{key: value, 'another_key': 'test'})
        assert key in row
        assert 'nonexistent_key' not in row
    
    def test_str_representation(self):
        """Проверка строкового представления."""
        test_data = dict(self.TEST_DATA[:3])  # Берем первые 3 тестовых значения
        row = DataRow(**test_data)
        assert str(row) == str(test_data)
        
    def test_iter(self):
        """Проверка итерации по ключам."""
        test_data = {'a': 1, 'b': 2, 'c': 3}
        row = DataRow(**test_data)
        assert set(row) == set(test_data.keys())
        
    @pytest.mark.parametrize('test_data,expected_length', [
        ({'a': 1, 'b': 2, 'c': 3}, 3),
        ({'single': 1}, 1),
        ({}, 0),  # Пустой словарь
        ({'a': None, 'b': None}, 2),  # None значения
    ])
    def test_len(self, test_data, expected_length):
        """Проверка получения количества элементов."""
        row = DataRow(**test_data)
        assert len(row) == expected_length
        
    def test_update_existing_key(self):
        """Проверка обновления существующего ключа."""
        row = DataRow(key='old_value')
        row['key'] = 'new_value'
        assert row['key'] == 'new_value'
        
    def test_non_string_keys(self):
        """Проверка работы с нестроковыми ключами."""
        row = DataRow()
        test_cases = [
            123,  # целое число
            3.14,  # число с плавающей точкой
            True,  # булево значение
            (1, 2),  # кортеж
        ]
        
        for key in test_cases:
            row[str(key)] = 'value'
            assert str(key) in row
            assert row[str(key)] == 'value'
