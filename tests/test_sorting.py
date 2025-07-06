import pytest, logging.config
from pathlib import Path
from typing import Dict, Any

from core.csv_processor import DataProcessor
from tests.base_test import CSVTestBase

from core.logger import logger_config

logging.config.dictConfig(logger_config)
logger = logging.getLogger('test_sorting_logger')

class TestSorting(CSVTestBase):
    """Тесты для сортировки данных."""
    
    @pytest.mark.parametrize('csv_file', 
                          [pytest.param(f, id=f.name) 
                           for f in (Path(__file__).parent.parent / 'data').glob('*.csv')],
                          indirect=True)
    def test_sort_ascending(self, processor: DataProcessor, csv_data: Dict[str, Any]):
        """Тестирование сортировки по возрастанию."""
        logger.debug("\n" + "="*80)
        logger.debug(f"Запуск теста сортировки по возрастанию для файла: {processor.file_path.name}")
        
        if not processor._data:
            logger.warning("Нет данных для тестирования")
            pytest.skip('Нет данных для тестирования')
            
        # Выбираем первую нечисловую колонку для сортировки (предполагаем, что это текст)
        text_columns = [col for col, col_info in csv_data.items() 
                       if not col_info.is_numeric]
        
        if not text_columns:
            logger.warning("Нет текстовых колонок для тестирования сортировки")
            pytest.skip('Нет текстовых колонок для тестирования сортировки')
            
        sort_column = text_columns[0]
        logger.debug(f"Выбрана колонка для сортировки: '{sort_column}'")
        
        # Логируем первые 5 значений до сортировки
        before_sort = [str(row.get(sort_column, '')) for row in processor._data[:5]]
        logger.debug(f"Значения ДО сортировки (первые 5): {', '.join(before_sort)}")
        
        # Сортируем данные
        sorted_data = processor.sort_data(processor._data, f"{sort_column}=asc")
        
        # Логируем первые 5 значений после сортировки
        after_sort = [str(row.get(sort_column, '')) for row in sorted_data[:5]]
        logger.debug(f"Значения ПОСЛЕ сортировки (первые 5): {', '.join(after_sort)}")
        
        # Проверяем, что данные отсортированы по возрастанию
        values = [row[sort_column].lower() if row[sort_column] is not None else '' 
                 for row in sorted_data]
        
        logger.debug(f"Проверка сортировки по возрастанию для колонки: {sort_column}")
        logger.debug(f"Количество строк: {len(values)}")
        logger.debug(f"Первые 5 значений: {values[:5]}")
        logger.debug(f"Последние 5 значений: {values[-5:] if len(values) > 5 else values}")
        
        try:
            assert values == sorted(values), f'Данные не отсортированы по возрастанию для колонки {sort_column}'
            logger.debug("✓ Проверка сортировки по возрастанию пройдена успешно")
        except AssertionError as e:
            logger.error(f"Ошибка при сортировке: {e}")
            logger.error(f"Первые 10 значений: {values[:10]}")
            logger.error(f"Ожидаемый порядок: {sorted(values)[:10]}")
            raise
    
    @pytest.mark.parametrize('csv_file', 
                          [pytest.param(f, id=f.name) 
                           for f in (Path(__file__).parent.parent / 'data').glob('*.csv')],
                          indirect=True)
    def test_sort_descending(self, processor: DataProcessor, csv_data: Dict[str, Any]):
        """Тестирование сортировки по убыванию."""
        logger.debug("\n" + "="*80)
        logger.debug(f"Запуск теста сортировки по убыванию для файла: {processor.file_path.name}")
        
        if not processor._data:
            logger.warning("Нет данных для тестирования")
            pytest.skip('Нет данных для тестирования')
            
        # Выбираем первую нечисловую колонку для сортировки (предполагаем, что это текст)
        text_columns = [col for col, col_info in csv_data.items() 
                       if not col_info.is_numeric]
        
        if not text_columns:
            logger.warning("Нет текстовых колонок для тестирования сортировки")
            pytest.skip('Нет текстовых колонок для тестирования сортировки')
            
        sort_column = text_columns[0]
        logger.debug(f"Выбрана колонка для сортировки: '{sort_column}'")
        
        # Логируем первые 5 значений до сортировки
        before_sort = [str(row.get(sort_column, '')) for row in processor._data[:5]]
        logger.debug(f"Значения ДО сортировки (первые 5): {', '.join(before_sort)}")
        
        # Сортируем данные
        sorted_data = processor.sort_data(processor._data, f"{sort_column}=desc")
        
        # Логируем первые 5 значений после сортировки
        after_sort = [str(row.get(sort_column, '')) for row in sorted_data[:5]]
        logger.debug(f"Значения ПОСЛЕ сортировки (первые 5): {', '.join(after_sort)}")
        
        # Проверяем, что данные отсортированы по убыванию
        values = [row[sort_column].lower() if row[sort_column] is not None else '' 
                 for row in sorted_data]
        
        logger.debug(f"Проверка сортировки по убыванию для колонки: {sort_column}")
        logger.debug(f"Количество строк: {len(values)}")
        logger.debug(f"Первые 5 значений: {values[:5]}")
        logger.debug(f"Последние 5 значений: {values[-5:] if len(values) > 5 else values}")
        
        try:
            assert values == sorted(values, reverse=True), \
                f'Данные не отсортированы по убыванию для колонки {sort_column}'
            logger.debug("✓ Проверка сортировки по убыванию пройдена успешно")
        except AssertionError as e:
            logger.error(f"Ошибка при сортировке: {e}")
            logger.error(f"Первые 10 значений: {values[:10]}")
            logger.error(f"Ожидаемый порядок: {sorted(values, reverse=True)[:10]}")
            raise
    
    @pytest.mark.parametrize('csv_file', 
                          [pytest.param(f, id=f.name) 
                           for f in (Path(__file__).parent.parent / 'data').glob('*.csv')],
                          indirect=True)
    def test_sort_numeric_columns(self, processor: DataProcessor, csv_data: Dict[str, Any]):
        """Тестирование сортировки числовых колонок."""
        logger.debug("\n" + "="*80)
        logger.debug(f"Запуск теста сортировки числовых колонок для файла: {processor.file_path.name}")
        
        if not processor._data:
            logger.warning("Нет данных для тестирования")
            pytest.skip('Нет данных для тестирования')
        
        # Ищем числовые колонки
        numeric_columns = [col for col, col_info in csv_data.items()
                          if col_info.is_numeric]
        
        if not numeric_columns:
            logger.warning("Нет числовых колонок для тестирования сортировки")
            pytest.skip('Нет числовых колонок для тестирования сортировки')
        
        # Тестируем сортировку для первой числовой колонки
        sort_column = numeric_columns[0]
        logger.debug(f"Выбрана числовая колонка для сортировки: '{sort_column}'")
        
        # Логируем информацию о колонке
        col_info = csv_data[sort_column]
        logger.debug(f"Информация о колонке: min={col_info.min_value}, max={col_info.max_value}, "
                    f"avg={col_info.avg_value:.2f}, samples={col_info.sample_values}")
        
        # Сортируем по возрастанию
        logger.debug("\nСортировка по возрастанию:")
        sorted_asc = processor.sort_data(processor._data, f"{sort_column}=asc")
        values_asc = [float(row[sort_column]) if row[sort_column] is not None and str(row[sort_column]).strip() else 0.0 
                     for row in sorted_asc]
        
        if values_asc:  # Если есть значения для проверки
            logger.debug(f"Первые 5 значений: {values_asc[:5]}")
            logger.debug(f"Последние 5 значений: {values_asc[-5:] if len(values_asc) > 5 else values_asc}")
            
            try:
                assert values_asc == sorted(values_asc), \
                    f'Числовые данные не отсортированы по возрастанию для колонки {sort_column}'
                logger.debug("✓ Проверка сортировки по возрастанию пройдена успешно")
            except AssertionError as e:
                logger.error(f"Ошибка при сортировке по возрастанию: {e}")
                logger.error(f"Первые 10 значений: {values_asc[:10]}")
                logger.error(f"Ожидаемый порядок: {sorted(values_asc)[:10]}")
                raise
        else:
            logger.warning("Нет числовых значений для проверки сортировки по возрастанию")
        
        # Сортируем по убыванию
        logger.debug("\nСортировка по убыванию:")
        sorted_desc = processor.sort_data(processor._data, f"{sort_column}=desc")
        values_desc = [float(row[sort_column]) if row[sort_column] is not None and str(row[sort_column]).strip() else 0.0 
                      for row in sorted_desc]
        
        if values_desc:  # Если есть значения для проверки
            logger.debug(f"Первые 5 значений: {values_desc[:5]}")
            logger.debug(f"Последние 5 значений: {values_desc[-5:] if len(values_desc) > 5 else values_desc}")
            
            try:
                assert values_desc == sorted(values_asc, reverse=True), \
                    f'Числовые данные не отсортированы по убыванию для колонки {sort_column}'
                logger.debug("✓ Проверка сортировки по убыванию пройдена успешно")
            except AssertionError as e:
                logger.error(f"Ошибка при сортировке по убыванию: {e}")
                logger.error(f"Первые 10 значений: {values_desc[:10]}")
                logger.error(f"Ожидаемый порядок: {sorted(values_asc, reverse=True)[:10]}")
                raise
        else:
            logger.warning("Нет числовых значений для проверки сортировки по убыванию")
    
    @pytest.mark.parametrize('csv_file', 
                          [pytest.param(f, id=f.name) 
                           for f in (Path(__file__).parent.parent / 'data').glob('*.csv')],
                          indirect=True)
    def test_sort_invalid_column(self, processor: DataProcessor):
        """Тестирование обработки несуществующей колонки."""
        logger.debug("\n" + "="*80)
        logger.debug(f"Запуск теста с несуществующей колонкой для файла: {processor.file_path.name}")
        
        if not processor._data:
            logger.warning("Нет данных для тестирования")
            pytest.skip('Нет данных для тестирования')
        
        invalid_column = "nonexistent_column"
        logger.debug(f"Попытка сортировки по несуществующей колонке: '{invalid_column}'")
        
        with pytest.raises(ValueError, match=f"Колонка \"{invalid_column}\" не найдена") as exc_info:
            processor.sort_data(processor._data, f"{invalid_column}=asc")
        
        logger.debug(f"✓ Ожидаемая ошибка при сортировке по несуществующей колонке: {exc_info.value}")
    
    @pytest.mark.parametrize('csv_file',
                          [pytest.param(f, id=f.name)
                           for f in (Path(__file__).parent.parent / 'data').glob('*.csv')],
                          indirect=True)
    def test_sort_invalid_order(self, processor: DataProcessor, csv_data: Dict[str, Any]):
        """Тестирование обработки неверного направления сортировки."""
        logger.debug("\n" + "="*80)
        logger.debug(f"Запуск теста с неверным направлением сортировки для файла: {processor.file_path.name}")
        
        if not processor._data:
            logger.warning("Нет данных для тестирования")
            pytest.skip('Нет данных для тестирования')
            
        # Выбираем первую колонку для тестирования
        if not csv_data:
            logger.warning("Нет колонок для тестирования")
            pytest.skip('Нет колонок для тестирования')
            
        test_column = next(iter(csv_data.keys()))
        invalid_order = "invalid_order"
        logger.debug(f"Попытка сортировки с неверным направлением: '{test_column}={invalid_order}'")
        
        with pytest.raises(ValueError, match='Неподдерживаемое направление сортировки') as exc_info:
            processor.sort_data(processor._data, f"{test_column}={invalid_order}")
        
        logger.debug(f"✓ Ожидаемая ошибка при неверном направлении сортировки: {exc_info.value}")

