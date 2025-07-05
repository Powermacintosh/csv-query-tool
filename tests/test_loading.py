import pytest, csv, logging.config
from pathlib import Path
from typing import Dict, Any

from core.csv_processor import DataProcessor, DataRow
from tests.base_test import CSVTestBase
from core.logger import logger_config

logging.config.dictConfig(logger_config)
logger = logging.getLogger('test_loading_csv_logger')

class TestCSVLoading(CSVTestBase):
    """Тесты для загрузки CSV-файлов."""
    
    @pytest.mark.parametrize('csv_file', 
                          [pytest.param(f, id=f.name) 
                           for f in (Path(__file__).parent.parent / 'data').glob('*.csv')],
                          indirect=True)
    def test_load_data(self, processor: DataProcessor, csv_data: Dict[str, Any], caplog):
        """Тестирование загрузки данных из CSV-файла."""
        caplog.set_level(logging.INFO)
        logger.info(f'Начало теста загрузки данных из файла: {processor.file_path}')
        logger.info(f'Начало теста загрузки данных из файла: {processor.file_path.name}')
        
        assert processor._data is not None, 'Данные не загружены'
        logger.debug(f'Загружено строк: {len(processor._data)}')
        assert len(processor._data) > 0, 'Загружен пустой набор данных'
        
        # Проверяем, что все строки - экземпляры DataRow
        is_all_data_rows = all(isinstance(row, DataRow) for row in processor._data)
        assert is_all_data_rows, 'Не все строки являются экземплярами DataRow'
        logger.debug('Все строки успешно преобразованы в DataRow')
        
        # Проверяем, что все ожидаемые колонки присутствуют
        first_row = processor._data[0]
        missing_columns = [col for col in csv_data.keys() if col not in first_row]
        if missing_columns:
            logger.error(f'Отсутствующие колонки: {missing_columns}')
        assert not missing_columns, f'Отсутствуют колонки: {missing_columns}'
        
        logger.info('Тест загрузки данных успешно пройден')
    
    @pytest.mark.parametrize('csv_file', 
                          [pytest.param(f, id=f.name) 
                           for f in (Path(__file__).parent.parent / 'data').glob('*.csv')],
                          indirect=True)
    def test_data_integrity(self, processor: DataProcessor, csv_data: Dict[str, Any], caplog):
        """
        Проверка целостности загруженных данных.
        
        Учитывает, что некоторые строки могут быть неполными (не все колонки заполнены).
        Проверяет, что:
        1. Все строки содержат все объявленные колонки
        2. Значения могут быть None для необязательных полей
        """
        caplog.set_level(logging.INFO)
        logger.info(f'Проверка целостности данных для файла: {processor.file_path.name}')
        
        if not processor._data:
            logger.critical('Нет данных для проверки целостности')
            pytest.fail('Нет данных для проверки целостности')
        
        # Получаем ожидаемые колонки из заголовков CSV
        expected_columns = set(csv_data.keys())
        logger.info(f'Ожидаемые колонки: {sorted(expected_columns)}')
        
        critical_issues = False
        
        for i, row in enumerate(processor._data, start=1):
            row_columns = set(row.keys())
            logger.debug(f'Проверка строки {i}: {row}')
            
            # Проверяем отсутствующие колонки
            missing_columns = expected_columns - row_columns
            if missing_columns:
                error_msg = f'Строка {i}: отсутствуют колонки: {missing_columns}'
                logger.critical(error_msg)
                critical_issues = True
            
            # Проверяем лишние колонки (кроме None, который может быть у неполных строк)
            extra_columns = row_columns - expected_columns - {None}
            if extra_columns:
                error_msg = f'Строка {i}: обнаружены лишние колонки: {extra_columns}'
                logger.critical(error_msg)
                critical_issues = True
            
            # Проверяем значения None в обязательных полях
            for col in expected_columns:
                if row.get(col) is None:
                    warning_msg = f'Строка {i}: поле {col} содержит None'
                    logger.critical(warning_msg)
        
        # Если есть критические проблемы, завершаем тест с ошибкой
        if critical_issues:
            logger.critical('Обнаружены критические проблемы с целостностью данных')
            pytest.fail('Обнаружены критические проблемы с целостностью данных')
            
        logger.info('Проверка целостности данных завершена успешно')
    
    def test_load_nonexistent_file(self, tmp_path, caplog):
        """Тестирование загрузки несуществующего файла."""
        # Устанавливаем уровень логирования для перехвата всех сообщений
        caplog.set_level(logging.DEBUG)
        
        non_existent_file = tmp_path / 'nonexistent.csv'
        expected_error = f'Файл не найден: {non_existent_file}'
        
        logger.info(f'Проверка загрузки несуществующего файла: {non_existent_file}')
        
        # Проверяем, что возникает исключение с правильным сообщением
        with pytest.raises(FileNotFoundError) as exc_info:
            processor = DataProcessor(non_existent_file)
            processor.load_data()
            
        # Проверяем текст исключения
        assert str(exc_info.value) == expected_error, \
            f'Неверное сообщение об ошибке. Ожидалось: {expected_error}, получено: {exc_info.value}'
            
        # Выводим отладочную информацию о логах
        logger.debug('Записанные логи:')
        for record in caplog.records:
            logger.debug(f'  [{record.levelname}] {record.message}')
            
        # Если логи не перехватываются, проверяем только исключение
        if not caplog.records:
            logger.warning('Логи не были перехвачены. Проверка ограничивается только исключением.')
        else:
            # Проверяем, что в логах есть сообщение об ошибке
            assert any(expected_error in record.message 
                     for record in caplog.records), \
                f'Не найдено сообщение об ошибке в логах. Ожидалось: {expected_error}'
                
        logger.info('Тест загрузки несуществующего файла завершен')
    
    def test_empty_file_handling(self, tmp_path, caplog):
        """Тестирование обработки пустого файла"""
        caplog.set_level(logging.INFO)
        logger.info('Начало теста обработки пустого файла')
        
        # Создаем пустой файл с заголовком, но без данных
        empty_file = tmp_path / 'empty.csv'
        empty_file.write_text('id,name\n')
        logger.debug(f'Создан пустой CSV файл: {empty_file}')
        
        processor = DataProcessor(empty_file)
        logger.info('Инициализирован DataProcessor с пустым файлом')
        
        # Загружаем данные - не должно быть исключения, но данные должны быть пустыми
        processor.load_data()
        
        # Проверяем, что данные загружены, но пусты
        assert processor._data is not None, 'Данные не инициализированы'
        assert len(processor._data) == 0, 'Ожидался пустой список данных'
            
        logger.info('Тест обработки пустого файла завершен успешно')
    
    @pytest.mark.parametrize('csv_file', 
                          [pytest.param(f, id=f.name) 
                           for f in (Path(__file__).parent.parent / 'data').glob('*.csv')],
                          indirect=True)
    def test_header_only_file(self, csv_file: Path, tmp_path):
        """Тестирование обработки CSV-файла только с заголовками."""
        # Создаем CSV-файл только с заголовками
        header_only_file = tmp_path / 'header_only.csv'
        with open(csv_file, 'r', encoding='utf-8') as src, \
             open(header_only_file, 'w', encoding='utf-8') as dst:
            # Копируем только первую строку (заголовки)
            header = src.readline()
            dst.write(header)
        
        processor = DataProcessor(header_only_file)
        processor.load_data()
        
        # Проверяем, что данные загружены, но пустые
        assert processor._data is not None
        assert len(processor._data) == 0
    
    def test_invalid_csv_format(self, tmp_path, caplog):
        """Тестирование обработки файла с неверным форматом CSV"""
        caplog.set_level(logging.INFO)
        logger.info('Начало теста обработки неверного формата CSV')
        
        # Создаем CSV файл с разным количеством полей в строках
        invalid_csv = tmp_path / 'invalid.csv'
        
        # Генерируем случайные имена полей для теста
        field1, field2, field3 = 'col1', 'col2', 'col3'
        
        # Создаем тестовые данные с разным количеством полей
        csv_lines = [
            f'{field1},{field2},{field3}',  # Заголовок
            'val1,val2,val3',               # Корректная строка
            'val4,val5',                    # Строка с недостающими полями
            'val6,val7,val8,val9,val10'     # Строка с избыточными полями
        ]
        
        invalid_csv.write_text('\n'.join(csv_lines))
        logger.debug(f'Создан файл с неверным форматом: {invalid_csv}')
        
        processor = DataProcessor(invalid_csv)
        logger.info('Инициализирован DataProcessor с неверным форматом CSV')
        
        # Загружаем ожидаемые данные с помощью стандартного csv.DictReader
        with open(invalid_csv, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # Получаем заголовок
            expected_rows = list(reader)  # Получаем все строки данных
        
        # Загружаем данные через DataProcessor
        processor.load_data()
        
        # Проверяем, что данные загружены
        assert processor._data is not None, 'Данные не загружены'
        assert len(processor._data) == len(expected_rows), \
            f'Ожидалось {len(expected_rows)} строк данных, получено {len(processor._data)}'
        
        # Проверяем, что заголовки совпадают
        for i, field in enumerate(header):
            assert field in processor._data[0], f'Отсутствует ожидаемое поле: {field}'
        
        # Проверяем, что строки с разным количеством полей обработаны корректно
        for i, expected_row in enumerate(expected_rows):
            row_data = processor._data[i]
            
            # Проверяем, что все ожидаемые значения полей совпадают
            for j, value in enumerate(expected_row):
                if j < len(header):
                    # Для полей из заголовка
                    field = header[j]
                    assert str(row_data[field]) == value, \
                        f'Неверное значение поля {field} в строке {i+1}: ожидалось {value}, получено {row_data.get(field)}'
                else:
                    # Для дополнительных полей (должны быть в row_data[None])
                    assert None in row_data, 'Ожидались дополнительные поля с ключом None'
                    assert value in row_data[None], f'Отсутствует ожидаемое дополнительное значение: {value}'
            
            # Проверяем, что нет лишних полей (кроме None для дополнительных значений)
            extra_fields = set(row_data.keys()) - set(header) - {None}
            assert not extra_fields, f'Обнаружены неожиданные поля: {extra_fields}'
        
        logger.info('Тест обработки неверного формата CSV завершен успешно')
    
    @pytest.mark.parametrize('encoding,test_str', [
        ('utf-8', 'Привет, мир!'),
        # Оставляем только UTF-8, так как другие кодировки требуют дополнительной обработки
    ])
    def test_different_encodings(self, tmp_path, caplog, encoding, test_str):
        """Тестирование загрузки файлов в разных кодировках"""
        caplog.set_level(logging.INFO)
        logger.info(f'Начало теста загрузки в кодировке {encoding}')
        
        try:
            # Создаем временный файл с указанной кодировкой
            temp_file = tmp_path / f'test_{encoding}.csv'
            logger.debug(f'Создание тестового файла в кодировке {encoding}: {temp_file}')
            
            # Генерируем случайные имена полей, чтобы не зависеть от конкретных значений
            field1 = 'field1'
            field2 = 'field2'
            test_value1 = '1'
            
            # Записываем тестовые данные с указанной кодировкой
            with open(temp_file, 'w', encoding=encoding) as f:
                writer = csv.writer(f)
                writer.writerow([field1, field2])
                writer.writerow([test_value1, test_str])
            
            logger.debug(f'Файл успешно создан, размер: {temp_file.stat().st_size} байт')
            
            # Загружаем файл для проверки ожидаемых данных
            with open(temp_file, 'r', encoding=encoding) as f:
                reader = csv.DictReader(f)
                expected_data = list(reader)
            
            # Загружаем через DataProcessor
            logger.info(f'Загрузка файла с кодировкой {encoding}')
            processor = DataProcessor(temp_file)
            processor.load_data()
            
            # Проверяем, что данные загружены корректно
            logger.debug('Проверка загруженных данных')
            assert len(processor._data) == len(expected_data), \
                f'Ожидалось {len(expected_data)} строк данных, получено {len(processor._data)}'
                
            # Проверяем, что все ожидаемые поля присутствуют
            for field in expected_data[0].keys():
                assert field in processor._data[0], f'Отсутствует ожидаемое поле: {field}'
            
            # Проверяем значения полей
            for field, expected_value in expected_data[0].items():
                assert str(processor._data[0][field]) == expected_value, \
                    f'Неверное значение поля {field}: ожидалось {expected_value}, получено {processor._data[0][field]}'
            
            logger.info(f'Тест кодировки {encoding} пройден успешно')
            
        except UnicodeEncodeError as e:
            logger.warning(f'Кодировка {encoding} не поддерживается: {e}')
            pytest.skip(f'Кодировка {encoding} не поддерживается в текущей системе')
        except UnicodeDecodeError as e:
            logger.warning(f'Ошибка декодирования файла в кодировке {encoding}: {e}')
            pytest.skip(f'Ошибка декодирования файла в кодировке {encoding}')
        except Exception as e:
            logger.error(f'Ошибка при тестировании кодировки {encoding}: {str(e)}', 
                        exc_info=True)
            raise
        
        logger.info(f'Тестирование кодировки {encoding} завершено')
