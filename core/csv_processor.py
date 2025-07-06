import csv, logging.config
from enum import Enum
from pathlib import Path
from tabulate import tabulate
from dataclasses import dataclass
from core.logger import logger_config
from typing import Dict, List, Optional, Union

logging.config.dictConfig(logger_config)
logger = logging.getLogger('tool_logger')

class FilterOperator(Enum):
    """Поддерживаемые операторы фильтрации."""
    EQUAL = '='
    GREATER = '>'
    LESS = '<'


class AggregationOperation(Enum):
    """Поддерживаемые операции агрегации."""
    AVG = 'avg'
    MIN = 'min'
    MAX = 'max'


@dataclass
class FilterCondition:
    """Условие фильтрации."""
    column: str
    operator: FilterOperator
    value: str

    @classmethod
    def from_string(cls, condition_str: str) -> Optional['FilterCondition']:
        """
        Создает условие фильтрации из строки.
        
        Args:
            condition_str: Строка с условием фильтрации (например, "price>100")
            
        Returns:
            FilterCondition или None, если строка пустая или содержит только пробелы
            
        Raises:
            ValueError: Если строка содержит ошибки форматирования
        """
        # Обработка пустых условий
        if not condition_str or not condition_str.strip():
            return None
            
        condition_str = condition_str.strip()
        logger.debug('Обрабатываем условие: %s', condition_str)
        
        operators = {op.value: op for op in FilterOperator}
        supported_operators = list(operators.keys())
        logger.debug('Доступные операторы: %s', supported_operators)
        
        # Проверяем наличие недопустимых операторов
        invalid_operators = ['<>', '!=', '<=', '>=', '==']
        for invalid_op in invalid_operators:
            if invalid_op in condition_str:
                error_msg = (
                    f'Неподдерживаемый оператор "{invalid_op}". '
                    f'Используйте один из: {", ".join(supported_operators)}'
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
        
        # Проверяем наличие хотя бы одного поддерживаемого оператора
        has_any_operator = any(op in condition_str for op in supported_operators)
        if not has_any_operator:
            error_msg = (
                f'Не найден поддерживаемый оператор в условии: "{condition_str}". '
                f'Используйте один из: {", ".join(supported_operators)}'
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Ищем поддерживаемые операторы в порядке убывания длины
        for op_str in sorted(supported_operators, key=len, reverse=True):
            op_pos = condition_str.find(op_str)
            if op_pos != -1:
                column = condition_str[:op_pos].strip()
                value = condition_str[op_pos + len(op_str):].strip()
                
                logger.debug('Найден оператор: %s, колонка: %s, значение: %s', 
                           op_str, column, value)
                
                if not column:
                    error_msg = 'Не указано имя колонки для фильтрации'
                    logger.error(error_msg)
                    raise ValueError(error_msg)
                    
                if not value:
                    error_msg = 'Не указано значение для сравнения'
                    logger.error(error_msg)
                    raise ValueError(error_msg)
                    
                return cls(column=column, operator=operators[op_str], value=value)
        
        # Эта строка должна быть недостижима из-за проверки has_any_operator выше
        error_msg = (
            f'Не удалось разобрать условие: "{condition_str}". '
            f'Используйте формат: "поле{''.join(supported_operators)}значение"'
        )
        logger.error(error_msg)
        raise ValueError(error_msg)


@dataclass
class Aggregation:
    """Условие агрегации."""
    column: str
    operation: AggregationOperation

    @classmethod
    def from_string(cls, agg_str: str) -> 'Aggregation':
        """
        Создает условие агрегации из строки.
        
        Args:
            agg_str: Строка в формате "column=operation"
            
        Returns:
            Aggregation: Объект с условием агрегации
            
        Raises:
            ValueError: Если строка имеет неверный формат или операция не поддерживается
        """
        if not agg_str or not agg_str.strip():
            error_msg = 'Пустая строка агрегации. Используйте формат: column=operation'
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        # Проверяем наличие знака равенства
        if '=' not in agg_str:
            error_msg = (
                f'Неверный формат строки агрегации: "{agg_str}". '
                f'Используйте формат: column=operation, где operation одно из: '
                f'{", ".join(op.value for op in AggregationOperation)}'
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        try:
            # Разделяем строку на части
            parts = agg_str.split('=', 1)
            if len(parts) != 2:
                raise ValueError('Ожидается ровно один знак "="')
                
            column = parts[0].strip()
            op_str = parts[1].strip()
            
            # Проверяем, что колонка указана
            if not column:
                error_msg = 'Не указано имя колонки для агрегации'
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Проверяем, что операция указана
            if not op_str:
                error_msg = (
                    f'Не указана операция агрегации. '
                    f'Доступные операции: {", ".join(op.value for op in AggregationOperation)}'
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
                
            # Пробуем получить операцию агрегации
            try:
                operation = AggregationOperation(op_str.lower())
            except ValueError:
                available_ops = ', '.join(f"'{op.value}'" for op in AggregationOperation)
                error_msg = (
                    f'Неподдерживаемая операция агрегации: "{op_str}". '
                    f'Используйте одну из: {available_ops}'
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
                
            return cls(column=column, operation=operation)
            
        except ValueError as e:
            # Если уже обработали ошибку, просто пробрасываем её
            if str(e).startswith(('Неверный формат', 'Не указано', 'Неподдерживаемая операция')):
                raise
                
            # Для остальных ошибок формируем общее сообщение
            error_msg = (
                f'Неверный формат строки агрегации: "{agg_str}". '
                f'Используйте: column=operation, где operation одно из: '
                f'{", ".join(op.value for op in AggregationOperation)}'
            )
            logger.error('%s: %s', error_msg, str(e))
            raise ValueError(error_msg) from e


class DataRow(Dict[str, str]):
    """Строка данных с типизированным доступом к значениям."""
    
    def get_numeric(self, key: str) -> Optional[float]:
        """Возвращает числовое значение или None, если преобразование невозможно."""
        value = self.get(key)
        if value is None:
            return None
            
        try:
            return float(value)
        except (ValueError, TypeError):
            return None


class DataProcessor:
    """Обработчик CSV-данных с поддержкой фильтрации и агрегации."""
    
    def __init__(self, file_path: Union[str, Path]):
        self.file_path = Path(file_path)
        self._data: List[DataRow] = []
        
    def load_data(self) -> None:
        """Загружает данные из CSV файла."""
        if not self.file_path.exists():
            error_msg = f'Файл не найден: {self.file_path}'
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
            
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                self._data = [DataRow(row) for row in reader]
        except csv.Error as e:
            error_msg = f'Ошибка при чтении CSV файла: {e}'
            logger.error(error_msg)
            raise ValueError(error_msg) from e
    
    def filter_data(self, condition: Optional[str] = None) -> List[DataRow]:
        """
        Фильтрует данные по условию.
        
        Args:
            condition: Строка условия в формате "column>value" или None/пустая строка для возврата всех данных
            
        Returns:
            Отфильтрованный список строк или копию всех данных, если условие пустое
            
        Raises:
            ValueError: Если условие фильтрации содержит ошибки
        """
        if not condition or not condition.strip():
            return self._data.copy()
            
        try:
            filter_cond = FilterCondition.from_string(condition)
            result = []
            
            # Проверяем существование колонки в данных
            if not self._data:
                return []
                
            available_columns = set(self._data[0].keys())
            if filter_cond.column not in available_columns:
                raise ValueError(
                    f'Колонка "{filter_cond.column}" не найдена. '
                    f'Доступные колонки: {", ".join(sorted(available_columns))}'
                )
            
            has_matches = False
            for row in self._data:
                if filter_cond.column not in row:
                    continue
                    
                cell_value = row[filter_cond.column]
                
                # Пытаемся сравнить как числа, если возможно
                if (num_value := row.get_numeric(filter_cond.column)) is not None:
                    try:
                        filter_num = float(filter_cond.value)
                        if self._compare_numeric(num_value, filter_cond.operator, filter_num):
                            result.append(row.copy())
                            has_matches = True
                        continue
                    except ValueError:
                        pass
                
                # Иначе сравниваем как строки
                if self._compare_strings(cell_value, filter_cond.operator, filter_cond.value):
                    result.append(row.copy())
                    has_matches = True
            
            # Для обратной совместимости с тестами возвращаем пустой список, а не вызываем исключение
            # при несовпадении типов данных и оператора
            if not has_matches and result:
                first_value = next((row[filter_cond.column] for row in self._data if filter_cond.column in row), "")
                if first_value and not first_value.strip().replace('.', '', 1).isdigit() and filter_cond.operator != FilterOperator.EQUAL:
                    # Логируем предупреждение, но не прерываем выполнение
                    logger.warning(
                        'Оператор %s не может быть применен к строковому значению. '
                        'Используйте "=" для сравнения строк.',
                        filter_cond.operator.value
                    )
                    return []
                    
            return result
            
        except ValueError as e:
            # Перехватываем только ValueError, чтобы не перехватывать другие исключения
            error_msg = str(e)
            logger.error('Ошибка при фильтрации: %s', error_msg)
            raise ValueError(error_msg) from None
        except Exception as e:
            error_msg = f'Непредвиденная ошибка при фильтрации: {str(e)}'
            logger.error(error_msg, exc_info=True)
            raise ValueError(error_msg) from e
    
    def aggregate_data(self, data: List[DataRow], aggregation: str) -> Optional[dict]:
        """
        Выполняет агрегацию данных.
        
        Args:
            data: Данные для агрегации
            aggregation: Строка агрегации в формате "column=operation"
            
        Returns:
            Словарь с информацией об агрегации или None, если данные невалидны
            {
                'operation': str,  # Название операции (avg, min, max)
                'column': str,     # Название колонки
                'value': float,    # Результат агрегации
                'count': int       # Количество обработанных значений
            }
            
        Raises:
            ValueError: Если агрегация содержит ошибки
        """
        if not data:
            return None
            
        try:
            agg = Aggregation.from_string(aggregation)
            values = []
            
            # Проверяем существование колонки
            available_columns = set(data[0].keys())
            if agg.column not in available_columns:
                raise ValueError(
                    f'Колонка "{agg.column}" не найдена. '
                    f'Доступные колонки: {", ".join(sorted(available_columns))}'
                )
            
            # Собираем числовые значения
            non_numeric_count = 0
            for row in data:
                if agg.column not in row:
                    continue
                    
                try:
                    value = row[agg.column]
                    if value is not None and str(value).strip():
                        values.append(float(value))
                except (ValueError, TypeError):
                    non_numeric_count += 1
            
            # Проверяем, есть ли числовые значения
            if not values:
                # Для обратной совместимости с тестами возвращаем None, а не вызываем исключение
                logger.warning(
                    'Не удалось выполнить агрегацию: колонка %s не содержит числовых значений',
                    agg.column
                )
                return None
                
            # Если есть нечисловые значения, выводим предупреждение
            if non_numeric_count > 0:
                logger.warning(
                    'Пропущено %d нечисловых значений в колонке %s',
                    non_numeric_count, agg.column
                )
            
            result = {
                'operation': agg.operation.value,
                'column': agg.column,
                'count': len(values)
            }
            
            # Вычисляем результат агрегации
            try:
                if agg.operation == AggregationOperation.AVG:
                    result['value'] = sum(values) / len(values)
                elif agg.operation == AggregationOperation.MIN:
                    result['value'] = min(values)
                elif agg.operation == AggregationOperation.MAX:
                    result['value'] = max(values)
                else:
                    raise ValueError(f'Неподдерживаемая операция агрегации: {agg.operation}')
            except Exception as e:
                raise ValueError(f'Ошибка при вычислении агрегации: {str(e)}') from e
                
            return result
                
        except ValueError as e:
            # Перехватываем только ValueError, чтобы не перехватывать другие исключения
            error_msg = str(e)
            logger.error('Ошибка при агрегации: %s', error_msg)
            raise ValueError(error_msg) from None
        except Exception as e:
            error_msg = f'Непредвиденная ошибка при агрегации: {str(e)}'
            logger.error(error_msg, exc_info=True)
            raise ValueError(error_msg) from e
    
    @staticmethod
    def _compare_numeric(value: float, operator: FilterOperator, other: float) -> bool:
        """Сравнивает числовые значения."""
        if operator == FilterOperator.EQUAL:
            return abs(value - other) < 1e-9  # Для сравнения float
        elif operator == FilterOperator.GREATER:
            return value > other
        elif operator == FilterOperator.LESS:
            return value < other
        return False
    
    @staticmethod
    def _compare_strings(value: Optional[str], operator: FilterOperator, other: str) -> bool:
        """
        Сравнивает строковые значения.
        
        Args:
            value: Значение для сравнения (может быть None)
            operator: Оператор сравнения
            other: Значение для сравнения с value
            
        Returns:
            bool: Результат сравнения. Если value равно None, возвращает False.
        """
        if value is None:
            return False
            
        if operator == FilterOperator.EQUAL:
            return value == other
        elif operator == FilterOperator.GREATER:
            return value > other
        elif operator == FilterOperator.LESS:
            return value < other
        return False
    
    def display_results(self, data: List[DataRow]) -> None:
        """Выводит данные в виде таблицы."""
        if not data:
            error_msg = 'Нет данных для отображения'
            logger.error(error_msg)
            return
            
        print(tabulate(data, headers='keys', tablefmt='grid'))



