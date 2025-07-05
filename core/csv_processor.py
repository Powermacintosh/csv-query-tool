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
        invalid_operators = ['<>', '!=', '<=', '>=']
        for invalid_op in invalid_operators:
            if invalid_op in condition_str:
                error_msg = 'Неподдерживаемый оператор условия'
                logger.error('%s: %s', error_msg, invalid_op)
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
        
        error_msg = f'Не найден поддерживаемый оператор в условии: {condition_str}'
        logger.error(error_msg)
        raise ValueError(error_msg)


@dataclass
class Aggregation:
    """Условие агрегации."""
    column: str
    operation: AggregationOperation

    @classmethod
    def from_string(cls, agg_str: str) -> 'Aggregation':
        """Создает условие агрегации из строки."""
        try:
            column, op_str = agg_str.split('=', 1)
            column = column.strip()
            op_str = op_str.strip()
            
            if not column:
                error_msg = 'Не указано имя колонки для агрегации'
                logger.error(error_msg)
                raise ValueError(error_msg)
                
            try:
                operation = AggregationOperation(op_str)
            except ValueError as e:
                error_msg = f'Неподдерживаемая операция агрегации: {op_str}. '
                error_msg += f'Доступно: {', '.join(op.value for op in AggregationOperation)}'
                logger.error(error_msg)
                raise ValueError(error_msg) from e
                
            return cls(column=column, operation=operation)
            
        except ValueError as e:
            error_msg = f'Неверный формат агрегации: {agg_str}. '
            error_msg += 'Используйте "column=operation" (например, "price=avg")'
            logger.error(error_msg)
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
        """
        if not condition or not condition.strip():
            return self._data.copy()
            
        try:
            filter_cond = FilterCondition.from_string(condition)
            result = []
            
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
                        continue
                    except ValueError:
                        pass
                
                # Иначе сравниваем как строки
                if self._compare_strings(cell_value, filter_cond.operator, filter_cond.value):
                    result.append(row.copy())
                    
            return result
            
        except Exception as e:
            error_msg = f'Ошибка при фильтрации данных: {e}'
            logger.error(error_msg)
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
        """
        try:
            agg = Aggregation.from_string(aggregation)
            values = []
            
            for row in data:
                if agg.column not in row:
                    continue
                try:
                    values.append(float(row[agg.column]))
                except (ValueError, TypeError):
                    continue
            
            if not values:
                return None
                
            result = {
                'operation': agg.operation.value,
                'column': agg.column,
                'count': len(values)
            }
            
            if agg.operation == AggregationOperation.AVG:
                result['value'] = sum(values) / len(values)
            elif agg.operation == AggregationOperation.MIN:
                result['value'] = min(values)
            elif agg.operation == AggregationOperation.MAX:
                result['value'] = max(values)
                
            return result
                
        except Exception as e:
            error_msg = f'Ошибка при агрегации данных: {e}'
            logger.error(error_msg)
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



