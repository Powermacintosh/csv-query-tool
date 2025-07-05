import csv
from pathlib import Path
from typing import Dict, Set, Optional, Any
from dataclasses import dataclass
from enum import Enum, auto


class ColumnType(Enum):
    INTEGER = auto()
    FLOAT = auto()
    STRING = auto()
    BOOLEAN = auto()
    UNKNOWN = auto()


@dataclass
class ColumnInfo:
    name: str
    type: ColumnType
    sample_values: Set[str]
    is_numeric: bool = False
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    avg_value: Optional[float] = None


def detect_type(value: str) -> ColumnType:
    """Определяет тип значения."""
    if not value:
        return ColumnType.UNKNOWN
    
    # Пробуем определить булево значение
    lower_val = value.lower()
    if lower_val in ('true', 'false', 'yes', 'no'):
        return ColumnType.BOOLEAN
    
    # Пробуем определить целое число
    if value.isdigit() or (value.startswith('-') and value[1:].isdigit()):
        return ColumnType.INTEGER
    
    # Пробуем определить число с плавающей точкой
    try:
        float(value)
        # Проверяем, что это действительно float, а не int в строковом формате
        if '.' in value or 'e' in value.lower():
            return ColumnType.FLOAT
        return ColumnType.INTEGER
    except ValueError:
        pass
    
    # Если не удалось определить числовой тип, считаем строкой
    return ColumnType.STRING


def analyze_csv_file(file_path: Path, sample_size: int = 10) -> Dict[str, ColumnInfo]:
    """
    Анализирует CSV-файл и возвращает информацию о колонках.
    
    Args:
        file_path: Путь к CSV-файлу
        sample_size: Количество строк для анализа (0 - все строки)
    
    Returns:
        Словарь с информацией о колонках
    """
    columns: Dict[str, ColumnInfo] = {}
    total_rows = 0
    
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        # Инициализируем информацию о колонках
        for col in reader.fieldnames:
            columns[col] = ColumnInfo(
                name=col,
                type=ColumnType.UNKNOWN,
                sample_values=set(),
                is_numeric=False
            )
        
        # Анализируем строки
        for i, row in enumerate(reader):
            if sample_size > 0 and i >= sample_size:
                break
                
            for col, value in row.items():
                if not value:
                    continue
                    
                col_info = columns[col]
                col_info.sample_values.add(value)
                
                # Обновляем тип колонки на основе текущего значения
                value_type = detect_type(value)
                if col_info.type == ColumnType.UNKNOWN:
                    col_info.type = value_type
                elif col_info.type != value_type:
                    # Если тип изменился, корректируем его
                    if col_info.type == ColumnType.INTEGER and value_type == ColumnType.FLOAT:
                        col_info.type = ColumnType.FLOAT
                    elif col_info.type != ColumnType.STRING:
                        col_info.type = ColumnType.STRING
            
            total_rows += 1
    
    # Анализируем числовые колонки
    for col_info in columns.values():
        if col_info.type in (ColumnType.INTEGER, ColumnType.FLOAT):
            col_info.is_numeric = True
            
            # Преобразуем значения в числа и находим min/max/avg
            numeric_values = []
            for val in col_info.sample_values:
                try:
                    num_val = float(val)
                    numeric_values.append(num_val)
                except (ValueError, TypeError):
                    continue
            
            if numeric_values:
                col_info.min_value = min(numeric_values)
                col_info.max_value = max(numeric_values)
                col_info.avg_value = sum(numeric_values) / len(numeric_values)
    
    return columns


def get_test_values_for_column(col_info: ColumnInfo) -> Dict[str, Any]:
    """Генерирует тестовые значения для колонки."""
    if not col_info.sample_values:
        return {}
    
    sample = next(iter(col_info.sample_values))
    
    if col_info.type == ColumnType.BOOLEAN:
        return {
            'eq': sample,
            'neq': 'true' if sample.lower() == 'false' else 'false'
        }
    elif col_info.is_numeric:
        return {
            'eq': sample,
            'gt': str(float(sample) + 1) if col_info.max_value is None or float(sample) < col_info.max_value else str(float(sample) - 1),
            'lt': str(float(sample) - 1) if col_info.min_value is None or float(sample) > col_info.min_value else str(float(sample) + 1)
        }
    else:  # STRING или UNKNOWN
        return {
            'eq': sample,
            'neq': f"not_{sample}"
        }
