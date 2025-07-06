import sys
import os
import argparse
from typing import Optional

from core.csv_processor import DataProcessor
from tabulate import tabulate


def print_error(message: str, details: Optional[str] = None) -> None:
    """Выводит сообщение об ошибке в stderr."""
    print(f'Ошибка: {message}', file=sys.stderr)
    if details:
        print(f'Подробности: {details}', file=sys.stderr)

def main() -> None:
    """Точка входа в приложение."""
    parser = argparse.ArgumentParser(
        description='Утилита для работы с CSV файлами',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            'Примеры использования:\n'
            '  Просмотр данных: python main.py --file data.csv\n'
            '  Фильтрация: python main.py --file data.csv --where "price>100"\n'
            '  Агрегация: python main.py --file data.csv --aggregate "price=avg"\n'
            '  Комбинирование: python main.py --file data.csv --where "brand=Apple" --aggregate "price=max"\n\n'
            'Доступные операторы фильтрации: >, <, =\n'
            'Доступные операции агрегации: avg, min, max'
        )
    )
    
    parser.add_argument(
        '--file', 
        required=True, 
        help='Путь к CSV файлу',
        metavar='ПУТЬ_К_ФАЙЛУ'
    )
    parser.add_argument(
        '--where', 
        action='append', 
        help='Условие фильтрации (например, "brand=apple" или "price>500")',
        metavar='УСЛОВИЕ'
    )
    parser.add_argument(
        '--aggregate', 
        action='append', 
        help='Агрегация (например, "price=avg")',
        metavar='КОЛОНКА=ОПЕРАЦИЯ'
    )
    
    try:
        args = parser.parse_args()
    except Exception as e:
        print_error('Ошибка при разборе аргументов командной строки', str(e))
        sys.exit(1)
    
    if args.where and len(args.where) > 1:
        print_error('Можно указать только одно условие --where')
        print('Использование: --where "колонка=значение" или --where "колонка>значение"')
        sys.exit(1)
        
    if args.aggregate and len(args.aggregate) > 1:
        print_error('Можно указать только одну агрегацию --aggregate')
        print('Использование: --aggregate "колонка=операция" (операции: avg, min, max)')
        sys.exit(1)
    
    # Преобразуем списки в одиночные значения для обратной совместимости
    where_condition = args.where[0] if args.where else None
    aggregate_condition = args.aggregate[0] if args.aggregate else None
    
    try:
        # Проверяем существование файла
        if not os.path.isfile(args.file):
            raise FileNotFoundError(f'Файл не найден: {args.file}')
            
        if not args.file.lower().endswith('.csv'):
            print('Предупреждение: файл не имеет расширения .csv', file=sys.stderr)
        
        # Инициализация и загрузка данных
        processor = DataProcessor(args.file)
        processor.load_data()
        
        # Применение фильтрации
        filtered_data = processor.filter_data(where_condition)
        
        # Проверяем, есть ли данные после фильтрации
        if where_condition and not filtered_data:
            print('Ничего не найдено по указанному условию фильтрации')
            return
        
        # Применение агрегации или вывод данных
        if aggregate_condition:
            try:
                result = processor.aggregate_data(filtered_data, aggregate_condition)
                if result is not None:
                    # Определяем названия операций
                    operation_names = {
                        'avg': 'Среднее значение',
                        'min': 'Минимальное значение',
                        'max': 'Максимальное значение',
                        'count': 'Количество значений:'
                    }
                    
                    # Получаем читаемое название операции
                    operation_name = operation_names.get(result['operation'], result['operation'].capitalize())
                    
                    # Формируем данные для таблицы
                    table_data = [
                        ['Операция:', operation_name],
                        ['Колонка:', result['column']],
                        ['Количество значений:', result['count']],
                        ['Результат:', f"{result['value']:.2f}"]
                    ]
                    
                    # Выводим таблицу с результатом
                    print(tabulate(table_data, tablefmt='grid', colalign=('right', 'left')))
                else:
                    print('\nНе удалось выполнить агрегацию: нет подходящих данных')
                    
            except ValueError as e:
                print_error('Ошибка при выполнении агрегации', str(e))
                print('Проверьте правильность формата: --aggregate "колонка=операция"')
                print('Доступные операции: avg, min, max')
                sys.exit(1)
        else:
            # Вывод отфильтрованных данных в виде таблицы
            if not filtered_data:
                print('Нет данных для отображения')
            else:
                processor.display_results(filtered_data)
                print(f'\nНайдено записей: {len(filtered_data)}')
                
    except FileNotFoundError as e:
        print_error('Файл не найден', str(e))
        sys.exit(1)
    except PermissionError:
        print_error('Ошибка доступа к файлу', f'Нет прав на чтение файла: {args.file}')
        sys.exit(1)
    except Exception as e:
        print_error('Произошла непредвиденная ошибка', str(e))
        sys.exit(1)


if __name__ == '__main__':
    main()