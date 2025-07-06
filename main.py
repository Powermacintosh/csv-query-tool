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
    # Настройки валидации аргументов
    ARG_VALIDATION = {
        'file': {
            'required': True,
            'help': 'Путь к CSV файлу',
            'metavar': 'ПУТЬ_К_ФАЙЛУ'
        },
        'where': {
            'required': False,
            'help': 'Условие фильтрации (например, "brand=apple" или "price>500")',
            'action': 'append',
            'metavar': 'УСЛОВИЕ'
        },
        'aggregate': {
            'required': False,
            'help': 'Агрегация (например, "price=avg")',
            'action': 'append',
            'metavar': 'КОЛОНКА=ОПЕРАЦИЯ'
        },
        'order_by': {
            'required': False,
            'help': 'Сортировка результатов (например, "price=desc" или "name=asc")',
            'metavar': 'КОЛОНКА=ПОРЯДОК',
            'default': None
        }
    }

    parser = argparse.ArgumentParser(
        description='Утилита для работы с CSV файлами',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            'Примеры использования:\n'
            '  Просмотр данных: python main.py --file data.csv\n'
            '  Фильтрация: python main.py --file data.csv --where "price>100"\n'
            '  Сортировка: python main.py --file data.csv --order-by "price=desc"\n'
            '  Агрегация: python main.py --file data.csv --aggregate "price=avg"\n'
            '  Комбинирование: python main.py --file data.csv --where "category=Electronics" --order-by "price=asc" --aggregate "price=avg"\n\n'
            'Доступные операторы фильтрации: >, <, =\n'
            'Доступные операции агрегации: avg, min, max\n'
            'Формат сортировки: column=order, где order: asc (по возрастанию) или desc (по убыванию)'
        )
    )
    
    # Создаем парсер аргументов на основе ARG_VALIDATION
    for arg_name, arg_params in ARG_VALIDATION.items():
        parser.add_argument(
            f'--{arg_name.replace("_", "-")}',
            help=arg_params['help'],
            **{k: v for k, v in arg_params.items() if k not in ['required', 'help']}
        )
    
    try:
        args = parser.parse_args()
    except Exception as e:
        print_error('Ошибка при разборе аргументов командной строки', str(e))
        sys.exit(1)
        

    
    # Проверка на несколько однотипных аргументов
    seen_args = set()
    arg_names = []
    
    # Собираем все имена аргументов (с --)
    for arg in sys.argv[1:]:
        if arg.startswith('--'):
            arg_names.append(arg)
    
    # Проверяем дубликаты
    for arg_name in arg_names:
        if arg_name in seen_args:
            print_error(f'Аргумент {arg_name} может быть указан только один раз')
            sys.exit(1)
        seen_args.add(arg_name)
    
    # Валидация аргументов
    for arg_name, validation in ARG_VALIDATION.items():
        arg_value = getattr(args, arg_name, None)
        
        # Проверка обязательных аргументов
        if validation['required'] and not arg_value:
            print_error(f'Не указан обязательный аргумент --{arg_name}')
            print(f'Использование: {validation["help"]}')
            sys.exit(1)
    
    # Получаем значения аргументов
    where_condition = args.where[0] if args.where else None
    aggregate_condition = args.aggregate[0] if args.aggregate else None
    order_by = args.order_by
    file_path = args.file
    
    try:
        # Проверяем существование файла
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f'Файл не найден: {file_path}')
            
        if not file_path.lower().endswith('.csv'):
            print('Предупреждение: файл не имеет расширения .csv', file=sys.stderr)
        
        # Инициализация и загрузка данных
        processor = DataProcessor(file_path)
        processor.load_data()
        
        # Применение фильтрации
        filtered_data = processor.filter_data(where_condition)
        
        # Проверяем, есть ли данные после фильтрации
        if where_condition and not filtered_data:
            print('Ничего не найдено по указанному условию фильтрации')
            return
            
        # Применение сортировки, если указана
        if order_by:
            filtered_data = processor.sort_data(filtered_data, order_by)
        
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