import sys, argparse
from core.csv_processor import DataProcessor

from tabulate import tabulate


def main() -> None:
    """Точка входа в приложение."""
    parser = argparse.ArgumentParser(description='Обработчик CSV файлов')
    
    # Добавляем аргументы с action='append', чтобы собирать все вхождения
    parser.add_argument('--file', required=True, help='Путь к CSV файлу')
    parser.add_argument('--where', action='append', help='Условие фильтрации (например, "brand=apple" или "price>500")')
    parser.add_argument('--aggregate', action='append', help='Агрегация (например, "price=avg")')
    
    args = parser.parse_args()
    
    # Проверяем количество аргументов
    if args.where and len(args.where) > 1:
        print('Ошибка: можно указать только одно условие --where', file=sys.stderr)
        sys.exit(1)
        
    if args.aggregate and len(args.aggregate) > 1:
        print('Ошибка: можно указать только одну агрегацию --aggregate', file=sys.stderr)
        sys.exit(1)
    
    # Преобразуем списки в одиночные значения для обратной совместимости
    args.where = args.where[0] if args.where else None
    args.aggregate = args.aggregate[0] if args.aggregate else None
    
    try:
        # Инициализация и загрузка данных
        processor = DataProcessor(args.file)
        processor.load_data()
        
        # Применение фильтрации
        filtered_data = processor.filter_data(args.where)
        
        # Применение агрегации или вывод данных
        if args.aggregate:
            result = processor.aggregate_data(filtered_data, args.aggregate)
            if result is not None:

                # Заголовок с информацией о запросе
                operation_name = {
                    'avg': 'Среднее значение',
                    'min': 'Минимальное значение',
                    'max': 'Максимальное значение'
                }.get(result['operation'], result['operation'].capitalize())
                
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
            processor.display_results(filtered_data)
            
    except Exception as e:
        print(f'Ошибка: {e}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()