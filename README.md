# CSV Query Tool

Утилита для обработки CSV-файлов с поддержкой фильтрации и агрегации данных.

## Возможности

- Чтение CSV-файлов с произвольными колонками
- Фильтрация данных с операторами `>`, `<`, `=`
- Агрегация данных: среднее (avg), минимум (min), максимум (max)
- Красивый вывод в виде таблицы

## Требования

- Python 3.13+
- Зависимости: `tabulate`

## Установка

1. Клонируйте репозиторий
2. Установите зависимости:

```bash
poetry install
```

## Использование

### Базовый синтаксис

```bash
python main.py --file <filename.csv> [--where 'column>value'] [--aggregate 'column=operation']
```

### Примеры

1. Вывести все данные из файла:

```bash
python main.py --file ./data/products.csv
```

![img/1.png](img/1.png)

2. Отфильтровать по точному совпадению:

```bash
python main.py --file ./data/products.csv --where 'brand=apple'
```

![img/2.png](img/2.png)

3. Найти товары дороже 500:

```bash
python main.py --file ./data/products.csv --where 'price>500'
```

![img/3.png](img/3.png)

4. Найти товары с рейтингом ниже 4.8:

```bash
python main.py --file ./data/products.csv --where 'rating<4.8'
```

![img/4.png](img/4.png)

5. Посчитать среднюю цену по всем товарам:

```bash
python main.py --file ./data/products.csv --aggregate 'price=avg'
```

![img/5.png](img/5.png)

6. Средняя цена товаров xiaomi:

```bash
python main.py --file ./data/products.csv --where 'brand=xiaomi' --aggregate 'price=avg'
```

![img/6.png](img/6.png)

7. Поиск самого дорогого товара:

```bash
python main.py --file ./data/products.csv --aggregate 'price=max'
```

![img/7.png](img/7.png)

8. Средняя цена премиальных товаров (от 800):

```bash
python main.py --file ./data/products.csv --where 'price>800' --aggregate 'price=avg'
```

![img/8.png](img/8.png)

9. Минимальная цена на товары Samsung:

```bash
python main.py --file ./data/products.csv --where 'brand=samsung' --aggregate 'price=min'
```

![img/9.png](img/9.png)

10. Средняя цена товаров с рейтингом выше 4.5:

```bash
python main.py --file ./data/products.csv --where 'rating>4.5' --aggregate 'price=avg'
```

![img/10.png](img/10.png)

11. Максимальный рейтинг товаров Apple:

```bash
python main.py --file ./data/products.csv --where 'brand=apple' --aggregate 'rating=max'
```

![img/11.png](img/11.png)

## Форматы данных

- Фильтрация: `--where 'column>value'` (поддерживаются >, <, =)
- Агрегация: `--aggregate 'column=operation'` (доступно: avg, min, max)

## Ограничения

- Все данные считываются как строки, но для числовых колонок доступно числовое сравнение
- Фильтрация работает с любыми типами данных (сравнение строк чувствительно к регистру)
- Агрегация работает только с числовыми колонками
- Поддерживается только один фильтр и одна агрегация за запуск

## Запуск тестов

```bash
# Запуск всех тестов
pytest

# Запуск тестов в компактном режиме
pytest -q
```
