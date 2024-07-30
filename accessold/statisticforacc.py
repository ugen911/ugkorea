"""
Этот скрипт выполняет следующие задачи:
1. Подключается к базе данных с использованием функции get_db_engine.
2. Извлекает данные из таблицы "prodazhi" с колонками "kod", "kolichestvo" и "period".
3. Преобразует колонку "period" в формат datetime.
4. Добавляет колонку "year_month" для группировки данных по месяцам.
5. Группирует данные по колонкам "kod" и "year_month", суммируя значения в колонке "kolichestvo".
6. Преобразует сгруппированные данные в таблицу, где каждый столбец представляет месяц продаж.
7. Определяет текущий месяц и добавляет колонку "Текущий месяц".
8. Определяет последние 12 месяцев, включая текущий, и добавляет отсутствующие месяцы с нулевыми продажами.
9. Считает продажи за последний год, исключая текущий месяц.
10. Перемещает колонку "Текущий месяц" в конец таблицы.
11. Преобразует все колонки, кроме "kod", в целочисленный формат.
12. Сохраняет результат в таблицу "salespivot" в схеме "public" базы данных.
13. Проверяет успешность сохранения данных, выводя количество записей в новой таблице.

Функции и библиотеки, используемые в скрипте:
- pandas для работы с данными.
- sqlalchemy для подключения к базе данных и выполнения SQL-запросов.
- get_db_engine для получения объекта подключения к базе данных.
"""

import pandas as pd
from ugkorea.db.database import get_db_engine
from sqlalchemy.sql import text

# Подключение к базе данных
engine = get_db_engine()

# Получение данных из таблицы prodazhi с нужными колонками
query_prodazhi = "SELECT kod, kolichestvo, period FROM prodazhi"
prodazhi_df = pd.read_sql(query_prodazhi, engine)
prodazhi_df.name = "prodazhi"

# Преобразуем колонку period в формат datetime для prodazhi_df
prodazhi_df['period'] = pd.to_datetime(prodazhi_df['period'], format='%d.%m.%Y')

# Добавляем колонку year_month для группировки по месяцам
prodazhi_df['year_month'] = prodazhi_df['period'].dt.to_period('M')

# Группируем данные по kod и year_month и суммируем kolichestvo
prodazhi_df['kolichestvo'] = pd.to_numeric(prodazhi_df['kolichestvo'], errors='coerce').fillna(0)
monthly_sales = prodazhi_df.groupby(['kod', 'year_month'])['kolichestvo'].sum().reset_index()

# Разворачиваем таблицу так, чтобы каждый столбец представлял собой месяц продаж
sales_pivot = monthly_sales.pivot(index='kod', columns='year_month', values='kolichestvo').fillna(0)

# Получаем текущий месяц
current_month = sales_pivot.columns.max()

# Преобразуем столбцы Period в строки
sales_pivot.columns = sales_pivot.columns.astype(str)

# Переименовываем столбец текущего месяца
sales_pivot.rename(columns={str(current_month): 'Текущий месяц'}, inplace=True)

# Переносим индекс в колонку 'kod'
sales_pivot.reset_index(inplace=True)

# Определяем последние 12 месяцев, включая текущий
last_12_months = [str(current_month - i) for i in range(1, 13)]

# Добавляем отсутствующие месяцы с нулевыми продажами
for month in last_12_months:
    if month not in sales_pivot.columns:
        sales_pivot[month] = 0

# Считаем продажи за последний год, исключая текущий месяц
sales_pivot['За последний год'] = sales_pivot[last_12_months].sum(axis=1)

# Перемещаем колонку 'Текущий месяц' в конец
current_month_col = sales_pivot.pop('Текущий месяц')
sales_pivot['Текущий месяц'] = current_month_col

# Преобразуем все колонки в int, кроме 'kod'
for col in sales_pivot.columns[1:]:
    sales_pivot[col] = sales_pivot[col].astype(int)

# Сохраняем результат в таблицу базы данных
sales_pivot.to_sql('salespivot', engine, schema='public', if_exists='replace', index=False)

# Проверка успешности сохранения
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM public.salespivot"))
    count = result.fetchone()[0]

if count > 0:
    print(f"Таблица успешно сохранена в базу данных. Количество записей: {count}")
else:
    print("Ошибка при сохранении таблицы в базу данных.")
