import pandas as pd
from ugkorea.db.database import get_db_engine
import os

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

# Добавляем колонки для текущего месяца и предыдущих 19 месяцев
for i in range(20):
    month_col = current_month - i
    col_name = 'Текущий месяц' if i == 0 else (f'prev sells' if i == 1 else f'prev-{i-1} sells')
    if month_col in sales_pivot.columns:
        sales_pivot[col_name] = sales_pivot[month_col]
    else:
        sales_pivot[col_name] = 0

# Определяем период для prev-18 sells
prev_18_period = current_month - 19
oldest_period = prev_18_period - 1

# Добавляем колонку oldest sells
if oldest_period in sales_pivot.columns:
    sales_pivot['oldest sells'] = sales_pivot[oldest_period]
else:
    sales_pivot['oldest sells'] = 0

# Считаем продажи за прошедший год, исключая текущий месяц
sales_pivot['За год'] = sales_pivot[['prev sells'] + [f'prev-{i} sells' for i in range(1, 13)]].sum(axis=1)

# Оставляем только нужные колонки
final_columns = ['Текущий месяц', 'prev sells'] + [f'prev-{i} sells' for i in range(1, 19)] + ['oldest sells', 'За год']
sales_pivot = sales_pivot[final_columns]

# Преобразуем все колонки в int
sales_pivot = sales_pivot.astype(int)

# Переносим индекс в колонку 'kod'
sales_pivot.reset_index(inplace=True)

# Выводим итоговую информацию после добавления и переименования столбцов
print(sales_pivot.head())

# Определяем путь для сохранения файла
output_path = r'\\26.218.196.12\заказы\Евгений\sales_pivot.xlsx'

# Сохранение результирующего DataFrame в XLSX файл
sales_pivot.to_excel(output_path, index=False)

print(f"The data has been successfully saved to an XLSX file at {output_path}.")
