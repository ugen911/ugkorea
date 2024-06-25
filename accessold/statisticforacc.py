import pandas as pd
from ugkorea.db.database import get_db_engine

# Подключение к базе данных
engine = get_db_engine()

# Получение данных из таблицы prodazhi с нужными колонками
query_prodazhi = "SELECT kod, kolichestvo, period FROM prodazhi"
prodazhi_df = pd.read_sql(query_prodazhi, engine)
prodazhi_df.name = "prodazhi"

# Получение данных из таблицы ostatkitovarovnakonetskazhdogomesjatsa с нужными колонками и переименованием
query_stock = """
SELECT 
    konetsmesjatsa AS period, 
    kolichestvo, 
    nomenklaturakod AS kod 
FROM ostatkitovarovnakonetskazhdogomesjatsa
"""
stock_df = pd.read_sql(query_stock, engine)
stock_df.name = "stock"

# Получение данных из таблицы tsenynakonetsmesjatsa с фильтрацией и переименованием
query_price = """
SELECT 
    konetsmesjatsa AS period, 
    tsena AS price, 
    kod 
FROM tsenynakonetsmesjatsa 
WHERE tiptsen = 'Основной тип цен продажи'
"""
price_df = pd.read_sql(query_price, engine)
price_df.name = "price"

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

# Преобразование колонки kolichestvo в числовой тип данных, замена некорректных значений на 0 и преобразование в int
# Добавление последнего дня месяца к дате
stock_df['period'] = stock_df['period'].apply(lambda x: pd.to_datetime(f"01.{x}", format='%d.%m.%Y') + pd.offsets.MonthEnd(1))
stock_df['kolichestvo'] = pd.to_numeric(stock_df['kolichestvo'], errors='coerce').fillna(0).astype(int)
stock_df['year_month'] = stock_df['period'].dt.to_period('M')


# Переименование колонок для удобства
stock_df = stock_df.rename(columns={'period': 'date', 'kolichestvo': 'stock', 'nomenklaturakod': 'kod'})


# Проверка наличия некорректных значений в колонке 'kod'
invalid_kod_rows = stock_df[stock_df['kod'].str.contains('ck', na=False)]
if not invalid_kod_rows.empty:
    print("Найдены строки с 'ck' перед установкой индекса:")
    print(invalid_kod_rows)

# Установка колонки 'kod' в качестве индекса
stock_df.set_index('kod', inplace=True)

# Разворачиваем таблицу так, чтобы каждый столбец представлял собой месяц запасов
stock_pivot = stock_df.pivot_table(index='kod', columns='year_month', values='stock', fill_value=0)

# Получаем текущий месяц
current_month = stock_pivot.columns.max()

# Добавляем колонки для текущего месяца и предыдущих 19 месяцев
for i in range(20):
    month_col = current_month - i
    col_name = 'Текущий месяц' if i == 0 else (f'prev stock' if i == 1 else f'prev-{i-1} stock')
    if month_col in stock_pivot.columns:
        stock_pivot[col_name] = stock_pivot[month_col]
    else:
        stock_pivot[col_name] = 0


# Определяем период для prev-18 stock
prev_18_period = current_month - 19
oldest_period = prev_18_period - 1

# Добавляем колонку oldest stock
if oldest_period in stock_pivot.columns:
    stock_pivot['oldest stock'] = stock_pivot[oldest_period]
else:
    stock_pivot['oldest stock'] = 0

# Перенос индекса в колонку kod
stock_pivot.reset_index(inplace=True)

# Оставляем только колонки, в названии которых есть "stock"
columns_to_keep = [col for col in stock_pivot.columns if 'stock' in str(col) or col == 'kod']
filtered_stock_pivot = stock_pivot[columns_to_keep]


# Преобразование всех колонок, содержащих "stock", в целочисленный тип
stock_columns = [col for col in filtered_stock_pivot.columns if 'stock' in col]
filtered_stock_pivot[stock_columns] = filtered_stock_pivot[stock_columns].astype(int)

# Проверка и вывод итоговой информации после фильтрации колонок
print("Итоговая таблица (filtered_stock_pivot):")
print(filtered_stock_pivot.head())
