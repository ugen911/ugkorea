from ugkorea.db.database import get_db_engine
from sqlalchemy import Table, MetaData, select, or_, func
import pandas as pd
from datetime import datetime
import os

# Получаем движок базы данных
engine = get_db_engine()

# Создаем объект MetaData
metadata = MetaData()

# Рефлексируем необходимые таблицы
nomenklatura = Table('nomenklatura', metadata, autoload_with=engine)
priceold = Table('priceold', metadata, autoload_with=engine)
stockold = Table('stockold', metadata, autoload_with=engine)

# Список брендов, которые нужно искать
brands = ['CHERY', 'GEELY', 'CHANGAN', 'Haval', 'Exeed', 'Jetour', 'Omoda', 'Tank']

# Создаем условие для фильтрации с использованием оператора OR
conditions = [nomenklatura.c.naimenovaniepolnoe.ilike(f"%{brand}%") for brand in brands]

# Объединяем таблицы с помощью SQL JOIN с применением TRIM для удаления пробелов по краям 'kod'
query = select(
    nomenklatura.c.kod,
    nomenklatura.c.artikul,
    nomenklatura.c.proizvoditel,
    nomenklatura.c.naimenovaniepolnoe,
    nomenklatura.c.datasozdanija,
    priceold.c.tsenazakup,
    priceold.c.tsenarozn,
    stockold.c.osnsklad
).select_from(
    nomenklatura
    .join(priceold, func.trim(nomenklatura.c.kod) == func.trim(priceold.c.kod), isouter=True)
    .join(stockold, func.trim(nomenklatura.c.kod) == func.trim(stockold.c.kod), isouter=True)
).where(or_(*conditions))

# Выполняем запрос и загружаем данные в DataFrame
final_df = pd.read_sql(query, engine)

# Получаем список уникальных значений 'kod' из final_df (предварительно очищая пробелы)
kod_values = final_df['kod'].str.strip().tolist()

# Рефлексируем таблицу 'salespivot'
salespivot = Table('salespivot', metadata, autoload_with=engine)

# Формируем запрос для выбора строк из 'salespivot', где 'kod' соответствует значениям из final_df с учетом удаления пробелов
query = select(salespivot).where(func.trim(salespivot.c.kod).in_(kod_values))

# Выполняем запрос и загружаем данные в DataFrame
salespivot_df = pd.read_sql(query, engine)

# Преобразуем колонку 'year_month' в формат даты
salespivot_df['year_month'] = pd.to_datetime(salespivot_df['year_month'], format='%Y-%m')

# Определим текущую дату
current_date = datetime.now()

# Определим последние 12 месяцев, включая текущий месяц
last_12_months = pd.date_range(end=current_date, periods=12, freq='MS').strftime('%Y-%m')

# Фильтруем данные только за последние 12 месяцев, включая текущий месяц
filtered_salespivot_df = salespivot_df[salespivot_df['year_month'].dt.strftime('%Y-%m').isin(last_12_months)]

# Создаем сводную таблицу (pivot), где строки — это 'kod', колонки — это 'year_month', а значения — это 'kolichestvo'
pivot_table = filtered_salespivot_df.pivot_table(
    index='kod',
    columns=filtered_salespivot_df['year_month'].dt.strftime('%Y-%m'),
    values='kolichestvo',
    aggfunc='sum',
    fill_value=0
)

# Добавляем колонку с суммой продаж за все месяцы
pivot_table['Total_Sales'] = pivot_table.sum(axis=1)

# Добавляем отсутствующие месяцы в сводную таблицу и заполняем их нулями
for month in last_12_months:
    if month not in pivot_table.columns:
        pivot_table[month] = 0

# Сортируем столбцы с датами в хронологическом порядке
columns_with_dates = sorted([col for col in pivot_table.columns if col != 'Total_Sales'], key=lambda x: pd.to_datetime(x, format='%Y-%m'))
columns_sorted = columns_with_dates + ['Total_Sales']

# Перестраиваем таблицу с новым порядком столбцов
pivot_table = pivot_table[columns_sorted]

# Объединяем итоговую сводную таблицу с final_df по полю 'kod'
final_with_sales_df = pd.merge(final_df, pivot_table, on='kod', how='left')

# Добавляем отсутствующие месяцы в итоговый DataFrame и заполняем их нулями
for month in last_12_months:
    if month not in final_with_sales_df.columns:
        final_with_sales_df[month] = 0

# Заполняем NaN значения в колонках с продажами нулями
columns_to_fill = list(last_12_months) + ['Total_Sales']
final_with_sales_df[columns_to_fill] = final_with_sales_df[columns_to_fill].fillna(0)

# Используем фильтрацию с помощью логических условий
final_with_sales_df = final_with_sales_df[(final_with_sales_df['tsenazakup'] != 0) | (final_with_sales_df['tsenarozn'] != 0)]

# Сортируем DataFrame по колонке 'naimenovaniepolnoe' в алфавитном порядке
final_with_sales_df = final_with_sales_df.sort_values(by='naimenovaniepolnoe')

# Переставляем колонки в итоговом DataFrame
final_with_sales_df = final_with_sales_df[['kod', 'artikul', 'proizvoditel', 'naimenovaniepolnoe', 'datasozdanija', 
                                           'tsenazakup', 'tsenarozn', 'osnsklad'] + columns_with_dates + ['Total_Sales']]

# Попробуем сначала сохранить файл локально, если не получится, попробуем по сети
local_path = r"D:\NAS\общая\АРХИВ\Евгений Т"
network_path = r"\\26.218.196.12\общая\АРХИВ\Евгений Т"
file_name = "Chinaparts.xlsx"

try:
    # Сохраняем локально
    output_file = os.path.join(local_path, file_name)
    final_with_sales_df.to_excel(output_file, index=False)
    print(f"Итоговый DataFrame сохранен локально в файл: {output_file}")
except Exception as e:
    print(f"Не удалось сохранить файл локально: {e}")
    try:
        # Сохраняем по сети, если локальный путь недоступен
        output_file = os.path.join(network_path, file_name)
        final_with_sales_df.to_excel(output_file, index=False)
        print(f"Итоговый DataFrame сохранен по сети в файл: {output_file}")
    except Exception as e:
        print(f"Не удалось сохранить файл по сети: {e}")

# Выводим первые 5 строк итогового DataFrame
print(final_with_sales_df.head())
