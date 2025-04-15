import pandas as pd
from ugkorea.db.database import get_db_engine
from sqlalchemy.sql import text

# Подключение к базе данных
engine = get_db_engine()

# Получение данных из таблицы prodazhi с нужными колонками
query_prodazhi = "SELECT kod, kolichestvo, summa, period FROM prodazhi"
prodazhi_df = pd.read_sql(query_prodazhi, engine)
prodazhi_df.name = "prodazhi"

# Преобразуем колонку period в формат datetime для prodazhi_df
prodazhi_df['period'] = pd.to_datetime(prodazhi_df['period'], format='%d.%m.%Y')

# Добавляем колонку year_month для группировки по месяцам
prodazhi_df['year_month'] = prodazhi_df['period'].dt.to_period('M')

# Удаляем неразрывные пробелы \xa0, обычные пробелы и заменяем запятую на точку для корректного преобразования
prodazhi_df["summa"] = (
    prodazhi_df["summa"]
    .astype(str)
    .str.replace("\xa0", "", regex=True)  # Удаление неразрывных пробелов
    .str.replace(" ", "")  # Удаление обычных пробелов
    .str.replace(",", ".")  # Замена запятой на точку
    .str.strip()
)
prodazhi_df["summa"] = pd.to_numeric(prodazhi_df["summa"], errors="coerce").fillna(0)

# Убираем пробелы и преобразуем kolichestvo в числовой формат
prodazhi_df["kolichestvo"] = prodazhi_df["kolichestvo"].astype(str).str.strip()
prodazhi_df["kolichestvo"] = pd.to_numeric(
    prodazhi_df["kolichestvo"], errors="coerce"
).fillna(0)

monthly_sales = prodazhi_df.groupby(['kod', 'year_month']).agg({
    'kolichestvo': 'sum',
    'summa': 'sum'
}).reset_index()

# Преобразуем данные в длинный формат
sales_long = monthly_sales.pivot_table(index=['kod', 'year_month'], values=['kolichestvo', 'summa']).reset_index()

# Преобразуем столбцы year_month в строки
sales_long['year_month'] = sales_long['year_month'].astype(str)
sales_long['kod'] = sales_long['kod'].astype(str).str.strip()
# Сохраняем результат в таблицу базы данных
sales_long.to_sql('salespivot', engine, schema='public', if_exists='replace', index=False)

# Проверка успешности сохранения
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM public.salespivot"))
    count = result.fetchone()[0]

if count > 0:
    print(f"Таблица salespivot успешно сохранена в базу данных. Количество записей: {count}")
else:
    print("Ошибка при сохранении таблицы salespivot в базу данных.")

# Вывод первых 20 строк из таблицы salespivot
print("Первые 20 строк из таблицы salespivot:")
sales_pivot_first_20 = pd.read_sql("SELECT * FROM public.salespivot LIMIT 20", engine)
print(sales_pivot_first_20)

# Работа с таблицей postuplenija
query_postuplenija = "SELECT kod, kolichestvo, tsena, data FROM postuplenija"
postuplenija_df = pd.read_sql(query_postuplenija, engine)
postuplenija_df.name = "postuplenija"

# Преобразуем колонку data в формат datetime для postuplenija_df
postuplenija_df['data'] = pd.to_datetime(postuplenija_df['data'], format='%Y-%m-%d %H:%M:%S')

# Добавляем колонку year_month для группировки по месяцам
postuplenija_df['year_month'] = postuplenija_df['data'].dt.to_period('M')

# Рассчитываем сумму (kolichestvo * tsena)
postuplenija_df['kolichestvo'] = pd.to_numeric(postuplenija_df['kolichestvo'], errors='coerce').fillna(0)
# Убираем пробелы и заменяем запятую на точку для корректного преобразования
postuplenija_df["tsena"] = (
    postuplenija_df["tsena"]
    .astype(str)
    .str.replace("\xa0", "", regex=True)  # Удаление неразрывных пробелов
    .str.replace(" ", "")  # Удаление пробелов, используемых как разделители тысяч
    .str.replace(",", ".")  # Замена запятой на точку
    .str.strip()
)
postuplenija_df['tsena'] = pd.to_numeric(postuplenija_df['tsena'], errors='coerce').fillna(0)
postuplenija_df['summa'] = postuplenija_df['kolichestvo'] * postuplenija_df['tsena']

# Группируем данные по kod и year_month и суммируем kolichestvo и summa
monthly_supplies = postuplenija_df.groupby(['kod', 'year_month']).agg({
    'kolichestvo': 'sum',
    'summa': 'sum'
}).reset_index()

# Преобразуем данные в длинный формат
supplies_long = monthly_supplies.pivot_table(index=['kod', 'year_month'], values=['kolichestvo', 'summa']).reset_index()

# Преобразуем столбцы year_month в строки
supplies_long['year_month'] = supplies_long['year_month'].astype(str)
supplies_long['kod'] = supplies_long['kod'].astype(str).str.strip()
# Сохраняем результат в таблицу базы данных
supplies_long.to_sql('suppliespivot', engine, schema='public', if_exists='replace', index=False)

# Проверка успешности сохранения
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM public.suppliespivot"))
    count = result.fetchone()[0]

if count > 0:
    print(f"Таблица suppliespivot успешно сохранена в базу данных. Количество записей: {count}")
else:
    print("Ошибка при сохранении таблицы suppliespivot в базу данных.")

# Вывод первых 20 строк из таблицы suppliespivot
print("Первые 20 строк из таблицы suppliespivot:")
supplies_pivot_first_20 = pd.read_sql("SELECT * FROM public.suppliespivot LIMIT 20", engine)
print(supplies_pivot_first_20)
