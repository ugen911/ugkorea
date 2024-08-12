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

# Получаем текущий месяц
current_month = pd.Period.now('M')

# Определяем последние 12 месяцев, включая текущий
last_12_months = [current_month - i for i in range(12)]

# Фильтруем данные за последние 12 месяцев
filtered_sales = monthly_sales[monthly_sales['year_month'].isin(last_12_months)]

# Добавляем отсутствующие месяцы с нулевыми продажами для каждого kod
full_sales = filtered_sales.set_index(['kod', 'year_month']).unstack(fill_value=0).stack(future_stack=True).reset_index()

# Преобразуем данные в длинный формат
sales_long = full_sales.pivot_table(index=['kod', 'year_month'], values='kolichestvo').reset_index()

# Преобразуем столбцы year_month в строки
sales_long['year_month'] = sales_long['year_month'].astype(str)

# Убираем колонку 'За последний год'
# (эта часть просто убирается из кода, чтобы не вычислять и не добавлять её)

# Сохраняем результат в таблицу базы данных
sales_long.to_sql('salespivot', engine, schema='public', if_exists='replace', index=False)

# Проверка успешности сохранения
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM public.salespivot"))
    count = result.fetchone()[0]

if count > 0:
    print(f"Таблица успешно сохранена в базу данных. Количество записей: {count}")
else:
    print("Ошибка при сохранении таблицы в базу данных.")

