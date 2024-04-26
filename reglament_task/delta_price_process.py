from db.database import get_db_engine
from sqlalchemy import text, inspect
import pandas as pd

engine = get_db_engine()
inspector = inspect(engine)

with engine.connect() as conn:
    query = text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'prices'")
    result = conn.execute(query)
    tables = [row[0] for row in result.fetchall()]

dataframes = {}

for table in tables:
    query = f"SELECT * FROM prices.{table}"
    df = pd.read_sql(query, engine)
    # Преобразуем столбец 'дата' в datetime, если он не в формате datetime
    if 'дата' in df.columns:
        df['дата'] = pd.to_datetime(df['дата'])
    dataframes[table] = df

date_dfs = {}

for table_name, df in dataframes.items():
    if 'дата' in df.columns:
        for date, group_df in df.groupby('дата'):
            formatted_date = date.strftime('%Y%m%d')  # Теперь date - это datetime объект
            new_name = f"{table_name}_{formatted_date}"
            date_dfs[new_name] = group_df

# Перебор всех датафреймов и вывод первых пяти строк каждого
for name, df in date_dfs.items():
    print(f"Первые 5 строк датафрейма {name}:")
    print(df.head())
    print("\n")  # Добавляем пустую строку между датафреймами для лучшей читаемости


import pandas as pd

# Предположим, что date_dfs уже заполнен и содержит нужные датафреймы

# Группируем датафреймы по колонке 'поставщик'
suppliers_frames = {}
for name, df in date_dfs.items():
    for supplier in df['поставщик'].unique():
        if supplier not in suppliers_frames:
            suppliers_frames[supplier] = []
        suppliers_frames[supplier].append((name, df[df['поставщик'] == supplier]))

# Объединяем датафреймы для каждого поставщика
merged_dataframes = {}
for supplier, frames in suppliers_frames.items():
    base_df = None
    for name, df in frames:
        # Извлекаем дату из имени датафрейма
        date_part = name.split('_')[-1]
        # Переименовываем колонку 'количество' в 'остаток_дата'
        df = df.rename(columns={'количество': f'остаток_{date_part}'})
        if base_df is None:
            base_df = df
        else:
            # Слияние по 'производитель' и 'артикул'
            base_df = pd.merge(base_df, df, on=['производитель', 'артикул'], how='inner')
    
    # Если после всех слияний base_df не None, добавляем в результат
    if base_df is not None:
        # Выбираем колонки 'производитель', 'артикул' и все колонки 'остаток'
        required_columns = ['производитель', 'артикул'] + [col for col in base_df.columns if 'остаток_' in col]
        base_df = base_df[required_columns]

        merged_dataframes[supplier] = base_df

# Выводим результаты
for supplier, df in merged_dataframes.items():
    print(f"Данные для поставщика {supplier}:")
    print(df.head())
    print("\n")


# Пример кода, который обрабатывает данные описанным способом
for supplier, df in merged_dataframes.items():
    # Получаем список колонок с остатками, упорядоченный по дате в названии
    stock_columns = sorted([col for col in df.columns if 'остаток_' in col], key=lambda x: x.split('_')[-1])
    
    # Инициализация колонок поставка и расход
    df['поставка'] = 0
    df['расход'] = 0
    
    # Перебираем колонки и вычисляем разности
    for i in range(1, len(stock_columns)):
        prev_col = stock_columns[i-1]
        current_col = stock_columns[i]
        
        # Разность текущего и предыдущего остатка
        difference = df[current_col] - df[prev_col]
        
        # Распределение разности на поставки и расход
        df['поставка'] += difference.clip(lower=0)
        df['расход'] += (-difference).clip(lower=0)
    
    # Сортировка датафрейма по колонке 'расход' в убывающем порядке и вывод первых 20 строк
    sorted_df = df[['производитель', 'артикул', 'поставка', 'расход'] + stock_columns].sort_values(by='расход', ascending=False).head(20)
    final_columns = ['производитель', 'артикул', 'поставка', 'расход']
    df = sorted_df[final_columns]
    # Выводим результаты для каждого поставщика
    print(f"Первые 20 строк данных для поставщика {supplier}, отсортированные по убыванию расхода:")
    print(df)
    print("\n")
    
    table_name = f"{supplier}"
    
    # Выгрузка в базу данных
    with engine.connect() as conn:
        if not inspector.has_table(table_name, schema='analitic'):
            # Если таблицы нет, создаем и загружаем данные
            df.to_sql(name=table_name, con=conn, schema='analitic', if_exists='replace', index=False)
        else:
            # Если таблица есть, делаем обновление данных
            # Чтение существующих данных
            existing_df = pd.read_sql_table(table_name, con=conn, schema='analitic')
            # Объединение с новыми данными по 'производитель' и 'артикул'
            combined_df = pd.merge(existing_df, df, on=['производитель', 'артикул'], suffixes=('', '_new'))
            # Обновление колонок 'поставка' и 'расход'
            combined_df['поставка'] = combined_df['поставка'] + combined_df['поставка_new']
            combined_df['расход'] = combined_df['расход'] + combined_df['расход_new']
            # Удаление временных столбцов
            combined_df.drop(columns=['поставка_new', 'расход_new'], inplace=True)
            # Замена старых данных в таблице
            combined_df.to_sql(name=table_name, con=conn, schema='analitic', if_exists='replace', index=False)