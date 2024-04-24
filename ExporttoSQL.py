import pandas as pd
from sqlalchemy import create_engine
import sys
import subprocess

# Параметры подключения к базе данных
db_user = 'postgres'
db_password = '89232808797'  # Замените на реальный пароль
db_host = 'localhost'
db_port = '5432'
db_name = 'ugkorea'
db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

engine = create_engine(db_url)

# Проверка подключения
try:
    connection = engine.connect()
    print("Подключение к PostgreSQL успешно установлено.")
    connection.close()
except Exception as e:
    print(f"Не удалось подключиться к PostgreSQL: {e}")

# Выполнение внешнего скрипта
file_path = r"C:\Users\evgen\repo\ugkorea\Mimportprice.py"
try:
    subprocess.run([sys.executable, file_path], check=True)
except FileNotFoundError:
    print("Файл Mimportprice.py не найден.")
except subprocess.CalledProcessError as e:
    print(f"Ошибка выполнения скрипта {file_path}: {e}")

# Импорт датафреймов
sys.path.append(r"C:\Users\evgen\repo\ugkorea")
from Mgetdataframe import dataframes_dict

# Подключаемся снова к БД для добавления данных
with engine.connect() as connection:
    for table_name, df in dataframes_dict.items():
        # Выводим названия колонок датафрейма перед выполнением каких-либо операций
        print(f"Колонки в датафрейме для таблицы {table_name}: {df.columns.tolist()}")
    
    for table_name, df in dataframes_dict.items():
        # Проверяем названия колонок, если названия другие, то нужно будет их заменить в коде ниже
        if 'дата' not in df.columns:
            print(f"В датафрейме для таблицы {table_name} отсутствует колонка 'дата'.")
            continue

        # Получаем список дат в таблице
        existing_dates = pd.read_sql(f"SELECT DISTINCT дата FROM {table_name}", connection)['дата']
        
        # Фильтруем датафреймы, исключая даты, которые уже есть в таблице
        df_filtered = df[~df['дата'].isin(existing_dates)]

        if df_filtered.empty:
            print(f"Нет новых данных для добавления в таблицу {table_name}.")
        else:
            try:
                df_filtered.to_sql(table_name, connection, if_exists='append', index=False)
                print(f"Данные из датафрейма {table_name} добавлены в таблицу {table_name}.")
            except Exception as e:
                print(f"Ошибка при добавлении данных в таблицу {table_name}: {e}")

