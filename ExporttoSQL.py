import pandas as pd
from sqlalchemy import create_engine
import sys
import subprocess
import os

# Извлечение параметров подключения из переменных окружения
db_user = os.getenv('DB_USER', 'postgres')
db_password = os.getenv('DB_PASSWORD', 'default_password')
db_host = os.getenv('DB_HOST', 'localhost')
db_port = os.getenv('DB_PORT', '5432')
db_name = os.getenv('DB_NAME', 'ugkorea')
db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

engine = create_engine(db_url)

try:
    with engine.connect() as connection:
        print("Подключение к PostgreSQL успешно установлено.")
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

# Подключаемся снова к БД для добавления данных в схему 'prices'
with engine.connect() as connection:
    connection.begin()  # Начало транзакции
    try:
        for table_name, df in dataframes_dict.items():
            if 'дата' not in df.columns:
                print(f"В датафрейме для таблицы {table_name} отсутствует колонка 'дата'.")
                continue

            # Обратите внимание на изменение в указании имени таблицы
            full_table_name = f"prices.{table_name}"  # Использование схемы 'prices'
            existing_dates = pd.read_sql(f"SELECT DISTINCT дата FROM {full_table_name}", connection)['дата']
            df_filtered = df[~df['дата'].isin(existing_dates)]

            if df_filtered.empty:
                print(f"Нет новых данных для добавления в таблицу {full_table_name}.")
            else:
                df_filtered.to_sql(table_name, connection, schema='prices', if_exists='append', index=False)
                num_rows_added = df_filtered.shape[0]
                print(f"Данные из датафрейма {table_name} добавлены в таблицу {full_table_name}.")
                print(f"В таблицу {full_table_name} добавлено {num_rows_added} строк.")
                print("Первые 5 строк добавленных данных:")
                print(df_filtered.head())
        connection.commit()  # Подтверждение транзакции
    except Exception as e:
        print(f"Ошибка при работе с базой данных: {e}")
        connection.rollback()  # Откат изменений в случае ошибки

