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

