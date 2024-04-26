
import pandas as pd
import sys
import subprocess
from outlook_utils import outlook_utile
from Mgetdataframe import get_df_main
import os

# Добавляем путь к директории, где находится database.py
project_directory = os.path.abspath('C:\\Users\\evgen\\repo\\ugkorea')

from database import get_db_engine

# Выводим текущий список путей поиска модулей для диагностики
print("Список путей поиска модулей:", sys.path)

folder_path = r"C:\Users\evgen\repo\ugkorea\Output"

# Выполнение внешнего скрипта
def run_external_script(file_path):
    try:
        subprocess.run([sys.executable, file_path], check=True)
        print(f"Скрипт {file_path} выполнен успешно.")
    except FileNotFoundError:
        print("Файл не найден.")
    except subprocess.CalledProcessError as e:
        print(f"Ошибка выполнения скрипта {file_path}: {e}")

# Импорт и добавление данных в базу
def import_and_load_data():
    sys.path.append(r"C:\Users\evgen\repo\ugkorea")
    dataframes_dict = get_df_main(folder_path)
    engine = get_db_engine()

    with engine.connect() as connection:
        connection.begin()  # Начало транзакции
        try:
            for table_name, df in dataframes_dict.items():
                if 'дата' not in df.columns:
                    print(f"В датафрейме для таблицы {table_name} отсутствует колонка 'дата'.")
                    continue

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

def main():
    get_db_engine()
    outlook_utile()
    import_and_load_data()

if __name__ == "__main__":
    main()
