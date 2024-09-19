import pandas as pd
import os
import sys
import subprocess
from sqlalchemy import text
from ugkorea.from_folder_to_df.main_get_dataframe import get_df_main, find_repo_folder
from ugkorea.db.database import get_db_engine
from datetime import datetime, timedelta


# Задаём структуру папок для поиска
target_folder_structure = "repo\\ugkorea\\Output"

# Начинаем поиск с диска C:
folder_path = find_repo_folder("C:\\", target_folder_structure)

# Выполнение внешнего скрипта
def run_external_script(file_path):
    try:
        subprocess.run([sys.executable, file_path], check=True)
        print(f"Скрипт {file_path} выполнен успешно.")
    except FileNotFoundError:
        print("Файл не найден.")
    except subprocess.CalledProcessError as e:
        print(f"Ошибка выполнения скрипта {file_path}: {e}")

# Проверка наличия таблицы в базе данных
def check_table_exists(connection, schema, table_name):
    query = text(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = :schema AND table_name = :table_name
        );
    """)
    result = connection.execute(query, {'schema': schema, 'table_name': table_name})
    return result.scalar()

# Удаление данных старше 60 дней
def delete_old_data(connection, full_table_name):
    # Вычисляем дату 60 дней назад от текущей даты
    cutoff_date = (datetime.now() - timedelta(days=60)).date()
    
    # Выполняем запрос на удаление данных, где дата меньше чем 60 дней назад
    delete_query = text(f"""
        DELETE FROM {full_table_name}
        WHERE TO_DATE(дата, 'YYYY-MM-DD') < :cutoff_date;
    """)
    
    # Выполняем запрос на удаление старых данных
    connection.execute(delete_query, {'cutoff_date': cutoff_date})
    print(f"Удалены данные старше {cutoff_date} из таблицы {full_table_name}.")

# Импорт и добавление данных в базу
def import_and_load_data():
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
                
                # Проверяем наличие таблицы
                if not check_table_exists(connection, 'prices', table_name):
                    print(f"Таблица {full_table_name} не существует. Создаю новую таблицу.")
                    df.to_sql(table_name, connection, schema='prices', if_exists='replace', index=False)
                    print(f"Таблица {full_table_name} успешно создана и данные добавлены.")
                    continue

                # Удаляем данные старше 60 дней
                delete_old_data(connection, full_table_name)

                # Получаем существующие даты
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

def clean_output_folder(folder_path):
    """Очистка папки от файлов."""
    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(f"Не удалось удалить {file_path}: {e}")


def main():
    import_and_load_data()
    clean_output_folder(folder_path=folder_path)

if __name__ == "__main__":
    main()
