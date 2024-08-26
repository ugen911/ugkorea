import psycopg2
import os
import pandas as pd
from transliterate import translit
import logging
import warnings
from ugkorea.db.config import remote_db_config, server_db_config  # Импорт конфигураций

# Настройка логгирования
logging.basicConfig(filename='data_upload_errors.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# Пути к директориям
network_path = r'\\26.218.196.12\заказы\Евгений\Access\Табличные выгрузки1С'
local_path = r'D:\NAS\заказы\Евгений\Access\Табличные выгрузки1С'

def check_directory_availability(path):
    return os.path.exists(path)

def to_snake_case(s):
    s = translit(s, 'ru', reversed=True)
    s = s.lower().replace(' ', '_').replace('.', '_').replace('"', '').replace("'", '').replace(',', '').replace(';', '').replace('!', '').replace('?', '')
    return s

def upload_csv_files(directory_path, db_config):
    try:
        # Установление соединения с базой данных
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        
        created_tables = []

        for filename in os.listdir(directory_path):
            if filename.endswith('.csv'):
                file_path = os.path.join(directory_path, filename)
                # Удаление префиксов и транслитерация
                modified_name = filename.replace('Выгрузка_', '').replace('Выгрузка', '')
                # Преобразование в snake_case с транслитерацией для имени таблицы
                table_name = to_snake_case(os.path.splitext(modified_name)[0])

                try:
                    with warnings.catch_warnings(record=True) as w:
                        warnings.simplefilter("always")
                        data = pd.read_csv(file_path, sep=';', on_bad_lines='warn')
                        for warning in w:
                            if issubclass(warning.category, pd.errors.ParserWarning):
                                logging.warning(f"Warning while parsing {filename}: {warning.message}")

                    # Удаление пробелов и непечатных символов по краям в каждой колонке
                    data = data.apply(lambda col: col.str.strip() if col.dtype == 'object' else col)

                    # Преобразование данных в кодировку UTF-8
                    data = data.apply(lambda col: col.map(lambda x: x.encode('utf-8').decode('utf-8') if isinstance(x, str) else x))

                    # Преобразование названий колонок в нижний регистр и snake_case с учетом транслитерации
                    data.columns = [to_snake_case(col) for col in data.columns]

                    # Преобразование столбцов с датами
                    date_columns = [col for col in data.columns if 'data' in col]  # 'дата' transliterates to 'data'
                    for col in date_columns:
                        data[col] = pd.to_datetime(data[col], errors='coerce', dayfirst=True)
                        # Заменяем NaT на None (NULL для PostgreSQL)
                        data[col] = data[col].astype(object).where(data[col].notna(), None)

                    # Создание таблицы в базе данных
                    create_table_query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        {', '.join([f"{col} TEXT" for col in data.columns])}
                    )
                    """
                    cursor.execute(create_table_query)
                    connection.commit()

                    # Вставка данных в таблицу
                    for i, row in data.iterrows():
                        insert_query = f"""
                        INSERT INTO {table_name} ({', '.join(data.columns)}) 
                        VALUES ({', '.join(['%s' for _ in data.columns])})
                        """
                        cursor.execute(insert_query, tuple(row))
                    connection.commit()

                    print(f"Данные из {filename} успешно загружены в таблицу {table_name}.")
                    created_tables.append(table_name)

                except Exception as e:
                    logging.error(f"Ошибка загрузки данных из файла {filename}: {e}")
                    print(f"Не удалось загрузить данные из {filename}: {e}")

        return created_tables

    except Exception as e:
        logging.error(f"Ошибка при подключении к базе данных: {e}")
        print(f"Ошибка при подключении к базе данных: {e}")

    finally:
        if 'connection' in locals() and connection:
            cursor.close()
            connection.close()
            print("Соединение с базой данных закрыто")

def print_first_five_rows(table_name, db_config):
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
        rows = cursor.fetchall()
        print(f"Первые 5 строк из таблицы '{table_name}':")
        for row in rows:
            print(row)
    except Exception as e:
        logging.error(f"Ошибка при чтении данных из таблицы {table_name}: {e}")
        print(f"Ошибка при чтении данных из таблицы {table_name}: {e}")
    finally:
        if 'connection' in locals() and connection:
            cursor.close()
            connection.close()
            print("Соединение с базой данных закрыто")

if __name__ == "__main__":
    # Проверка доступности локальной папки
    if check_directory_availability(local_path):
        print(f"Используется локальная папка: {local_path}")
        created_tables = upload_csv_files(local_path, server_db_config)
    else:
        print(f"Локальная папка недоступна, используется сетевая папка: {network_path}")
        created_tables = upload_csv_files(network_path, remote_db_config)

    # Вывод первых 5 строк из каждой созданной таблицы
    for table_name in created_tables:
        if check_directory_availability(local_path):
            print_first_five_rows(table_name, server_db_config)
        else:
            print_first_five_rows(table_name, remote_db_config)
