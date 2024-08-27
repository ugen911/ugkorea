from ugkorea.db.database import get_db_engine  # Импорт конфигурации подключения
import os
import pandas as pd
from transliterate import translit
import logging
import warnings

# Настройка логгирования
logging.basicConfig(filename='data_upload_errors.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')


def to_snake_case(s):
    s = translit(s, 'ru', reversed=True)
    s = s.lower().replace(' ', '_').replace('.', '_').replace('"', '').replace("'", '').replace(',', '').replace(';', '').replace('!', '').replace('?', '')
    return s

def upload_csv_files(directory_path):
    engine = get_db_engine()
    if not engine:
        return

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
                
                # Преобразование данных в кодировку UTF-8
                data = data.apply(lambda col: col.map(lambda x: x.encode('utf-8').decode('utf-8') if isinstance(x, str) else x))
                
                # Преобразование названий колонок в нижний регистр и snake_case с учетом транслитерации
                data.columns = [to_snake_case(col) for col in data.columns]
                
                date_columns = [col for col in data.columns if 'data' in col]  # 'дата' transliterates to 'data'
                for col in date_columns:
                    data[col] = pd.to_datetime(data[col], errors='coerce', dayfirst=True)

                data.to_sql(table_name, engine, if_exists='replace', index=False)
                print(f"Данные из {filename} успешно загружены в таблицу {table_name}.")
                created_tables.append(table_name)
            except Exception as e:
                logging.error(f"Ошибка загрузки данных из файла {filename}: {e}")
                print(f"Не удалось загрузить данные из {filename}: {e}")

    return created_tables

def print_first_five_rows(engine, table_name):
    query = f"SELECT * FROM {table_name} LIMIT 5"
    with engine.connect() as connection:
        result = pd.read_sql(query, connection)
        print(f"Первые 5 строк из таблицы '{table_name}':")
        print(result)

if __name__ == "__main__":
    # Проверка доступности пути на диске D:
    local_path = r'D:\NAS\заказы\Евгений\Access\Табличные выгрузки1С'
    network_path = r'\\26.218.196.12\заказы\Евгений\Access\Табличные выгрузки1С'
    
    # Если путь доступен на диске D, используем его, иначе — сетевой путь
    directory_path = local_path if os.path.exists(local_path) else network_path
    
    created_tables = upload_csv_files(directory_path)

    # Вывод первых 5 строк из каждой созданной таблицы
    engine = get_db_engine()
    for table_name in created_tables:
        print_first_five_rows(engine, table_name)
