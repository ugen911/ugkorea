from ugkorea.db.database import get_db_engine
import os
import pandas as pd
from transliterate import translit, get_available_language_codes
import logging

engine = get_db_engine()

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

    for filename in os.listdir(directory_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory_path, filename)
            # Удаление префиксов и транслитерация
            modified_name = filename.replace('Выгрузка_', '').replace('Выгрузка', '')
            # Преобразование в snake_case с транслитерацией для имени таблицы
            table_name = to_snake_case(os.path.splitext(modified_name)[0])

            try:
                data = pd.read_csv(file_path, sep=';', on_bad_lines='warn')
                
                # Преобразование названий колонок в нижний регистр и snake_case с учетом транслитерации
                data.columns = [to_snake_case(col) for col in data.columns]
                
                date_columns = [col for col in data.columns if 'data' in col]  # 'дата' transliterates to 'data'
                for col in date_columns:
                    data[col] = pd.to_datetime(data[col], errors='coerce', dayfirst=True)

                data.to_sql(table_name, engine, if_exists='replace', index=False)
                print(f"Данные из {filename} успешно загружены в таблицу {table_name}.")
            except Exception as e:
                logging.error(f"Ошибка загрузки данных из файла {filename}: {e}")
                print(f"Не удалось загрузить данные из {filename}: {e}")

if __name__ == "__main__":
    network_path = r'\\26.218.196.12\заказы\Евгений\Access\Табличные выгрузки1С'
    upload_csv_files(network_path)

