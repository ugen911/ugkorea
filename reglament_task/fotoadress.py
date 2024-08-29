import os
import re
import pandas as pd
from ugkorea.db.database import get_db_engine

# Путь к папке с файлами
folder_path = r'C:\Users\evgen\Downloads'

try:
    # Проверяем, существует ли папка
    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"Папка не найдена: {folder_path}")

    # Получаем список всех файлов в папке
    files = os.listdir(folder_path)

    # Фильтруем файлы, оставляя только нужные
    catalog_files = [f for f in files if re.match(r'catalog(\s\(\d+\))?\.csv', f)]

    # Проверяем, есть ли нужные файлы
    if not catalog_files:
        raise FileNotFoundError("Файлы catalog не найдены")

    # Сортируем файлы по числу в скобках
    catalog_files.sort(key=lambda f: int(re.search(r'\((\d+)\)', f).group(1)) if re.search(r'\((\d+)\)', f) else 0, reverse=True)

    # Берем самый свежий файл
    latest_file = catalog_files[0]

    # Полный путь к самому свежему файлу
    latest_file_path = os.path.join(folder_path, latest_file)

    # Чтение и обработка файла
    df = pd.read_csv(latest_file_path)
    df = df.dropna()
    df['photos'] = df['photos'].str.lower().replace({'.jpg':'_big.JPG'}, regex=True)

    def photo(row):
        plus = 'https://ugkorea.com/pictures/product/big/'
        string = ''
        row = row.split(sep=',')
        for i in row:
            string = string + plus + i + ','
        return string

    df['adress'] = df['photos'].apply(photo)
    output_path = r'C:\Users\evgen\YandexDisk\ЮК\photoadress.xlsx'
    df[['sku', 'adress']].to_excel(output_path, index=False)

    print(f"Файл успешно обработан и сохранен: {output_path}")

    # Подключение к базе данных
    engine = get_db_engine()

    # Выгрузка данных в таблицу photoadress
    df[['sku', 'adress']].to_sql('photoadress', con=engine, if_exists='replace', index=False)

    print("Данные успешно выгружены в базу данных в таблицу photoadress.")

except FileNotFoundError as e:
    print(e)
except Exception as e:
    print(f"Произошла ошибка: {e}")
