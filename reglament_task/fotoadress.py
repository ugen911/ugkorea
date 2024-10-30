import os
import re
import pandas as pd
from ugkorea.db.database import get_db_engine

def process_catalog_files():
    # Получаем путь к папке "Downloads" текущего пользователя
    folder_path = os.path.join(os.path.expanduser('~'), 'Downloads')

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
        df.rename(columns={'sku': 'kod'}, inplace=True)
        
        # Задаем путь для сохранения файла
        output_dir = os.path.join(os.path.expanduser('~'), 'YandexDisk', 'ЮК')
        output_path = os.path.join(output_dir, 'photoadress.xlsx')
        
        # Проверка доступности директории
        if os.path.exists(output_dir):
            # Сохранение файла
            df[['kod', 'adress']].to_excel(output_path, index=False)
            print(f"Файл успешно обработан и сохранен: {output_path}")
        else:
            print(f"Директория {output_dir} недоступна. Сохранение файла пропущено.")

        # Подключение к базе данных
        engine = get_db_engine()

        # Выгрузка данных в таблицу photoadress
        df[['kod', 'adress']].to_sql('photoadress', con=engine, if_exists='replace', index=False)

        print("Данные успешно выгружены в базу данных в таблицу photoadress.")

    except FileNotFoundError as e:
        print(e)
    except Exception as e:
        print(f"Произошла ошибка: {e}")

def get_and_process_data():
    # Подключение к базе данных
    engine = get_db_engine()

    # Загрузка таблиц из базы данных
    nomenklaturaold = pd.read_sql('SELECT * FROM nomenklaturaold', con=engine)
    photoadress = pd.read_sql('SELECT * FROM photoadress', con=engine)
    stockold = pd.read_sql('SELECT * FROM stockold', con=engine)

    # Очистка колонок 'kod' от пробелов
    for df in [nomenklaturaold, photoadress, stockold]:
        df['kod'] = df['kod'].str.strip()

    # Объединение таблиц
    merged_df = pd.merge(nomenklaturaold, photoadress, on='kod', how='left')
    merged_df = pd.merge(merged_df, stockold, on='kod', how='left')

    # Преобразование колонки stockold в float и заполнение NaN значений нулями
    merged_df["osnsklad"] = merged_df["osnsklad"].astype(float).fillna(0)
    # merged_df.to_excel('merged.xlsx')

    # Фильтрация данных
    filtered_df = merged_df[
        (merged_df['osnsklad'] > 0) & 
        (merged_df['adress'].isna() | merged_df['adress'].str.strip().eq(''))
    ]

    # Условия для фильтрации строк по 'naimenovanie'
    exclude_starts = [
        "Автокондиционер",
        "Автосигнализация",
        "Автоэлектрика",
        "Болт М",
        "Бутылка",
        "Гайка М",
        "Эфир",
        "Заклепки для",
        "Гильза силового провода",
        "Герметик-прокладка",
        "Шайба",
        "Упл.кольцо",
        "Хомут",
        "Клемма",
        "Расходные материалы",
        "Смазка",
        "ТАРА",
        "Тормозная жидкость",
        "Шпилька",
        "Шплинт",
        "Шиномонтаж",
        "Штифт",
        "Очиститель тормозов",
        "Антифриз",
        "Инструмент",
        "Тройник",
        "Ввертыш",
        "Болт крепления номера",
        "FELIX",
        "Шина",
    ]

    exclude_contains = ["масло моторное", "масло трансмиссионное"]

    # Фильтрация строк
    filtered_df = filtered_df[
        ~filtered_df['naimenovanie'].str.startswith(tuple(exclude_starts)) &
        ~filtered_df['naimenovanie'].str.contains('|'.join(exclude_contains), case=False, na=False)
    ]

    # Выбор необходимых колонок, включая osnsklad
    result_df = filtered_df[['kod', 'artikul', 'proizvoditel', 'naimenovanie', 'edizm', 'stellazh', 'osnsklad']]

    # Сортировка по колонке 'naimenovanie'
    result_df = result_df.sort_values(by='naimenovanie')

    # Попытка сохранить файл в первую директорию
    output_dir1 = r'D:\NAS\заказы\Евгений'
    output_dir2 = r'\\26.218.196.12\заказы\Евгений'
    file_name = 'ПозициибезФотоСайт.xlsx'
    output_path = os.path.join(output_dir1, file_name)

    try:
        if os.path.exists(output_dir1):
            result_df.to_excel(output_path, index=False)
            print(f"Файл успешно сохранен: {output_path}")
        elif os.path.exists(output_dir2):
            output_path = os.path.join(output_dir2, file_name)
            result_df.to_excel(output_path, index=False)
            print(f"Файл успешно сохранен: {output_path}")
        else:
            print("Обе директории недоступны. Файл не сохранен.")
    except Exception as e:
        print(f"Произошла ошибка при сохранении файла: {e}")

# Основной исполняемый код
if __name__ == "__main__":
    process_catalog_files()
    get_and_process_data()
