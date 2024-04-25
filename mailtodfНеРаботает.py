import os
import pandas as pd
from datetime import datetime
import zipfile
import tempfile
import warnings

# Игнорирование предупреждений библиотеки
warnings.filterwarnings("ignore", message="Workbook contains no default style, apply openpyxl's default")

# Путь к папке с файлами
folder_path = r"C:\Users\evgen\repo\ugkorea\Output"


# Конфигурация для каждого датафрейма
configs = {
    'berg': {
        'rename_columns': {
            'Артикул': 'артикул', 'Наименование': 'наименование',
            'Бренд': 'производитель', 'Склад': 'склад',
            'Количество': 'количество', 'Цена руб': 'цена'
        },
        'column_order': ['артикул', 'наименование', 'производитель', 'склад', 'количество', 'цена'],
        'convert_types': {'цена': float, 'количество': int},
        'replace_values': {'цена': [(',', '.')]}
    },
    'shateekat': {
        'rename_columns': {
            'Бренд': 'производитель', 'Каталожный номер': 'артикул',
            'Описание': 'наименование', 'Остаток': 'количество', 'Цена': 'цена'
        },
        'column_order': ['артикул', 'наименование', 'производитель', 'количество', 'цена'],
        'convert_types': {'цена': float, 'количество': int},
        'replace_values': {'количество': [('>', '')]}
    },
    'shatepodolsk': {
        'rename_columns': {
            'Бренд': 'производитель', 'Каталожный номер': 'артикул',
            'Описание': 'наименование', 'Остаток': 'количество', 'Цена': 'цена'
        },
        'column_order': ['артикул', 'наименование', 'производитель', 'количество', 'цена'],
        'convert_types': {'цена': float, 'количество': int},
        'replace_values': {'количество': [('>', '')]}
    },
    'favorit': {
        'rename_columns': {
            'Производитель': 'производитель', 'Номер по каталогу': 'артикул',
            'Наименование': 'наименование', 'Цена по договору': 'цена', 'Количество': 'количество'
        },
        'column_order': ['артикул', 'наименование', 'производитель', 'цена', 'количество'],
        'convert_types': {'цена': float, 'количество': int}
    },
    'forumcenter': {
        'rename_columns': {
            'ГРУППА': 'производитель', '№ ПРОИЗВ.': 'артикул',
            'НАИМЕНОВАНИЕ': 'наименование', 'ЦЕНА, РУБ': 'цена', 'НАЛичие': 'количество'
        },
        'column_order': ['производитель', 'артикул', 'наименование', 'цена', 'количество'],
        'convert_types': {'цена': float, 'количество': int}
    },
    'forumnvs': {
        'rename_columns': {
            'ГРУППА': 'производитель', '№ ПРОИЗВ.': 'артикул',
            'НАИМЕНОВАНИЕ': 'наименование', 'ЦЕНА, РУБ': 'цена', 'НАЛичие': 'количество'
        },
        'column_order': ['производитель', 'артикул', 'наименование', 'цена', 'количество'],
        'convert_types': {'цена': float, 'количество': int}
    },
    'tiss': {
        'rename_columns': {
            'Бренд': 'производитель', 'Наименование товаров': 'наименование',
            'Катал. номер': 'артикул', 'ОПТ': 'цена', 'Кол-во всего': 'количество'
        },
        'column_order': ['производитель', 'наименование', 'артикул', 'цена', 'количество'],
        'convert_types': {'цена': float, 'количество': int},
        'replace_values': {'цена': [(',', '.')]}
    }
}


def add_columns_to_df(df, name):
    """Добавление столбцов с датой и поставщиком."""
    df['дата'] = pd.to_datetime('today').strftime("%Y-%m-%d")
    df['поставщик'] = name
    return df

def find_headers(file_path):
    """Нахождение строки с заголовками в Excel файле."""
    temp_df = pd.read_excel(file_path, nrows=5)
    for i, row in temp_df.iterrows():
        if any(isinstance(cell, str) for cell in row):
            return i
    return None

def read_excel_file(file_path):
    """Чтение Excel файла с автоматическим нахождением заголовков."""
    headers_row = find_headers(file_path)
    if headers_row is not None:
        return pd.read_excel(file_path, header=headers_row)
    else:
        print(f"Не удалось найти заголовки в файле: {file_path}")
        return None

def read_csv_file(file_path):
    """Чтение CSV файла с заданными параметрами."""
    try:
        return pd.read_csv(file_path, sep=';', encoding='utf-8')
    except Exception as e:
        print(f"Ошибка при чтении файла CSV: {e}")
        return None

def standardize_dataframe_name(file_name):
    """Преобразует имя файла в стандартизированное имя датафрейма."""
    base_name = os.path.splitext(file_name)[0].replace(' ', '_').upper()
    if 'BERG' in base_name:
        return 'berg'
    elif 'EKATERINBURG' in base_name:
        return 'shateekat'
    elif 'PODOLSK' in base_name:
        return 'shatepodolsk'
    elif 'FAVORIT' in base_name:
        return 'favorit'
    elif 'FORUM' in base_name:
        return 'forumcenter' if 'CENTER' in base_name else 'forumnvs'
    elif 'TISS' in base_name:
        return 'tiss'
    return base_name.lower()

def process_dataframe(df, name):
    config = configs.get(name)
    if not isinstance(config, dict):
        raise ValueError(f"No valid configuration for dataframe with name '{name}'. Check your configs dictionary.")

    # Переименование столбцов с проверкой на их наличие
    rename_columns = {k: v for k, v in config['rename_columns'].items() if k in df.columns}
    df.rename(columns=rename_columns, inplace=True)

    # Применение замен только к строковым столбцам
    for column, replacements in config.get('replace_values', {}).items():
        if column in df.columns and pd.api.types.is_string_dtype(df[column]):
            for old, new in replacements:
                df[column] = df[column].str.replace(old, new, regex=True)

    # Конвертация типов данных
    for column, dtype in config['convert_types'].items():
        if column in df.columns:
            df[column] = pd.to_numeric(df[column].replace(',', '.', regex=True), errors='coerce').fillna(0).astype(dtype)

    # Переупорядочение столбцов
    df = df[[col for col in config['column_order'] if col in df.columns]]

    return df




def process_all_files():
    """Основная функция для обработки файлов и вывода информации."""
    try:
        processed_dataframes = extract_and_process_files(folder_path)
        for name, df in processed_dataframes:
            df = add_columns_to_df(df, name)
            print(f"Создан DataFrame: {name}, Первые строки:")
            print(df.head())
    except Exception as e:
        print(f"Ошибка при обработке файлов: {e}")



def extract_and_process_files(directory_path):
    """Рекурсивная обработка и извлечение файлов из директории."""
    all_dataframes = []
    for item in os.listdir(directory_path):
        path = os.path.join(directory_path, item)
        if item.endswith('.zip'):
            with zipfile.ZipFile(path, 'r') as zip_ref, tempfile.TemporaryDirectory() as temp_dir:
                zip_ref.extractall(temp_dir)
                all_dataframes.extend(extract_and_process_files(temp_dir))
        elif os.path.isfile(path):
            df = create_dataframe(path)
            if df is not None:
                df_name = standardize_dataframe_name(item)
                df = process_dataframe(df, df_name)
                all_dataframes.append((df_name, df))
    return all_dataframes

def create_dataframe(file_path):
    """Фабрика для создания DataFrame из разных типов файлов."""
    if file_path.endswith(('.xlsx', '.xls')):
        return read_excel_file(file_path)
    elif file_path.endswith('.csv'):
        return read_csv_file(file_path)
    else:
        print(f"Формат файла не поддерживается: {file_path}")
        return None

def process_all_files():
    """Основная функция для обработки файлов и вывода информации."""
    processed_dataframes = extract_and_process_files(folder_path)
    for name, df in processed_dataframes:
        df = add_columns_to_df(df, name)
        print(f"Создан DataFrame: {name}, Первые строки:")
        print(df.head())

if __name__ == "__main__":
    process_all_files()
