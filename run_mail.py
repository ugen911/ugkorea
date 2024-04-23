import os
import pandas as pd
from datetime import datetime
import zipfile
import tempfile
import warnings

warnings.filterwarnings("ignore", message="Workbook contains no default style, apply openpyxl's default")

folder_path = r"C:\Users\evgen\repo\ugkorea\Output"

def find_headers(file_path):
    temp_df = pd.read_excel(file_path, nrows=5)
    for i, row in temp_df.iterrows():
        if any(isinstance(cell, str) for cell in row):
            return i
    return None

def read_excel_file(file_path):
    headers_row = find_headers(file_path)
    if headers_row is not None:
        return pd.read_excel(file_path, header=headers_row)
    else:
        print(f"Не удалось найти заголовки в файле: {file_path}")
        return None

def read_csv_file(file_path):
    try:
        return pd.read_csv(file_path, sep=';', encoding='utf-8')
    except Exception as e:
        print(f"Ошибка при чтении файла CSV: {e}")
        return None

def standardize_dataframe_name(file_name):
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
    if 'tiss' in name:
        df.columns = df.iloc[4]
        df.drop(index=4, inplace=True)
    elif name in ['forumcenter', 'forumnvs']:
        df.columns = df.iloc[1]
        df.drop(index=1, inplace=True)
    df.dropna(axis=1, how='all', inplace=True)
    return df

def create_dataframe(file_path):
    if file_path.endswith(('.xlsx', '.xls')):
        return read_excel_file(file_path)
    elif file_path.endswith('.csv'):
        return read_csv_file(file_path)
    else:
        print(f"Формат файла не поддерживается: {file_path}")
        return None

def process_files_in_directory(directory_path):
    all_created_dataframes = {}
    all_items = os.listdir(directory_path)
    for item_name in all_items:
        item_path = os.path.join(directory_path, item_name)
        if item_name.endswith('.zip'):
            print(f"Распаковка архива: {item_name}")
            with zipfile.ZipFile(item_path, 'r') as zip_ref:
                with tempfile.TemporaryDirectory() as temp_folder:
                    zip_ref.extractall(temp_folder)
                    all_created_dataframes.update(process_files_in_directory(temp_folder))
        elif os.path.isfile(item_path):
            df = create_dataframe(item_path)
            if df is not None:
                dataframe_name = standardize_dataframe_name(item_name)
                df = process_dataframe(df, dataframe_name)
                all_created_dataframes[dataframe_name] = df
                print(f"Создан DataFrame: {dataframe_name}, Первые строки:")
                print(df.head())
    return all_created_dataframes

# Обработка файлов в заданной папке
all_dataframes = process_files_in_directory(folder_path)

# Добавление колонок 'поставщик' и 'дата' к каждому DataFrame
today_date = datetime.now().date()
for name, df in all_dataframes.items():
    df['поставщик'] = name
    df['дата'] = today_date

print("\nИмена всех созданных датафреймов:")
for df_name in all_dataframes:
    print(df_name)
    print(all_dataframes[df_name].head())
