import os
import pandas as pd
import zipfile
import tempfile
import warnings
import chardet
from ugkorea.from_folder_to_df.configs import configs

warnings.filterwarnings("ignore", message="Workbook contains no default style, apply openpyxl's default")


def find_repo_folder(start_path, folder_structure):
    """Рекурсивный поиск папки с указанной структурой."""
    for root, dirs, files in os.walk(start_path):
        # Создаём полный путь для проверки
        full_path = os.path.join(root, folder_structure)
        if os.path.isdir(full_path):
            return full_path
    return None


# Задаём структуру папок для поиска
target_folder_structure = "repo\\ugkorea\\Output"
    
# Начинаем поиск с диска C:
folder_path = find_repo_folder("C:\\", target_folder_structure)
    
###folder_path = r"C:\Users\evgen\repo\ugkorea\Output"

def add_columns_to_df(df, name):
    # Добавление колонки с текущей датой
    df['дата'] = pd.to_datetime('today').strftime("%Y-%m-%d")
    # Добавление колонки с именем датафрейма
    df['поставщик'] = name
    return df


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

def detect_encoding(file_path):
    with open(file_path, 'rb') as file:
        raw_data = file.read(10000)  # Читаем часть файла для анализа
        result = chardet.detect(raw_data)
        return result['encoding']

def read_csv_file(file_path):
    try:
        encoding = detect_encoding(file_path)
        return pd.read_csv(file_path, sep=';', encoding=encoding)
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
    elif 'FORUM' and 'CENTER' in base_name:
        return 'forumcenter'
    elif 'FORUM' and 'NVS' in base_name:
        return 'forumnvs'
    elif 'FORUM' and 'KRNK' in base_name:
        return 'forumkrsk'
    elif 'TISS' in base_name:
        return 'tiss'
    return base_name.lower()

def process_dataframe(df, name):
    if 'tiss' in name:
        df.columns = df.iloc[4]
        df.drop(index=4, inplace=True)
        df.dropna(axis=1, how='all', inplace=True)
    elif name in ['forumcenter', 'forumnvs', 'forumkrsk']:
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
    all_created_dataframes = []
    all_items = os.listdir(directory_path)
    for item_name in all_items:
        item_path = os.path.join(directory_path, item_name)
        if item_name.endswith('.zip'):
            print(f"Распаковка архива: {item_name}")
            with zipfile.ZipFile(item_path, 'r') as zip_ref:
                with tempfile.TemporaryDirectory() as temp_folder:
                    zip_ref.extractall(temp_folder)
                    all_created_dataframes.extend(process_files_in_directory(temp_folder))
        elif os.path.isfile(item_path):
            df = create_dataframe(item_path)
            if df is not None:
                dataframe_name = standardize_dataframe_name(item_name)
                df = process_dataframe(df, dataframe_name)
                globals()[dataframe_name] = df
                all_created_dataframes.append(dataframe_name)
                print(f"Создан DataFrame: {dataframe_name}, Первые строки:")
                print(df.head())
    return all_created_dataframes

# Обработка файлов в заданной папке
all_created_dataframes = process_files_in_directory(folder_path)
print("\nИмена всех созданных датафреймов:")
for df_name in all_created_dataframes:
    print(df_name)


def collect_dataframes(configs):
    dataframes = {}
    for name in configs.keys():
        dataframe = globals().get(name)
        if dataframe is not None:
            dataframes[name] = dataframe
        else:
            print(f"DataFrame '{name}' is not defined.")
    return dataframes

# Определение функции обработки
def process_dataframe(df, config):
    df.rename(columns=config['rename_columns'], inplace=True)
    df.dropna(subset=['цена', 'производитель', 'артикул'], inplace=True)
    df = df[(df['цена'] != 0) & (df['производитель'] != 0) & (df['артикул'] != 0)]
    
    for column, replacements in config.get('replace_values', {}).items():
        if pd.api.types.is_string_dtype(df[column]):
            for old, new in replacements:
                df[column] = df[column].str.replace(old, new, regex=True)
        else:
            print(f"Warning: Cannot apply string operations on non-string column '{column}'")
    
    df = df[config['column_order']]
    
    # Преобразование типов данных
    for column, dtype in config['convert_types'].items():
        try:
            if pd.api.types.is_string_dtype(df[column]):
                df.loc[:, column] = df[column].str.replace(',', '.').astype(dtype)
            else:
                df.loc[:, column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(dtype)
        except Exception as e:
            print(f"Error converting {column} to {dtype}: {e}")
    
    return df

# Конфигурация для каждого датафрейма
configs = configs

def get_df_main(folder_path=folder_path):
    # Создаем словарь для хранения датафреймов
    dataframes= {}
    
    # Цикл, где мы обрабатываем и добавляем колонки:
    for name, config in configs.items():
        df = globals().get(name)
        if df is not None:
            processed_df = process_dataframe(df, config)
            df_with_new_columns = add_columns_to_df(processed_df, name)
            dataframes[name] = df_with_new_columns
            print(name)
            print(df_with_new_columns.head())
        else:
            print(f"DataFrame '{name}' is not defined.")
    
    return dataframes

   
   
if __name__ == "__main__":
    get_df_main()    
