import os
import pandas as pd
import zipfile
import tempfile

# Путь к папке с файлами
folder_path = r"C:\Users\evgen\repo\ugkorea\Output"

# Функция для создания DataFrame из файла
def create_dataframe(file_path):
    if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
        # Читаем первые несколько строк для определения заголовков
        headers_row = None
        for row in pd.read_excel(file_path, nrows=5).itertuples():
            if any(isinstance(cell, str) for cell in row):
                headers_row = row.Index
                break
        
        if headers_row is not None:
            df = pd.read_excel(file_path, header=headers_row)
        else:
            print(f"Не удалось найти заголовки в файле: {file_path}")
            return None
            
    elif file_path.endswith('.csv'):
        try:
            # Читаем CSV с разделителем ';'
            df = pd.read_csv(file_path, sep=';', encoding='utf-8')
        except Exception as e:
            print(f"Ошибка при чтении файла CSV: {e}")
            return None
    else:
        print(f"Формат файла не поддерживается: {file_path}")
        return None
    return df


# Функция для обработки файлов во временной папке
def process_files_in_temp(temp_folder):
    # Список для хранения имен созданных датафреймов
    created_dataframes = []

    # Получаем список всех файлов во временной папке
    all_files = [f for f in os.listdir(temp_folder) if os.path.isfile(os.path.join(temp_folder, f))
                 and (f.endswith('.xls') or f.endswith('.xlsx') or f.endswith('.csv'))]

    # Проходим по каждому файлу
    for file_name in all_files:
        file_path = os.path.join(temp_folder, file_name)
        
        # Создаем DataFrame из файла
        df = create_dataframe(file_path)
        
        # Проверяем, удалось ли создать DataFrame
        if df is not None:
            # Создаем имя для DataFrame
            dataframe_name = os.path.splitext(file_name)[0]  # Имя файла без расширения
            dataframe_name = dataframe_name.replace(' ', '_')  # Заменяем пробелы на нижнее подчеркивание
            
            # Переименовываем DataFrame, если применимо
            if 'BERG' in dataframe_name.upper():
                dataframe_name = 'berg'
            elif 'EKATERINBURG' in dataframe_name.upper():
                dataframe_name = 'shateekat'
            elif 'PODOLSK' in dataframe_name.upper():
                dataframe_name = 'shatepodolsk'
            elif 'FAVORIT' in dataframe_name.upper():
                dataframe_name = 'favorit'
            elif 'FORUM' in dataframe_name.upper():
                if 'CENTER' in dataframe_name.upper():
                    dataframe_name = 'forumcenter'
                elif 'NVS' in dataframe_name.upper():
                    dataframe_name = 'forumnvs'
            elif 'TISS' in dataframe_name.upper():
                dataframe_name = 'tiss'
                # Устанавливаем имена колонок в 5 строке
                df.columns = df.iloc[4]
                df = df.drop([4])  # Удаляем пятую строку после установки имен колонок
                # Удаляем первые 10 строк
                df = df.drop(df.head(10).index)
                # Удаляем пустые колонки в датафрейме
                df = df.dropna(axis=1, how='all')
            else:
                # Удаляем все пустые и неименованные колонки
                df = df.dropna(axis=1, how='all')
            
            # Если это dataframe forumcenter или forumnvs, устанавливаем имена колонок второй строки
            if dataframe_name in ['forumcenter', 'forumnvs']:
                df.columns = df.iloc[1]
                df = df.drop(1)  # Удаляем вторую строку после установки имен колонок
            
            # Присваиваем DataFrame переменной с новым именем
            globals()[dataframe_name] = df
            
            created_dataframes.append(dataframe_name)  # Добавляем имя датафрейма в список
            
            # Выводим информацию о создании DataFrame
            print(f"Создан DataFrame: {dataframe_name}")
            
            # Выводим первые строки DataFrame
            print(f"Первые строки DataFrame {dataframe_name}:")
            print(df.head())
        else:
            print(f"Не удалось создать DataFrame из файла: {file_path}")

    # Возвращаем список созданных датафреймов
    return created_dataframes


# Список для хранения имен всех созданных датафреймов
all_created_dataframes = []

# Получаем список всех файлов и папок в папке
all_items = os.listdir(folder_path)

# Проходим по каждому элементу
for item_name in all_items:
    item_path = os.path.join(folder_path, item_name)
    
    # Если элемент - zip-архив, распаковываем его
    if item_name.endswith('.zip'):
        print(f"Распаковка архива: {item_name}")
        try:
            with zipfile.ZipFile(item_path, 'r') as zip_ref:
                # Создаем временную папку для распаковки
                with tempfile.TemporaryDirectory() as temp_folder:
                    # Распаковываем архив во временную папку
                    zip_ref.extractall(temp_folder)
                    
                    # Обрабатываем файлы во временной папке
                    created_dataframes = process_files_in_temp(temp_folder)
                    all_created_dataframes.extend(created_dataframes)
        except Exception as e:
            print(f"Ошибка при распаковке архива {item_name}: {e}")
    
    # Если элемент - обычный файл
    elif os.path.isfile(item_path):
        # Создаем DataFrame из файла
        df = create_dataframe(item_path)
        
        # Проверяем, удалось ли создать DataFrame
        if df is not None:
            # Создаем имя для DataFrame
            dataframe_name = os.path.splitext(item_name)[0]  # Имя файла без расширения
            dataframe_name = dataframe_name.replace(' ', '_')  # Заменяем пробелы на нижнее подчеркивание
            
            # Переименовываем DataFrame, если применимо
            if 'BERG' in dataframe_name.upper():
                dataframe_name = 'berg'
            elif 'EKATERINBURG' in dataframe_name.upper():
                dataframe_name = 'shateekat'
            elif 'PODOLSK' in dataframe_name.upper():
                dataframe_name = 'shatepodolsk'
            elif 'FAVORIT' in dataframe_name.upper():
                dataframe_name = 'favorit'
            elif 'FORUM' in dataframe_name.upper():
                if 'CENTER' in dataframe_name.upper():
                    dataframe_name = 'forumcenter'
                elif 'NVS' in dataframe_name.upper():
                    dataframe_name = 'forumnvs'
            elif 'TISS' in dataframe_name.upper():
                dataframe_name = 'tiss'
                # Устанавливаем имена колонок в 5 строке
                df.columns = df.iloc[4]
                df = df.drop([4])  # Удаляем пятую строку после установки имен колонок
                # Удаляем первые 10 строк
                df = df.drop(df.head(10).index)
                # Удаляем пустые колонки в датафрейме
                df = df.dropna(axis=1, how='all')
            else:
                # Удаляем все пустые и неименованные колонки
                df = df.dropna(axis=1, how='all')
            
            # Присваиваем DataFrame переменной с новым именем
            globals()[dataframe_name] = df
            
            all_created_dataframes.append(dataframe_name)  # Добавляем имя датафрейма в список
            
            # Выводим информацию о создании DataFrame
            print(f"Создан DataFrame: {dataframe_name}")
            
            # Выводим первые строки DataFrame
            print(f"Первые строки DataFrame {dataframe_name}:")
            print(df.head())
        else:
            print(f"Не удалось создать DataFrame из файла: {item_path}")

# Удаление пустых и неименованных колонок из всех датафреймов
for df_name in all_created_dataframes:
    df = globals()[df_name]
    globals()[df_name] = df.dropna(axis=1, how='all')

# Выводим имена всех созданных датафреймов
print("\nИмена всех созданных датафреймов:")
for df_name in all_created_dataframes:
    print(df_name)




if 'berg' in all_created_dataframes:
    berg = globals().get('berg')  # Получаем датафрейм berg

    # Оставляем только нужные колонки и переименовываем их
    berg = berg[['Артикул', 'Наименование', 'Бренд', 'Склад', 'Количество', 'Цена руб']]
    berg.columns = ['артикул', 'наименование', 'производитель', 'склад', 'количество', 'цена']

    # Преобразование типов данных
    try:
        berg['цена'] = berg['цена'].str.replace(',', '.').astype(float)  # Преобразовываем цену в тип float
    except AttributeError:
        pass  # If 'цена' is already numeric, continue without error

    berg['количество'] = berg['количество'].astype(int)  # Преобразовываем количество в тип int

    # Выводим информацию о датафрейме и первые 5 строк
    print("Информация о датафрейме berg:")
    print(berg.info())

    print("\nПервые строки обработанного DataFrame berg:")
    print(berg.head())

    # Обновляем значение в глобальном пространстве имен
    globals()['berg'] = berg


# Отдельная обработка датафреймов shateekat и shatepodolsk
for df_name in ['shateekat', 'shatepodolsk']:
    if df_name in all_created_dataframes:
        df = globals()[df_name]  # Получаем датафрейм

        # Удаляем символ '>' из значений столбца "количество"
        df['Остаток'] = df['Остаток'].str.replace('>', '')

        # Переименовываем столбцы
        df = df.rename(columns={'Бренд': 'производитель',
                                'Каталожный номер': 'артикул',
                                'Описание': 'наименование',
                                'Остаток': 'количество',
                                'Цена': 'цена'})

        # Изменяем порядок столбцов
        df = df[['артикул', 'наименование', 'производитель', 'количество', 'цена']]

        # Преобразование типов данных
        df['цена'] = df['цена'].astype(float)  # Преобразовываем цену в тип float
        df['количество'] = df['количество'].astype(int)  # Преобразовываем количество в тип int

        # Выводим информацию о датафрейме и первые 5 строк
        print(f"\nИнформация о датафрейме {df_name}:")
        print(df.info())

        print(f"\nПервые строки обработанного DataFrame {df_name}:")
        print(df.head())

        # Обновляем значение в глобальном пространстве имен
        globals()[df_name] = df


# Обработка датафрейма "favorit"
if 'favorit' in all_created_dataframes:
    df = favorit  # Получаем датафрейм

    # Переименовываем столбцы
    df = df.rename(columns={'Производитель': 'производитель',
                             'Номер по каталогу': 'артикул',
                             'Наименование': 'наименование',
                             'Цена по договору': 'цена',
                             'Количество': 'количество'})

    # Изменяем порядок столбцов
    df = df[['артикул', 'наименование', 'производитель', 'цена', 'количество']]

    # Преобразование типов данных
    if df['цена'].dtype == 'object':  # Check if 'цена' column is of object (string) type
        df['цена'] = df['цена'].str.replace(',', '.').astype(float)  # Преобразовываем цену в тип float

    df['количество'] = df['количество'].astype(int)  # Преобразовываем количество в тип int

    # Выводим информацию о датафрейме и первые 5 строк
    print("\nИнформация о датафрейме favorit:")
    print(df.info())

    print("\nПервые строки обработанного DataFrame favorit:")
    print(df.head())

    # Обновляем значение в глобальном пространстве имен
    favorit = df


# Assuming 'tiss' is already defined in the context
if 'tiss' in globals():
    df = tiss  # Access the DataFrame

    # Rename columns
    df = df.rename(columns={
        'Бренд': 'производитель',
        'Наименование товаров': 'наименование',
        'Катал. номер': 'артикул',
        'ОПТ': 'цена',
        'Кол-во всего': 'количество'
    })

    # Change column order
    df = df[['производитель', 'наименование', 'артикул', 'цена', 'количество']]

    # Handling NaNs more appropriately
    df['цена'] = df['цена'].fillna('0')  # Replace NaNs with '0' (string)

    # Conversion of 'цена' from string, handling non-numeric issues
    try:
        df['цена'] = df['цена'].replace(',', '.', regex=True).astype(float)
    except ValueError as e:
        print("Error converting 'цена':", e)
        print(df['цена'])

    # Handling 'количество' without using inplace to avoid warnings
    df['количество'] = df['количество'].fillna(0).astype(int)

    # Output DataFrame information and preview
    print("\nИнформация о датафрейме tiss:")
    print(df.info())

    print("\nПервые строки обработанного DataFrame tiss:")
    print(df.head())

    # Update the 'tiss' DataFrame in the global namespace
    tiss = df
else:
    print("DataFrame 'tiss' is not defined.")



# Обработка датафреймов "forumcenter" и "forumnvs"
for df_name in ['forumcenter', 'forumnvs']:
    if df_name in all_created_dataframes:
        df = globals()[df_name]  # Получаем датафрейм

        # Переименовываем столбцы
        df = df.rename(columns={'ГРУППА': 'производитель',
                                 '№ ПРОИЗВ.': 'артикул',
                                 'НАИМЕНОВАНИЕ': 'наименование',
                                 'ЦЕНА, РУБ': 'цена',
                                 'НАЛичие': 'количество'})

        # Изменяем порядок столбцов
        df = df[['производитель', 'артикул', 'наименование', 'цена', 'количество']]

        # Выводим информацию о типах данных
        print(f"\nТипы данных в датафрейме {df_name}:")
        print(df.dtypes)

        # Преобразование типов данных
        df['цена'] = df['цена'].astype(float)  # Преобразовываем цену в тип float
        df['количество'] = pd.to_numeric(df['количество'], errors='coerce')  # Преобразовываем количество в числовой тип

        # Заменяем пропущенные значения в столбце 'количество' на 0
        df['количество'].fillna(0, inplace=True)

        # Преобразовываем количество в тип int
        df['количество'] = df['количество'].astype(int)

        # Выводим информацию о датафрейме и первые 5 строк
        print(f"\nИнформация о датафрейме {df_name}:")
        print(df.info())

        print(f"\nПервые строки обработанного DataFrame {df_name}:")
        print(df.head())

        # Обновляем значение в глобальном пространстве имен
        globals()[df_name] = df

# Выводим имена датафреймов и первые 5 строк каждого
for df_name in all_created_dataframes:
    print(df_name)
    df = globals()[df_name]
    print(df.head())



