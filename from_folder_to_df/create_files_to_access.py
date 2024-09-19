from ugkorea.db.database import get_db_engine
import pandas as pd
from sqlalchemy import text
import os
from datetime import datetime, timedelta

engine = get_db_engine()


# Функция для получения списка таблиц в указанной схеме
def get_tables_in_schema(engine, schema):
    with engine.connect() as connection:
        query = text(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'")
        result = connection.execute(query)
        return [row[0] for row in result]

# Функция для получения данных из таблицы с указанным именем в указанной схеме
def get_data_from_table(engine, schema, table):
    with engine.connect() as connection:
        query = f"SELECT * FROM {schema}.{table} WHERE дата = (SELECT MAX(дата) FROM {schema}.{table})"
        return pd.read_sql_query(query, connection)

# Схема, в которой находятся таблицы
schema = 'prices'

# Получение списка таблиц в указанной схеме
tables_to_import = get_tables_in_schema(engine, schema)

# Получение данных из каждой таблицы и сохранение в отдельный датафрейм
dataframes = {}
for table in tables_to_import:
    dataframes[table] = get_data_from_table(engine, schema, table)

# Вывод первых строк каждого датафрейма
for table, df in dataframes.items():
    print(f"DataFrame '{table}':")
    print(df.head())
    print("\n")


def filter_by_date(df):
    # Проверяем наличие колонки "дата" в датафрейме
    if 'дата' in df.columns:
        try:
            # Преобразуем значение 'дата' в формат datetime
            df['дата'] = pd.to_datetime(df['дата'])
            # Определяем сегодняшнюю дату
            today = datetime.now().date()
            # Оставляем только строки, где разница между датой и сегодняшней датой меньше или равна 4 дням
            df = df[df['дата'] >= today - timedelta(days=4)]
        except Exception as e:
            print(f"Error occurred while filtering by date: {e}")
    return df


# Создание пустого словаря для хранения переименованных датафреймов
renamed_dataframes = {}

for table in dataframes.keys():
    # Проверка, присутствует ли таблица в списке необходимых таблиц
    if table in ['berg', 'favorit', 'forumcenter', 'forumnvs', 'shateekat', 'shatepodolsk', 'tiss']:
        # Получение датафрейма для текущей таблицы
        df = dataframes[table]
        # Отбор строк по дате
        df = filter_by_date(df)
        
        # Если таблица - 'berg', оставляем только нужные колонки и переименовываем датафрейм в 'Berg'
        if table == 'berg':
            df = df[['артикул', 'наименование', 'производитель', 'склад', 'цена']]
            renamed_dataframes['Berg'] = df
        
        # Если таблица - 'favorit', оставляем только нужные колонки и переименовываем датафрейм в 'Favorit'
        elif table == 'favorit':
            df = df[['артикул', 'наименование', 'производитель', 'цена']]
            renamed_dataframes['Favorit'] = df
        
        # Если таблица - 'forumcenter', оставляем только нужные колонки и переименовываем датафрейм в 'ForumCenter'
        elif table == 'forumcenter':
            df = df[['артикул', 'наименование', 'производитель', 'цена']]
            renamed_dataframes['ForumAutoCenter'] = df
        
        # Если таблица - 'forumnvs', оставляем только нужные колонки и переименовываем датафрейм в 'ForumNVS'
        elif table == 'forumnvs':
            df = df[['артикул', 'наименование', 'производитель', 'цена']]
            renamed_dataframes['ForumAutoNSK'] = df
        
        # Если таблица - 'shateekat', оставляем только нужные колонки и переименовываем датафрейм в 'Shateekat'
        elif table == 'shateekat':
            df = df[['артикул', 'наименование', 'производитель',  'цена']]
            renamed_dataframes['ShateEkat'] = df
        
        # Если таблица - 'shatepodolsk', оставляем только нужные колонки и переименовываем датафрейм в 'Shatepodolsk'
        elif table == 'shatepodolsk':
            df = df[['артикул', 'наименование', 'производитель', 'цена']]
            renamed_dataframes['ShatePodolsk'] = df
        
        # Если таблица - 'tiss', оставляем только нужные колонки и переименовываем датафрейм в 'Tiss'
        elif table == 'tiss':
            df = df[['артикул', 'наименование', 'производитель', 'цена']]
            renamed_dataframes['Tiss'] = df

# Вывод первых строк каждого переименованного датафрейма
for table, df in renamed_dataframes.items():
    print(f"DataFrame '{table}':")
    print(df.head())
    print("\n")


# Создание пустого словаря для хранения переименованных и отформатированных датафреймов
renamed_and_formatted_dataframes = {}

# Цикл по всем датафреймам в словаре renamed_dataframes
for table, df in renamed_dataframes.items():
    # Замена названия колонки
    df = df.rename(columns={'производитель': 'бренд'})
    
    # Преобразование названий колонок к верхнему регистру
    df.columns = map(str.capitalize, df.columns)
    
    # Добавление отформатированного датафрейма в словарь
    renamed_and_formatted_dataframes[table] = df

# Вывод первых строк каждого переименованного и отформатированного датафрейма
for table, df in renamed_and_formatted_dataframes.items():
    print(f"DataFrame '{table}':")
    print(df.head())
    print("\n")


# Путь к локальной папке
local_folder = r'D:\NAS\заказы\Евгений\New\Файлы для работы Access\Прайсы для переоценки'
# Путь к сетевой папке
network_folder = r'\\26.218.196.12\заказы\Евгений\New\Файлы для работы Access\Прайсы для переоценки'

# Проверка доступности локальной папки
if os.path.exists(local_folder):
    output_folder = local_folder
    print(f"Локальная папка доступна: {output_folder}")
else:
    # Если локальная папка недоступна, проверяем сетевую папку
    try:
        # Попытка проверить доступность сетевой папки
        if os.path.exists(network_folder):
            output_folder = network_folder
            print(f"Сетевая папка доступна: {output_folder}")
        else:
            raise FileNotFoundError(f"Ни локальная, ни сетевая папки не найдены.")
    except Exception as e:
        print(f"Ошибка при доступе к сетевой папке: {e}")
        output_folder = None

if output_folder:
    # Цикл по каждому датафрейму в словаре renamed_and_formatted_dataframes
    for table, df in renamed_and_formatted_dataframes.items():
        # Формирование пути к файлу
        file_name = f"{table}.xlsx"
        file_path = os.path.join(output_folder, file_name)

        # Сохранение датафрейма в файл Excel с кодировкой windows-1251
        df.to_excel(file_path, index=False)

        print(f"Создан файл '{file_name}' в папке '{output_folder}'")
else:
    print("Нет доступных папок для сохранения файлов.")