import os
import pandas as pd
from sqlalchemy.exc import OperationalError
from sqlalchemy import text
import random
import string
import re
import transliterate
from ugkorea.db.database import get_db_engine
from ugkorea.db.config import remote_db_config  # Импорт конфигурации для проверки

def load_data(file_path):
    try:
        data = pd.read_excel(file_path)
        return data
    except FileNotFoundError:
        print(f"Файл '{file_path}' не найден.")
        exit()

def snake_case(column_name):
    # Удаляем пробелы по краям, затем приводим к нижнему регистру, транслитерируем и заменяем пробелы на нижнее подчеркивание
    column_name = column_name.strip()
    column_name = column_name.lower()
    column_name = transliterate.translit(column_name, 'ru', reversed=True)
    column_name = re.sub(r'\s+', '_', column_name)
    column_name = re.sub(r'[^a-z0-9_]', '', column_name)
    return column_name

def clean_string_columns(df):
    """
    Убираем пробелы и непечатные символы для всех строковых колонок в датафрейме.
    """
    # Применяем функцию только к строковым столбцам
    df = df.apply(lambda col: col.str.strip() if col.dtype == 'object' else col)
    df = df.apply(lambda col: col.str.encode('utf-8').str.decode('utf-8').str.strip() if col.dtype == 'object' else col)
    return df

def prepare_data(data):
    # Преобразование названий столбцов в snake_case
    data.columns = [snake_case(col) for col in data.columns]
    
    # Переименовываем конкретное поле
    data.rename(columns={'polnoenaimenovanie': 'naimenovanie'}, inplace=True)
    
    # Подготовка данных номенклатуры, включая столбец 'stellazh'
    nomenklatura = data[['kod', 'naimenovanie', 'artikul', 'proizvoditel', 'edizm', 'pometkaudalenija', 'stellazh']]
    nomenklatura = nomenklatura[['kod', 'artikul', 'proizvoditel', 'naimenovanie', 'edizm', 'stellazh', 'pometkaudalenija']]
    
    # Подготовка данных склада и преобразование в float
    stock = data[['kod', 'konost']].copy()
    stock['osnsklad'] = data['konost'].fillna(0).replace(',', '.', regex=True).astype(float)
    stock.drop(columns=['konost'], inplace=True)
    stock['zakazy_sklad'] = data['ostatokzakazy'].fillna(0).replace(',', '.', regex=True).astype(float)
    
    # Подготовка данных цены и преобразование в float
    price = data[['kod', 'tsenazakup', 'tsenarozn']].copy()
    price['tsenazakup'] = price['tsenazakup'].fillna(0).replace(',', '.', regex=True).astype(float)
    price['tsenarozn'] = price['tsenarozn'].fillna(0).replace(',', '.', regex=True).astype(float)
    
    return nomenklatura, stock, price

def generate_analog_groups(data):
    data.columns = [snake_case(col) for col in data.columns]
    data.rename(columns={'kod_1s': 'kod', 'kod_analoga': 'analog'}, inplace=True)
    
    analog_dict = {}
    
    for index, row in data.iterrows():
        code = row['kod']
        analog = row['analog']
        
        if code not in analog_dict:
            analog_dict[code] = set()
        if analog not in analog_dict:
            analog_dict[analog] = set()
        
        analog_dict[code].add(analog)
        analog_dict[analog].add(code)
    
    def find_group(code, visited, group):
        if code not in visited:
            visited.add(code)
            group.add(code)
            for analog in analog_dict[code]:
                find_group(analog, visited, group)
    
    visited = set()
    groups = []
    
    for code in analog_dict:
        if code not in visited:
            group = set()
            find_group(code, visited, group)
            groups.append(group)
    
    group_ids = {}
    for group in groups:
        group_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        for code in group:
            group_ids[code] = group_id
    
    return group_ids

def prepare_analog_data(file_path):
    data = load_data(file_path)
    if data is not None:
        # Удаляем строки, где одна из ячеек пуста или содержит только непечатные символы
        def is_invalid(value):
            if pd.isna(value):
                return True
            value_str = str(value).strip()
            if not value_str:
                return True
            if all(char not in string.printable for char in value_str):
                return True
            return False

        # Предполагаем, что у data две колонки
        data = data[~(data.iloc[:, 0].apply(is_invalid) | data.iloc[:, 1].apply(is_invalid))]

        group_ids = generate_analog_groups(data)
        analog_df = pd.DataFrame(list(group_ids.items()), columns=['kod_1s', 'gruppa_analogov'])
        analog_df.drop_duplicates(subset=['kod_1s'], inplace=True)
        return analog_df
    return None

def export_to_db(engine, df, table_name):
    try:
        # Приводим названия столбцов к snake_case и очищаем их от пробелов
        df.columns = [snake_case(col) for col in df.columns]
        # Очищаем данные в строковых столбцах от лишних пробелов и непечатных символов
        df = clean_string_columns(df)
        
        # Если в DataFrame существует столбец "kod" или "kod_1s", делаем его индексом
        if 'kod' in df.columns:
            df.set_index('kod', inplace=True)
        elif 'kod_1s' in df.columns:
            df.set_index('kod_1s', inplace=True)
        
        df.to_sql(table_name, engine, index=True, if_exists='replace')
        print(f"Таблица '{table_name}' была успешно создана или заменена.")
    except OperationalError as e:
        print(f"Ошибка при экспорте данных в таблицу '{table_name}': {e}")

def print_first_five_rows(engine, table_name):
    query = f"SELECT * FROM {table_name} LIMIT 5"
    with engine.connect() as connection:
        result = connection.execute(text(query))
        rows = result.fetchall()
        print(f"Первые 5 строк из таблицы '{table_name}':")
        for row in rows:
            print(row)

def load_middle_max_price_data(file_path):
    try:
        data = pd.read_excel(file_path)
        return data
    except FileNotFoundError:
        print(f"Файл '{file_path}' не найден.")
        exit()

def prepare_middle_max_price_data(data):
    # Преобразование названий колонок в snake_case
    data.columns = [snake_case(col) for col in data.columns]

    # Выбираем нужные колонки, используя новые названия
    middle_max_price = data[['kod_nomenklatury', 'srednjaja_tsena_zakupki', 'maksimalnaja_tsena_tovara']].copy()

    # Преобразуем строковые значения в числовые и заменяем пустые значения на 0
    middle_max_price['srednjaja_tsena_zakupki'] = middle_max_price['srednjaja_tsena_zakupki'].fillna(0).replace(',', '.', regex=True).astype(float)
    middle_max_price['maksimalnaja_tsena_tovara'] = middle_max_price['maksimalnaja_tsena_tovara'].fillna(0).replace(',', '.', regex=True).astype(float)

    # Переименовываем колонки для соответствия структуре базы данных
    middle_max_price.rename(columns={
        'kod_nomenklatury': 'kod',
        'srednjaja_tsena_zakupki': 'middleprice',
        'maksimalnaja_tsena_tovara': 'maxprice'
    }, inplace=True)

    return middle_max_price

def main():
    # Подключение к базе данных
    engine = get_db_engine()
    if engine is None:
        print("Не удалось подключиться к базе данных.")
        return
    
    # Определение путей к файлам в зависимости от доступности
    local_path = r'D:\NAS\заказы\Евгений\New\Файлы для работы Access\ОстаткиДляАнализа.xls'
    remote_path = r'\\26.218.196.12\заказы\Евгений\New\Файлы для работы Access\ОстаткиДляАнализа.xls'
    middle_max_file_local = r'D:\NAS\заказы\Евгений\New\Файлы для работы Access\СредняяИМаксимальная.xls'
    middle_max_file_remote = r'\\26.218.196.12\заказы\Евгений\New\Файлы для работы Access\СредняяИМаксимальная.xls'

    if os.path.exists(local_path):
        data_file_path = local_path
        analog_file_path = r'D:\NAS\заказы\Евгений\New\Файлы для работы Access\analogi.xls'
        middle_max_file_path = middle_max_file_local
    elif os.path.exists(remote_path):
        data_file_path = remote_path
        analog_file_path = r'\\26.218.196.12\заказы\Евгений\New\Файлы для работы Access\analogi.xls'
        middle_max_file_path = middle_max_file_remote
    else:
        print("Ни один из путей к файлам не доступен.")
        return

    # Загрузка и обработка основных данных
    data = load_data(data_file_path)
    if data is not None:
        data = clean_string_columns(data)  # Удаление пробелов и непечатных символов из данных
        nomenklatura, stock, price = prepare_data(data)
        export_to_db(engine, nomenklatura, 'nomenklaturaold')
        export_to_db(engine, stock, 'stockold')
        export_to_db(engine, price, 'priceold')

    # Загрузка и обработка данных аналогов
    analog_data = prepare_analog_data(analog_file_path)
    if analog_data is not None:
        analog_data = clean_string_columns(analog_data)  # Очистка строковых данных
        export_to_db(engine, analog_data, 'groupanalogiold')

    # Загрузка и обработка данных средней и максимальной цены
    middle_max_data = load_middle_max_price_data(middle_max_file_path)
    if middle_max_data is not None:
        middle_max_data = clean_string_columns(middle_max_data)  # Очистка строковых данных
        middle_max_price = prepare_middle_max_price_data(middle_max_data)
        export_to_db(engine, middle_max_price, 'middlemaxprice')

    # Вывод первых 5 строк из каждой созданной таблицы
    print_first_five_rows(engine, 'nomenklaturaold')
    print_first_five_rows(engine, 'stockold')
    print_first_five_rows(engine, 'priceold')
    print_first_five_rows(engine, 'groupanalogiold')
    print_first_five_rows(engine, 'middlemaxprice')

    print("Все операции выполнены успешно.")

if __name__ == "__main__":
    main()
