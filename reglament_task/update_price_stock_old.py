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
    column_name = column_name.lower()
    column_name = transliterate.translit(column_name, 'ru', reversed=True)
    column_name = re.sub(r'\s+', '_', column_name)
    column_name = re.sub(r'[^a-z0-9_]', '', column_name)
    return column_name

def prepare_data(data):
    data.columns = [snake_case(col) for col in data.columns]
    data.rename(columns={'polnoenaimenovanie': 'naimenovanie'}, inplace=True)
    
    nomenklatura = data[['kod', 'naimenovanie', 'artikul', 'proizvoditel', 'edizm', 'pometkaudalenija']]
    nomenklatura = nomenklatura[['kod', 'artikul', 'proizvoditel', 'naimenovanie', 'edizm', 'pometkaudalenija']]
    
    stock = data[['kod', 'konost']].copy()
    stock['osnsklad'] = stock['konost'].fillna(0).astype(int)
    stock.drop(columns=['konost'], inplace=True)
    stock['zakazy_sklad'] = data['ostatokzakazy'].fillna(0).astype(int)
    
    price = data[['kod', 'tsenazakup', 'tsenarozn']]
    
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
        group_ids = generate_analog_groups(data)
        analog_df = pd.DataFrame(list(group_ids.items()), columns=['kod_1s', 'gruppa_analogov'])
        analog_df.drop_duplicates(subset=['kod_1s'], inplace=True)
        return analog_df
    return None

def export_to_db(engine, df, table_name, index_col='kod'):
    try:
        df.columns = [snake_case(col) for col in df.columns]
        df.set_index(index_col, inplace=True)
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

def main():
    # Подключение к базе данных
    engine = get_db_engine()
    if engine is None:
        print("Не удалось подключиться к базе данных.")
        return
    
    # Определение путей к файлам в зависимости от базы данных
    if remote_db_config['host'] == '26.218.196.12':
        data_file_path = r'\\26.218.196.12\заказы\Евгений\New\Файлы для работы Access\ОстаткиДляАнализа.xls'
        analog_file_path = r'\\26.218.196.12\заказы\Евгений\New\Файлы для работы Access\analogi.xls'
    else:
        data_file_path = r'D:\NAS\заказы\Евгений\New\Файлы для работы Access\ОстаткиДляАнализа.xls'
        analog_file_path = r'D:\NAS\заказы\Евгений\New\Файлы для работы Access\analogi.xls'
    
    data = load_data(data_file_path)
    if data is not None:
        # Удаление непечатных символов и преобразование в кодировку UTF-8
        data = data.apply(lambda col: col.map(lambda x: x.encode('utf-8').decode('utf-8').strip() if isinstance(x, str) else x))
        
        nomenklatura, stock, price = prepare_data(data)
        export_to_db(engine, nomenklatura, 'nomenklaturaold')
        export_to_db(engine, stock, 'stockold')
        export_to_db(engine, price, 'priceold')

    analog_data = prepare_analog_data(analog_file_path)
    if analog_data is not None:
        # Удаление непечатных символов и преобразование в кодировку UTF-8
        analog_data = analog_data.apply(lambda col: col.map(lambda x: x.encode('utf-8').decode('utf-8').strip() if isinstance(x, str) else x))
        
        export_to_db(engine, analog_data, 'groupanalogiold', index_col='kod_1s')

    # Вывод первых 5 строк из каждой созданной таблицы
    print_first_five_rows(engine, 'nomenklaturaold')
    print_first_five_rows(engine, 'stockold')
    print_first_five_rows(engine, 'priceold')
    print_first_five_rows(engine, 'groupanalogiold')

    print("Все операции выполнены успешно.")

if __name__ == "__main__":
    main()
