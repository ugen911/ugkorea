import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from db.database import get_db_engine
import random
import string

def load_data(file_path):
    try:
        data = pd.read_excel(file_path)
        print(f"Данные из файла '{file_path}' успешно загружены.")
        return data
    except FileNotFoundError:
        print(f"Файл '{file_path}' не найден.")
        exit()

def prepare_data(data):
    data.columns = map(str.lower, data.columns)
    data.rename(columns={'полноенаименование': 'наименование'}, inplace=True)
    nomenklatura = data[['код', 'наименование', 'артикул', 'производитель', 'едизм', 'пометкаудаления']]
    nomenklatura = nomenklatura[['код', 'артикул', 'производитель', 'наименование', 'едизм', 'пометкаудаления']]
    stock = data[['код', 'коност']].copy()
    stock['оснсклад'] = stock['коност'].fillna(0).astype(int)
    stock.drop(columns=['коност'], inplace=True)
    stock['заказысклад'] = data['остатокзаказы'].fillna(0).astype(int)
    price = data[['код', 'ценазакуп', 'ценарозн']]
    return nomenklatura, stock, price

def generate_analog_groups(data):
    data.columns = map(str.lower, data.columns)
    data.rename(columns={'код 1с': 'код', 'код аналога': 'аналог'}, inplace=True)
    
    # Создаем словарь для хранения кодов и их групп аналогов
    analog_dict = {}
    
    for index, row in data.iterrows():
        code = row['код']
        analog = row['аналог']
        
        if code not in analog_dict:
            analog_dict[code] = set()
        if analog not in analog_dict:
            analog_dict[analog] = set()
        
        analog_dict[code].add(analog)
        analog_dict[analog].add(code)
    
    # Определяем группы аналогов
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
    
    # Генерируем уникальные идентификаторы для групп
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
        analog_df = pd.DataFrame(list(group_ids.items()), columns=['Код 1С', 'Группа аналогов'])
        analog_df.drop_duplicates(subset=['Код 1С'], inplace=True)
        return analog_df
    return None

def export_to_db(engine, df, table_name, index_col='код'):
    try:
        df.set_index(index_col, inplace=True)
        df.to_sql(table_name, engine, index=True, if_exists='replace')
        print(f"Таблица '{table_name}' успешно создана и данные успешно экспортированы.")
    except OperationalError as e:
        print(f"Ошибка при экспорте данных в таблицу '{table_name}': {e}")

def main():
    engine = get_db_engine()
    if engine is None:
        print("Не удалось подключиться к базе данных.")
        return

    data_file_path = r'\\26.218.196.12\заказы\Евгений\New\Файлы для работы Access\ОстаткиДляАнализа.xls'
    analog_file_path = r'\\26.218.196.12\заказы\Евгений\New\Файлы для работы Access\analogi.xls'

    data = load_data(data_file_path)
    if data is not None:
        nomenklatura, stock, price = prepare_data(data)
        export_to_db(engine, nomenklatura, 'nomenklaturaold')
        export_to_db(engine, stock, 'stockold')
        export_to_db(engine, price, 'priceold')

    analog_data = prepare_analog_data(analog_file_path)
    if analog_data is not None:
        export_to_db(engine, analog_data, 'groupanalogiold', index_col='Код 1С')

if __name__ == "__main__":
    main()
