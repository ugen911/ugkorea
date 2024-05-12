import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError  # Правильное место импорта
from db.database import get_db_engine

def load_data():
    try:
        data = pd.read_excel(r'\\26.218.196.12\заказы\Евгений\New\Файлы для работы Access\ОстаткиДляАнализа.xls')
        print("Данные успешно загружены.")
        return data
    except FileNotFoundError:
        print("Файл не найден.")
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

def export_to_db(engine, df, table_name):
    try:
        df.set_index('код', inplace=True)
        df.to_sql(table_name, engine, index=True, if_exists='replace')
        print(f"Таблица '{table_name}' успешно создана и данные успешно экспортированы.")
    except OperationalError as e:
        print(f"Ошибка при экспорте данных в таблицу '{table_name}': {e}")

def main():
    engine = get_db_engine()
    if engine is None:
        print("Не удалось подключиться к базе данных.")
        return

    data = load_data()
    if data is not None:
        nomenklatura, stock, price = prepare_data(data)
        export_to_db(engine, nomenklatura, 'nomenklatura')
        export_to_db(engine, stock, 'stock')
        export_to_db(engine, price, 'price')

if __name__ == "__main__":
    main()

