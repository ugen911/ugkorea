import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from db.database import get_db_engine

engine = get_db_engine()

# Открываем файл и читаем данные
try:
    data = pd.read_excel(r'\\26.218.196.12\заказы\Евгений\New\Файлы для работы Access\ОстаткиДляАнализа.xls')
except FileNotFoundError:
    print("Файл не найден.")
    exit()

# Переименовываем колонки в нижний регистр
data.columns = map(str.lower, data.columns)

# Переименовываем колонку 'полноенаименование' в 'наименование'
data.rename(columns={'полноенаименование': 'наименование'}, inplace=True)

# Создаем датафрейм nomenklatura и добавляем нужные колонки
nomenklatura = data[['код', 'наименование', 'артикул', 'производитель', 'едизм', 'пометкаудаления']]

# Меняем порядок колонок в датафрейме nomenklatura
nomenklatura = nomenklatura[['код', 'артикул', 'производитель', 'наименование', 'едизм', 'пометкаудаления']]

# Создаем датафрейм stock и добавляем нужные колонки
stock = data[['код', 'коност']].copy()
stock['оснсклад'] = stock['коност'].fillna(0).astype(int)
stock.drop(columns=['коност'], inplace=True)

# Преобразуем значения NaN в колонке 'производитель' в строку 'неопределено'
data['производитель'] = data['производитель'].fillna('неопределено')


# Добавляем колонку 'заказысклад' в датафрейм stock
stock['заказысклад'] = data['остатокзаказы'].fillna(0).astype(int)

# Создаем датафрейм price и включаем в него нужные колонки
price = data[['код', 'ценазакуп', 'ценарозн']]

# Экспорт данных в базу данных и вывод сообщений
try:
    # Установка колонки "код" в качестве индекса (primary key)
    nomenklatura.set_index('код', inplace=True)
    # Экспорт DataFrame в таблицу SQL
    nomenklatura.to_sql('nomenklatura', engine, index=True, if_exists='replace')
    print("Таблица 'nomenklatura' успешно создана и данные успешно экспортированы.")
except OperationalError as e:
    print(f"Ошибка при экспорте данных в таблицу 'nomenklatura': {e}")

try:
    # Установка колонки "код" в качестве индекса (primary key)
    stock.set_index('код', inplace=True)
    # Экспорт DataFrame в таблицу SQL
    stock.to_sql('stock', engine, index=True, if_exists='replace')
    print("Таблица 'stock' успешно создана и данные успешно экспортированы.")
except OperationalError as e:
    print(f"Ошибка при экспорте данных в таблицу 'stock': {e}")

try:
    # Установка колонки "код" в качестве индекса (primary key)
    price.set_index('код', inplace=True)
    # Экспорт DataFrame в таблицу SQL
    price.to_sql('price', engine, index=True, if_exists='replace')
    print("Таблица 'price' успешно создана и данные успешно экспортированы.")
except OperationalError as e:
    print(f"Ошибка при экспорте данных в таблицу 'price': {e}")
