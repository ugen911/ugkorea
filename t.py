from sqlalchemy import MetaData, Table, Column, text
from sqlalchemy.schema import CreateTable, DropTable
from ugkorea.db.database import get_db_engine

# Получаем движок базы данных
engine = get_db_engine()

# Функция для клонирования колонок
def clone_columns(table):
    return [Column(col.name, col.type, primary_key=col.primary_key, nullable=col.nullable, default=col.default)
            for col in table.columns]

# Подключаемся к базе данных
with engine.connect() as connection:
    # Загружаем метаданные схемы 'prices'
    meta_prices = MetaData(schema='prices')
    meta_prices.reflect(bind=connection)
    
    # Загружаем таблицу bergapi из схемы prices
    bergapi_table = Table('bergapi', meta_prices, autoload_with=connection)

    # Загружаем метаданные схемы 'api'
    meta_api = MetaData(schema='api')

    # Клонируем колонки из оригинальной таблицы
    bergapi_columns = clone_columns(bergapi_table)

    # Создаем таблицу bergapi в схеме api с клонированными колонками
    bergapi_table_api = Table('bergapi', meta_api, *bergapi_columns)
    
    # Создаем таблицу в схеме api
    connection.execute(CreateTable(bergapi_table_api))

    # Копируем данные из схемы 'prices' в схему 'api'
    copy_query = text("""
        INSERT INTO api.bergapi
        SELECT * FROM prices.bergapi
    """)
    connection.execute(copy_query)
    
    # Удаляем таблицу bergapi из схемы prices
    connection.execute(DropTable(bergapi_table))
    
    print("Таблица перенесена и удалена из схемы 'prices'.")
