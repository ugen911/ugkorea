import os
from sqlalchemy import create_engine
from sqlalchemy import inspect
from urllib.parse import quote_plus

# def get_db_engine():
#     db_user = quote_plus(os.getenv('DB_USER', 'postgres'))
#     db_password = quote_plus(os.getenv('DB_PASSWORD', '785612'))
#     db_host = quote_plus(os.getenv('DB_HOST', '28.218..196.12')) 
#     db_port = os.getenv('DB_PORT', '5432')
#     db_name = quote_plus(os.getenv('DB_NAME', 'avto6'))

#     db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

#     print("Database URL (without password):", f'postgresql://{db_user}:*****@{db_host}:{db_port}/{db_name}')

#     try:
#         engine = create_engine(db_url)
#         engine.connect().close()
#         print("Успешное подключение к базе данных.")
#         return engine
#     except Exception as e:
#         print(f"Ошибка при подключении к базе данных: {e}")
#         return None

def get_db_engine():
    db_user = os.getenv('postgres')
    db_password = os.getenv('785612')
    db_host = os.getenv('28.218.196.12')
    db_port = os.getenv( '5432')
    db_name = os.getenv('avto6')
    db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    
    try:
        engine = create_engine(db_url)
        # Если подключение успешно, выведите сообщение об успешном подключении
        print("Успешное подключение к базе данных.")
        return engine
    except Exception as e:
        print(f"Ошибка при подключении к базе данных: {e}")
        return None



def find_table_with_columns(engine, column_names):
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    for table_name in tables:
        columns = [col['name'] for col in inspector.get_columns(table_name)]
        if set(column_names).issubset(columns):
            return table_name
    return None

# Подключение к базе данных
engine = get_db_engine()

# if engine is not None:
#     try:
#         # Список необходимых колонок
#         required_columns = [
#             "Ссылка", "ВерсияДанных", "ПометкаУдаления", "Родитель", "ЭтоГруппа", "Код", "Наименование",
#             "НаименованиеИностранное", "НаименованиеПолное", "Артикул", "ТипНоменклатуры", "ВидНоменклатуры",
#             "БазоваяЕдиницаИзмерения", "ОсновнаяЕдиницаИзмерения", "СтавкаНДС", "ВалютаУчета",
#             "СпособРаспределенияДопРасходов", "СтатьяДопРасходов", "ГТД", "СтранаПроисхождения", "СрокХраненияТовара",
#             "ПроцентНаценки", "Производитель", "КартинкаАвтокаталога", "СчетУчетаНДСПоРеализации",
#             "КратностьПоставок", "СнятаСПроизводства", "ЗапретПродажи", "ЗапретЗакупки", "ФайлКартинки", "Вес",
#             "АртикулДляПоиска", "КодТНВЭД", "ЦеноваяГруппа", "КодОКПД2", "Прослеживаемый", "РС_ДатаВвода",
#             "Автор", "ДатаСоздания", "Комментарий", "СоставНабора", "ДополнительныеРеквизиты", "Предопределенный",
#             "ИмяПредопределенныхДанных", "Представление"
#         ]
#         # Поиск таблицы
#         table_name = find_table_with_columns(engine, required_columns)
#         print(f"Таблица с необходимыми колонками: {table_name}")
#     except Exception as e:
#         print(f"Ошибка при работе с базой данных: {e}")

