from db.database import get_db_engine
from sqlalchemy import create_engine, inspect, text
import inflection

# Получаем движок базы данных
engine = get_db_engine()

# Функция для удаления пробелов в начале и в конце строки для всех текстовых полей в таблице
def trim_whitespace_from_table(table_name: str, engine):
    with engine.connect() as conn:
        # Получаем информацию о таблице
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        
        # Генерируем и выполняем SQL-запрос для обновления текстовых полей
        for column in columns:
            if column['type'].__class__.__name__ in ['VARCHAR', 'TEXT']:
                column_name_quoted = f'"{column["name"]}"'
                table_name_quoted = f'"{table_name}"'
                update_query = text(f"""
                    UPDATE {table_name_quoted}
                    SET {column_name_quoted} = TRIM({column_name_quoted})
                    WHERE {column_name_quoted} IS NOT NULL;
                """)
                conn.execute(update_query)
                print(f"Обновлен столбец {column['name']} в таблице {table_name}")

# Функция для переименования колонок в snake_case и нижний регистр
def rename_columns_to_snake_case(table_name: str, engine):
    with engine.connect() as conn:
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        for column in columns:
            old_name = column['name']
            new_name = inflection.underscore(old_name).lower()
            if old_name != new_name:
                table_name_quoted = f'"{table_name}"'
                old_name_quoted = f'"{old_name}"'
                new_name_quoted = f'"{new_name}"'
                alter_query = text(f"""
                    ALTER TABLE {table_name_quoted}
                    RENAME COLUMN {old_name_quoted} TO {new_name_quoted};
                """)
                conn.execute(alter_query)
                print(f"Переименован столбец {old_name} в {new_name} в таблице {table_name}")

# Получаем список всех таблиц
inspector = inspect(engine)
tables = inspector.get_table_names()

# Проходим по каждой таблице и удаляем пробелы, переименовываем колонки
for table in tables:
    rename_columns_to_snake_case(table, engine)
    trim_whitespace_from_table(table, engine)

print("Все пробелы удалены и имена колонок приведены к нижнему регистру и snake_case.")
