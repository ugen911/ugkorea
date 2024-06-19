"""
db_copy.py

Скрипт для копирования данных из удаленной базы данных PostgreSQL в локальную базу данных PostgreSQL.
Поддерживает два режима работы: базовый и расширенный.

Базовый режим (по умолчанию):
- Копирует все данные из всех схем, кроме схемы 'analitic'.

Расширенный режим:
- Копирует все данные из всех схем, включая схему 'analitic'.

Использование:
- Базовый режим (по умолчанию):
  python db_copy.py

- Расширенный режим:
  python db_copy.py extended

Конфигурация параметров подключения к базам данных находится в файле config.py.
"""

import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError
from ugkorea.db.config import remote_db_config, local_db_config
import logging
import sys

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_connection_string(config):
    return f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"

def copy_table(table_name, schema_name, source_engine, target_engine):
    try:
        logging.info(f"Начало копирования таблицы '{schema_name}.{table_name}'")
        # Читаем данные из удаленной таблицы с указанием схемы
        query = f"SELECT * FROM {schema_name}.{table_name}"
        logging.info(f"Выполнение запроса: {query}")
        df = pd.read_sql(query, source_engine)
        logging.info(f"Количество строк, считанных из таблицы '{schema_name}.{table_name}': {len(df)}")
        # Записываем данные в локальную таблицу с указанием схемы
        df.to_sql(table_name, target_engine, schema=schema_name, if_exists='replace', index=False)
        logging.info(f"Таблица '{schema_name}.{table_name}' успешно скопирована.")
    except SQLAlchemyError as e:
        logging.error(f"Ошибка при копировании таблицы '{schema_name}.{table_name}': {e}")
    except Exception as e:
        logging.error(f"Неожиданная ошибка при копировании таблицы '{schema_name}.{table_name}': {e}")

def main(mode='basic'):
    try:
        logging.info("Создание движков для удаленной и локальной баз данных")
        # Создаем движки для удаленной и локальной баз данных
        remote_engine = create_engine(create_connection_string(remote_db_config))
        local_engine = create_engine(create_connection_string(local_db_config))

        logging.info("Создание инспектора для получения информации о базе данных")
        # Создаем инспектора для получения информации о базе данных
        inspector = inspect(remote_engine)

        # Получаем список всех схем в удаленной базе данных
        schemas = inspector.get_schema_names()
        logging.info(f"Найдено схем: {len(schemas)}")

        for schema in schemas:
            # Пропускаем стандартные схемы, которые не нужно копировать
            if schema in ['information_schema', 'pg_catalog']:
                logging.info(f"Пропуск схемы '{schema}'")
                continue
            
            # Пропускаем схему 'analitic' в базовом режиме
            if mode == 'basic' and schema == 'analitic':
                logging.info(f"Пропуск схемы '{schema}' в базовом режиме")
                continue

            # Получаем список всех таблиц в текущей схеме
            tables = inspector.get_table_names(schema=schema)
            logging.info(f"Таблицы для копирования из схемы '{schema}': {tables}")

            # Копируем каждую таблицу в текущей схеме
            for table in tables:
                copy_table(table, schema, remote_engine, local_engine)

        logging.info("Все таблицы успешно скопированы.")
    except SQLAlchemyError as e:
        logging.error(f"Ошибка подключения к базе данных: {e}")
    except Exception as e:
        logging.error(f"Неожиданная ошибка: {e}")

if __name__ == "__main__":
    mode = 'basic'  # Режим по умолчанию
    if len(sys.argv) > 1 and sys.argv[1] == 'extended':
        mode = 'extended'
    main(mode)
