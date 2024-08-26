from sqlalchemy import create_engine, text
import os
import logging
from ugkorea.db.config import remote_db_config, server_db_config  # Импорт конфигураций

# Настройка логгирования
logging.basicConfig(filename='data_upload_errors.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

def get_db_engine():
    """
    Определяет, использовать ли локальную или сетевую базу данных, и возвращает SQLAlchemy engine для подключения к базе данных.
    Также выполняет тестовый запрос к базе данных для проверки подключения.
    """
    # Пути к директориям
    network_path = r'\\26.218.196.12\заказы\Евгений\Access\Табличные выгрузки1С'
    local_path = r'D:\NAS\заказы\Евгений\Access\Табличные выгрузки1С'

    if os.path.exists(network_path):
        print(f"Используется сетевая база данных (путь: {network_path})")
        db_config = remote_db_config
    else:
        print(f"Сетевая папка недоступна, используется локальная база данных (путь: {local_path})")
        db_config = server_db_config
    
    try:
        # Создание SQLAlchemy engine
        connection_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        engine = create_engine(connection_string)
        
        # Тестовый запрос для проверки подключения (например, версия PostgreSQL)
        with engine.connect() as connection:
            result = connection.execute(text("SELECT version();"))
            db_version = result.fetchone()
            print(f"Подключено к базе данных. Версия: {db_version[0]}")
        
        return engine
    except Exception as e:
        logging.error(f"Ошибка при подключении к базе данных: {e}")
        print(f"Ошибка при подключении к базе данных: {e}")
        return None
