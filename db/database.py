# Файл database.py
import os
from sqlalchemy import create_engine


def get_db_engine():
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'default_password')
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'ugkorea')
    db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    
    try:
        engine = create_engine(db_url)
        # Если подключение успешно, выведите сообщение об успешном подключении
        print("Успешное подключение к базе данных.")
        return engine
    except Exception as e:
        print(f"Ошибка при подключении к базе данных: {e}")
        return None