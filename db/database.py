# Файл database.py
from ugkorea.db.config import local_db_config
import sqlalchemy
from sqlalchemy import create_engine, text


def get_db_engine():
    connection_string = f"postgresql+psycopg2://{local_db_config['user']}:{local_db_config['password']}@{local_db_config['host']}:{local_db_config['port']}/{local_db_config['database']}"
    engine = create_engine(connection_string)

    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            if result.scalar() == 1:
                print("Connection to the database has been established successfully.")
            else:
                print("Connection test failed.")
    except sqlalchemy.exc.SQLAlchemyError as e:
        print(f"An error occurred: {e}")

    return engine
