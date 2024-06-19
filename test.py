
import pandas as pd
from sqlalchemy import create_engine
from ugkorea.db.config import db_config

# Создаем строку подключения
connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"

# Создаем движок SQLAlchemy
engine = create_engine(connection_string)

# Читаем таблицу nomenklaturaold в DataFrame
query = "SELECT * FROM nomenklaturaold"
df = pd.read_sql(query, engine)

# Выводим первые несколько строк таблицы
print(df.head())
