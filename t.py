from ugkorea.db.database import get_db_engine
import pandas as pd

# Получаем подключение к базе данных
engine = get_db_engine()

# Загружаем данные из таблицы nomenklaturaprimenjaemost
df_nomenklaturaprimenjaemost = pd.read_sql("SELECT kod, model FROM nomenklaturaprimenjaemost", engine)

# Выводим первые 50 строк таблицы
print(df_nomenklaturaprimenjaemost.head(50))
