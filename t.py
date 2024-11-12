# Подключение к базе данных и импорт необходимых библиотек
from ugkorea.db.database import get_db_engine
import pandas as pd
import os

# Подключение к базе данных
engine = get_db_engine()

salespivot_df = pd.read_sql("SELECT * FROM rasprodat", engine)

salespivot_df.to_csv("rasprodat_df.csv")
