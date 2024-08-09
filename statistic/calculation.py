import logging
from ugkorea.statistic.loaddata import get_final_data
from ugkorea.db.database import get_db_engine


engine = get_db_engine()
import pandas as pd


# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

final_data, nomenklatura_ml = get_final_data(engine)


print(final_data.head())
print(nomenklatura_ml.head())



