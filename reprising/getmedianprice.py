import pandas as pd
from ugkorea.db.database import get_db_engine

def get_median_price_by_kod(engine):
    # SQL-запрос для получения медианного значения price для каждого kod
    query = """
        SELECT kod, 
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price
        FROM analitic.deliverypriceintime
        GROUP BY kod;
    """

    # Выполняем запрос и загружаем результат в DataFrame
    median_prices_df = pd.read_sql(query, engine)

    # Убираем пробелы с концов значений в колонке 'kod'
    median_prices_df["kod"] = median_prices_df["kod"].str.strip()

    # Возвращаем DataFrame с медианными значениями
    return median_prices_df
