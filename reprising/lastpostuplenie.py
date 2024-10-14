import pandas as pd


# Загружаем данные с фильтром hozoperatsija = "Поступление товаров" и proveden = "Да"
def load_postuplenija_data(engine):
    query = """
    SELECT kod, data
    FROM postuplenija
    WHERE hozoperatsija = 'Поступление товаров'
    AND proveden = 'Да'
    """

    # Выполняем запрос и загружаем данные в DataFrame
    df = pd.read_sql(query, engine)

    # Преобразуем колонку data в формат даты
    df["data"] = pd.to_datetime(df["data"], errors="coerce")

    # Убираем пробелы по концам строк в колонке kod
    df["kod"] = df["kod"].str.strip()

    return df


# Функция для вычисления последней даты поступления для каждого kod
def calculate_last_postuplenija(df):
    # Убираем строки с пустыми значениями в колонке data
    df_clean = df.dropna(subset=["data"])

    # Вычисляем последнюю дату для каждого kod
    last_dates_df = df_clean.groupby("kod")["data"].max().reset_index()

    return last_dates_df


# Пример использования
if __name__ == "__main__":
    # Подключаемся к базе данных
    from ugkorea.db.database import get_db_engine

    engine = get_db_engine()

    # Загружаем данные
    postuplenija_df = load_postuplenija_data(engine)

    # Вычисляем последнюю дату поступления
    last_postuplenija_df = calculate_last_postuplenija(postuplenija_df)

    # Выводим результат
    print(last_postuplenija_df)
