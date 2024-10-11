import pandas as pd
import numpy as np


def load_forreprice_data(engine):
    # Функция для приведения значений к одному регистру и удаления пробелов
    def normalize_kod_column(df, column="kod"):
        df[column] = df[column].str.strip().str.upper()
        return df

    # Загрузка и нормализация данных из таблиц
    def load_and_normalize_table(query, column="kod"):
        df = pd.read_sql(query, engine)
        df = normalize_kod_column(df, column)
        return df

    # Загрузка данных с нормализацией
    nomenklatura_query = """
    SELECT kod, artikul, proizvoditel, naimenovaniepolnoe, 
           bazovajaedinitsa AS edizm, datasozdanija
    FROM nomenklatura
    WHERE vidnomenklatury = 'Товар'
    """
    nomenklatura_df = load_and_normalize_table(nomenklatura_query)

    nomenklaturaold_query = """
    SELECT kod, stellazh, pometkaudalenija
    FROM nomenklaturaold
    """
    nomenklaturaold_df = load_and_normalize_table(nomenklaturaold_query)

    deliveryminprice_query = """
    SELECT kod, price AS delprice, sklad AS delsklad
    FROM deliveryminprice
    """
    deliveryminprice_df = load_and_normalize_table(deliveryminprice_query)

    middlemaxprice_query = """
    SELECT kod, middleprice, maxprice
    FROM middlemaxprice
    """
    middlemaxprice_df = load_and_normalize_table(middlemaxprice_query)

    priceold_query = """
    SELECT kod, tsenazakup, tsenarozn
    FROM priceold
    """
    priceold_df = load_and_normalize_table(priceold_query)

    stockold_query = """
    SELECT kod, osnsklad AS ostatok
    FROM stockold
    """
    stockold_df = load_and_normalize_table(stockold_query)

    typedetailgen_query = """
    SELECT kod, type_detail
    FROM typedetailgen
    """
    typedetailgen_df = load_and_normalize_table(typedetailgen_query)

    groupanalogiold_query = """
    SELECT kod_1s AS kod, gruppa_analogov
    FROM groupanalogiold
    """
    groupanalogiold_df = load_and_normalize_table(groupanalogiold_query)

    # Соединение таблиц по колонке 'kod' с использованием левого соединения
    merged_df = (
        nomenklatura_df.merge(nomenklaturaold_df, on="kod", how="left")
        .merge(deliveryminprice_df, on="kod", how="left")
        .merge(middlemaxprice_df, on="kod", how="left")
        .merge(priceold_df, on="kod", how="left")
        .merge(stockold_df, on="kod", how="left")
        .merge(typedetailgen_df, on="kod", how="left")
        .merge(groupanalogiold_df, on="kod", how="left")
    )

    # Заменяем текстовые 'NaN', пустые строки и 'None' на np.nan
    merged_df["gruppa_analogov"] = merged_df["gruppa_analogov"].replace(
        ["NaN", "None", ""], np.nan
    )

    # Присвоение уникальных кодов, если gruppa_analogov имеет NaN
    def assign_unique_codes(df, column):
        unique_code_start = 1000000  # Начало уникальных кодов
        df_copy = df.copy()  # Копируем DataFrame перед изменениями
        df_copy[column] = df_copy[column].fillna(
            pd.Series(
                [unique_code_start + i for i in range(df_copy[column].isna().sum())],
                index=df_copy[df_copy[column].isna()].index,
            )
        )
        return df_copy

    merged_df = assign_unique_codes(merged_df, "gruppa_analogov")

    # Переименовываем колонку naimenovaniepolnoe в naimenovanie
    merged_df.rename(columns={"naimenovaniepolnoe": "naimenovanie"}, inplace=True)

    # Фильтрация по остаткам (ostatok > 0)
    filtered_df = merged_df[merged_df["ostatok"] > 0]

    # Сортировка по алфавиту в колонке naimenovanie
    filtered_df = filtered_df.sort_values(by="naimenovanie")

    # Получение списка kod из filtered_df
    filtered_kods = filtered_df["kod"].tolist()

    # Загрузка данных из других таблиц с учетом нормализации и фильтрации
    priceendmonth_query = """
    SELECT kod, data AS year_month, tsena
    FROM priceendmonth
    """
    priceendmonth_df = load_and_normalize_table(priceendmonth_query)

    salespivot_query = """
    SELECT kod, year_month, kolichestvo, summa
    FROM salespivot
    """
    salespivot_df = load_and_normalize_table(salespivot_query)

    stockendmonth_query = """
    SELECT nomenklaturakod AS kod, month AS year_month, balance AS stock
    FROM stockendmonth
    """
    stockendmonth_df = load_and_normalize_table(stockendmonth_query)

    suppliespivot_query = """
    SELECT kod, year_month, kolichestvo, summa
    FROM suppliespivot
    """
    suppliespivot_df = load_and_normalize_table(suppliespivot_query)

    postuplenija_query = """
    SELECT kod, data, kolichestvo
    FROM postuplenija
    WHERE proveden = 'Да' AND hozoperatsija = 'Поступление товаров'
    """
    postuplenija_df = load_and_normalize_table(postuplenija_query)

    # Фильтрация данных по kod, которые есть в filtered_df
    priceendmonth_filtered = priceendmonth_df[
        priceendmonth_df["kod"].isin(filtered_kods)
    ]
    salespivot_filtered = salespivot_df[salespivot_df["kod"].isin(filtered_kods)]
    stockendmonth_filtered = stockendmonth_df[
        stockendmonth_df["kod"].isin(filtered_kods)
    ]
    suppliespivot_filtered = suppliespivot_df[
        suppliespivot_df["kod"].isin(filtered_kods)
    ]
    postuplenija_filtered = postuplenija_df[postuplenija_df["kod"].isin(filtered_kods)]

    # Возвращаем все датафреймы
    return (
        filtered_df,
        priceendmonth_filtered,
        salespivot_filtered,
        stockendmonth_filtered,
        suppliespivot_filtered,
        postuplenija_filtered,
    )


# Проверка работы функции при запуске скрипта
if __name__ == "__main__":
    from ugkorea.db.database import get_db_engine

    engine = get_db_engine()
    (
        filtered_df,
        priceendmonth_filtered,
        salespivot_filtered,
        stockendmonth_filtered,
        suppliespivot_filtered,
        postuplenija_filtered,
    ) = load_forreprice_data(engine)

    # Вывод для проверки
    print("Filtered DF:")
    print(filtered_df.head())
    print("Price End Month Filtered:")
    print(priceendmonth_filtered.head())
