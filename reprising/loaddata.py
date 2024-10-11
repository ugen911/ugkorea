import pandas as pd
import numpy as np
from ugkorea.db.database import get_db_engine

# Подключение к базе данных
engine = get_db_engine()


# Функция для удаления пробелов в строковых колонках
def strip_columns(df):
    return df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)


# Функция для присвоения уникальных кодов NaN значениям в колонке gruppa_analogov
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


# Загрузка данных из таблиц
nomenklatura_query = """
SELECT kod, artikul, proizvoditel, naimenovaniepolnoe, 
       bazovajaedinitsa AS edizm, datasozdanija
FROM nomenklatura
WHERE vidnomenklatury = 'Товар'
"""
nomenklatura_df = pd.read_sql(nomenklatura_query, engine)
nomenklatura_df = strip_columns(nomenklatura_df)

nomenklaturaold_query = """
SELECT kod, stellazh, pometkaudalenija
FROM nomenklaturaold
"""
nomenklaturaold_df = pd.read_sql(nomenklaturaold_query, engine)
nomenklaturaold_df = strip_columns(nomenklaturaold_df)

deliveryminprice_query = """
SELECT kod, price AS delprice, sklad AS delsklad
FROM deliveryminprice
"""
deliveryminprice_df = pd.read_sql(deliveryminprice_query, engine)
deliveryminprice_df = strip_columns(deliveryminprice_df)

middlemaxprice_query = """
SELECT kod, middleprice, maxprice
FROM middlemaxprice
"""
middlemaxprice_df = pd.read_sql(middlemaxprice_query, engine)
middlemaxprice_df = strip_columns(middlemaxprice_df)

priceold_query = """
SELECT kod, tsenazakup, tsenarozn
FROM priceold
"""
priceold_df = pd.read_sql(priceold_query, engine)
priceold_df = strip_columns(priceold_df)

stockold_query = """
SELECT kod, osnsklad AS ostatok
FROM stockold
"""
stockold_df = pd.read_sql(stockold_query, engine)
stockold_df = strip_columns(stockold_df)

typedetailgen_query = """
SELECT kod, type_detail
FROM typedetailgen
"""
typedetailgen_df = pd.read_sql(typedetailgen_query, engine)
typedetailgen_df = strip_columns(typedetailgen_df)

groupanalogiold_query = """
SELECT kod_1s AS kod, gruppa_analogov
FROM groupanalogiold
"""
groupanalogiold_df = pd.read_sql(groupanalogiold_query, engine)
groupanalogiold_df = strip_columns(groupanalogiold_df)

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
merged_df = assign_unique_codes(merged_df, "gruppa_analogov")

# Переименовываем колонку naimenovaniepolnoe в naimenovanie
merged_df.rename(columns={"naimenovaniepolnoe": "naimenovanie"}, inplace=True)

# Фильтрация по остаткам (ostatok > 0)
filtered_df = merged_df[merged_df["ostatok"] > 0]

# Сортировка по алфавиту в колонке naimenovanie
filtered_df = filtered_df.sort_values(by="naimenovanie")

# Вывод данных
print(filtered_df.head())
