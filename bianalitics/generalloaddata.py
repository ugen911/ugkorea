from ugkorea.db.database import get_db_engine
import pandas as pd
import os
import numpy as np
from openpyxl import load_workbook
from datetime import datetime
import re

def load_foranalitics_data(engine):
    # Функция для приведения значений к одному регистру и удаления пробелов
    def normalize_kod_column(df, column="kod"):
        if column in df.columns:  # Проверка на наличие колонки
            df[column] = df[column].str.strip().str.upper()
        return df

    # Функция для загрузки и нормализации данных

    def load_and_normalize_table(query, column="kod"):
        df = pd.read_sql(query, engine)
        df = normalize_kod_column(df, column)  # Нормализация с проверкой наличия колонки
        return df

    # Загрузка данных с нормализацией
    nomenklatura_query = """
    SELECT kod, artikul, proizvoditel, naimenovaniepolnoe, 
           bazovajaedinitsa AS edizm, datasozdanija, vidnomenklatury
    FROM nomenklatura
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

    # Сортировка по алфавиту в колонке naimenovanie
    filtered_df = merged_df.sort_values(by="naimenovanie")

    def clean_and_convert_to_float(df, column_name):
        df[column_name] = (
            df[column_name]
            .astype(str)
            .str.strip()
            .replace(['', ' '], '0')  # Заменяем пустые строки и строки с пробелами на '0'
            .str.replace(r'[^\d,.-]', '', regex=True)
            .str.replace(',', '.')
        )
        df[column_name] = pd.to_numeric(df[column_name], errors='coerce').fillna(0.0)
        return df

    def convert_period_to_date(df, column_name):
        df[column_name] = pd.to_datetime(df[column_name], errors='coerce', dayfirst=True)
        return df

    postuplenija_query = """
    SELECT ssylka, kod, data, tsena, kolichestvo, hozoperatsija, kontragent
    FROM postuplenija
    WHERE proveden = 'Да' 
    """
    postuplenija_df = load_and_normalize_table(postuplenija_query)
    postuplenija_df = clean_and_convert_to_float(postuplenija_df, 'tsena')
    postuplenija_df["kolichestvo"] = postuplenija_df["kolichestvo"].astype(float)

    korrektirovki_query = """
    SELECT ssylka, kod, data, tsena, kolichestvo, hozoperatsija, dokumentosnovanie
    FROM korrektirovki
    WHERE proveden = 'Да' 
    """
    korrektirovki_df = load_and_normalize_table(korrektirovki_query)
    korrektirovki_df = clean_and_convert_to_float(korrektirovki_df, "tsena")
    korrektirovki_df["kolichestvo"] = korrektirovki_df["kolichestvo"].astype(float)

    # Загрузка данных из таблиц prodazhi и realizatsija и объединение
    prodaja_query = """
    SELECT kod, period, kolichestvo, summa, hozoperatsija, pokupatel, dokumentprodazhi, skladkompanii
    FROM prodazhi 
    """
    realizatsija_query = """
    SELECT ssylka AS dokumentprodazhi, kod, dokumentosnovanie, avtor, rs_zakaznarjad
    FROM realizatsija
    """
    prodaja_df = load_and_normalize_table(prodaja_query)
    realizatsija_df = load_and_normalize_table(realizatsija_query, column="kod")

    # Очистка полей от пробелов и непечатных символов перед объединением
    prodaja_df["dokumentprodazhi"] = prodaja_df["dokumentprodazhi"].str.strip()
    realizatsija_df["dokumentprodazhi"] = realizatsija_df["dokumentprodazhi"].str.strip()
    prodaja_df["kod"] = prodaja_df["kod"].str.strip()
    realizatsija_df["kod"] = realizatsija_df["kod"].str.strip()

    # Левое объединение prodazhi и realizatsija
    prodaja_df = prodaja_df.merge(
        realizatsija_df,
        left_on=["dokumentprodazhi", "kod"],
        right_on=["dokumentprodazhi", "kod"],
        how="left"
    )

    # Преобразование и очистка полей в prodaja_df
    prodaja_df = clean_and_convert_to_float(prodaja_df, "summa")
    prodaja_df = convert_period_to_date(prodaja_df, "period")
    prodaja_df = clean_and_convert_to_float(prodaja_df, "kolichestvo")

    # Перенос данных из filtered_df в prodaja_df по полю kod
    additional_fields = [
        "naimenovanie",
        "proizvoditel",
        "vidnomenklatury",
        "type_detail",
        "gruppa_analogov",
    ]
    prodaja_df = prodaja_df.merge(
        filtered_df[["kod"] + additional_fields], on="kod", how="left"
    )
    postuplenija_df = postuplenija_df.merge(
        filtered_df[["kod"] + additional_fields], on="kod", how="left"
    )

    # Загрузка таблицы avtoraboty_zakaznarjad
    avtoraboty_query = "SELECT avtorabota, tsena, kolichestvo, sostojanie, kontragent, vidremonta, identifikatorraboty, marka, model, vin, ssylka FROM avtoraboty_zakaznarjad"
    avtoraboty_df = load_and_normalize_table(avtoraboty_query)
    avtoraboty_df = clean_and_convert_to_float(avtoraboty_df, "tsena")
    avtoraboty_df["kolichestvo"] = avtoraboty_df["kolichestvo"].astype(float)
    avtoraboty_df["summa"] = avtoraboty_df["kolichestvo"] * avtoraboty_df["tsena"]
    avtoraboty_df.drop(columns="tsena", inplace=True)

    ispolniteli_query = (
        "SELECT ssylka, identifikatorraboty, ispolnitel FROM ispolniteli_zakaznarjad"
    )

    ispolniteli_df = load_and_normalize_table(ispolniteli_query)

    # Объединение avtoraboty_df и ispolniteli_df
    avtoraboty_df = avtoraboty_df.merge(
        ispolniteli_df, on=["ssylka", "identifikatorraboty"], how="left"
    )

    # Объединение avtoraboty_df с prodaja_df
    prodaja_df = prodaja_df.merge(
        avtoraboty_df,
        left_on=["dokumentprodazhi", "kolichestvo", "summa"],
        right_on=["ssylka", "kolichestvo", "summa"],
        how="left",
        suffixes=("", "_avtoraboty"),
    )

    # Обновление колонок в prodaja_df
    prodaja_df["naimenovanie"] = prodaja_df.apply(
        lambda row: (
            row["avtorabota"] if pd.notna(row["avtorabota"]) else row["naimenovanie"]
        ),
        axis=1,
    )
    for col in ["vidremonta", "sostojanie", "ispolnitel"]:
        if f"{col}_avtoraboty" in prodaja_df.columns:  # Проверка на наличие суффикса
            prodaja_df[col] = prodaja_df[col].combine_first(prodaja_df[f"{col}_avtoraboty"])

    # Удаление временных колонок
    prodaja_df.drop(
        columns=[
            f"{col}_avtoraboty"
            for col in ["avtorabota", "vidremonta", "sostojanie", "ispolnitel"]
            if f"{col}_avtoraboty" in prodaja_df.columns
        ],
        inplace=True,
    )

    prodaja_df.drop(
        columns=[
            "avtorabota",
            "sostojanie",
            "kontragent",
            "vidremonta",
            "identifikatorraboty",
            "ssylka",
        ],
        inplace=True,
    )

    zakazy_query = "SELECT ssylka, avtor AS avtorzakaz, hozoperatsija AS hozzakaz FROM zakazypokupatelej"
    zakazy_df = load_and_normalize_table(zakazy_query)

    prodaja_df = prodaja_df.merge(
        zakazy_df, left_on="dokumentosnovanie", right_on="ssylka", how="left"
    )

    prodaja_df.drop(
        columns=[
            "ssylka",
        ],
        inplace=True,
    )
    prodaja_df = prodaja_df.drop_duplicates()
    postuplenija_df = postuplenija_df.drop_duplicates()
    filtered_df = filtered_df.drop_duplicates()

    # Возвращаем все датафреймы
    return (filtered_df, postuplenija_df, prodaja_df, korrektirovki_df)
