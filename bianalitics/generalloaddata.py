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
        df = normalize_kod_column(
            df, column
        )  # Нормализация с проверкой наличия колонки
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

    filtered_df = filtered_df.drop_duplicates()

    def clean_and_convert_to_float(df, column_name):
        df[column_name] = (
            df[column_name]
            .astype(str)
            .str.strip()
            .replace(
                ["", " "], "0"
            )  # Заменяем пустые строки и строки с пробелами на '0'
            .str.replace(r"[^\d,.-]", "", regex=True)
            .str.replace(",", ".")
        )
        df[column_name] = pd.to_numeric(df[column_name], errors="coerce").fillna(0.0)
        return df

    def convert_period_to_date(df, column_name):
        df[column_name] = pd.to_datetime(
            df[column_name], errors="coerce", dayfirst=True
        )
        return df

    vyrabotka_query = "SELECT datazakrytija AS data, slesar, avtorabota, vidremonta, dokumentprodazhi, kolichestvo, summa FROM vyrabotkaslesarej"
    vyrabotka_df = load_and_normalize_table(vyrabotka_query)
    vyrabotka_df = clean_and_convert_to_float(vyrabotka_df, "kolichestvo")
    vyrabotka_df = clean_and_convert_to_float(vyrabotka_df, "summa")
    vyrabotka_df["slesar"] = vyrabotka_df["slesar"].fillna("Субподряд")

    postuplenija_query = """
    SELECT ssylka, kod, data, tsena, kolichestvo, hozoperatsija, kontragent
    FROM postuplenija
    WHERE proveden = 'Да' 
    """
    postuplenija_df = load_and_normalize_table(postuplenija_query)
    postuplenija_df = clean_and_convert_to_float(postuplenija_df, "tsena")
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
    realizatsija_df["dokumentprodazhi"] = realizatsija_df[
        "dokumentprodazhi"
    ].str.strip()
    prodaja_df["kod"] = prodaja_df["kod"].str.strip()
    realizatsija_df["kod"] = realizatsija_df["kod"].str.strip()

    # Преобразование и очистка полей в prodaja_df
    prodaja_df = clean_and_convert_to_float(prodaja_df, "summa")
    prodaja_df = convert_period_to_date(prodaja_df, "period")
    prodaja_df = clean_and_convert_to_float(prodaja_df, "kolichestvo")

    # Группировка по указанным колонкам и суммирование kolichestvo и summa
    prodaja_df = prodaja_df.groupby(
        [
            "kod",
            "period",
            "hozoperatsija",
            "pokupatel",
            "dokumentprodazhi",
            "skladkompanii",
        ],
        as_index=False,
    ).agg({"kolichestvo": "sum", "summa": "sum"})

    # Левое объединение prodazhi и realizatsija
    prodaja_df = prodaja_df.merge(
        realizatsija_df,
        left_on=["dokumentprodazhi", "kod"],
        right_on=["dokumentprodazhi", "kod"],
        how="left",
    )

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
    avtoraboty_query = "SELECT  kod::TEXT AS kod, data, avtorabota, tsena, kolichestvo, sostojanie, kontragent, vidremonta, identifikatorraboty, marka, model, vin, ssylka, avtor FROM avtoraboty_zakaznarjad"
    avtoraboty_df = load_and_normalize_table(avtoraboty_query)
    avtoraboty_df = clean_and_convert_to_float(avtoraboty_df, "tsena")
    avtoraboty_df = convert_period_to_date(avtoraboty_df, "data")
    avtoraboty_df["kolichestvo"] = avtoraboty_df["kolichestvo"].astype(float)
    avtoraboty_df["summa"] = avtoraboty_df["kolichestvo"] * avtoraboty_df["tsena"]
    avtoraboty_df.drop(columns="tsena", inplace=True)

    ispolniteli_query = (
        "SELECT ssylka, identifikatorraboty, ispolnitel FROM ispolniteli_zakaznarjad"
    )

    ispolniteli_df = load_and_normalize_table(ispolniteli_query)

    # Агрегация исполнителей для каждой работы
    ispolniteli_df = (
        ispolniteli_df.groupby(["ssylka", "identifikatorraboty"])["ispolnitel"]
        .apply(", ".join)
        .reset_index()
    )

    # Объединение avtoraboty_df и ispolniteli_df
    avtoraboty_df = avtoraboty_df.merge(
        ispolniteli_df, on=["ssylka", "identifikatorraboty"], how="left"
    )

    avtoraboty_df = avtoraboty_df[avtoraboty_df["sostojanie"] == "Закрыт"]
    avtoraboty_df = avtoraboty_df.drop(columns=["identifikatorraboty", "sostojanie"])
    # Удаление строк, где kod = '00000003' и hozoperatsija = 'Заказ-наряд'
    # prodaja_df = prodaja_df[
    #     ~(
    #         (prodaja_df["kod"] == "00000003")
    #         & (prodaja_df["hozoperatsija"] == "Заказ-наряд")
    #     )
    # ]

    # Подготовка данных для переноса
    avtoraboty_df["hozoperatsija"] = "Заказ-наряд"
    avtoraboty_df["vidnomenklatury"] = "Услуга"
    avtoraboty_df["skladkompanii"] = "Основной цех"
    avtoraboty_df.rename(
        columns={
            "data": "period",
            "avtorabota": "naimenovanie",
            "kolichestvo": "kolichestvo",
            "kontragent": "pokupatel",
            "ssylka": "dokumentprodazhi",
        },
        inplace=True,
    )

    # Удаление дубликатов
    avtoraboty_df_forprodaji = avtoraboty_df[
        ["dokumentprodazhi", "marka", "model", "vin", "vidremonta", "ispolnitel"]
    ].drop_duplicates()

    # Убедимся, что 'ispolnitel' не содержит NaN
    avtoraboty_df_forprodaji["ispolnitel"] = avtoraboty_df_forprodaji["ispolnitel"].fillna("")

    # Агрегация исполнителей для каждой работы
    avtoraboty_df_forprodaji = (
        avtoraboty_df_forprodaji.groupby(["dokumentprodazhi", "marka", "model", "vin", "vidremonta"])[
            "ispolnitel"
        ]
        .apply(lambda x: ", ".join(sorted(filter(None, x))))  # Сортируем и объединяем не пустые значения
        .reset_index()
    )

    prodaja_df = prodaja_df.merge(
        avtoraboty_df_forprodaji,
        on="dokumentprodazhi",
        how="left"
    )

    # Объединение данных Перенесли все работы включая гарантийные и обслуживание собственных транспортных средств, далее перенесем товары из гарантийных
    # prodaja_df = pd.concat([prodaja_df, avtoraboty_df], ignore_index=True)

    zakaz_narad_tovary_query = """
    SELECT 
        kod, 
        data AS period, 
        nomenklatura AS naimenovanie, 
        tsena, 
        kolichestvo,  
        kontragent AS pokupatel, 
        vidremonta, 
        hozoperatsija, 
        ssylka AS dokumentprodazhi
    FROM 
        tovary_zakaznarjad 
    WHERE 
        sostojanie = 'Закрыт' 
    """
    zakaz_narad_tovar_df = load_and_normalize_table(zakaz_narad_tovary_query)
    zakaz_narad_tovar_df = clean_and_convert_to_float(zakaz_narad_tovar_df, "tsena")
    zakaz_narad_tovar_df = convert_period_to_date(zakaz_narad_tovar_df, "period")
    zakaz_narad_tovar_df["kolichestvo"] = zakaz_narad_tovar_df["kolichestvo"].astype(
        float
    )
    zakaz_narad_tovar_df["summa"] = (
        zakaz_narad_tovar_df["kolichestvo"] * zakaz_narad_tovar_df["tsena"]
    )
    zakaz_narad_tovar_df.drop(columns="tsena", inplace=True)
    zakaz_narad_tovar_df["vidnomenklatury"] = "Товар"
    zakaz_narad_tovar_df["skladkompanii"] = "Основной цех"

    zakaz_naryad = pd.concat([avtoraboty_df, zakaz_narad_tovar_df], ignore_index=True)
    # prodaja_df = pd.concat([prodaja_df, zakaz_narad_tovar_df], ignore_index=True)
    prodaja_df = prodaja_df.sort_values(by="period").reset_index(
        drop=True
    )  # Сортируем по дате и сбрасываем индексы

    zakazy_query = "SELECT ssylka AS dokumentosnovanie, avtor AS avtorzakaz, hozoperatsija AS hozzakaz FROM zakazypokupatelej"
    zakazy_df = load_and_normalize_table(zakazy_query)
    # Удаление дубликатов
    zakazy_df = zakazy_df.drop_duplicates()

    prodaja_df = prodaja_df.merge(
        zakazy_df, left_on="dokumentosnovanie", right_on="dokumentosnovanie", how="left"
    )

    # Условие 1: Если hozoperatsija = "Реализация товаров" и rs_zakaznarjad не пустое
    prodaja_df.loc[
        (prodaja_df["hozoperatsija"] == "Реализация товаров")
        & prodaja_df["rs_zakaznarjad"].notna(),
        "hozoperatsija",
    ] = "Заказ-наряд"

    # Условие 2: Если rs_zakaznarjad не пустое и vidnomenklatury = "Товар"
    # Создаем вспомогательный DataFrame для быстрого поиска
    valid_matches = prodaja_df[prodaja_df["ispolnitel"].notna()][
        ["dokumentprodazhi", "ispolnitel"]
    ].drop_duplicates()

    # Используем map для быстрого сопоставления
    mapping_dict = valid_matches.set_index("dokumentprodazhi")["ispolnitel"].to_dict()
    prodaja_df["mapped_ispolnitel"] = prodaja_df["rs_zakaznarjad"].map(mapping_dict)

    # Обновляем колонку ispolnitel только там, где rs_zakaznarjad не пустое и vidnomenklatury = "Товар"
    prodaja_df.loc[
        (prodaja_df["rs_zakaznarjad"].notna())
        & (prodaja_df["vidnomenklatury"] == "Товар"),
        "ispolnitel",
    ] = prodaja_df["mapped_ispolnitel"]

    # Удаляем временную колонку
    prodaja_df.drop(columns=["mapped_ispolnitel"], inplace=True)

    # Новый порядок колонок
    new_column_order = [
        "kod",
        "period",
        "kolichestvo",
        "summa",
        "hozoperatsija",
        "pokupatel",
        "dokumentprodazhi",
        "skladkompanii",
        "dokumentosnovanie",
        "avtor",
        "rs_zakaznarjad",
        "naimenovanie",
        "proizvoditel",
        "vidnomenklatury",
        "type_detail",
        "gruppa_analogov",
        "marka",
        "model",
        "vin",
        "ispolnitel",
        "avtorzakaz",
        "hozzakaz",
        "vidremonta",
    ]

    # Перестановка колонок
    prodaja_df = prodaja_df[new_column_order]

    # Создаем словарь для быстрого доступа к vidremonta на основе dokumentprodazhi
    vidremonta_mapping = (
        prodaja_df.dropna(subset=["vidremonta"])
        .set_index("dokumentprodazhi")["vidremonta"]
        .to_dict()
    )

    # Обновляем vidremonta для строк, где rs_zakaznarjad не пустой, а vidremonta пустой
    prodaja_df["vidremonta"] = prodaja_df.apply(
        lambda row: (
            vidremonta_mapping.get(row["rs_zakaznarjad"], row["vidremonta"])
            if pd.notna(row["rs_zakaznarjad"]) and pd.isna(row["vidremonta"])
            else row["vidremonta"]
        ),
        axis=1,
    )
    # Сортировка по 'period' и 'dokumentprodazhi'
    prodaja_df = prodaja_df.sort_values(by=["period", "dokumentprodazhi"]).reset_index(
        drop=True
    )

    # Шаг 1: Найти все строки, где hozzakaz = 'Заказ покупателя', и создать датафрейм с 'dokumentprodazhi' и 'kod'
    hozzakaz_df = prodaja_df[prodaja_df["hozzakaz"] == "Заказ покупателя"][
        ["dokumentprodazhi", "kod"]
    ].drop_duplicates()

    # Шаг 2: Сформировать SQL-запрос для получения данных из таблицы prodazhi с учетом TRIM
    dokumentprodazhi_kod_list = tuple(hozzakaz_df.itertuples(index=False, name=None))
    if dokumentprodazhi_kod_list:
        indzakaz_query = f"""
        SELECT 
            TRIM(dokumentprodazhi) AS dokumentprodazhi,
            TRIM(kod) AS kod,
            kolichestvo,
            sebestoimost
        FROM prodazhi
        WHERE (TRIM(dokumentprodazhi), TRIM(kod)) IN {dokumentprodazhi_kod_list}
        """

        # Шаг 3: Загрузить данные из базы данных
        indzakaz_df = pd.read_sql(indzakaz_query, engine)

        indzakaz_df = clean_and_convert_to_float(indzakaz_df, "sebestoimost")
        indzakaz_df = clean_and_convert_to_float(indzakaz_df, "kolichestvo")

        indzakaz_df["sebestoimost"] = (
            indzakaz_df["sebestoimost"] / indzakaz_df["kolichestvo"]
        )

        indzakaz_df.drop(columns=["kolichestvo"], inplace=True)

        # Шаг 4: Оставить только строки с максимальной себестоимостью для каждой пары (dokumentprodazhi, kod)
        indzakaz_df = indzakaz_df.groupby(
            ["dokumentprodazhi", "kod"], as_index=False
        ).agg({"sebestoimost": "max"})

        # Шаг 5: Левое соединение prodaja_df с получившимися данными
        prodaja_df = prodaja_df.merge(
            indzakaz_df, on=["dokumentprodazhi", "kod"], how="left"
        )

    # Возвращаем все датафреймы
    return (
        vyrabotka_df, filtered_df, postuplenija_df, prodaja_df, korrektirovki_df, zakaz_naryad
    )
