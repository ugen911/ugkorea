import pandas as pd
import re
from ugkorea.db.database import get_db_engine


def autocoreec_data(engine):
    """
    Загрузка данных из таблиц autocoreec и nomenklatura, нормализация артикулов и производителей,
    объединение данных и возврат результата.

    :param engine: Объект подключения к базе данных
    :return: DataFrame с объединенными и нормализованными данными
    """
    # Загрузка таблицы autocoreec
    query_autocoreec = "SELECT * FROM konkurents.autocoreec"
    autocoreec_df = pd.read_sql(query_autocoreec, engine)

    # Сразу после загрузки выполняем замены значений в колонке manufacturer
    manufacturer_replacements = {
        "Hyundai/Kia/Mobis": "HyundaiKia",
        "HYUNDAI/KIA/MOBIS": "HyundaiKia",
        "Valeo PHC": "Valeo PHC",
        "GM/Opel/Chevrolet": "General Motors",
        "Ssang Yong": "SsangYong",
        "SSANG YONG": "SsangYong",
    }
    autocoreec_df["manufacturer"] = autocoreec_df["manufacturer"].replace(
        manufacturer_replacements
    )

    # Нормализация колонок autocoreec
    autocoreec_df["artikul_clean"] = (
        autocoreec_df["catalog_number"]
        .str.replace(r"[^a-zA-Z0-9\s]", "", regex=True)
        .str.lower()
        .str.strip()
    )
    autocoreec_df["proizvoditel_clean"] = (
        autocoreec_df["manufacturer"]
        .str.replace(r"[^a-zA-Z0-9\s]", "", regex=True)
        .str.lower()
        .str.strip()
    )

    # Справочник для замены производителей
    replacement_dict = {
        r"(?i)Hyundai\s*Kia|Hyundai-Kia|Hyundai/Kia|Hyundai/Kia/Mobis": "hyundaikia",
        r"(?i)JS\s*ASAKASHI": "asakashi",
        r"(?i)Asam-sa": "asam",
        r"(?i)DONGIL\s*SUPER\s*STAR": "dongil",
        r"(?i)MK\s*KASHIYAMA": "kashiyama",
        r"(?i)KAYABA": "kyb",
        r"(?i)LEMF[OÖ]ERDER": "lemforder",
        r"(?i)LESJOEFORS": "lesjofors",
        r"(?i)LYNX\s*AUTO|Lynxauto": "lynx",
        r"(?i)PARTS-MALL|PartSmall|PMC": "parts mall",
        r"(?i)VICTOR\s*REINZ": "reinz",
        r"(?i)Sangsin\s*Brake": "sangsin",
        r"(?i)PHC": "valeo phc",
        r"(?i)VERNET[-\s]CALORSTAT": "vernet",
        r"(?i)GM/Opel/Chevrolet": "general motors",
        r"(?i)Ssang\s*Yong|SSANG\s*YONG": "ssangyong",
    }

    # Применение замены производителей в autocoreec
    for pattern, replacement in replacement_dict.items():
        autocoreec_df["proizvoditel_clean"] = autocoreec_df[
            "proizvoditel_clean"
        ].str.replace(pattern, replacement.lower(), regex=True)

    # Загрузка и нормализация данных из таблицы nomenklatura
    query_nomenklatura = """
    SELECT kod, artikul, proizvoditel
    FROM nomenklatura
    WHERE vidnomenklatury = 'Товар'
    """
    nomenklatura_df = pd.read_sql(query_nomenklatura, engine)
    nomenklatura_df["artikul_clean"] = (
        nomenklatura_df["artikul"]
        .str.replace(r"[^a-zA-Z0-9\s]", "", regex=True)
        .str.lower()
        .str.strip()
    )
    nomenklatura_df["proizvoditel_clean"] = (
        nomenklatura_df["proizvoditel"]
        .str.replace(r"[^a-zA-Z0-9\s]", "", regex=True)
        .str.lower()
        .str.strip()
    )

    # Объединение таблиц по нормализованным колонкам
    merged_df = pd.merge(
        autocoreec_df,
        nomenklatura_df,
        on=["artikul_clean", "proizvoditel_clean"],
        how="inner",
    )

    # Удаление пробелов в колонке kod и других нужных колонках
    merged_df["kod"] = merged_df["kod"].str.strip()
    merged_df["artikul"] = merged_df["artikul"].str.strip()
    merged_df["proizvoditel"] = merged_df["proizvoditel"].str.strip()

    # Выбор нужных колонок и возврат результата
    result_df = merged_df[["kod", "stock", "price"]]

    return result_df


if __name__ == "__main__":
    # Подключение к базе данных
    engine = get_db_engine()

    # Вызов функции и вывод первых строк результата
    result = autocoreec_data(engine)
    print(result.head())
