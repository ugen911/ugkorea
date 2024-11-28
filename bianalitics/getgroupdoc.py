import pandas as pd


def get_saildocument(df):
    # Удаление ненужных колонок
    columns_to_drop = [
        "kod",
        "naimenovanie",
        "proizvoditel",
        "vidnomenklatury",
        "type_detail",
        "gruppa_analogov",
    ]
    df = df.drop(columns=columns_to_drop, errors="ignore")

    # Группировка по 'dokumentprodazhi' и агрегация
    grouped_df = df.groupby("dokumentprodazhi", as_index=False).agg(
        {
            "period": "first",
            "hozoperatsija": "first",
            "pokupatel": "first",
            "skladkompanii": "first",
            "dokumentosnovanie": "first",
            "avtor": "first",
            "rs_zakaznarjad": "first",
            "marka": "first",
            "model": "first",
            "vin": "first",
            "ispolnitel": "first",
            "avtorzakaz": "first",
            "hozzakaz": "first",
            "kolichestvo": "sum",
            "summa": "sum",
        }
    )

    # Сортировка по дате (колонка 'period')
    grouped_df = grouped_df.sort_values(by="period")

    return grouped_df
