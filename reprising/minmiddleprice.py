import pandas as pd
import numpy as np


def adjust_new_price(df: pd.DataFrame) -> pd.DataFrame:
    """
    Корректирует столбец 'new_price' в DataFrame.

    Для каждой строки вычисляется минимально допустимая цена как максимум из:
      - middleprice * 1.3
      - maxprice * 1.1
    Полученное значение округляется вверх до ближайших 10 рублей.
    Если текущий new_price меньше этого значения, он заменяется на округлённое.

    Параметры:
        df (pd.DataFrame): Датафрейм, содержащий среди прочих столбцов
                           'middleprice', 'maxprice' и 'new_price'.

    Возвращает:
        pd.DataFrame: Датафрейм с откорректированным столбцом 'new_price'.
    """
    # Вычисляем минимальную допустимую цену для каждой строки
    min_price = np.maximum(df["middleprice"] * 1.3, df["maxprice"] * 1.1, df["tsenazakup"] * 1.35)
    # Округляем полученное значение вверх до ближайших 10 рублей
    min_price = np.ceil(min_price / 10) * 10

    # Если new_price меньше минимально допустимой цены, то заменяем его
    df["new_price"] = np.where(df["new_price"] < min_price, min_price, df["new_price"])

    return df
