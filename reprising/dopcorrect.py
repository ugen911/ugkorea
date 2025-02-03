import pandas as pd
import numpy as np


def correct_new_price_filters(df):
    """
    Корректирует значения в колонке `new_price` в датафрейме на основании условий для товаров с фильтрами.

    Условия:
    - Если значение в колонке `naimenovanie` начинается с "Фильтр" и не содержит слова "ориг" (без учета регистра),
      а также:
      - значение в колонке `abc` равно "A" или "B";
      - значение в колонке `xyz` равно "X" или "Y";
      то `new_price` не должен превышать `tsenazakup * 1.2`.
      Если `new_price` больше `tsenazakup * 1.2`, то значение корректируется до `tsenazakup * 1.2`,
      округленного до ближайших 10 рублей вверх.
    - Если `new_price` меньше `middleprice + 40%` или `maxprice + 10%` (при наличии этих значений),
      то `new_price` увеличивается до максимального из этих значений, округленного до 10 рублей вверх.

    Параметры:
    - df (pd.DataFrame): Входной датафрейм, содержащий колонки `naimenovanie`, `new_price`, `tsenazakup`,
      `middleprice`, `maxprice`, `abc` и `xyz`.

    Возвращает:
    - pd.DataFrame: Датафрейм с корректировками в колонке `new_price` на основании условий.
    """

    # Условие для строк, где наименование начинается с "Фильтр", не содержит "ориг",
    # а также значение abc равно "A" или "B" и значение xyz равно "X" или "Y"
    mask = (
        df["naimenovanie"].str.startswith("Фильтр")
        & ~df["naimenovanie"].str.contains("ориг", case=False)
        & df["abc"].isin(["A", "B"])
        & df["xyz"].isin(["X", "Y"])
    )

    # Применение условия для корректировки верхней границы new_price
    df.loc[mask, "new_price"] = np.where(
        df.loc[mask, "new_price"] > df.loc[mask, "tsenazakup"] * 2.2,
        np.ceil(df.loc[mask, "tsenazakup"] * 2.2 / 10)
        * 10,  # Округление до 10 рублей вверх
        df.loc[mask, "new_price"],
    )

    # Применение условия для корректировки нижней границы new_price
    middleprice_adjusted = df.loc[mask, "middleprice"] * 1.4
    maxprice_adjusted = df.loc[mask, "maxprice"] * 1.15
    min_threshold = np.maximum(
        middleprice_adjusted, maxprice_adjusted
    )  # Выбор максимума из двух значений

    # Проверка, чтобы new_price не был меньше middleprice + 40% и maxprice + 10%
    df.loc[mask, "new_price"] = np.where(
        df.loc[mask, "new_price"] < min_threshold,
        np.ceil(min_threshold / 10) * 10,  # Округление до 10 рублей вверх
        df.loc[mask, "new_price"],
    )

    # Возвращаем полный датафрейм
    return df
