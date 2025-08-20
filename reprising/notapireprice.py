import numpy as np
import pandas as pd
from datetime import datetime

def not_api_calculate_new_prices(
    filtered_df, salespivot, base_percent=1.53, reduced_base_percent=1.4
):
    # Получаем текущую дату
    current_date = datetime.now()

    # Создаем пустую колонку для новых цен
    filtered_df["new_price"] = np.nan

    # Фильтруем строки, подходящие под условия
    condition = (
        (~filtered_df["delsklad"].str.contains("api", case=False, na=False))  # Исключаем API
        & filtered_df["delprice"].notna()  # delprice не пустое
        & (filtered_df["delprice"] > 0)     # delprice положительное
    )

    # Переменные для удобства
    delprice    = filtered_df.loc[condition, "delprice"]
    maxprice    = filtered_df.loc[condition, "maxprice"]
    middleprice = filtered_df.loc[condition, "middleprice"]
    medianprice = filtered_df.loc[condition, "median_price"]
    naimenovanie= filtered_df.loc[condition, "naimenovanie"]
    ostatok     = filtered_df.loc[condition, "ostatok"]
    data        = pd.to_datetime(filtered_df.loc[condition, "data"], format="%Y-%m-%d")

    # Подготовка salespivot
    salespivot["year_month"] = pd.to_datetime(salespivot["year_month"], format="%Y-%m")

    # Проверка продаж за последние 12 месяцев
    def check_sales_last_12_months(kod):
        last_12 = current_date - pd.DateOffset(months=12)
        sales_data = salespivot[
            (salespivot["kod"] == kod) & (salespivot["year_month"] >= last_12)
        ]
        return not sales_data.empty

    # Рассчитываем базовый процент
    def get_base_percent(row):
        if (
            not check_sales_last_12_months(row["kod"])
            and row["ostatok"] > 0
            and (current_date - row["data"]).days > 365
        ):
            return reduced_base_percent
        return base_percent

    filtered_df.loc[condition, "base_percent"] = filtered_df.loc[condition].apply(
        get_base_percent, axis=1
    )

    # Начальный расчет new_price по base_percent
    filtered_df.loc[condition, "new_price"] = (
        np.ceil(delprice * filtered_df.loc[condition, "base_percent"] / 10) * 10
    )

    # Корректировка по диапазонам delprice
    delprice_correction = np.select(
        [
            delprice <= 100,
            delprice <= 300,
            delprice <= 1500,
            delprice >= 20000,
            delprice >= 10000,
        ],
        [
            np.ceil(delprice * 2.0 / 10) * 10,
            np.ceil(delprice * 1.7 / 10) * 10,
            np.ceil(delprice * 1.6 / 10) * 10,
            np.ceil(delprice * 1.25 / 10) * 10,
            np.ceil(delprice * 1.3 / 10) * 10,
        ],
        default=filtered_df.loc[condition, "new_price"],
    )
    filtered_df.loc[condition, "new_price"] = delprice_correction

    # Доп. корректировка для некоторых категорий по наименованию
    mask_naimenovanie = naimenovanie.str.startswith(
        ("Тормозная жид", "Предохранитель", "Шайба", "Щетка", "Антифриз", "Хомут")
    )
    corrected_price = np.ceil(delprice[mask_naimenovanie] * 1.6 / 10) * 10
    filtered_df.loc[condition & mask_naimenovanie, "new_price"] = np.where(
        filtered_df.loc[condition & mask_naimenovanie, "new_price"] < corrected_price,
        corrected_price,
        filtered_df.loc[condition & mask_naimenovanie, "new_price"],
    )

    # Заполняем пропуски по base_percent
    nan_mask = filtered_df.loc[condition, "new_price"].isna()
    filtered_df.loc[condition & nan_mask, "new_price"] = (
        np.ceil(
            delprice[nan_mask]
            * filtered_df.loc[condition & nan_mask, "base_percent"]
            / 10
        )
        * 10
    )

    # Ограничение: new_price не выше 2.2 * median_price
    cap_mask = condition & medianprice.notna()
    filtered_df.loc[cap_mask, "new_price"] = np.minimum(
        filtered_df.loc[cap_mask, "new_price"],
        np.ceil(medianprice[cap_mask] * 2.2 / 10) * 10
    )

    # Проверки на максимальную и среднюю цену
    condition_maxprice = (
        condition
        & maxprice.notna()
        & (filtered_df.loc[condition, "new_price"] < maxprice * 1.3)
    )
    condition_middleprice = (
        condition
        & middleprice.notna()
        & (filtered_df.loc[condition, "new_price"] < middleprice * 1.5)
    )

    filtered_df.loc[condition_maxprice, "new_price"] = (
        np.ceil(maxprice[condition_maxprice] * 1.3 / 10) * 10
    )
    filtered_df.loc[condition_middleprice, "new_price"] = (
        np.ceil(middleprice[condition_middleprice] * 1.5 / 10) * 10
    )

    return filtered_df
