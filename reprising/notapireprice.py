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
        (
            ~filtered_df["delsklad"].str.contains("api", case=False, na=False)
        )  # Исключаем delsklad с "api"
        & filtered_df["delprice"].notna()  # delprice не пустое
        & (filtered_df["delprice"] > 0)  # delprice положительное
    )

    # Присваиваем переменные для удобства работы
    delprice = filtered_df.loc[condition, "delprice"]
    maxprice = filtered_df.loc[condition, "maxprice"]
    middleprice = filtered_df.loc[condition, "middleprice"]
    naimenovanie = filtered_df.loc[condition, "naimenovanie"]
    ostatok = filtered_df.loc[condition, "ostatok"]
    data = pd.to_datetime(filtered_df.loc[condition, "data"], format="%Y-%m-%d")

    # Получаем salespivot с информацией о продажах
    salespivot["year_month"] = pd.to_datetime(salespivot["year_month"], format="%Y-%m")

    # Проверяем, были ли продажи за последние 12 месяцев
    def check_sales_last_12_months(kod):
        last_12_months = current_date - pd.DateOffset(months=12)
        sales_data = salespivot[
            (salespivot["kod"] == kod) & (salespivot["year_month"] >= last_12_months)
        ]
        return not sales_data.empty  # True если продажи были

    # Применяем логику уменьшения наценки
    def get_base_percent(row):
        kod = row["kod"]
        if (
            not check_sales_last_12_months(kod)
            and row["ostatok"] > 0
            and (current_date - row["data"]).days > 365
        ):
            return reduced_base_percent
        return base_percent

    # Вычисляем base_percent для каждой строки
    filtered_df.loc[condition, "base_percent"] = filtered_df.loc[condition].apply(
        get_base_percent, axis=1
    )

    # Присваиваем new_price для всех строк по умолчанию на основе base_percent
    filtered_df.loc[condition, "new_price"] = (
        np.ceil(delprice * filtered_df.loc[condition, "base_percent"] / 10) * 10
    )

    # Условие по delprice (корректировка для заданных диапазонов цен)
    delprice_correction = np.select(
        [
            delprice <= 100,
            delprice <= 300,
            delprice <= 500,
            delprice >= 20000,
            delprice >= 10000,
        ],
        [
            np.ceil(delprice * 2 / 10) * 10,
            np.ceil(delprice * 1.7 / 10) * 10,
            np.ceil(delprice * 1.6 / 10) * 10,
            np.ceil(delprice * 1.25 / 10) * 10,
            np.ceil(delprice * 1.3 / 10) * 10,
            
        ],
        default=filtered_df.loc[
            condition, "new_price"
        ],  # Используем текущее значение new_price
    )

    filtered_df.loc[condition, "new_price"] = delprice_correction

    # Условие по наименованию (для строк, не попавших под предыдущее условие)
    mask_naimenovanie = naimenovanie.str.startswith(
        ("Тормозная жид", "Предохранитель", "Шайба", "Щетка", "Антифриз", "Хомут")
    )

    # Рассчитываем корректированную цену для категорий товаров
    corrected_price = np.ceil(delprice[mask_naimenovanie] * 1.6 / 10) * 10

    # Применяем корректировку только если рассчитанная цена больше текущего new_price
    filtered_df.loc[condition & mask_naimenovanie, "new_price"] = np.where(
        filtered_df.loc[condition & mask_naimenovanie, "new_price"] < corrected_price,
        corrected_price,
        filtered_df.loc[condition & mask_naimenovanie, "new_price"],
    )

    # Для всех остальных строк, используем base_percent
    nan_mask = filtered_df.loc[condition, "new_price"].isna()
    filtered_df.loc[condition & nan_mask, "new_price"] = (
        np.ceil(
            delprice[nan_mask]
            * filtered_df.loc[condition & nan_mask, "base_percent"]
            / 10
        )
        * 10
    )

    # Применяем проверки только к строкам, где выполняется условие
    condition_maxprice = (
        condition
        & maxprice.notna()
        & (filtered_df.loc[condition, "new_price"] < maxprice * 1.1)
    )
    condition_middleprice = (
        condition
        & middleprice.notna()
        & (filtered_df.loc[condition, "new_price"] < middleprice * 1.3)
    )

    # Проверки на maxprice
    filtered_df.loc[condition_maxprice, "new_price"] = (
        np.ceil(maxprice[condition_maxprice] * 1.1 / 10) * 10
    )

    # Проверки на middleprice
    filtered_df.loc[condition_middleprice, "new_price"] = (
        np.ceil(middleprice[condition_middleprice] * 1.3 / 10) * 10
    )

    # Возвращаем обновленный filtered_df
    return filtered_df
