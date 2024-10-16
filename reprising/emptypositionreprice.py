import pandas as pd
import numpy as np


def update_prices(filtered_df, priceendmonth):
    """
    Функция корректирует цены в filtered_df на основе данных priceendmonth и условий.

    Parameters:
    filtered_df (pd.DataFrame): Основной датафрейм с информацией о позициях.
    priceendmonth (pd.DataFrame): Датафрейм с историческими данными о ценах на конец месяца.

    Returns:
    pd.DataFrame: Обновленный датафрейм filtered_df со всеми строками и корректировками цен.
    """
    # Преобразуем колонки в соответствующие форматы
    filtered_df["datasozdanija"] = pd.to_datetime(
        filtered_df["datasozdanija"], format="%Y-%m-%d", errors="coerce"
    )
    priceendmonth["year_month"] = pd.to_datetime(
        priceendmonth["year_month"], format="%Y-%m", errors="coerce"
    )

    # Получаем текущую дату и дату 12 месяцев назад
    current_date = pd.Timestamp.now()
    twelve_months_ago = current_date - pd.DateOffset(months=12)

    # Фильтруем строки, где new_price пустой или равен tsenarozn, и при этом delsklad и median_price должны быть пустыми
    condition = (
        (filtered_df["new_price"].isna())
        | (filtered_df["new_price"] == filtered_df["tsenarozn"])
    ) & (filtered_df["delsklad"].isna() & filtered_df["median_price"].isna())
    filtered_positions = filtered_df[condition]

    # Оставляем только строки, где datasozdanija пустая или старше 12 месяцев от текущей даты
    filtered_positions = filtered_positions[
        (filtered_positions["datasozdanija"].isna())
        | (filtered_positions["datasozdanija"] <= twelve_months_ago)
    ]

    # Определяем текущий месяц и год
    last_year_same_month = (current_date - pd.DateOffset(years=1)).strftime("%Y-%m")

    # Соединяем filtered_positions с priceendmonth по kod и month_year
    merged = pd.merge(
        filtered_positions,
        priceendmonth,
        left_on="kod",
        right_on="kod",
        suffixes=("", "_priceendmonth"),
    )

    # Фильтруем данные, оставляя только строки с совпадением по month_year
    merged = merged[merged["year_month"].dt.strftime("%Y-%m") == last_year_same_month]

    # Применяем логику на основе tsenarozn и цены прошлого года
    def calculate_new_price(row):
        tsenarozn = row["tsenarozn"]
        price_last_year = row["tsena"]

        if price_last_year == tsenarozn:
            if tsenarozn <= 500:
                return np.ceil((tsenarozn * 1.12) / 10) * 10
            elif tsenarozn <= 1000:
                return np.ceil((tsenarozn * 1.10) / 10) * 10
            elif tsenarozn <= 5000:
                return np.ceil((tsenarozn * 1.07) / 10) * 10
            elif tsenarozn <= 10000:
                return np.ceil((tsenarozn * 1.05) / 10) * 10
            else:
                return np.ceil((tsenarozn * 1.03) / 10) * 10
        return row["new_price"]

    # Вычисляем new_price для каждой строки
    merged["new_price"] = merged.apply(calculate_new_price, axis=1)

    # Обновляем значения new_price в исходном filtered_df
    filtered_df.update(merged[["kod", "new_price"]])

    # Заполняем оставшиеся пустые значения "new_price" значениями из "tsenarozn"
    filtered_df["new_price"] = filtered_df["new_price"].fillna(filtered_df["tsenarozn"])

    return filtered_df
