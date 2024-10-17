import pandas as pd
import numpy as np


def indeksation(filtered_df, priceendmonth):
    """
    Функция корректирует цены в filtered_df на основе данных priceendmonth и условий.

    Parameters:
    filtered_df (pd.DataFrame): Основной датафрейм с информацией о позициях.
    priceendmonth (pd.DataFrame): Датафрейм с историческими данными о ценах на конец месяца.

    Returns:
    pd.DataFrame: Обновленный датафрейм filtered_df со всеми строками и корректировками цен.
    """
    # Преобразуем колонку year_month в соответствующий формат
    priceendmonth["year_month"] = pd.to_datetime(
        priceendmonth["year_month"], format="%Y-%m", errors="coerce"
    )

    # Получаем текущую дату и дату ровно год назад (тот же месяц и день)
    current_date = pd.Timestamp.now()
    last_year_same_month = (current_date - pd.DateOffset(years=1)).strftime("%Y-%m")

    # Фильтруем строки, где new_price пустой или равен tsenarozn, и при этом delsklad и median_price должны быть пустыми
    condition = (
        (filtered_df["new_price"].isna())
        | (filtered_df["new_price"] == filtered_df["tsenarozn"])
    ) & (filtered_df["delsklad"].isna() & filtered_df["median_price"].isna())
    filtered_positions = filtered_df[condition]

    # Соединяем filtered_positions с priceendmonth по kod
    merged = pd.merge(
        filtered_positions,
        priceendmonth,
        left_on="kod",
        right_on="kod",
        suffixes=("", "_priceendmonth"),
    )

    # Фильтруем данные, оставляя только строки с совпадением по year_month
    merged = merged[merged["year_month"].dt.strftime("%Y-%m") == last_year_same_month]

    # Отладочная печать: сколько kod были найдены совпадения
    matching_kod_count = merged["kod"].nunique()
    print(
        f"Количество kod, найденных с совпадением по year_month: {matching_kod_count}"
    )

    # Вычисляем новую цену для каждой строки, если цена прошлого года совпадает с текущей
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

    # Применяем логику и считаем количество проиндексированных строк
    indexed_count = 0
    for index, row in merged.iterrows():
        new_price = calculate_new_price(row)
        if new_price != row["new_price"]:
            merged.at[index, "new_price"] = new_price
            indexed_count += 1

    # Отладочная печать: сколько строк было проиндексировано
    print(f"Количество строк, проиндексированных по цене: {indexed_count}")

    # Обновляем значения new_price в исходном filtered_df
    filtered_df.update(merged[["kod", "new_price"]])

    # Проверяем, остались ли пустые строки в new_price
    empty_new_price_count = filtered_df["new_price"].isna().sum()
    print(f"Количество строк с пустыми значениями new_price: {empty_new_price_count}")

    # Заполняем оставшиеся пустые значения "new_price" значениями из "tsenarozn"
    filtered_df["new_price"] = filtered_df["new_price"].fillna(filtered_df["tsenarozn"])

    # Отладочная печать: проверка, сколько строк осталось с пустыми значениями
    final_empty_count = filtered_df["new_price"].isna().sum()
    print(
        f"Количество строк с пустыми значениями new_price после заполнения: {final_empty_count}"
    )

    return filtered_df
