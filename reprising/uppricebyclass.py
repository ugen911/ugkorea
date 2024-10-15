import numpy as np
import pandas as pd


def adjust_prices_by_class(filtered_df, salespivot, suppliespivot):
    # Сначала находим позиции, где new_price отсутствует
    missing_price_condition = filtered_df["new_price"].isna()

    # Разделяем товары на оригинальные и неоригинальные по наличию 'ориг.' в наименовании
    filtered_df["is_original"] = filtered_df["naimenovanie"].str.contains(
        "ориг.", case=False, na=False
    )

    # Группируем по type_detail и наличию 'ориг.', считаем медианную цену для каждой группы
    median_prices_by_class = (
        filtered_df.loc[~filtered_df["new_price"].isna()]
        .groupby(["type_detail", "is_original"])
        .filter(lambda x: len(x) >= 5)  # Минимум 5 позиций для расчета
        .groupby(["type_detail", "is_original"])["new_price"]
        .median()
    )

    # Проверяем наличие данных о продажах и поступлениях за последние 24 месяца
    current_date = pd.Timestamp.now()
    last_24_months = current_date - pd.DateOffset(months=24)

    def has_recent_sales_or_supplies(kod):
        sales_data = salespivot[
            (salespivot["kod"] == kod) & (salespivot["year_month"] >= last_24_months)
        ]
        supplies_data = suppliespivot[
            (suppliespivot["kod"] == kod)
            & (suppliespivot["year_month"] >= last_24_months)
        ]
        return not sales_data.empty or not supplies_data.empty

    # Проходим по позициям, где new_price пустая
    for index, row in filtered_df.loc[missing_price_condition].iterrows():
        type_detail = row["type_detail"]
        is_original = row["is_original"]
        tsenarozn = row["tsenarozn"]
        kod = row["kod"]

        # Проверяем, есть ли медианная цена для этого класса и оригинальности
        if (type_detail, is_original) in median_prices_by_class:
            median_price = median_prices_by_class[(type_detail, is_original)]

            # Поднимаем цену до 50% от медианной цены, если tsenarozn ниже 50%
            if tsenarozn < (median_price * 0.5):
                new_price = np.ceil((median_price * 0.5) / 10) * 10
                filtered_df.at[index, "new_price"] = new_price

            # Если нет данных о продажах и поступлениях, опускаем цену до медианной + 10%
            elif tsenarozn > (median_price * 1.1) and not has_recent_sales_or_supplies(
                kod
            ):
                new_price = np.ceil((median_price * 1.1) / 10) * 10
                filtered_df.at[index, "new_price"] = new_price

    # Применяем проверки maxprice и middleprice
    maxprice = filtered_df["maxprice"]
    medianprice = filtered_df["middleprice"]
    condition_api = filtered_df["delsklad"].str.contains("api", case=False, na=False)

    # Проверка на maxprice
    condition_maxprice = (
        condition_api & maxprice.notna() & (filtered_df["new_price"] < maxprice * 1.1)
    )
    filtered_df.loc[condition_maxprice, "new_price"] = (
        np.ceil(maxprice[condition_maxprice] * 1.1 / 10) * 10
    )

    # Проверка на middleprice
    condition_middleprice = (
        condition_api
        & medianprice.notna()
        & (filtered_df["new_price"] < medianprice * 1.3)
    )
    filtered_df.loc[condition_middleprice, "new_price"] = (
        np.ceil(medianprice[condition_middleprice] * 1.3 / 10) * 10
    )

    # Возвращаем обновленный датафрейм
    return filtered_df
