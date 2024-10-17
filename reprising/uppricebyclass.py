import numpy as np
import pandas as pd


def adjust_prices_by_class(filtered_df, salespivot, suppliespivot):
    # Сначала находим позиции, где new_price отсутствует
    missing_price_condition = filtered_df["new_price"].isna() & (
        filtered_df["delprice"].isna() | filtered_df["middleprice"].isna()
    )

    # Разделяем товары на оригинальные и неоригинальные по наличию 'ориг.' в наименовании
    filtered_df["is_original"] = filtered_df["naimenovanie"].str.contains(
        "ориг.", case=False, na=False
    )

    # Группируем по type_detail и наличию 'ориг.', считаем медианную цену для каждой группы
    median_prices_by_class = (
        filtered_df.loc[
            (~filtered_df["new_price"].isna())
            | (~filtered_df["delprice"].isna())
            | (~filtered_df["median_price"].isna())
        ]
        .groupby(["type_detail", "is_original"])
        .filter(lambda x: len(x) >= 5)
        .groupby(["type_detail", "is_original"])["tsenarozn"]
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

    # Добавляем метку для позиций, которые были обработаны в adjust_prices_by_class
    filtered_df["processed_by_class"] = False  # Новая колонка для отметки

    for index, row in filtered_df.loc[missing_price_condition].iterrows():
        type_detail = row["type_detail"]
        is_original = row["is_original"]
        tsenarozn = row["tsenarozn"]
        kod = row["kod"]

        if (type_detail, is_original) in median_prices_by_class:
            median_price = median_prices_by_class[(type_detail, is_original)]

            if tsenarozn < (median_price * 0.6):
                new_price = np.ceil((median_price * 0.6) / 10) * 10
                filtered_df.at[index, "new_price"] = new_price
                filtered_df.at[index, "processed_by_class"] = (
                    True  # Отмечаем позицию как обработанную
                )

            elif tsenarozn > (median_price * 1.1) and not has_recent_sales_or_supplies(kod):
                new_price = np.ceil((median_price * 1.1) / 10) * 10
                filtered_df.at[index, "new_price"] = new_price
                filtered_df.at[index, "processed_by_class"] = (
                    True  # Отмечаем позицию как обработанную
                )

    # Применяем проверки maxprice и middleprice
    maxprice = filtered_df["maxprice"]
    medianprice = filtered_df["middleprice"]
    

    # Проверка на maxprice
    condition_maxprice = (
        filtered_df["processed_by_class"] & maxprice.notna() & (filtered_df["new_price"] < maxprice * 1.1)
    )
    filtered_df.loc[condition_maxprice, "new_price"] = (
        np.ceil(maxprice[condition_maxprice] * 1.1 / 10) * 10
    )

    # Проверка на medianprice
    condition_middleprice = (
        filtered_df["processed_by_class"]
        & medianprice.notna()
        & (filtered_df["new_price"] < medianprice * 1.3)
    )
    filtered_df.loc[condition_middleprice, "new_price"] = (
        np.ceil(medianprice[condition_middleprice] * 1.3 / 10) * 10
    )

    # Удаляем колонку "processed_by_class" после использования
    filtered_df = filtered_df.drop(columns=["processed_by_class"])

    # Возвращаем обновленный датафрейм
    return filtered_df
