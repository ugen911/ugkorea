import numpy as np
import pandas as pd


def adjust_prices_by_class(filtered_df, salespivot, suppliespivot):
    """
    Обновляет цены для позиций без new_price или delprice/middleprice,
    учитывая медианные цены по классам и данные о продажах/закупках.
    """
    # --- Улучшение: приводим year_month в pivot-таблицах к datetime для корректных сравнений ---
    salespivot = salespivot.copy()
    suppliespivot = suppliespivot.copy()
    salespivot["year_month"] = pd.to_datetime(
        salespivot["year_month"], format="%Y-%m", errors="coerce"
    )
    suppliespivot["year_month"] = pd.to_datetime(
        suppliespivot["year_month"], format="%Y-%m", errors="coerce"
    )

    # 1) Выбираем позиции, где нет цены
    missing_price_condition = (
        filtered_df["new_price"].isna()
        & (
            filtered_df["delprice"].isna()
            | filtered_df["middleprice"].isna()
        )
    )

    # 2) Отмечаем оригинальные товары
    filtered_df["is_original"] = filtered_df[
        "naimenovanie"
    ].str.contains("ориг.", case=False, na=False)

    # 3) Вычисляем медиану tsenarozn по (type_detail, is_original) для групп с >=5 записей
    median_prices_by_class = (
        filtered_df.loc[
            ~filtered_df[["new_price", "delprice", "median_price"]]
            .isna()
            .all(axis=1)
        ]
        .groupby(["type_detail", "is_original"])  
        .filter(lambda x: len(x) >= 5)
        .groupby(["type_detail", "is_original"])["tsenarozn"]
        .median()
    )

    # 4) Параметры по времени
    current_date = pd.Timestamp.now()
    last_24_months = current_date - pd.DateOffset(months=24)

    # 5) Функция проверки активности за 24 месяца
    def has_recent_sales_or_supplies(kod):
        sold = salespivot[
            (salespivot["kod"] == kod)
            & (salespivot["year_month"] >= last_24_months)
        ]
        bought = suppliespivot[
            (suppliespivot["kod"] == kod)
            & (suppliespivot["year_month"] >= last_24_months)
        ]
        return not sold.empty or not bought.empty

    # 6) МАРКЕР обработки
    filtered_df["processed_by_class"] = False

    # 7) Основная логика для missing_price_condition
    for idx, row in filtered_df.loc[missing_price_condition].iterrows():
        key = (row["type_detail"], row["is_original"])
        tsenarozn = row["tsenarozn"]
        kod = row["kod"]

        if key in median_prices_by_class:
            median_price = median_prices_by_class[key]
            # Снижаем, если слишком низко
            if tsenarozn < median_price * 0.6:
                filtered_df.at[idx, "new_price"] = np.ceil(median_price * 0.6 / 10) * 10
                filtered_df.at[idx, "processed_by_class"] = True
            # Повышаем, если слишком высоко и нет активности
            elif tsenarozn > median_price * 1.1 and not has_recent_sales_or_supplies(kod):
                filtered_df.at[idx, "new_price"] = np.ceil(median_price * 1.1 / 10) * 10
                filtered_df.at[idx, "processed_by_class"] = True

    # 8) Применяем ограничения по maxprice и middleprice
    maxprice = filtered_df["maxprice"]
    middleprice = filtered_df["middleprice"]

    cond_max = (
        filtered_df["processed_by_class"]
        & maxprice.notna()
        & (filtered_df["new_price"] < maxprice * 1.1)
    )
    filtered_df.loc[cond_max, "new_price"] = (
        np.ceil(maxprice[cond_max] * 1.1 / 10) * 10
    )

    cond_mid = (
        filtered_df["processed_by_class"]
        & middleprice.notna()
        & (filtered_df["new_price"] < middleprice * 1.3)
    )
    filtered_df.loc[cond_mid, "new_price"] = (
        np.ceil(middleprice[cond_mid] * 1.3 / 10) * 10
    )

    return filtered_df
