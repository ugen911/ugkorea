import pandas as pd
import numpy as np


def adjust_new_price_for_non_liquid(filtered_df, salespivot):
    current_date = pd.Timestamp.now()
    last_24_months = current_date - pd.DateOffset(months=24)
    last_18_months = current_date - pd.DateOffset(months=18)
    last_12_months = current_date - pd.DateOffset(months=12)

    # Преобразуем колонку 'data' в datetime
    filtered_df["data"] = pd.to_datetime(
        filtered_df["data"], format="%d.%m.%Y %H:%M", errors="coerce"
    )

    # Фильтр для наличия 'api' в колонке delsklad
    api_filter = filtered_df["delsklad"].str.contains("api", case=False, na=False)

    # Фильтруем позиции с рассчитанным new_price, если delprice или median_price заполнены и delsklad содержит "api"
    condition = (
        filtered_df["new_price"].notna()
        & (filtered_df["delprice"].notna() | filtered_df["median_price"].notna())
        & api_filter
    )

    # Добавляем колонки для процента наценки и изменения в рублях
    filtered_df["base_percent"] = np.nan
    filtered_df["price_diff"] = np.nan

    # Проверка на наличие покупок за указанный период
    def is_non_liquid(data):
        if pd.isna(
            data
        ):  # Если дата пустая, считаем, что последний приход был раньше 24 месяцев
            return True
        else:
            return data <= last_24_months

    # Проверка на количество месяцев с продажами
    def has_sales_more_than_two(kod, start_period, end_period):
        sales_data = salespivot[
            (salespivot["kod"] == kod)
            & (salespivot["year_month"] >= start_period)
            & (salespivot["year_month"] < end_period)
        ]
        return sales_data.shape[0] > 2  # True, если было больше 2 месяцев с продажами

    # Общая логика расчета цены
    def calculate_price(
        row,
        lower_bound_percent,
        upper_bound_percent,
        sales_threshold=False,
        sales_period=None,
    ):
        median_price = row["median_price"]
        delprice = row["delprice"]

        if pd.notna(median_price) and pd.notna(delprice):
            lower_bound = max(
                median_price * lower_bound_percent, delprice * lower_bound_percent
            )
            upper_bound = max(
                median_price * upper_bound_percent, delprice * upper_bound_percent
            )
        elif pd.notna(median_price):
            lower_bound = median_price * lower_bound_percent
            upper_bound = median_price * upper_bound_percent
        elif pd.notna(delprice):
            lower_bound = delprice * lower_bound_percent
            upper_bound = delprice * upper_bound_percent

        # Наценка при наличии более двух месяцев с продажами
        if sales_threshold:
            if sales_period == "24+":
                upper_bound *= 1.2
            elif sales_period == "18-24":
                upper_bound *= 1.25
            elif sales_period == "12-18":
                upper_bound *= 1.37

        new_price = np.ceil(upper_bound / 10) * 10
        return new_price

    # Применяем фильтры
    for index, row in filtered_df[condition].iterrows():
        kod = row["kod"]
        last_purchase_date = row["data"]

        # Логика для полностью неликвидных позиций (не покупались 24 месяца и больше)
        if is_non_liquid(last_purchase_date):
            if has_sales_more_than_two(kod, last_24_months, current_date):
                filtered_df.at[index, "new_price"] = calculate_price(
                    row, 0.7, 0.8, sales_threshold=True, sales_period="24+"
                )
            else:
                filtered_df.at[index, "new_price"] = calculate_price(row, 0.7, 0.8)

        # Логика для неликвида (не покупались 18-24 месяца)
        elif (
            last_purchase_date <= last_24_months and last_purchase_date > last_18_months
        ):
            if has_sales_more_than_two(kod, last_18_months, last_24_months):
                filtered_df.at[index, "new_price"] = calculate_price(
                    row, 0.8, 0.9, sales_threshold=True, sales_period="18-24"
                )
            else:
                filtered_df.at[index, "new_price"] = calculate_price(row, 0.8, 0.9)

        # Логика для неликвида (не покупались 12-18 месяцев)
        elif (
            last_purchase_date <= last_18_months and last_purchase_date > last_12_months
        ):
            if has_sales_more_than_two(kod, last_12_months, last_18_months):
                filtered_df.at[index, "new_price"] = calculate_price(
                    row, 1.0, 1.1, sales_threshold=True, sales_period="12-18"
                )
            else:
                filtered_df.at[index, "new_price"] = calculate_price(row, 1.0, 1.1)

        # Проверка и корректировка на maxprice и median_price
        maxprice = row["maxprice"]
        medianprice = row["median_price"]

        if pd.notna(maxprice) and pd.notna(medianprice):
            if is_non_liquid(last_purchase_date):
                max_limit = np.ceil((maxprice * 1.3) / 10) * 10
                median_limit = np.ceil((medianprice * 1.5) / 10) * 10
            elif (
                last_purchase_date <= last_24_months
                and last_purchase_date > last_18_months
            ):
                max_limit = np.ceil((maxprice * 1.3) / 10) * 10
                median_limit = np.ceil((medianprice * 1.5) / 10) * 10
            elif (
                last_purchase_date <= last_18_months
                and last_purchase_date > last_12_months
            ):
                max_limit = np.ceil((maxprice * 1.2) / 10) * 10
                median_limit = np.ceil((medianprice * 1.4) / 10) * 10
            else:
                max_limit = np.nan
                median_limit = np.nan

            if pd.notna(max_limit) and pd.notna(median_limit):
                final_limit = max(max_limit, median_limit)

                # Если new_price ниже пределов maxprice или median_price, подтягиваем его
                if filtered_df.at[index, "new_price"] < final_limit:
                    filtered_df.at[index, "new_price"] = final_limit

    # Вычисление base_percent и разницы в рублях между new_price и tsenarozn для всех строк
    mask = filtered_df["delprice"].notna() & filtered_df["new_price"].notna()
    filtered_df.loc[mask, "base_percent"] = (
        (filtered_df.loc[mask, "new_price"] - filtered_df.loc[mask, "delprice"])
        / filtered_df.loc[mask, "delprice"]
    ) * 100

    tsenarozn_mask = filtered_df["tsenarozn"].notna() & filtered_df["new_price"].notna()
    filtered_df.loc[tsenarozn_mask, "price_diff"] = (
        filtered_df.loc[tsenarozn_mask, "new_price"]
        - filtered_df.loc[tsenarozn_mask, "tsenarozn"]
    )

    return filtered_df
