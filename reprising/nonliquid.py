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

    # Рассчитываем среднюю цену tsenarozn для каждой группы type_detail и is_original (если не менее 5 элементов в группе)
    group_avg_price = filtered_df.groupby(["type_detail", "is_original"])[
        "tsenarozn"
    ].transform(lambda x: x.mean() if len(x) >= 5 else np.nan)

    # Добавляем рассчитанные средние цены в основной датафрейм
    filtered_df["avg_tsenarozn"] = group_avg_price

    # Инициализируем колонку для логирования
    filtered_df["price_log"] = ""

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

    # Общая логика расчета цены с округлением до ближайших 10 вверх
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

        # Округляем до ближайших 10 вверх
        new_price = np.ceil(upper_bound / 10) * 10
        return new_price

    # Применяем фильтры и корректируем цены
    for index, row in filtered_df[condition].iterrows():
        kod = row["kod"]
        last_purchase_date = row["data"]
        avg_tsenarozn = row["avg_tsenarozn"]
        current_tsenarozn = row["tsenarozn"]

        # Инициализация лога для данной строки
        price_log = []

        # Логика для полностью неликвидных позиций (не покупались 24 месяца и больше)
        if is_non_liquid(last_purchase_date):
            price_log.append("Position is non-liquid for 24+ months.")
            if has_sales_more_than_two(kod, last_24_months, current_date):
                new_price = calculate_price(
                    row, 0.7, 0.8, sales_threshold=True, sales_period="24+"
                )
                price_log.append(
                    f"Calculated new_price with 24+ months and sales > 2: {new_price}"
                )
            else:
                new_price = calculate_price(row, 0.7, 0.8)
                price_log.append(
                    f"Calculated new_price with 24+ months and sales <= 2: {new_price}"
                )

            # Проверка, если new_price выше tsenarozn и avg_tsenarozn рассчитана
            if pd.notna(avg_tsenarozn) and new_price > current_tsenarozn:
                max_limit = np.ceil((avg_tsenarozn * 0.8) / 10) * 10
                if new_price > max_limit:
                    filtered_df.at[index, "new_price"] = max_limit
                    price_log.append(
                        f"Adjusted new_price to max limit (80% of avg_tsenarozn): {max_limit}"
                    )
                else:
                    filtered_df.at[index, "new_price"] = new_price
                    price_log.append(f"New_price set to calculated value: {new_price}")
            elif pd.isna(avg_tsenarozn) and new_price > current_tsenarozn:
                filtered_df.at[index, "new_price"] = current_tsenarozn
                price_log.append(
                    f"Avg_tsenarozn not available; set new_price to tsenarozn: {current_tsenarozn}"
                )

            # Дополнительная проверка: если new_price ниже tsenarozn
            if new_price < current_tsenarozn:
                if pd.notna(avg_tsenarozn):
                    min_limit = np.ceil((avg_tsenarozn * 0.5) / 10) * 10
                    if new_price < min_limit:
                        filtered_df.at[index, "new_price"] = min_limit
                        price_log.append(
                            f"Adjusted new_price to min limit (50% of avg_tsenarozn): {min_limit}"
                        )
                else:
                    if new_price < np.ceil((current_tsenarozn * 0.7) / 10) * 10:
                        filtered_df.at[index, "new_price"] = (
                            np.ceil(current_tsenarozn / 10) * 10
                        )
                        price_log.append(
                            f"Avg_tsenarozn not available; set new_price to tsenarozn: {current_tsenarozn}"
                        )

        # Логика для неликвида (не покупались 18-24 месяца)
        elif (
            last_purchase_date <= last_24_months and last_purchase_date > last_18_months
        ):
            price_log.append("Position is non-liquid for 18-24 months.")
            if has_sales_more_than_two(kod, last_18_months, last_24_months):
                new_price = calculate_price(
                    row, 0.8, 0.9, sales_threshold=True, sales_period="18-24"
                )
                price_log.append(
                    f"Calculated new_price with 18-24 months and sales > 2: {new_price}"
                )
            else:
                new_price = calculate_price(row, 0.8, 0.9)
                price_log.append(
                    f"Calculated new_price with 18-24 months and sales <= 2: {new_price}"
                )

            if pd.notna(avg_tsenarozn) and new_price > current_tsenarozn:
                max_limit = np.ceil((avg_tsenarozn * 0.9) / 10) * 10
                if new_price > max_limit:
                    filtered_df.at[index, "new_price"] = max_limit
                    price_log.append(
                        f"Adjusted new_price to max limit (90% of avg_tsenarozn): {max_limit}"
                    )
                else:
                    filtered_df.at[index, "new_price"] = new_price
                    price_log.append(f"New_price set to calculated value: {new_price}")
            elif pd.isna(avg_tsenarozn) and new_price > current_tsenarozn:
                filtered_df.at[index, "new_price"] = current_tsenarozn
                price_log.append(
                    f"Avg_tsenarozn not available; set new_price to tsenarozn: {current_tsenarozn}"
                )

            # Дополнительная проверка: если new_price ниже tsenarozn
            if new_price < current_tsenarozn:
                if pd.notna(avg_tsenarozn):
                    min_limit = np.ceil((avg_tsenarozn * 0.6) / 10) * 10
                    if new_price < min_limit:
                        filtered_df.at[index, "new_price"] = min_limit
                        price_log.append(
                            f"Adjusted new_price to min limit (60% of avg_tsenarozn): {min_limit}"
                        )
                else:
                    if new_price < np.ceil((current_tsenarozn * 0.7) / 10) * 10:
                        filtered_df.at[index, "new_price"] = (
                            np.ceil(current_tsenarozn / 10) * 10
                        )
                        price_log.append(
                            f"Avg_tsenarozn not available; set new_price to tsenarozn: {current_tsenarozn}"
                        )

        # Логика для неликвида (не покупались 12-18 месяцев)
        elif (
            last_purchase_date <= last_18_months and last_purchase_date > last_12_months
        ):
            price_log.append("Position is non-liquid for 12-18 months.")
            if has_sales_more_than_two(kod, last_12_months, last_18_months):
                new_price = calculate_price(
                    row, 1.0, 1.1, sales_threshold=True, sales_period="12-18"
                )
                price_log.append(
                    f"Calculated new_price with 12-18 months and sales > 2: {new_price}"
                )
            else:
                new_price = calculate_price(row, 1.0, 1.1)
                price_log.append(
                    f"Calculated new_price with 12-18 months and sales <= 2: {new_price}"
                )

            if pd.notna(avg_tsenarozn) and new_price > current_tsenarozn:
                max_limit = np.ceil((avg_tsenarozn * 1.0) / 10) * 10
                if new_price > max_limit:
                    filtered_df.at[index, "new_price"] = max_limit
                    price_log.append(
                        f"Adjusted new_price to max limit (100% of avg_tsenarozn): {max_limit}"
                    )
                else:
                    filtered_df.at[index, "new_price"] = new_price
                    price_log.append(f"New_price set to calculated value: {new_price}")
            elif pd.isna(avg_tsenarozn) and new_price > current_tsenarozn:
                filtered_df.at[index, "new_price"] = current_tsenarozn
                price_log.append(
                    f"Avg_tsenarozn not available; set new_price to tsenarozn: {current_tsenarozn}"
                )

            # Дополнительная проверка: если new_price ниже tsenarozn
            if new_price < current_tsenarozn:
                if pd.notna(avg_tsenarozn):
                    min_limit = np.ceil((avg_tsenarozn * 0.7) / 10) * 10
                    if new_price < min_limit:
                        filtered_df.at[index, "new_price"] = min_limit
                        price_log.append(
                            f"Adjusted new_price to min limit (70% of avg_tsenarozn): {min_limit}"
                        )
                else:
                    if new_price < np.ceil((current_tsenarozn * 0.7) / 10) * 10:
                        filtered_df.at[index, "new_price"] = (
                            np.ceil(current_tsenarozn / 10) * 10
                        )
                        price_log.append(
                            f"Avg_tsenarozn not available; set new_price to tsenarozn: {current_tsenarozn}"
                        )

        # Проверка и корректировка на maxprice и middleprice
        maxprice = row["maxprice"]
        middleprice = row["middleprice"]

        # Проверяем maxprice, чтобы new_price не был ниже maxprice + 10%
        if pd.notna(maxprice):
            max_limit = np.ceil((maxprice * 1.1) / 10) * 10
            if filtered_df.at[index, "new_price"] < max_limit:
                filtered_df.at[index, "new_price"] = max_limit
                price_log.append(
                    f"Adjusted new_price to not be below maxprice + 10%: {max_limit}"
                )

        # Проверяем middleprice, чтобы new_price не был ниже middleprice + 30%
        if pd.notna(middleprice):
            min_limit = np.ceil((middleprice * 1.3) / 10) * 10
            if filtered_df.at[index, "new_price"] < min_limit:
                filtered_df.at[index, "new_price"] = min_limit
                price_log.append(
                    f"Adjusted new_price to not be below middleprice + 30%: {min_limit}"
                )

        # Записываем лог в колонку price_log
        filtered_df.at[index, "price_log"] = "\n".join(price_log)

    # Вычисляем base_percent в конце, после всех корректировок new_price
    mask = filtered_df["delprice"].notna() & filtered_df["new_price"].notna()
    filtered_df.loc[mask, "base_percent"] = (
        (filtered_df.loc[mask, "new_price"] - filtered_df.loc[mask, "delprice"])
        / filtered_df.loc[mask, "delprice"]
    ) * 100

    return filtered_df
