import numpy as np
import pandas as pd
from datetime import datetime


def calculate_new_prices_for_api(
    filtered_df, salespivot, suppliespivot, base_percent=1.6, reduced_base_percent=1.45
):
    # Получаем текущую дату
    current_date = datetime.now()

    # Фильтруем строки, где sklad содержит "api"
    condition_api = (
        (
            filtered_df["delsklad"].str.contains("api", case=False, na=False)
        )  # Включаем только delsklad с "api"
        & filtered_df["delprice"].notna()  # delprice не пустое
        & (filtered_df["delprice"] > 0)  # delprice положительное
    )

    # Фильтруем строки, где sklad не содержит "api"
    condition_non_api = ~filtered_df["delsklad"].str.contains(
        "api", case=False, na=False
    )

    # Присваиваем переменные для удобства работы
    delprice = filtered_df.loc[condition_api, "delprice"]
    middleprice = filtered_df.loc[condition_api, "middleprice"]
    medianprice = filtered_df.loc[condition_api, "median_price"]
    maxprice = filtered_df.loc[condition_api, "maxprice"]
    abc = filtered_df.loc[condition_api, "abc"]
    xyz = filtered_df.loc[condition_api, "xyz"]
    tsenarozn = filtered_df.loc[condition_api, "tsenarozn"]
    data = pd.to_datetime(filtered_df.loc[condition_api, "data"], format="%Y-%m-%d")
    datasozdanija = pd.to_datetime(
        filtered_df.loc[condition_api, "datasozdanija"],
        format="%Y-%m-%d",
        errors="coerce",
    )

    # Получаем даты из salespivot и suppliespivot
    salespivot["year_month"] = pd.to_datetime(salespivot["year_month"], format="%Y-%m")
    suppliespivot["year_month"] = pd.to_datetime(
        suppliespivot["year_month"], format="%Y-%m"
    )

    # Проверяем, были ли продажи или покупки за последние 24 месяца
    def check_sales_and_supplies_last_24_months(kod):
        last_24_months = current_date - pd.DateOffset(months=24)
        sales_data = salespivot[
            (salespivot["kod"] == kod) & (salespivot["year_month"] >= last_24_months)
        ]
        supplies_data = suppliespivot[
            (suppliespivot["kod"] == kod)
            & (suppliespivot["year_month"] >= last_24_months)
        ]
        return (
            not sales_data.empty or not supplies_data.empty
        )  # True если были продажи или покупки

    # Применяем логику уменьшения наценки
    def get_base_percent(row):
        kod = row["kod"]
        if not check_sales_and_supplies_last_24_months(kod) and (
            pd.isna(row["datasozdanija"])
            or (current_date - row["datasozdanija"]).days > 730
        ):
            return reduced_base_percent
        return base_percent

    # Вычисляем base_percent для каждой строки
    filtered_df.loc[condition_api, "base_percent"] = filtered_df.loc[
        condition_api
    ].apply(get_base_percent, axis=1)

    # Условие по delprice
    new_price = np.select(
        [delprice <= 200, delprice <= 300, delprice >= 10000],
        [
            np.ceil(delprice * 1.8 / 10) * 10,
            np.ceil(delprice * 1.65 / 10) * 10,
            np.ceil(delprice * 1.35 / 10) * 10,
        ],
        default=np.nan,  # Все, что не попадает под условия, пока NaN
    )

    # Условие на дату, если прошло менее 14 дней — новая цена равна tsenarozn
    recent_mask = (current_date - data).dt.days < 14
    filtered_df.loc[condition_api & recent_mask, "new_price"] = tsenarozn

    # Для всех остальных строк, используем base_percent
    nan_mask = filtered_df.loc[condition_api, "new_price"].isna()
    filtered_df.loc[condition_api & nan_mask, "new_price"] = (
        np.ceil(
            delprice[nan_mask]
            * filtered_df.loc[condition_api & nan_mask, "base_percent"]
            / 10
        )
        * 10
    )

    # Условие на дату, если прошло менее 60 дней — не выше 2.2 * medianprice
    last_60_days_mask = (current_date - data).dt.days < 60
    valid_median_mask = medianprice.notna()
    filtered_df.loc[
        condition_api & last_60_days_mask & valid_median_mask, "new_price"
    ] = np.minimum(new_price, np.ceil(medianprice * 2.2 / 10) * 10)

    # Проверка условий abc, xyz и продаж/покупок за последние 2 года
    def check_sales_only_and_no_purchases(row):
        kod = row["kod"]
        last_2_years = current_date - pd.DateOffset(years=2)
        sales_data = salespivot[
            (salespivot["kod"] == kod) & (salespivot["year_month"] >= last_2_years)
        ]
        supplies_data = suppliespivot[
            (suppliespivot["kod"] == kod)
            & (suppliespivot["year_month"] >= last_2_years)
        ]
        return not sales_data.empty and supplies_data.empty

    special_condition = (
        (abc != "A")
        & (abc != "A1")
        & (xyz == "Z")
        & filtered_df.loc[condition_api].apply(
            check_sales_only_and_no_purchases, axis=1
        )
    )

    # Применяем ограничение на median_price * 1.4, если выполняются условия
    filtered_df.loc[condition_api & special_condition, "new_price"] = np.minimum(
        new_price, np.ceil(medianprice * 1.4 / 10) * 10
    )

    # Проверка: если abc не C, не null и xyz равен 'X1' или 'X', новая цена не меньше delprice * 1.3
    abc_xyz_condition = (abc.notna()) & (abc != "C") & (xyz.isin(["X1", "X"]))
    filtered_df.loc[condition_api & abc_xyz_condition, "new_price"] = np.maximum(
        filtered_df.loc[condition_api & abc_xyz_condition, "new_price"],
        np.ceil(delprice * 1.3 / 10) * 10,
    )

    # Проверка на среднюю цену группы товаров (type_detail, proizvoditel) перед проверкой maxprice и middleprice
    group_means = (
        filtered_df.loc[condition_non_api]
        .groupby(["type_detail", "proizvoditel"])["new_price"]
        .mean()
    )

    # Сравниваем с товарами, где sklad содержит "api"
    for index, row in filtered_df.loc[condition_api].iterrows():
        group_key = (row["type_detail"], row["proizvoditel"])
        if group_key in group_means:
            group_mean_price = group_means[group_key]
            if row["new_price"] > group_mean_price * 1.2:
                # Снижаем new_price до средней цены + 10%
                filtered_df.at[index, "new_price"] = (
                    np.ceil(group_mean_price * 1.1 / 10) * 10
                )

        # Проверка на минимальное значение +10% к delprice
        min_delprice = np.ceil(delprice.at[index] * 1.1 / 10) * 10
        if row["new_price"] < min_delprice:
            filtered_df.at[index, "new_price"] = min_delprice

        # Проверка на минимальное значение +25% к medianprice
        if not pd.isna(middleprice.at[index]):
            min_middleprice = np.ceil(middleprice.at[index] * 1.25 / 10) * 10
            if row["new_price"] < min_middleprice:
                filtered_df.at[index, "new_price"] = min_middleprice

    # Проверка: если delprice и new_price отсутствуют, но есть median_price
    missing_price_condition = (
        filtered_df["delprice"].isna()
        & filtered_df["new_price"].isna()
        & filtered_df["median_price"].notna()
    )
    for index, row in filtered_df.loc[missing_price_condition].iterrows():
        price = row["median_price"]
        if price <= 200:
            new_price = np.ceil(price * 2.2 / 10) * 10
        elif price <= 300:
            new_price = np.ceil(price * 2.0 / 10) * 10
        elif price >= 10000:
            new_price = np.ceil(price * 1.5 / 10) * 10
        else:
            new_price = np.ceil(price * 1.8 / 10) * 10

        # Проверка на tsenarozn
        if row["tsenarozn"] > new_price:
            new_price = row["tsenarozn"]

        filtered_df.at[index, "new_price"] = new_price

    # Применяем проверки только к строкам, где выполняется условие maxprice и middleprice
    condition_maxprice = (
        condition_api
        & maxprice.notna()
        & (filtered_df.loc[condition_api, "new_price"] < maxprice * 1.1)
    )
    condition_middleprice = (
        condition_api
        & middleprice.notna()
        & (filtered_df.loc[condition_api, "new_price"] < middleprice * 1.3)
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