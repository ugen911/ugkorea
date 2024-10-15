import numpy as np
import pandas as pd
from datetime import datetime


def calculate_new_prices_for_api(
    filtered_df, salespivot, suppliespivot, base_percent=1.6, reduced_base_percent=1.45
):
    # Получаем текущую дату
    current_date = datetime.now()

    # Создаем пустую колонку для новых цен
    filtered_df["new_price"] = np.nan

    # Фильтруем строки, где sklad содержит "api"
    condition = (
        (
            filtered_df["delsklad"].str.contains("api", case=False, na=False)
        )  # Включаем только delsklad с "api"
        & filtered_df["delprice"].notna()  # delprice не пустое
        & (filtered_df["delprice"] > 0)  # delprice положительное
    )

    # Присваиваем переменные для удобства работы
    delprice = filtered_df.loc[condition, "delprice"]
    medianprice = filtered_df.loc[condition, "middleprice"]
    maxprice = filtered_df.loc[condition, "maxprice"]
    abc = filtered_df.loc[condition, "abc"]
    xyz = filtered_df.loc[condition, "xyz"]
    tsenarozn = filtered_df.loc[condition, "tsenarozn"]
    data = pd.to_datetime(filtered_df.loc[condition, "data"], format="%Y-%m-%d")
    datasozdanija = pd.to_datetime(
        filtered_df.loc[condition, "datasozdanija"], format="%Y-%m-%d", errors="coerce"
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
    filtered_df.loc[condition, "base_percent"] = filtered_df.loc[condition].apply(
        get_base_percent, axis=1
    )

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
    filtered_df.loc[condition & recent_mask, "new_price"] = tsenarozn

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

    # Условие на дату, если прошло менее 60 дней — не выше 2.2 * medianprice
    last_60_days_mask = (current_date - data).dt.days < 60
    valid_median_mask = medianprice.notna()
    filtered_df.loc[condition & last_60_days_mask & valid_median_mask, "new_price"] = (
        np.minimum(new_price, np.ceil(medianprice * 2.2 / 10) * 10)
    )

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
        & filtered_df.loc[condition].apply(check_sales_only_and_no_purchases, axis=1)
    )

    # Применяем ограничение на median_price * 1.4, если выполняются условия
    filtered_df.loc[condition & special_condition, "new_price"] = np.minimum(
        new_price, np.ceil(medianprice * 1.4 / 10) * 10
    )

    # Проверка: если abc не C, не null и xyz равен 'X1' или 'X', новая цена не меньше delprice * 1.3
    abc_xyz_condition = (abc.notna()) & (abc != "C") & (xyz.isin(["X1", "X"]))
    filtered_df.loc[condition & abc_xyz_condition, "new_price"] = np.maximum(
        filtered_df.loc[condition & abc_xyz_condition, "new_price"],
        np.ceil(delprice * 1.3 / 10) * 10,
    )

    # Применяем проверки только к строкам, где выполняется условие maxprice и middleprice
    condition_maxprice = (
        condition
        & maxprice.notna()
        & (filtered_df.loc[condition, "new_price"] < maxprice * 1.1)
    )
    condition_middleprice = (
        condition
        & medianprice.notna()
        & (filtered_df.loc[condition, "new_price"] < medianprice * 1.3)
    )

    # Проверки на maxprice
    filtered_df.loc[condition_maxprice, "new_price"] = (
        np.ceil(maxprice[condition_maxprice] * 1.1 / 10) * 10
    )

    # Проверки на middleprice
    filtered_df.loc[condition_middleprice, "new_price"] = (
        np.ceil(medianprice[condition_middleprice] * 1.3 / 10) * 10
    )

    # Возвращаем обновленный filtered_df
    return filtered_df
