import numpy as np
import pandas as pd
from datetime import datetime


def preprocess_data(
    filtered_df: pd.DataFrame,
    salespivot: pd.DataFrame,
    suppliespivot: pd.DataFrame,
):
    """
    1) Копирует входные DF и сохраняет текущее время в current_date
    2) Приводит колонки 'data' и 'datasozdanija' в filtered_df к datetime
    3) Приводит 'year_month' в salespivot и suppliespivot к datetime
    4) Формирует маску condition_api для строк с 'api' в 'delsklad'
    """
    current_date = datetime.now()

    filtered_df = filtered_df.copy()
    salespivot = salespivot.copy()
    suppliespivot = suppliespivot.copy()

    filtered_df["data"] = pd.to_datetime(
        filtered_df["data"], format="%Y-%m-%d", errors="coerce"
    )
    filtered_df["datasozdanija"] = pd.to_datetime(
        filtered_df["datasozdanija"], format="%Y-%m-%d", errors="coerce"
    )

    salespivot["year_month"] = pd.to_datetime(
        salespivot["year_month"], format="%Y-%m", errors="coerce"
    )
    suppliespivot["year_month"] = pd.to_datetime(
        suppliespivot["year_month"], format="%Y-%m", errors="coerce"
    )

    condition_api = (
        filtered_df["delsklad"].str.contains("api", case=False, na=False)
        & filtered_df["delprice"].notna()
        & (filtered_df["delprice"] > 0)
    )

    return filtered_df, salespivot, suppliespivot, condition_api, current_date


def calculate_api_repricing(
    filtered_df: pd.DataFrame,
    condition_api: pd.Series,
    salespivot: pd.DataFrame,
    suppliespivot: pd.DataFrame,
    current_date: datetime,
    base_percent: float = 1.6,
    reduced_base_percent: float = 1.45,
):
    """
    Обновляет new_price для API-товаров:
      1) Жёсткие диапазоны delprice
      2) Для 300<delprice<10000: reduced_base_percent при отсутствии активности
         за 24 мес. И старой/пустой дате создания, иначе base_percent
      3) recent <14дн → tsenarozn
      4) cap по median_price*1.8 для <60дн
      5) общий cap по median_price*2.0
      6) минимум по tsenarozn*1.1, middleprice*1.3, maxprice*1.1
    """
    def has_activity_last_24_months(kod: str) -> bool:
        cutoff = current_date - pd.DateOffset(months=24)
        sold = salespivot[(salespivot["kod"] == kod) & (salespivot["year_month"] >= cutoff)]
        bought = suppliespivot[(suppliespivot["kod"] == kod) & (suppliespivot["year_month"] >= cutoff)]
        return not sold.empty or not bought.empty

    df_api = filtered_df.loc[condition_api].copy()
    dp = df_api["delprice"]

    low   = dp <= 200
    mid   = (dp > 200) & (dp <= 300)
    high  = dp >= 10000
    other = ~(low | mid | high)

    new_price = pd.Series(index=df_api.index, dtype=float)
    new_price[low]  = np.ceil(dp[low]  * 1.8  / 10) * 10
    new_price[mid]  = np.ceil(dp[mid]  * 1.65 / 10) * 10
    new_price[high] = np.ceil(dp[high] * 1.35 / 10) * 10

    def pick_pct(row):
        no_act = not has_activity_last_24_months(row["kod"])
        old = pd.isna(row["datasozdanija"]) or (current_date - row["datasozdanija"]).days > 730
        return reduced_base_percent if (no_act and old) else base_percent

    pcts = df_api.loc[other].apply(pick_pct, axis=1)
    new_price[other] = np.ceil(dp[other] * pcts / 10) * 10

    filtered_df.loc[condition_api, "new_price"] = new_price

    # 1) recent <14 дней → tsenarozn
    recent_mask = filtered_df["data"].notna() & ((current_date - filtered_df["data"]).dt.days < 14)
    idx14 = filtered_df.index[condition_api & recent_mask]
    filtered_df.loc[idx14, "new_price"] = filtered_df.loc[idx14, "tsenarozn"]

    # 2) cap по median_price*1.8 для <60 дней
    mask60 = filtered_df["data"].notna() & ((current_date - filtered_df["data"]).dt.days < 60)
    med_ok = filtered_df["median_price"].notna()
    idx60 = filtered_df.index[condition_api & mask60 & med_ok]
    cap60 = np.ceil(filtered_df.loc[idx60, "median_price"] * 1.8 / 10) * 10
    filtered_df.loc[idx60, "new_price"] = np.minimum(filtered_df.loc[idx60, "new_price"], cap60)

    # 3) общий cap по median_price*2.0
    idx_med = filtered_df.index[condition_api & med_ok]
    cap2 = np.ceil(filtered_df.loc[idx_med, "median_price"] * 2.0 / 10) * 10
    filtered_df.loc[idx_med, "new_price"] = np.minimum(filtered_df.loc[idx_med, "new_price"], cap2)

    # 4) минимум по tsenarozn*1.1, middleprice*1.3, maxprice*1.1
    ts = filtered_df.loc[condition_api, "tsenarozn"].fillna(0)
    mp = filtered_df.loc[condition_api, "middleprice"].fillna(0)
    mx = filtered_df.loc[condition_api, "maxprice"].fillna(0)

    min_ts = np.ceil(ts * 1.1 / 10) * 10
    min_mp = np.ceil(mp * 1.3 / 10) * 10
    min_mx = np.ceil(mx * 1.1 / 10) * 10

    final = filtered_df.loc[condition_api, "new_price"]
    final = np.maximum(final, min_ts)
    final = np.maximum(final, min_mp)
    final = np.maximum(final, min_mx)
    filtered_df.loc[condition_api, "new_price"] = final

    return filtered_df


def calculate_new_prices_for_api(
    filtered_df: pd.DataFrame,
    salespivot: pd.DataFrame,
    suppliespivot: pd.DataFrame,
    base_percent: float = 1.6,
    reduced_base_percent: float = 1.45,
):
    """
    Возвращает тот же filtered_df, где обновлены new_price
    только для строк с 'api' в 'delsklad'.
    """
    filtered_df, salespivot, suppliespivot, condition_api, current_date = preprocess_data(
        filtered_df, salespivot, suppliespivot
    )

    # Обновляем new_price только внутри calculate_api_repricing
    filtered_df = calculate_api_repricing(
        filtered_df,
        condition_api,
        salespivot,
        suppliespivot,
        current_date,
        base_percent,
        reduced_base_percent,
    )

    return filtered_df
