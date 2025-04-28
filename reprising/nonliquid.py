import pandas as pd
import numpy as np


def adjust_new_price_for_non_liquid(filtered_df, salespivot):
    """
    Для API-позиций с уже рассчитанным new_price «зажимает» его в диапазоны по возрасту закупки:
      • >36 мес: 70–80%
      • 24–36 мес: 90–110%
      • 18–24 мес: 110–130%
      • 12–18 мес: 1.37–1.40 (для base_price 300–10000) или 100–110% иначе

    Если delprice ≤ 300, по всем периодам нижняя граница не может быть ниже 1.4×delprice.
    Если 300 < delprice ≤ 1000, по всем периодам нижняя граница не может быть ниже 1.2×delprice.
    Плюс dynamic boost (до ×1.30) по числу месяцев с продажами и cap на 1.5×base_price.
    Затем доп. «зажим» по avg_tsenarozn, не ниже maxprice×1.1 и middleprice×1.3.
    Пересчитывает base_percent.
    """
    now      = pd.Timestamp.now()
    last_12  = now - pd.DateOffset(months=12)
    last_18  = now - pd.DateOffset(months=18)
    last_24  = now - pd.DateOffset(months=24)
    last_36  = now - pd.DateOffset(months=36)

    # 1) Преобразуем даты
    filtered_df["data"] = pd.to_datetime(
        filtered_df["data"], format="%d.%m.%Y %H:%M", errors="coerce"
    )
    filtered_df["datasozdanija"] = pd.to_datetime(
        filtered_df.get("datasozdanija", None), errors="coerce"
    )
    filtered_df["last_purchase"] = filtered_df["data"].fillna(
        filtered_df["datasozdanija"]
    )

    # 2) Средняя tsenarozn (группы ≥5 записей)
    filtered_df["avg_tsenarozn"] = (
        filtered_df
        .groupby(["type_detail", "is_original"])["tsenarozn"]
        .transform(lambda x: x.mean() if len(x) >= 5 else np.nan)
    )

    # 3) Маска «API + готовый new_price + есть delprice или median_price»
    api_mask = filtered_df["delsklad"].str.contains("api", case=False, na=False)
    pm_mask  = (
        filtered_df["new_price"].notna()
        & (filtered_df["delprice"].notna() | filtered_df["median_price"].notna())
    )
    mask     = api_mask & pm_mask

    def has_sales_more_than_two(kod, start, end):
        return (
            (salespivot["kod"] == kod)
            & (salespivot["year_month"] >= start)
            & (salespivot["year_month"] < end)
        ).sum() > 2

    boost_factors = {"36+":1.2, "24-36":1.25, "18-24":1.37, "12-18":1.37}
    avg_lowf      = {"36+":0.5,   "24-36":0.9,  "18-24":1.1, "12-18":0.7}
    avg_upf       = {"36+":0.8,   "24-36":1.1,  "18-24":1.3, "12-18":1.0}

    def clamp(orig, low, high):
        return np.ceil(min(max(orig, low), high) / 10) * 10

    # 4) Подсчёт уникальных месяцев продаж
    sales_months = salespivot.groupby("kod")["year_month"].nunique()

    # 5) Основной цикл
    for idx, row in filtered_df.loc[mask].iterrows():
        orig      = row["new_price"]
        dt        = row["last_purchase"]
        kod       = row["kod"]
        avg_tz    = row["avg_tsenarozn"]
        cur_tz    = row["tsenarozn"]

        # 5.1) Вычисляем base_price из median_price/delprice
        mp, dp = row["median_price"], row["delprice"]
        if pd.notna(mp) and pd.notna(dp):
            base_price = max(mp, dp)
        elif pd.notna(mp):
            base_price = mp
        else:
            base_price = dp

        # 5.2) Определяем период по дате закупки и устанавливаем low_pct/high_pct
        if pd.isna(dt) or dt <= last_36:
            period    = "36+"
            low_pct, high_pct = 0.7, 0.8
            boost     = has_sales_more_than_two(kod, last_36, now)

        elif dt <= last_24:
            period    = "24-36"
            low_pct, high_pct = 0.9, 1.1
            boost     = has_sales_more_than_two(kod, last_36, last_24)

        elif dt <= last_18:
            period    = "18-24"
            low_pct, high_pct = 1.1, 1.3
            boost     = has_sales_more_than_two(kod, last_24, last_18)

        elif dt <= last_12:
            period    = "12-18"
            if 300 <= base_price <= 10000:
                low_pct, high_pct = 1.37, 1.40
            else:
                low_pct, high_pct = 1.0, 1.1
            boost     = has_sales_more_than_two(kod, last_18, last_12)

        else:
            # моложе 12 мес — не трогаем
            continue

        # 5.3) Расчёт диапазона
        low  = base_price * low_pct
        high = base_price * high_pct

        # 5.4) Универсальное нижнее ограничение на основе delprice
        if dp <= 300:
            min_floor = dp * 1.4
            low       = max(low, min_floor)
            high      = max(high, min_floor)
        elif dp <= 1000:
            min_floor = dp * 1.2
            low       = max(low, min_floor)
            high      = max(high, min_floor)

        # 5.5) Boost и cap
        if boost:
            high *= boost_factors[period]
            high  = min(high, base_price * 1.5)

        # 5.6) Первичный clamp и округление
        new_price = clamp(orig, low, high)

        # 5.7) Clamp по avg_tsenarozn
        if pd.notna(avg_tz):
            min_a = np.ceil((avg_tz * avg_lowf[period]) / 10) * 10
            max_a = np.ceil((avg_tz * avg_upf[period]) / 10) * 10
            new_price = min(max(new_price, min_a), max_a)
        elif new_price > cur_tz:
            new_price = cur_tz

        # 5.8) Внешние пороги
        if pd.notna(row["maxprice"]):
            floor = np.ceil((row["maxprice"] * 1.1) / 10) * 10
            new_price = max(new_price, floor)
        if pd.notna(row["middleprice"]):
            floor = np.ceil((row["middleprice"] * 1.3) / 10) * 10
            new_price = max(new_price, floor)

        filtered_df.at[idx, "new_price"] = new_price

    # 6) Пересчёт base_percent
    valid = filtered_df["delprice"].notna() & filtered_df["new_price"].notna()
    filtered_df.loc[valid, "base_percent"] = (
        (filtered_df.loc[valid, "new_price"] - filtered_df.loc[valid, "delprice"])
        / filtered_df.loc[valid, "delprice"]
    ) * 100

    return filtered_df



def adjust_prices_without_delprice(filtered_df):
    """
    Корректирует позиции без delprice и delsklad, но с median_price и new_price:
    1) Ограничивает new_price сверху на основе tsenazakup и коэффициентов:
       • tsenazakup ≤ 1000       → ×3.0
       • 1000 < tsenazakup ≤ 2000→ ×2.5
       • 2000 < tsenazakup ≤ 5000→ ×2.2
       • 5000 < tsenazakup ≤ 10000→ ×2.0
       • tsenazakup > 10000      → ×1.8
       Результат округляется вверх до 10 руб.
    2) Ограничивает new_price снизу на 55% от медианы new_price по каждому type_detail
       (округление вверх до 10 руб).
    Возвращает копию filtered_df с изменёнными new_price.
    """
    df = filtered_df.copy()

    # 1) Отбираем нужные строки
    mask = (
        df["delprice"].isna()
        & df["delsklad"].isna()
        & df["median_price"].notna()
        & df["new_price"].notna()
    )

    # 2) Вычисляем верхний порог max_price (индексы уже соответствуют mask)
    tsu = df.loc[mask, "tsenazakup"]
    bins = [0, 1000, 2000, 5000, 10000, np.inf]
    factors = [3.0, 2.5, 2.2, 2.0, 1.8]
    factor_series = pd.cut(tsu, bins=bins, labels=factors, right=True).astype(float)
    max_price = np.ceil((tsu * factor_series) / 10) * 10

    # 3) Вычисляем нижний порог min_allowed (для всех строк)
    median_by_type = df.groupby("type_detail")["new_price"].transform("median")
    min_allowed = np.ceil(median_by_type * 0.55 / 10) * 10

    # 4) Для строк mask сразу применяем clip по заранее отсечённым Series
    #    .loc[mask] у max_price и min_allowed даст Series с точно такими же индексами,
    #    как df.loc[mask, "new_price"]
    df.loc[mask, "new_price"] = df.loc[mask, "new_price"].clip(
        lower=min_allowed.loc[mask],
        upper=max_price
    )

    return df



