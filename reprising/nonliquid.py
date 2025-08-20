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
    # Обработка datasozdanija с подстановкой по умолчанию 01.01.2022
    datasozd = filtered_df.get("datasozdanija", pd.Series(index=filtered_df.index))

    # Заменяем пустые строки и NaN на 01.01.2022
    datasozd = datasozd.astype(str).str.strip()
    datasozd = datasozd.replace({"": "01.01.2022", "NaT": "01.01.2022", "nan": "01.01.2022"})

    # Преобразуем в даты по формату DD.MM.YYYY
    filtered_df["datasozdanija"] = pd.to_datetime(
        datasozd,
        format="%d.%m.%Y",
        errors="coerce"
    )
 
    # Подстраховка — если остались NaT, ставим 01.01.2022
    filtered_df["datasozdanija"] = filtered_df["datasozdanija"].fillna(pd.Timestamp("2022-01-01"))

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
        orig    = row["new_price"]
        dt      = row["last_purchase"]
        kod     = row["kod"]
        mp      = row["median_price"]
        dp      = row["delprice"]
        tz      = row["tsenazakup"]

        # 5.1) Вычисляем base_price
        if pd.notna(mp) and pd.notna(dp):
            base_price = max(mp, dp)
        elif pd.notna(mp):
            base_price = mp
        else:
            base_price = dp

        # 5.2) Определяем период и low_pct/high_pct
        if pd.isna(dt) or dt <= last_36:
            period    = "36+"
            low_pct, high_pct = 0.4, 0.5
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
            # моложе 12 мес — пропускаем
            continue

        # 5.3) Расчёт диапазона
        low  = base_price * low_pct
        high = base_price * high_pct

        # 5.4) Универсальное нижнее ограничение по delprice
        if dp <= 300:
            min_floor = dp * 1.4
            low, high = max(low, min_floor), max(high, min_floor)
        elif dp <= 1000:
            min_floor = dp * 1.2
            low, high = max(low, min_floor), max(high, min_floor)

        # 5.5) Boost и cap
        if boost:
            high *= boost_factors[period]
            high  = min(high, base_price * 1.5)

        # 5.6) Первый clamp и округление
        new_price = clamp(orig, low, high)

        # 5.7) Ограничение по median_price
        if pd.notna(mp):
            min_m = np.ceil((mp * avg_lowf[period]) / 10) * 10
            max_m = np.ceil((mp * avg_upf[period]) / 10) * 10
            new_price = min(max(new_price, min_m), max_m)

        # 5.7.1) Дополнительный cap для периода "36+" по tsenazakup
        if period == "36+" and pd.notna(tz):
            if tz >= 1000:
                cap300 = np.ceil((tz * 3.0) / 10) * 10
                new_price = min(new_price, cap300)
            if tz > 8000:
                cap220 = np.ceil((tz * 2.2) / 10) * 10
                new_price = min(new_price, cap220)

        # 5.8) Внешние пороги по maxprice и middleprice
        if pd.notna(row["maxprice"]):
            floor_max = np.ceil((row["maxprice"] * 1.3) / 10) * 10
            new_price = max(new_price, floor_max)
        if pd.notna(row["middleprice"]):
            floor_mid = np.ceil((row["middleprice"] * 1.5) / 10) * 10
            new_price = max(new_price, floor_mid)

        # Записываем результат
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



def adjust_new_price_by_peer_median(filtered_df):
    """
    Для товаров без delprice, without delsklad и median_price и с периодом «36+»
    Ограничивает их new_price по медиане new_price похожих товаров (type_detail + proizvoditel),
    если таких товаров больше трёх, с коэффициентом ×1.5 и округлением до десятков,
    затем применяет внешние пороги по maxprice и middleprice.
    """
    import numpy as np
    import pandas as pd

    # 1) Определяем дату «36+»
    now     = pd.Timestamp.now()
    last_36 = now - pd.DateOffset(months=36)

    # 2) Готовим колонку last_purchase точно так же, как в оригинальной функции
    filtered_df["data"] = pd.to_datetime(
        filtered_df["data"], format="%d.%m.%Y %H:%M", errors="coerce"
    )
    filtered_df["datasozdanija"] = pd.to_datetime(
        filtered_df.get("datasozdanija", None), errors="coerce"
    )
    filtered_df["last_purchase"] = filtered_df["data"].fillna(
        filtered_df["datasozdanija"]
    )

    # 3) Формируем маску: период «36+» и отсутствуют delprice, delsklad и median_price
    mask_period = filtered_df["last_purchase"].isna() | (filtered_df["last_purchase"] <= last_36)
    mask_missing = (
        filtered_df["delprice"].isna()
        & (filtered_df["delsklad"].isna() | (filtered_df["delsklad"] == ""))
        & filtered_df["median_price"].isna()
    )
    mask = mask_period & mask_missing

    # 4) Проходим по каждому такому товару
    for idx, row in filtered_df.loc[mask].iterrows():
        тип       = row["type_detail"]
        произв    = row["proizvoditel"]
        orig      = row["new_price"]

        # 4.1) Находим «похожие» товары с уже рассчитанным new_price
        peers = filtered_df[
            (filtered_df["new_price"].notna())
            & (filtered_df["type_detail"] == тип)
            & (filtered_df["proizvoditel"] == произв)
        ]["new_price"]

        # 4.2) Если таких больше трёх — берём медиану и ограничиваем
        if len(peers) > 3:
            median_peer = peers.median()
            cap = np.ceil((median_peer * 1.3) / 10) * 10
            new_price = min(orig, cap)

            # 4.3) Внешние пороги по maxprice и middleprice (как в оригинале)
            if pd.notna(row["maxprice"]):
                floor_max = np.ceil((row["maxprice"] * 1.3) / 10) * 10
                new_price = max(new_price, floor_max)
            if pd.notna(row["middleprice"]):
                floor_mid = np.ceil((row["middleprice"] * 1.5) / 10) * 10
                new_price = max(new_price, floor_mid)

            filtered_df.at[idx, "new_price"] = new_price

    return filtered_df
