import pandas as pd
import numpy as np

import numpy as np
import pandas as pd


def indeksation(filtered_df, priceendmonth):
    """
    Функция корректирует цены в filtered_df на основе данных priceendmonth и условий.

    Parameters:
    filtered_df (pd.DataFrame): Основной датафрейм с информацией о позициях.
    priceendmonth (pd.DataFrame): Датафрейм с историческими данными о ценах на конец месяца.

    Returns:
    pd.DataFrame: Обновленный датафрейм filtered_df со всеми строками и корректировками цен.
    """
    # Преобразуем колонку year_month в datetime для точного сравнения
    priceendmonth["year_month"] = pd.to_datetime(
        priceendmonth["year_month"], format="%Y-%m", errors="coerce"
    )

    # Получаем текущую дату и месяц прошлого года
    current_date = pd.Timestamp.now()
    last_year_same_month = current_date - pd.DateOffset(years=1)

    # Фильтруем строки в filtered_df, где new_price пустой или равен tsenarozn, и проверяем, что delsklad и median_price также пусты
    condition = (
        (filtered_df["new_price"].isna())
        | (filtered_df["new_price"] == filtered_df["tsenarozn"])
    ) & (filtered_df["delsklad"].isna() & filtered_df["median_price"].isna())
    filtered_positions = filtered_df[condition]

    # Соединяем filtered_positions с priceendmonth по 'kod' и извлекаем только нужный год и месяц
    merged = pd.merge(
        filtered_positions, priceendmonth, on="kod", suffixes=("", "_priceendmonth")
    )

    # Отбираем строки, где 'year_month' в priceendmonth соответствует месяцу прошлого года
    merged = merged[merged["year_month"].dt.year == last_year_same_month.year]
    merged = merged[merged["year_month"].dt.month == last_year_same_month.month]

    # Отладочная печать: количество kod с совпадениями по месяцу прошлого года
    matching_kod_count = merged["kod"].nunique()
    print(
        f"Количество kod, найденных с совпадением по месяцу прошлого года: {matching_kod_count}"
    )

    # Добавляем колонку 'price_last_year', если ее еще нет
    if "price_last_year" not in filtered_df.columns:
        filtered_df["price_last_year"] = np.nan

    # Добавляем колонку для каскадных наценок
    if "is_indexed" not in filtered_df.columns:
        filtered_df["is_indexed"] = False

    # Обновляем 'price_last_year' в исходном DataFrame для позиций, которые нашли соответствие в прошлом году
    for kod, price_last_year in zip(merged["kod"], merged["tsena"]):
        filtered_df.loc[filtered_df["kod"] == kod, "price_last_year"] = price_last_year

    # Функция для расчета новой цены на основе условия
    def calculate_new_price(row):
        tsenarozn = row["tsenarozn"]
        price_last_year = row["price_last_year"]

        # Условие на совпадение с ценой прошлого года и отсутствие предыдущей индексации
        if price_last_year == tsenarozn and not row["is_indexed"]:
            if tsenarozn <= 500:
                return np.ceil((tsenarozn * 1.12) / 10) * 10
            elif tsenarozn <= 1000:
                return np.ceil((tsenarozn * 1.10) / 10) * 10
            elif tsenarozn <= 5000:
                return np.ceil((tsenarozn * 1.07) / 10) * 10
            elif tsenarozn <= 10000:
                return np.ceil((tsenarozn * 1.05) / 10) * 10
            else:
                return np.ceil((tsenarozn * 1.03) / 10) * 10
        return tsenarozn

    # Применяем функцию расчета новой цены и отмечаем, какие строки были проиндексированы
    indexed_count = 0
    for index, row in filtered_df.iterrows():
        if (
            not pd.isna(row["price_last_year"])
            and row["price_last_year"] == row["tsenarozn"]
        ):
            # Проверяем, что цена была изменена в течение года
            recent_price_changes = priceendmonth[
                (priceendmonth["kod"] == row["kod"])
                & (priceendmonth["year_month"].dt.year == current_date.year)
            ]

            if recent_price_changes.empty:
                new_price = calculate_new_price(row)
                if new_price != row["new_price"]:
                    filtered_df.at[index, "new_price"] = new_price
                    filtered_df.at[index, "is_indexed"] = (
                        True  # Отмечаем индексированные строки
                    )
                    indexed_count += 1

    # Отладочная печать: количество строк, проиндексированных по цене
    print(f"Количество строк, проиндексированных по цене: {indexed_count}")

    # Заполняем оставшиеся пустые значения "new_price" значениями из "tsenarozn"
    filtered_df["new_price"] = filtered_df["new_price"].fillna(filtered_df["tsenarozn"])

    # Отладочная печать: проверка количества строк с пустыми значениями после заполнения
    final_empty_count = filtered_df["new_price"].isna().sum()
    print(
        f"Количество строк с пустыми значениями new_price после заполнения: {final_empty_count}"
    )

    # Убираем колонку "is_indexed" перед возвратом, если она не нужна
    filtered_df.drop(columns=["is_indexed"], inplace=True)

    return filtered_df
