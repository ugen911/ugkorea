import pandas as pd
import numpy as np


def process_corrections_and_supplies(corrections_df, supplies_df):
    # Создаем пустой датафрейм для результата
    result_df = pd.DataFrame()

    # Получаем уникальные значения из колонки `dokumentosnovanie` в корректировках
    unique_docs = corrections_df["dokumentosnovanie"].unique()

    for doc in unique_docs:
        # Проверяем, начинается ли значение с "Поступление товаров"
        if doc.startswith("Поступление товаров"):
            # Выборка из поступлений по колонке ssylka
            supplies_subset = supplies_df[supplies_df["ssylka"] == doc]
        else:
            # Выборка из корректировок по колонке ssylka
            supplies_subset = corrections_df[corrections_df["ssylka"] == doc]

        # Выборка из корректировок по dokumentosnovanie
        corrections_subset = corrections_df[corrections_df["dokumentosnovanie"] == doc]

        # Сравнение kod между двумя выборками
        supply_kods = set(supplies_subset["kod"])
        correction_kods = set(corrections_subset["kod"])

        # Kods в поступлении, но отсутствуют в корректировках
        returned_kods = supply_kods - correction_kods
        for kod in returned_kods:
            row = supplies_subset[supplies_subset["kod"] == kod].copy()
            row["izmenenie_ceny"] = -row["tsena"]
            row["izmenenie_kolichestva"] = -row["kolichestvo"]
            result_df = pd.concat([result_df, row], ignore_index=True)

        # Kods в корректировках, но отсутствуют в поступлениях
        added_kods = correction_kods - supply_kods
        for kod in added_kods:
            row = corrections_subset[corrections_subset["kod"] == kod].copy()
            row["izmenenie_ceny"] = row["tsena"]
            row["izmenenie_kolichestva"] = row["kolichestvo"]
            result_df = pd.concat([result_df, row], ignore_index=True)

        # Общие Kods
        common_kods = supply_kods & correction_kods
        for kod in common_kods:
            supply_row = supplies_subset[supplies_subset["kod"] == kod].iloc[0]
            correction_row = corrections_subset[corrections_subset["kod"] == kod].iloc[
                0
            ]

            # Заменяем NaN в корректировке на 0
            supply_quantity = supply_row["kolichestvo"]
            supply_price = supply_row["tsena"]
            correction_quantity = (
                correction_row["kolichestvo"]
                if pd.notna(correction_row["kolichestvo"])
                else 0
            )
            correction_price = (
                correction_row["tsena"] if pd.notna(correction_row["tsena"]) else 0
            )

            # Проверка изменений в цене и количестве
            price_diff = correction_price - supply_price
            quantity_diff = correction_quantity - supply_quantity

            # Отладочная печать для конкретных строк
            if kod in ["ЦБ023078", "ЦБ007278"]:
                print(f"Отладка для kода {kod}:")
                print(f"Документ поступления: {supply_row['ssylka']}")
                print(f"Документ корректировки: {correction_row['ssylka']}")
                print(
                    f"Поступления — Цена: {supply_price}, Количество: {supply_quantity}"
                )
                print(
                    f"Корректировка — Цена: {correction_price}, Количество: {correction_quantity}"
                )
                print(f"Разница — Цена: {price_diff}, Количество: {quantity_diff}")

            # Если есть изменения в цене или количестве, добавляем строку
            if price_diff != 0 or quantity_diff != 0:
                row = correction_row.copy()
                row["izmenenie_ceny"] = (
                    float(price_diff) if price_diff != 0 else 0
                )  # Явное приведение
                row["izmenenie_kolichestva"] = (
                    float(quantity_diff) if quantity_diff != 0 else 0
                )  # Явное приведение
                result_df = pd.concat(
                    [result_df, pd.DataFrame([row])], ignore_index=True
                )

    return result_df
