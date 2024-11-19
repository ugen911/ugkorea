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

            # Отладочная печать только для кода ЦБ007278
            if kod == "ЦБ007278":
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

            # Добавляем строку в итоговый датафрейм, если есть изменения
            if price_diff != 0 or quantity_diff != 0:
                # Убедимся, что изменения фиксируются только один раз
                row = correction_row.copy()
                row["izmenenie_ceny"] = price_diff
                row["izmenenie_kolichestva"] = quantity_diff
                result_df = pd.concat(
                    [result_df, pd.DataFrame([row])], ignore_index=True
                )

    # Отладочная выборка из результата только для кода ЦБ007278 с нужными колонками
    debug_selection = result_df[result_df["kod"] == "ЦБ007278"][
        ["ssylka", "dokumentosnovanie", "izmenenie_ceny", "izmenenie_kolichestva"]
    ]
    print("\nОтладочная выборка из результирующего датафрейма для кода ЦБ007278:")
    print(debug_selection)

    # Удаляем строки, где `izmenenie_kolichestva` пустое
    result_df = result_df.dropna(subset=["izmenenie_kolichestva"])

    # Удаляем ненужные колонки
    columns_to_remove = [
        "tsena",
        "kolichestvo",
        "hozoperatsija",
        "kontragent",
        "naimenovanie",
        "proizvoditel",
        "vidnomenklatury",
        "type_detail",
        "gruppa_analogov",
    ]
    result_df = result_df.drop(
        columns=[col for col in columns_to_remove if col in result_df.columns],
        errors="ignore",
    )

    return result_df
