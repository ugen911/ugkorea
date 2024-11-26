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

            # Добавляем строку в итоговый датафрейм, если есть изменения
            if price_diff != 0 or quantity_diff != 0:
                row = correction_row.copy()
                row["izmenenie_ceny"] = price_diff
                row["izmenenie_kolichestva"] = quantity_diff
                result_df = pd.concat(
                    [result_df, pd.DataFrame([row])], ignore_index=True
                )

    # Обработка дополнительных условий
    processed_rows = set()  # Отслеживание уже обработанных строк
    for index, row in result_df.iterrows():
        # Условие 4.1: Цена и количество уменьшаются до 0
        if (
            row["tsena"] > 0
            and row["kolichestvo"] > 0
            and row["izmenenie_ceny"] == -row["tsena"]
            and row["izmenenie_kolichestva"] == -row["kolichestvo"]
        ):
            result_df.at[index, "kolichestvo"] = row["izmenenie_kolichestva"]
            processed_rows.add(index)  # Отмечаем строку как обработанную

        # Условие 4.2: Цена и количество остаются без изменений
        elif (
            row["tsena"] > 0
            and row["kolichestvo"] > 0
            and row["izmenenie_ceny"] == row["tsena"]
            and row["izmenenie_kolichestva"] == row["kolichestvo"]
        ):
            processed_rows.add(index)  # Отмечаем строку как обработанную

    # Применяем остальные условия только к необработанным строкам
    for index, row in result_df.iterrows():
        if index in processed_rows:
            continue  # Пропускаем обработанные строки

        # Условие 1
        if (
            pd.isna(row["kolichestvo"])
            and row["izmenenie_ceny"] == 0
            and row["izmenenie_kolichestva"] < 0
        ):
            result_df.at[index, "kolichestvo"] = row["izmenenie_kolichestva"]

        # Условие 2
        if (
            row["kolichestvo"] > 0
            and (row["izmenenie_kolichestva"] > 0 or row["izmenenie_kolichestva"] < 0)
            and row["izmenenie_ceny"] == 0
        ):
            result_df.at[index, "kolichestvo"] = row["izmenenie_kolichestva"]

        # Условие 3
        if row["izmenenie_ceny"] != 0 and row["izmenenie_kolichestva"] == 0:
            result_df.at[index, "tsena"] = row["izmenenie_ceny"]

        # Условие 5
        if (
            row["kolichestvo"] > 0
            and row["izmenenie_kolichestva"] < 0
            and row["izmenenie_ceny"] != 0
        ):
            result_df.at[index, "kolichestvo"] = row["izmenenie_kolichestva"]
            result_df.at[index, "tsena"] = (
                row["tsena"] + row["izmenenie_ceny"]
                if row["izmenenie_ceny"] >= 0
                else row["tsena"] - abs(row["izmenenie_ceny"])
            )

        # Условие 6
        if (
            row["kolichestvo"] > 0
            and row["izmenenie_kolichestva"] > 0
            and row["izmenenie_ceny"] != 0
        ):
            result_df.at[index, "kolichestvo"] = row["izmenenie_kolichestva"]
            result_df.at[index, "tsena"] = (
                row["tsena"] + row["izmenenie_ceny"]
                if row["izmenenie_ceny"] >= 0
                else row["tsena"] - abs(row["izmenenie_ceny"])
            )

    # Удаляем строки, где `izmenenie_kolichestva` пустое
    result_df = result_df.dropna(subset=["izmenenie_kolichestva"])

    # Удаляем ненужные колонки
    columns_to_remove = [
        "hozoperatsija",
        "kontragent",
        "naimenovanie",
        "proizvoditel",
        "vidnomenklatury",
        "type_detail",
        "gruppa_analogov",
        "izmenenie_ceny",
        "izmenenie_kolichestva"
    ]
    result_df = result_df.drop(
        columns=[col for col in columns_to_remove if col in result_df.columns],
        errors="ignore",
    )

    # Перенос данных из result_df в supplies_df
    updated_supplies = pd.concat([supplies_df, result_df], ignore_index=True)
    updated_supplies = updated_supplies.sort_values(by="data").reset_index(drop=True)
    updated_supplies = updated_supplies.drop_duplicates().reset_index(drop=True)

    # Обновляем kontragent после объединения
    for index, row in updated_supplies.iterrows():
        if pd.notna(row["dokumentosnovanie"]) and pd.isna(row["kontragent"]):
            # Поиск строки, где dokumentosnovanie совпадает с ssylka
            matching_row = updated_supplies[
                updated_supplies["ssylka"] == row["dokumentosnovanie"]
            ]
            if not matching_row.empty:
                # Заполняем kontragent значением из найденной строки
                updated_supplies.at[index, "kontragent"] = matching_row.iloc[0][
                    "kontragent"
                ]

    # Заполняем пустые поля на основе kod
    for index, row in updated_supplies.iterrows():
        if (
            pd.isna(row["naimenovanie"])
            or pd.isna(row["proizvoditel"])
            or pd.isna(row["vidnomenklatury"])
            or pd.isna(row["type_detail"])
            or pd.isna(row["gruppa_analogov"])
        ):
            matching_row = updated_supplies[
                (updated_supplies["kod"] == row["kod"])
                & pd.notna(updated_supplies["naimenovanie"])
                & pd.notna(updated_supplies["proizvoditel"])
                & pd.notna(updated_supplies["vidnomenklatury"])
                & pd.notna(updated_supplies["type_detail"])
                & pd.notna(updated_supplies["gruppa_analogov"])
            ]
            if not matching_row.empty:
                first_match = matching_row.iloc[0]
                updated_supplies.at[index, "naimenovanie"] = first_match["naimenovanie"]
                updated_supplies.at[index, "proizvoditel"] = first_match["proizvoditel"]
                updated_supplies.at[index, "vidnomenklatury"] = first_match[
                    "vidnomenklatury"
                ]
                updated_supplies.at[index, "type_detail"] = first_match["type_detail"]
                updated_supplies.at[index, "gruppa_analogov"] = first_match[
                    "gruppa_analogov"
                ]

    # Заполняем пустые hozoperatsija
    updated_supplies["hozoperatsija"] = updated_supplies["hozoperatsija"].fillna(
        "Корректировка поступления"
    )

    return updated_supplies
