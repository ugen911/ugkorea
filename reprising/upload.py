import numpy as np
import pandas as pd
import os


def regtament_view(filtered_df):
    """
    Функция для обработки filtered_df, оставляет колонки в заданной последовательности,
    вычисляет наценку и разницу цен, добавляет их в отдельные колонки, сортирует данные,
    фильтрует строки, где price_diff не равен 0, и добавляет итоговую сумму в последнюю строку.

    Parameters:
    filtered_df (pd.DataFrame): Основной датафрейм с информацией о позициях.

    Returns:
    pd.DataFrame: Обработанный датафрейм с дополнительными колонками.
    """
    # Задаем последовательность колонок
    columns_order = [
        "kod",
        "artikul",
        "proizvoditel",
        "gruppa_analogov",
        "naimenovanie",
        "edizm",
        "delprice",
        "delsklad",
        "median_price",
        "middleprice",
        "maxprice",
        "tsenazakup",
        "konkurents",
        "ostatok",
        "abc",
        "xyz",
        "tsenarozn",
        "new_price",
    ]

    # Оставляем только нужные колонки и в указанном порядке
    filtered_df = filtered_df[columns_order].copy()

    # Вычисляем наценку (new_price к delprice), если delprice не пустой
    filtered_df.loc[:, "markup_to_delprice"] = filtered_df.apply(
        lambda row: (
            (row["new_price"] - row["delprice"]) / row["delprice"] * 100
            if pd.notna(row["delprice"]) and row["delprice"] > 0
            else np.nan
        ),
        axis=1,
    )

    # Вычисляем фактическую наценку (new_price к tsenazakup), если tsenazakup не пустой
    filtered_df.loc[:, "fact_markup"] = filtered_df.apply(
        lambda row: (
            (row["new_price"] - row["tsenazakup"]) / row["tsenazakup"] * 100
            if pd.notna(row["tsenazakup"]) and row["tsenazakup"] > 0
            else np.nan
        ),
        axis=1,
    )

    # Вычисляем разницу new_price - tsenarozn
    filtered_df.loc[:, "price_diff"] = (
        filtered_df["new_price"] - filtered_df["tsenarozn"]
    )

    # Вычисляем процентное отклонение (new_price - tsenarozn) / tsenarozn * 100
    filtered_df.loc[:, "percent_diff"] = filtered_df.apply(
        lambda row: (
            (row["new_price"] - row["tsenarozn"]) / row["tsenarozn"] * 100
            if pd.notna(row["tsenarozn"]) and row["tsenarozn"] > 0
            else np.nan
        ),
        axis=1,
    )

    # Вычисляем разницу (new_price - tsenarozn) * ostatok
    filtered_df.loc[:, "price_diff_mult_ostatok"] = (
        filtered_df["price_diff"] * filtered_df["ostatok"]
    )

    # Сортируем по naimenovanie, затем по gruppa_analogov, и затем по proizvoditel
    filtered_df = filtered_df.sort_values(
        by=["naimenovanie", "gruppa_analogov", "proizvoditel"]
    )

    # Фильтруем только те строки, где price_diff не равен 0
    filtered_df = filtered_df[filtered_df["price_diff"] != 0]

    # Вычисляем сумму по колонке price_diff_mult_ostatok
    total_sum = filtered_df["price_diff_mult_ostatok"].sum()
    print(f"Total sum of price_diff_mult_ostatok: {total_sum}")

    # Добавляем строку с итоговой суммой в конец датафрейма
    total_sum_row = {col: "" for col in filtered_df.columns}
    total_sum_row["kod"] = "Итоговая сумма"
    total_sum_row["price_diff_mult_ostatok"] = total_sum

    # Присоединяем итоговую строку к датафрейму
    filtered_df = pd.concat(
        [filtered_df, pd.DataFrame([total_sum_row])], ignore_index=True
    )

    # Переименовываем колонки на русский язык
    filtered_df = filtered_df.rename(
        columns={
            "kod": "Код",
            "artikul": "Артикул",
            "proizvoditel": "Производитель",
            "gruppa_analogov": "Группа аналогов",
            "naimenovanie": "Наименование",
            "edizm": "Ед. изм.",
            "delprice": "Цена поставки",
            "delsklad": "Склад поставки",
            "median_price": "Медианная цена",
            "middleprice": "Средняя цена",
            "maxprice": "Максимальная цена",
            "tsenazakup": "Цена закупки",
            "konkurents": "Конкуренты",
            "ostatok": "Остаток",
            "abc": "ABC",
            "xyz": "XYZ",
            "tsenarozn": "Розничная цена",
            "new_price": "Новая цена",
            "markup_to_delprice": "Наценка к цене поставки",
            "fact_markup": "Фактическая наценка",
            "price_diff": "Разница в цене",
            "percent_diff": "Процент отклонения",
            "price_diff_mult_ostatok": "Разница цены * Остаток",
        }
    )

    return filtered_df



def export_data(df):
    # Путь к основному и резервному местам для выгрузки
    primary_path = r"\\26.218.196.12\заказы\Евгений\Переоценка"
    secondary_path = r"D:\NAS\заказы\Евгений\Переоценка"

    # Проверка доступности путей
    if os.path.exists(primary_path):
        export_path = primary_path
    elif os.path.exists(secondary_path):
        export_path = secondary_path
    else:
        print("Ни один из путей не доступен.")
        return

    # Выгрузка полного датафрейма
    full_export_path = os.path.join(export_path, "ПолныеДанные.xlsx")
    df.to_excel(full_export_path, index=False, freeze_panes=(1, 0))
    print(f"Полные данные выгружены в {full_export_path}")

    # Создание датафрейма с колонками Код и Новая цена, исключая последнюю строку
    reduced_df = df[["Код", "Новая цена"]].iloc[:-1]

    # Выгрузка файла с колонками Код и Новая цена без названий колонок и последней строки
    new_price_export_path = os.path.join(export_path, "НоваяЦена1С.xlsx")
    reduced_df.to_excel(new_price_export_path, index=False, header=False)
    print(f"Данные с новой ценой выгружены в {new_price_export_path}")


# Пример использования
# df = pd.DataFrame(...)  # Загрузите или создайте свой датафрейм
# export_data(df)
