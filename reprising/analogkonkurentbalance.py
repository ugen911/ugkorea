import numpy as np
import pandas as pd
from ugkorea.db.database import get_db_engine
from sqlalchemy.exc import ProgrammingError
from datetime import datetime, timedelta
from ugkorea.reprising.getkonkurent import autocoreec_data


engine = get_db_engine


def service_percentup(df):
    """
    Применяет корректировку цены в зависимости от xyz, median_service_percent и tsenarozn.
    """

    def adjust_row(row):
        xyz = row["xyz"]
        median_service_percent = row.get("median_service_percent", np.nan)
        tsenarozn = row.get("tsenarozn", np.nan)
        new_price = row["new_price"]

        # Проверка условий для корректировки
        if (
            xyz in ["X", "X1"]
            and median_service_percent > 80
            and not pd.isna(tsenarozn)
        ):
            if tsenarozn < 500:
                new_price = np.ceil((new_price * 1.15) / 10) * 10
            elif 500 <= tsenarozn < 1000:
                new_price = np.ceil((new_price * 1.10) / 10) * 10
            elif 1000 <= tsenarozn < 2000:
                new_price = np.ceil((new_price * 1.05) / 10) * 10

        return new_price

    df["new_price"] = df.apply(adjust_row, axis=1)
    return df


def konkurents_correct(filtered_df, konkurents):
    konkurents = konkurents.rename(columns={"price": "konkurents"})
    df = pd.merge(filtered_df, konkurents[["kod", "konkurents"]], on="kod", how="left")
    """
    Корректирует цену new_price на основе данных о конкурентах и других метрик.
    Если цена конкурента ниже, корректируем new_price, чтобы он был на 10% дешевле конкурента,
    но при этом не выходил за пределы, установленные на основе middleprice и maxprice.

    Parameters:
    df (pd.DataFrame): Датафрейм с данными о позициях.

    Returns:
    pd.DataFrame: Обновленный датафрейм с корректировками new_price.
    """

    # Преобразуем колонки в числовой тип, ошибки приведут к NaN
    df["konkurents"] = pd.to_numeric(df["konkurents"], errors="coerce")
    df["new_price"] = pd.to_numeric(df["new_price"], errors="coerce")
    df["middleprice"] = pd.to_numeric(df.get("middleprice", np.nan), errors="coerce")
    df["maxprice"] = pd.to_numeric(df.get("maxprice", np.nan), errors="coerce")

    def adjust_row(row):
        konkurents_price = row["konkurents"]
        new_price = row["new_price"]

        # Пропускаем строки, где нет данных о цене конкурента
        if pd.isna(konkurents_price):
            return new_price

        middleprice = row.get("middleprice", np.nan)
        maxprice = row.get("maxprice", np.nan)

        # Проверка: если konkurents ниже new_price, корректируем new_price
        if not pd.isna(new_price) and konkurents_price < new_price:
            # Целевая цена - на 10% ниже цены конкурента
            target_price = konkurents_price * 0.90

            # Проверяем, есть ли middleprice и maxprice
            if not pd.isna(middleprice) and not pd.isna(maxprice):
                # Берем максимальное значение из возможных вариантов:
                # - middleprice * 1.40 (средняя цена с наценкой 40%)
                # - maxprice * 1.10 (максимальная цена с наценкой 10%)
                # - target_price (цена, на 10% ниже цены конкурента)
                adjusted_price = max(middleprice * 1.40, maxprice * 1.10, target_price)
            else:
                # Если нет ограничений (middleprice или maxprice отсутствуют), берем target_price
                adjusted_price = target_price

            # Округление до 10 рублей вверх
            new_price = np.ceil(adjusted_price / 10) * 10

        return new_price

    # Применяем корректировку к каждой строке датафрейма
    df["new_price"] = df.apply(adjust_row, axis=1)
    return df


def sync_prices(filtered_df):
    """
    Синхронизирует цены внутри каждой группы по gruppa_analogov, производителю и edizm.
    Все позиции в группе получают одинаковую цену. Если наибольшая цена выше, она снижается
    до минимальной цены в группе или до maxprice + 10%, если это применимо. Если этого недостаточно,
    остальные цены поднимаются до этого значения.

    Parameters:
    filtered_df (pd.DataFrame): Основной датафрейм с информацией о позициях.

    Returns:
    pd.DataFrame: Обновленный датафрейм с синхронизированными ценами внутри групп.
    """
    # Создаем уникальный идентификатор группы на основе gruppa_analogov, proizvoditel и edizm
    filtered_df["group_id"] = (
        filtered_df["gruppa_analogov"].astype(str)
        + "_"
        + filtered_df["proizvoditel"].astype(str)
        + "_"
        + filtered_df["edizm"].astype(str)
    )

    def sync_group(group_df):
        # Определяем максимальную и минимальную цену в группе
        max_new_price = group_df["new_price"].max()
        min_new_price = group_df["new_price"].min()
        maxprice = group_df["maxprice"].max()

        # Рассчитываем пороговую цену: maxprice + 10%, если maxprice доступен
        limit_price = maxprice * 1.10 if not pd.isna(maxprice) else max_new_price

        # Если max_new_price больше min_new_price, пробуем уменьшить max_new_price
        if max_new_price > min_new_price:
            # Уменьшаем max_new_price до min_new_price, но не ниже limit_price
            adjusted_price = max(min_new_price, limit_price)
        else:
            # Если max_new_price уже не больше, оставляем его как есть
            adjusted_price = max_new_price

        # Если после корректировки adjusted_price все еще больше min_new_price,
        # поднимаем все остальные цены до adjusted_price
        group_df = group_df.copy()
        group_df["new_price"] = np.ceil(adjusted_price / 10) * 10

        return group_df

    # Применяем синхронизацию цен для каждой уникальной группы
    filtered_df = (
        filtered_df.groupby("group_id").apply(sync_group).reset_index(drop=True)
    )

    return filtered_df

#############До сюда сделано


def update_rasprodat(filtered_df, engine):
    """
    Обновляет таблицу rasprodat в базе данных на основе данных из filtered_df.
    Проверяет наличие продающихся и непродающихся позиций в каждой группе gruppa_analogov.
    Если таблица rasprodat уже существует, дополняет ее новыми kod. Если таблицы нет, создает новую.

    Parameters:
    filtered_df (pd.DataFrame): Основной датафрейм с информацией о позициях.
    engine (SQLAlchemy Engine): Подключение к базе данных.

    Returns:
    pd.DataFrame: Обновленный датафрейм filtered_df со всеми корректировками цен.
    """
    # Проверяем, существует ли таблица rasprodat в базе данных
    try:
        rasprodat_df = pd.read_sql("SELECT * FROM rasprodat", engine)
        table_exists = True
    except ProgrammingError:
        rasprodat_df = pd.DataFrame(columns=["kod"])
        table_exists = False

    # Определяем текущую дату и дату 18 месяцев назад
    current_date = datetime.now()
    threshold_date = current_date - timedelta(days=18 * 30)  # Приблизительно 18 месяцев

    # Группируем по gruppa_analogov и проверяем наличие продающихся и непродающихся позиций
    groups_to_remove = []
    for gruppa, group_df in filtered_df.groupby("gruppa_analogov"):
        # Определяем продающиеся и непродающиеся позиции
        selling_positions = group_df[
            group_df["abc"].isin(["A1", "A", "B"]) & group_df["xyz"].isin(["X1", "X"])
        ]
        non_selling_positions = group_df[
            (group_df["abc"].isin(["C", None]))
            & (group_df["xyz"].isin(["Y", "Z", None]))
            & (
                group_df["datasozdanija"].isna()
                | (pd.to_datetime(group_df["datasozdanija"]) <= threshold_date)
            )
        ]

        # Проверяем, если есть и продающиеся, и непродающиеся позиции
        if not selling_positions.empty and not non_selling_positions.empty:
            # Проходим по non_selling_positions и проверяем, есть ли позиции с таким же proizvoditel, но не в non_selling_positions
            to_remove = []
            for idx, row in non_selling_positions.iterrows():
                # Находим другие позиции с таким же производителем, но не в non_selling_positions
                same_proizvoditel = group_df[
                    (group_df["proizvoditel"] == row["proizvoditel"])
                    & (~group_df["kod"].isin(non_selling_positions["kod"]))
                ]

                # Если такие позиции есть, добавляем kod в список на удаление
                if not same_proizvoditel.empty:
                    to_remove.append(row["kod"])

            # Убираем из non_selling_positions те kod, которые должны быть удалены
            non_selling_positions = non_selling_positions[
                ~non_selling_positions["kod"].isin(to_remove)
            ]

            # Добавляем оставшиеся kod в список для удаления
            groups_to_remove.extend(non_selling_positions["kod"].tolist())

    # Создаем датафрейм с новыми kod, которые нужно добавить в rasprodat
    new_kod_df = pd.DataFrame(groups_to_remove, columns=["kod"])

    # Проверяем, какие kod уже существуют в rasprodat и какие нужно добавить
    if table_exists:
        new_kod_df = new_kod_df[~new_kod_df["kod"].isin(rasprodat_df["kod"])]

    # Объединяем старые и новые данные и сохраняем в базу данных
    updated_rasprodat_df = (
        pd.concat([rasprodat_df, new_kod_df]).drop_duplicates().reset_index(drop=True)
    )
    updated_rasprodat_df.to_sql("rasprodat", engine, if_exists="replace", index=False)

    # Теперь корректируем цены для всех позиций из rasprodat
    # Выбираем позиции из filtered_df, которые есть в обновленной таблице rasprodat
    positions_to_update = filtered_df[
        filtered_df["kod"].isin(updated_rasprodat_df["kod"])
    ]

    for idx, row in positions_to_update.iterrows():
        middleprice = row["middleprice"]
        maxprice = row["maxprice"]

        if not pd.isna(middleprice) and not pd.isna(maxprice):
            # Вычисляем минимальную цену на основе middleprice и maxprice
            min_price = max(
                np.ceil(middleprice * 1.3 / 10) * 10, np.ceil(maxprice * 1.1 / 10) * 10
            )
        else:
            # Если middleprice или maxprice отсутствуют, берем минимальную цену из группы gruppa_analogov
            group = filtered_df[
                filtered_df["gruppa_analogov"] == row["gruppa_analogov"]
            ]
            min_group_price = group["new_price"].min()

            # Делаем текущую позицию на 10% дешевле самой дешевой в группе
            min_price = (
                np.ceil(min_group_price * 0.9 / 10) * 10 if min_group_price > 0 else 10
            )

        # Обновляем new_price в исходном датафрейме
        filtered_df.loc[filtered_df["kod"] == row["kod"], "new_price"] = min_price

    print("Таблица rasprodat успешно обновлена, и цены скорректированы.")

    # Возвращаем обновленный датафрейм
    return filtered_df


def adjust_original_prices(filtered_df):
    """
    Корректирует цены в группах gruppa_analogov, чтобы оригинальные позиции всегда были дороже,
    чем неоригинальные, следуя заданным правилам.

    Parameters:
    filtered_df (pd.DataFrame): Основной датафрейм с информацией о позициях.

    Returns:
    pd.DataFrame: Обновленный датафрейм filtered_df со всеми корректировками цен.
    """
    # Группируем по gruppa_analogov
    for (gruppa, edizm), group_df in filtered_df.groupby(["gruppa_analogov", "edizm"]):

        # Отбираем оригинальные и неоригинальные позиции
        original_positions = group_df[group_df["is_original"] == True]
        non_original_positions = group_df[group_df["is_original"] == False]

        # Проверяем, есть ли оригинальные и неоригинальные позиции в группе
        if not original_positions.empty and not non_original_positions.empty:
            # Находим максимальную цену среди неоригинальных позиций
            max_non_orig_price = non_original_positions["new_price"].max()

            # Проверяем, превышает ли цена неоригинала цену всех оригиналов
            for non_orig_idx, non_orig_row in non_original_positions.iterrows():
                # Проверяем, есть ли оригинальные позиции, цена которых ниже текущей неоригинальной
                originals_below = original_positions[
                    original_positions["new_price"] <= non_orig_row["new_price"]
                ]

                if not originals_below.empty:
                    # Проверяем наличие delprice и delsklad у неоригинальной позиции
                    if (
                        pd.notna(non_orig_row["delprice"])
                        and "api" not in non_orig_row["delsklad"].lower()
                    ):
                        # Увеличиваем цену всех оригиналов на 15% больше, чем максимальная цена неоригинала в группе
                        target_price = max_non_orig_price * 1.15
                        target_price = np.ceil(target_price / 10) * 10
                        filtered_df.loc[
                            filtered_df["kod"].isin(original_positions["kod"]),
                            "new_price",
                        ] = target_price
                    else:
                        # Если delprice у неоригинала отсутствует или склад содержит "api"
                        for orig_idx, orig_row in originals_below.iterrows():
                            # Проверяем, есть ли у оригинала delprice и delsklad
                            if (
                                pd.notna(orig_row["delprice"])
                                and "api" not in orig_row["delsklad"].lower()
                            ):
                                # Пытаемся снизить цену неоригинала
                                target_price = (
                                    np.ceil((orig_row["new_price"] * 0.85) / 10) * 10
                                )
                                min_limit = max(
                                    (
                                        np.ceil(non_orig_row["middleprice"] * 1.3 / 10)
                                        * 10
                                        if pd.notna(non_orig_row["middleprice"])
                                        else 0
                                    ),
                                    (
                                        np.ceil(non_orig_row["maxprice"] * 1.1 / 10)
                                        * 10
                                        if pd.notna(non_orig_row["maxprice"])
                                        else 0
                                    ),
                                )

                                # Проверяем, чтобы target_price не опустился ниже min_limit
                                if target_price >= min_limit:
                                    filtered_df.loc[
                                        filtered_df["kod"] == non_orig_row["kod"],
                                        "new_price",
                                    ] = target_price
                                else:
                                    # Если не удалось снизить цену неоригинала, повышаем цену оригинала
                                    required_price = max_non_orig_price * 1.15
                                    required_price = np.ceil(required_price / 10) * 10
                                    filtered_df.loc[
                                        filtered_df["kod"] == orig_row["kod"],
                                        "new_price",
                                    ] = required_price
                            else:
                                # Если нет возможности снизить цену у неоригинала, поднимаем цену у всех оригиналов
                                required_price = max_non_orig_price * 1.15
                                required_price = np.ceil(required_price / 10) * 10
                                filtered_df.loc[
                                    filtered_df["kod"].isin(original_positions["kod"]),
                                    "new_price",
                                ] = required_price

    return filtered_df


def main(filtered_df, engine):
    """
    Главная функция, которая последовательно выполняет все функции корректировки цен.

    Parameters:
    filtered_df (pd.DataFrame): Основной датафрейм с информацией о позициях.
    engine (SQLAlchemy Engine): Подключение к базе данных.

    Returns:
    pd.DataFrame: Финальный откорректированный датафрейм.
    """
    print(
        "# Применяем корректировку цен на основе xyz, median_service_percent и tsenarozn"
    )

    filtered_df = service_percentup(filtered_df)

    konkurents = autocoreec_data(engine=engine)

    print('Корректируем цены на основе данных конкурентов')
    filtered_df = konkurents_correct(filtered_df, konkurents)

    print('# Синхронизируем цены внутри групп по gruppa_analogov и производителю')
    filtered_df = sync_prices(filtered_df)

    print("# Обновляем таблицу rasprodat и корректируем цены на основе таблицы")
    # Обновляем таблицу rasprodat и корректируем цены на основе таблицы
    filtered_df = update_rasprodat(filtered_df, engine)

    print("Корректируем цены для оригинальных позиций относительно неоригинальных")
    # Корректируем цены для оригинальных позиций относительно неоригинальных
    filtered_df = adjust_original_prices(filtered_df)
    

    print('Еще раз выравниваем цены с одинаковыми производителями в аналогах...')
    # Синхронизируем цены внутри групп по gruppa_analogov и производителю
    filtered_df = sync_prices(filtered_df)

    # Возвращаем финальный откорректированный датафрейм
    return filtered_df
