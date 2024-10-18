import numpy as np
import pandas as pd
from ugkorea.db.database import get_db_engine

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
            and median_service_percent > 90
            and not pd.isna(tsenarozn)
        ):
            if tsenarozn < 500:
                new_price = np.ceil((new_price * 1.10) / 10) * 10
            elif 500 <= tsenarozn < 1000:
                new_price = np.ceil((new_price * 1.07) / 10) * 10
            elif 1000 <= tsenarozn < 2000:
                new_price = np.ceil((new_price * 1.05) / 10) * 10

        return new_price

    df["new_price"] = df.apply(adjust_row, axis=1)
    return df


def konkurents_correct(filtered_df, konkurents):

    konkurents = konkurents.rename(columns={"price": "konkurents"})
    df = pd.merge(
        filtered_df, konkurents[["kod", "konkurents"]], on="kod", how="left"
    )
    """
    Корректирует цену new_price на основе данных о конкурентах и других метрик.
    Если цена конкурента ниже, корректируем new_price, чтобы он был на 10% дешевле конкурента,
    но при этом не выходил за пределы, установленные на основе middleprice и maxprice.

    Parameters:
    df (pd.DataFrame): Датафрейм с данными о позициях.

    Returns:
    pd.DataFrame: Обновленный датафрейм с корректировками new_price.
    """

    def adjust_row(row):
        konkurents_price = row["konkurents"]
        new_price = row["new_price"]
        middleprice = row.get("middleprice", np.nan)
        maxprice = row.get("maxprice", np.nan)

        # Проверка: если konkurents ниже new_price, корректируем new_price
        if not pd.isna(konkurents_price) and konkurents_price < new_price:
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
    Синхронизирует цены внутри каждой группы по gruppa_analogov и производителю.
    Все позиции в группе получают одинаковую цену. Если наибольшая цена выше, она снижается
    до минимальной цены в группе или до maxprice + 10%, если это применимо. Если этого недостаточно,
    остальные цены поднимаются до этого значения.

    Parameters:
    filtered_df (pd.DataFrame): Основной датафрейм с информацией о позициях.

    Returns:
    pd.DataFrame: Обновленный датафрейм с синхронизированными ценами внутри групп.
    """
    # Создаем уникальный идентификатор группы на основе gruppa_analogov и proizvoditel
    filtered_df["group_id"] = (
        filtered_df["gruppa_analogov"].astype(str)
        + "_"
        + filtered_df["proizvoditel"].astype(str)
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

    # Группируем датафрейм по уникальному идентификатору группы
    grouped = filtered_df.groupby("group_id")

    # Создаем копию датафрейма для обновления значений
    filtered_df = filtered_df.copy()

    # Применяем sync_group ко всем группам и обновляем значения в исходном датафрейме
    for group_id, group in grouped:
        updated_group = sync_group(group)
        # Обновляем цены в исходном датафрейме по kod
        for idx, row in updated_group.iterrows():
            filtered_df.loc[filtered_df["kod"] == row["kod"], "new_price"] = row[
                "new_price"
            ]

    # Удаляем временный уникальный идентификатор группы
    filtered_df.drop(columns=["group_id"], inplace=True)

    return filtered_df

#############До сюда сделано


def adjust_prices(filtered_df, konkurents, engine):
    """
    Функция корректирует цены в filtered_df на основе данных konkurents и заданных правил.

    Parameters:
    filtered_df (pd.DataFrame): Основной датафрейм с информацией о позициях.
    konkurents (pd.DataFrame): Датафрейм с данными о ценах конкурентов.
    engine (SQLAlchemy Engine): Подключение к базе данных для работы с таблицей rasprodazha.

    Returns:
    pd.DataFrame: Обновленный датафрейм filtered_df со всеми корректировками цен.
    """
    # Соединяем filtered_df и konkurents по 'kod', берем только колонку 'price' из konkurents
    konkurents = konkurents.rename(columns={"price": "konkurents"})
    merged_df = pd.merge(
        filtered_df, konkurents[["kod", "konkurents"]], on="kod", how="left"
    )

    # Применяем начальную корректировку к ценам
    merged_df = service_percentup(merged_df)

    # Применяем корректировку цен на основе конкурентов
    merged_df = konkurents_correct(merged_df)

    # Группировка по gruppa_analogov и производителю
    for gruppa, group_df in merged_df.groupby(["gruppa_analogov", "proizvoditel"]):
        # Проверка и синхронизация цен внутри группы
        max_new_price = group_df["new_price"].max()
        min_new_price = group_df["new_price"].min()

        if max_new_price != min_new_price:
            # Корректировка цены для всех элементов группы
            for idx in group_df.index:
                middleprice = merged_df.at[idx, "middleprice"]
                maxprice = merged_df.at[idx, "maxprice"]

                if merged_df.at[idx, "new_price"] == max_new_price:
                    if not pd.isna(maxprice):
                        merged_df.at[idx, "new_price"] = (
                            np.ceil(min(maxprice * 1.10, min_new_price + 1) / 10) * 10
                        )
                else:
                    merged_df.at[idx, "new_price"] = (
                        np.ceil(max(min_new_price, merged_df.at[idx, "new_price"]) / 10)
                        * 10
                    )

    # Проверка на позиции, от которых нужно избавиться
    filtered_for_sale = merged_df[
        (merged_df["is_original"] == False)
        & (merged_df["abc"].isin(["A", "B"]))
        & (merged_df["xyz"] == "X")
    ]
    for kod in filtered_for_sale["kod"].unique():
        konkurent = merged_df[
            (
                merged_df["gruppa_analogov"]
                == merged_df.loc[filtered_for_sale.index, "gruppa_analogov"].iloc[0]
            )
            & (merged_df["abc"].isin(["C", None]))
            & (merged_df["xyz"].isin(["Y", "Z", None]))
        ]
        if not konkurent.empty:
            # Проверка на наличие в rasprodazha и добавление, если необходимо
            konkurent_kod = konkurent["kod"].iloc[0]
            if not check_kod_in_rasprodazha(engine, konkurent_kod):
                add_kod_to_rasprodazha(engine, konkurent_kod)

            # Установка минимальной цены
            target_price = konkurent["new_price"].min() * 1.10
            middleprice = merged_df.loc[merged_df["kod"] == kod, "middleprice"].iloc[0]
            maxprice = merged_df.loc[merged_df["kod"] == kod, "maxprice"].iloc[0]

            if not pd.isna(middleprice) and not pd.isna(maxprice):
                adjusted_price = max(middleprice * 1.40, maxprice * 1.10, target_price)
            else:
                adjusted_price = target_price

            merged_df.loc[merged_df["kod"] == kod, "new_price"] = (
                np.ceil(adjusted_price / 10) * 10
            )

    # Проверка и корректировка цен оригиналов относительно неоригиналов
    for gruppa, group_df in merged_df.groupby("gruppa_analogov"):
        original = group_df[group_df["is_original"] == True]
        non_original = group_df[group_df["is_original"] == False]

        if not original.empty and not non_original.empty:
            for orig_idx in original.index:
                orig_price = merged_df.at[orig_idx, "new_price"]
                for non_orig_idx in non_original.index:
                    non_orig_price = merged_df.at[non_orig_idx, "new_price"]

                    # Если оригинал дешевле неоригинала
                    if orig_price < non_orig_price:
                        if not pd.isna(merged_df.at[non_orig_idx, "konkurents"]):
                            merged_df.at[orig_idx, "new_price"] = (
                                np.ceil((non_orig_price * 1.15) / 10) * 10
                            )
                        else:
                            middleprice = merged_df.at[non_orig_idx, "middleprice"]
                            maxprice = merged_df.at[non_orig_idx, "maxprice"]

                            if not pd.isna(middleprice) and not pd.isna(maxprice):
                                merged_df.at[non_orig_idx, "new_price"] = (
                                    np.ceil(
                                        min(middleprice * 1.20, maxprice * 1.10) / 10
                                    )
                                    * 10
                                )
                                merged_df.at[orig_idx, "new_price"] = (
                                    np.ceil((non_orig_price * 1.15) / 10) * 10
                                )
                            else:
                                merged_df.at[orig_idx, "new_price"] = (
                                    np.ceil((non_orig_price * 1.15) / 10) * 10
                                )

    return merged_df


def check_kod_in_rasprodazha(engine, kod):
    """
    Проверяет, есть ли kod в таблице rasprodazha.
    """
    query = f"SELECT COUNT(*) FROM rasprodazha WHERE kod = '{kod}'"
    with engine.connect() as conn:
        result = conn.execute(query).scalar()
    return result > 0


def add_kod_to_rasprodazha(engine, kod):
    """
    Добавляет kod в таблицу rasprodazha.
    """
    with engine.connect() as conn:
        conn.execute(f"INSERT INTO rasprodazha (kod) VALUES ('{kod}')")
