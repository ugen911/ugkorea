import pandas as pd
from ugkorea.db.database import get_db_engine

# Основная функция для загрузки, объединения и обработки данных
def load_and_process_data(engine):
    # Внутренняя функция для очистки пробелов (trim) в каждой колонке датафрейма
    def trim_columns(df):
        for col in df.columns:
            if df[col].dtype == "object":  # Только для строковых колонок
                df[col] = df[col].map(lambda x: x.strip() if isinstance(x, str) else x)
        return df

    # Загрузка данных из таблицы prodazhi
    query_prodazhi = """
        SELECT hozoperatsija, dokumentprodazhi, kolichestvo, summa, period, sebestoimost, kod 
        FROM prodazhi;
    """
    prodazhi_df = pd.read_sql(query_prodazhi, engine)

    # Загрузка данных из таблицы realizatsija, включая колонку avtor
    query_realizatsija = """
        SELECT ssylka, dokumentosnovanie, rs_zakaznarjad, kod, avtor
        FROM realizatsija;
    """
    realizatsija_df = pd.read_sql(query_realizatsija, engine)

    # Очистка пробелов для каждой таблицы
    prodazhi_df = trim_columns(prodazhi_df)
    realizatsija_df = trim_columns(realizatsija_df)

    # Левое соединение, где данные из realizatsija включаются только при совпадении
    result_df = pd.merge(
        prodazhi_df,
        realizatsija_df,
        how="left",
        left_on=["dokumentprodazhi", "kod"],
        right_on=["ssylka", "kod"],
    )

    # Убираем строки с hozoperatsija = "Возврат товаров от покупателя" ИЛИ kod = "00000003"
    result_df = result_df[
        ~(
            (result_df["hozoperatsija"] == "Возврат товаров от покупателя")
            | (result_df["kod"] == "00000003")
        )
    ]

    # Создаем новую колонку 'channel' с условиями "сервис" или "розница"
    result_df["channel"] = result_df.apply(
        lambda row: (
            "сервис"
            if (
                row["hozoperatsija"] == "Реализация товаров"
                and pd.notna(row["rs_zakaznarjad"])
            )
            or row["hozoperatsija"] == "Заказ-наряд"
            else "розница"
        ),
        axis=1,
    )

    # Перемещаем колонку 'kod' в самое левое место
    columns = ["kod"] + [col for col in result_df.columns if col != "kod"]
    result_df = result_df[columns]

    return result_df


# Функция для вычисления доли продаж через сервис и розницу для каждого kod по месяцам и годам
def calculate_sales_share_by_kod(df):
    # Преобразуем колонку 'period' в формат даты
    df["period"] = pd.to_datetime(df["period"], format="%d.%m.%Y")

    # Выделяем год и месяц из колонки 'period'
    df["month_year"] = df["period"].dt.to_period("M")

    # Группировка по kod, month_year и channel для подсчета количества продаж
    sales_per_kod = (
        df.groupby(["kod", "month_year", "channel"])
        .size()
        .reset_index(name="sales_count")
    )

    # Отдельно считаем продажи через сервис и розницу
    service_sales = sales_per_kod[sales_per_kod["channel"] == "сервис"]
    retail_sales = sales_per_kod[sales_per_kod["channel"] == "розница"]

    # Переименовываем колонки для удобства
    service_sales = service_sales.rename(columns={"sales_count": "count_service"})
    retail_sales = retail_sales.rename(columns={"sales_count": "count_retail"})

    # Объединяем продажи по сервису и рознице на уровне kod и month_year
    combined_sales = pd.merge(
        service_sales[["kod", "month_year", "count_service"]],
        retail_sales[["kod", "month_year", "count_retail"]],
        on=["kod", "month_year"],
        how="outer",
    ).fillna(0)

    # Рассчитываем общие продажи для каждого kod и month_year
    combined_sales["total_sales"] = (
        combined_sales["count_service"] + combined_sales["count_retail"]
    )

    # Рассчитываем процент продаж через сервис и розницу
    combined_sales["service_percent"] = (
        combined_sales["count_service"] / combined_sales["total_sales"]
    ) * 100
    combined_sales["retail_percent"] = (
        combined_sales["count_retail"] / combined_sales["total_sales"]
    ) * 100

    # Возвращаем итоговую таблицу
    return combined_sales[
        [
            "kod",
            "month_year",
            "service_percent",
            "count_service",
            "retail_percent",
            "count_retail",
        ]
    ]


# Пример вызова функции с передачей объекта engine
if __name__ == "__main__":

    engine = get_db_engine()
    df = load_and_process_data(engine)
    sales_share_df = calculate_sales_share_by_kod(df)
    print(sales_share_df.head())  # Пример вывода первых строк итогового датафрейма
