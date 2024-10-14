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


# Функция для вычисления медианной доли service_percent за последние 24 месяца для каждого kod
def calculate_median_service_percent(df):
    # Преобразуем колонку 'period' в формат даты
    df["period"] = pd.to_datetime(df["period"], format="%d.%m.%Y")

    # Выделяем год и месяц из колонки 'period'
    df["month_year"] = df["period"].dt.to_period("M")

    # Ограничиваем данные только последними 24 месяцами
    latest_period = df["month_year"].max()
    last_24_months = latest_period - 23  # Считаем от текущего месяца до 24-го назад
    df = df[df["month_year"] >= last_24_months]

    # Группировка по kod и month_year для подсчета количества продаж
    sales_per_kod = (
        df.groupby(["kod", "month_year", "channel"])
        .size()
        .reset_index(name="sales_count")
    )

    # Отдельно считаем продажи через сервис
    service_sales = sales_per_kod[sales_per_kod["channel"] == "сервис"]
    service_sales = service_sales.rename(columns={"sales_count": "count_service"})

    # Считаем общее количество продаж для каждого kod и месяца
    total_sales = (
        sales_per_kod.groupby(["kod", "month_year"])["sales_count"]
        .sum()
        .reset_index(name="total_sales")
    )

    # Объединяем данные по продажам в сервисе и общим продажам
    combined_sales = pd.merge(
        service_sales[["kod", "month_year", "count_service"]],
        total_sales[["kod", "month_year", "total_sales"]],
        on=["kod", "month_year"],
        how="right",
    ).fillna(0)

    # Рассчитываем процент продаж через сервис для каждого kod и месяца
    combined_sales["service_percent"] = (
        combined_sales["count_service"] / combined_sales["total_sales"]
    ) * 100

    # Рассчитываем медианное значение service_percent за последние 24 месяца для каждого kod
    median_service_percent = (
        combined_sales.groupby("kod")["service_percent"]
        .median()
        .reset_index(name="median_service_percent")
    )

    # Возвращаем итоговую таблицу с kod и медианным значением service_percent
    return median_service_percent


# Пример вызова функции с передачей объекта engine
if __name__ == "__main__":

    engine = get_db_engine()
    df = load_and_process_data(engine)
    sales_share_df = calculate_median_service_percent(df)
    print(sales_share_df.head())  # Пример вывода первых строк итогового датафрейма
