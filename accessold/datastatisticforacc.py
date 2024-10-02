from ugkorea.db.database import get_db_engine
import pandas as pd
import numpy as np

# Подключение к базе данных
engine = get_db_engine()

# Загрузка таблиц из базы данных
nomenklaturaold_df = pd.read_sql("SELECT * FROM nomenklaturaold", engine)
full_statistic_df = pd.read_sql(
    "SELECT kod, months_without_sales, std_sales_last_12_months, mean_sales_last_12_months, abc, xyz, total_sales_last_12_months, min_stock FROM full_statistic",
    engine,
)
priceendmonth_df = pd.read_sql("SELECT * FROM priceendmonth", engine)
stockendmonth_df = pd.read_sql("SELECT * FROM stockendmonth", engine)
salespivot_df = pd.read_sql("SELECT * FROM salespivot", engine)

# 1. Соединение nomenklaturaold и full_statistic по полю 'kod'
merged_df = pd.merge(nomenklaturaold_df, full_statistic_df, on="kod", how="left")


# Функция для создания сводной таблицы за последние 19 месяцев
def create_pivot_table(df, kod_col, date_col, value_col):
    df[date_col] = pd.to_datetime(df[date_col])
    latest_month = df[date_col].max()
    # Создаем список из последних 19 месяцев
    last_19_months = (
        pd.date_range(end=latest_month, periods=19, freq="M").strftime("%Y-%m").tolist()
    )

    # Фильтруем данные за последние 19 месяцев
    df_filtered = df[df[date_col].dt.strftime("%Y-%m").isin(last_19_months)]

    # Создаем сводную таблицу
    pivot_df = df_filtered.pivot_table(
        index=kod_col,
        columns=df_filtered[date_col].dt.strftime("%Y-%m"),
        values=value_col,
        fill_value=0,
    )

    # Добавляем недостающие месяцы как колонки
    for month in last_19_months:
        if month not in pivot_df.columns:
            pivot_df[month] = 0

    # Упорядочиваем колонки по дате
    pivot_df = pivot_df[last_19_months]

    return pivot_df


# 2. Создание сводных таблиц для priceendmonth, stockendmonth и salespivot
price_pivot_df = create_pivot_table(priceendmonth_df, "kod", "data", "tsena")
stock_pivot_df = create_pivot_table(
    stockendmonth_df, "nomenklaturakod", "month", "balance"
)
sales_pivot_df = create_pivot_table(salespivot_df, "kod", "year_month", "kolichestvo")

# 3. Объединение всех сводных таблиц с ранее объединенной таблицей merged_df
final_df = merged_df.copy()
final_df = pd.merge(final_df, price_pivot_df, on="kod", how="left")
final_df = pd.merge(
    final_df, stock_pivot_df, left_on="kod", right_on="nomenklaturakod", how="left"
)
final_df = pd.merge(final_df, sales_pivot_df, on="kod", how="left")

# 4. Замена None и NaN на 0
final_df = final_df.fillna(0)

# Результат
print(final_df.head())

# Сохранение результата в Excel (если необходимо)
final_df.to_excel("final_pivot_table.xlsx", index=False)
