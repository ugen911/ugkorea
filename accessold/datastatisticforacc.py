# Подключение к базе данных и импорт необходимых библиотек
from ugkorea.db.database import get_db_engine
import pandas as pd
import os

# Подключение к базе данных
engine = get_db_engine()

# Загрузка таблиц из базы данных
nomenklaturaold_df = pd.read_sql("SELECT kod FROM nomenklaturaold", engine)
full_statistic_df = pd.read_sql(
    "SELECT kod, months_without_sales, std_sales_last_12_months, mean_sales_last_12_months, total_sales_last_12_months, min_stock, abc FROM full_statistic",
    engine,
)
priceendmonth_df = pd.read_sql("SELECT * FROM priceendmonth", engine)
stockendmonth_df = pd.read_sql("SELECT * FROM stockendmonth", engine)
salespivot_df = pd.read_sql("SELECT * FROM salespivot", engine)

# Удаление пробелов по краям в колонке 'kod' для всех датафреймов
nomenklaturaold_df["kod"] = nomenklaturaold_df["kod"].str.strip()
full_statistic_df["kod"] = full_statistic_df["kod"].str.strip()
priceendmonth_df["kod"] = priceendmonth_df["kod"].str.strip()
stockendmonth_df["nomenklaturakod"] = stockendmonth_df["nomenklaturakod"].str.strip()
salespivot_df["kod"] = salespivot_df["kod"].str.strip()

# Переименование колонки 'nomenklaturakod' в 'kod' в таблице stockendmonth
stockendmonth_df.rename(columns={"nomenklaturakod": "kod"}, inplace=True)


# Функция для создания сводной таблицы за последние 20 месяцев и изменения названий колонок
def create_pivot_table(df, kod_col, date_col, value_col, prefix):
    df[date_col] = pd.to_datetime(df[date_col])
    latest_month = pd.to_datetime("now")  # Текущий месяц
    # Создаем список из последних 20 месяцев, включая предыдущий месяц и уходя назад
    last_20_months = (
        pd.date_range(end=latest_month, periods=20, freq="ME")
        .strftime("%Y-%m")
        .tolist()
    )

    # Фильтруем данные за последние 20 месяцев
    df_filtered = df[df[date_col].dt.strftime("%Y-%m").isin(last_20_months)]

    # Создаем сводную таблицу
    pivot_df = df_filtered.pivot_table(
        index=kod_col,
        columns=df_filtered[date_col].dt.strftime("%Y-%m"),
        values=value_col,
        fill_value=0,
    )

    # Добавляем недостающие месяцы как колонки
    for month in last_20_months:
        if month not in pivot_df.columns:
            pivot_df[month] = 0

    # Упорядочиваем колонки по дате
    pivot_df = pivot_df[last_20_months]

    # Переименовываем колонки в соответствии с правилами и добавляем отладочную печать
    new_columns = {}
    for i in range(20):
        new_name = (
            f"{prefix} {'prev' if i == 0 else f'prev-{i}' if i < 19 else 'oldest'}"
        )
        new_columns[last_20_months[-(i + 1)]] = new_name  # Используем обратный порядок
        # Отладочная печать
        print(
            f"Отладка: Актуальный месяц и год: {latest_month.strftime('%Y-%m')}. Колонка '{new_name}' обозначает месяц и год: {last_20_months[-(i + 1)]}"
        )

    pivot_df.rename(columns=new_columns, inplace=True)

    return pivot_df


# Создание сводных таблиц для priceendmonth, stockendmonth и salespivot
price_pivot_df = create_pivot_table(priceendmonth_df, "kod", "data", "tsena", "Price")
stock_pivot_df = create_pivot_table(
    stockendmonth_df, "kod", "month", "balance", "stock"
)
sales_pivot_df = create_pivot_table(
    salespivot_df, "kod", "year_month", "kolichestvo", "sells"
)

# Получение данных о продажах за текущий месяц из salespivot_df
salespivot_df["year_month"] = pd.to_datetime(salespivot_df["year_month"])
current_month = pd.to_datetime("now").strftime("%Y-%m")
sales_current_month_df = salespivot_df[
    salespivot_df["year_month"].dt.strftime("%Y-%m") == current_month
]

# Группировка данных по 'kod' для получения продаж за текущий месяц
sales_current_month_df = sales_current_month_df.groupby("kod", as_index=False)[
    "kolichestvo"
].sum()
sales_current_month_df.rename(
    columns={"kolichestvo": "sales_current_month"}, inplace=True
)

# Объединение всех сводных таблиц с данными из nomenklaturaold и full_statistic по колонке 'kod'
final_df = pd.merge(nomenklaturaold_df, full_statistic_df, on="kod", how="left")
final_df = pd.merge(final_df, price_pivot_df, on="kod", how="left")
final_df = pd.merge(final_df, stock_pivot_df, on="kod", how="left")
final_df = pd.merge(final_df, sales_pivot_df, on="kod", how="left")

# Добавление данных о продажах за текущий месяц к итоговой таблице
final_df = pd.merge(final_df, sales_current_month_df, on="kod", how="left")

# Замена None и NaN на 0
final_df = final_df.fillna(0)

# Результат
print(final_df.head())


# Путь для сохранения файла
save_path_1 = (
    r"D:\NAS\заказы\Евгений\New\Файлы для работы Access\final_pivot_table.xlsx"
)
save_path_2 = (
    r"\\26.218.196.12\заказы\Евгений\New\Файлы для работы Access\final_pivot_table.xlsx"
)

# Проверка доступности первой директории, если недоступна - использовать вторую
if os.path.exists(r"D:\NAS\заказы\Евгений\New\Файлы для работы Access"):
    final_df.to_excel(save_path_1, index=False)
    print(f"Файл успешно сохранен по адресу: {save_path_1}")
else:
    final_df.to_excel(save_path_2, index=False)
    print(f"Файл успешно сохранен по адресу: {save_path_2}")
