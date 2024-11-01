#### Считает продажи по классам выгружает в файл по убыванию

import pandas as pd
from ugkorea.db.database import get_db_engine
from datetime import datetime, timedelta

# Подключение к базе данных
engine = get_db_engine()

# Загрузка таблицы typedetailgen и удаление пробелов в колонке kod
typedetailgen_df = pd.read_sql("SELECT * FROM typedetailgen", engine)
typedetailgen_df["kod"] = typedetailgen_df["kod"].str.strip()

# Извлечение уникальных значений kod из typedetailgen для фильтрации salespivot
kod_values = tuple(typedetailgen_df["kod"].unique())

# Загрузка таблицы salespivot с фильтрацией по kod из typedetailgen и удалением пробелов
query = f"""
    SELECT * FROM salespivot 
    WHERE TRIM(kod) IN {kod_values}
"""
salespivot_df = pd.read_sql(query, engine)

# Преобразование year_month в формат даты
salespivot_df["year_month"] = pd.to_datetime(
    salespivot_df["year_month"], format="%Y-%m"
)

# Вычисление даты 12 месяцев назад от текущей даты
current_date = datetime.now()
start_date = current_date - timedelta(days=365)

# Фильтрация данных за последние 12 месяцев плюс текущий
salespivot_df = salespivot_df[salespivot_df["year_month"] >= start_date]

# Удаление пробелов в колонке kod в обеих таблицах перед объединением
salespivot_df["kod"] = salespivot_df["kod"].str.strip()
typedetailgen_df["kod"] = typedetailgen_df["kod"].str.strip()

# Объединение датафреймов salespivot_df и typedetailgen_df по колонке kod
merged_df = pd.merge(salespivot_df, typedetailgen_df, on="kod", how="inner")

# Группировка по type_detail и вычисление сумм по kolichestvo и summa
result_df = (
    merged_df.groupby("type_detail")
    .agg(total_kolichestvo=("kolichestvo", "sum"), total_summa=("summa", "sum"))
    .reset_index()
)

# Сортировка по убыванию total_kolichestvo
result_df = result_df.sort_values(by="total_kolichestvo", ascending=False)
result_df.to_excel('class.xlsx')
# Вывод первых нескольких строк для проверки
print(result_df.head())
