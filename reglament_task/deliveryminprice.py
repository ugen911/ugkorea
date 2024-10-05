import pandas as pd
import re
from ugkorea.db.database import get_db_engine
from datetime import datetime, timedelta

# Получаем подключение к базе данных
engine = get_db_engine()

# Определяем схему
schema_name = "prices"

# Правила замены производителей
replacement_dict = {
    r"(?i)Hyundai\s*Kia|Hyundai-Kia|Hyundai/Kia": "HyundaiKia",
    r"(?i)JS\s*ASAKASHI": "ASAKASHI",
    r"(?i)Asam-sa": "Asam",
    r"(?i)DONGIL\s*SUPER\s*STAR": "DONGIL",
    r"(?i)MK\s*KASHIYAMA": "KASHIYAMA",
    r"(?i)KAYABA": "KYB",
    r"(?i)LEMF[OÖ]ERDER": "LEMFORDER",
    r"(?i)LESJOEFORS": "Lesjofors",
    r"(?i)LYNX\s*AUTO|Lynxauto": "LYNX",
    r"(?i)PARTS-MALL|PartSmall|PMC": "PARTS MALL",
    r"(?i)VICTOR\s*REINZ": "REINZ",
    r"(?i)Sangsin\s*Brake": "Sangsin",
    r"(?i)PHC": "Valeo PHC",
    r"(?i)VERNET[-\s]CALORSTAT": "VERNET",
}

# Шаг 1: Загружаем таблицу full_statistic из схемы public
full_statistic_query = """
    SELECT kod, proizvoditel, artikul, min_stock
    FROM public.full_statistic;
"""
full_statistic_df = pd.read_sql(full_statistic_query, engine)


# Шаг 2: Проверка artikul на наличие маленькой латинской буквы в конце и создание дублей
def create_doubles(df):
    new_rows = []
    for index, row in df.iterrows():
        artikul = row["artikul"]
        # Проверяем, если последний символ маленькая латинская буква
        if re.search(r"[a-z]$", artikul):
            # Удаляем последний символ и создаем дубль строки
            new_artikul = artikul[:-1]
            new_row = row.copy()
            new_row["artikul"] = new_artikul
            new_rows.append(new_row)
    # Добавляем дубль строк в датафрейм
    return pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)


full_statistic_df = create_doubles(full_statistic_df)


# Очистка столбцов в full_statistic
def clean_string(s):
    return re.sub(r"[\s.-]", "", str(s)).lower()


full_statistic_df["artikul_clean"] = full_statistic_df["artikul"].apply(clean_string)
full_statistic_df["proizvoditel_clean"] = full_statistic_df["proizvoditel"].apply(
    clean_string
)

# Применяем правила замены производителей в full_statistic
full_statistic_df["proizvoditel"] = full_statistic_df["proizvoditel"].replace(
    replacement_dict, regex=True
)

# Получаем список таблиц в схеме prices
tables_query = f"""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = '{schema_name}';
"""
tables = pd.read_sql(tables_query, engine)["table_name"].tolist()

# Задаем текущую дату и лимит в 7 дней
current_date = datetime.now()
date_limit = current_date - timedelta(days=7)

# Для хранения данных из каждой таблицы
all_data = {}

# Цикл по каждой таблице
for table in tables:
    print(f"Обработка таблицы: {table}")

    # Получаем самую последнюю дату из таблицы
    date_query = f"""
        SELECT MAX(дата) AS last_date
        FROM {schema_name}.{table};
    """
    try:
        last_date_result = pd.read_sql(date_query, engine)
        last_date = last_date_result["last_date"].iloc[0]

        if last_date and pd.to_datetime(last_date) >= date_limit:
            # Получаем данные за последнюю дату
            data_query = f"""
                SELECT *
                FROM {schema_name}.{table}
                WHERE дата = '{last_date}';
            """
            data = pd.read_sql(data_query, engine)
        else:
            # Если данных нет, создаём пустой датафрейм с именами колонок из таблицы
            columns_query = f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{schema_name}' AND table_name = '{table}';
            """
            columns = pd.read_sql(columns_query, engine)["column_name"].tolist()
            data = pd.DataFrame(columns=columns)

        # Применяем правила замены производителей
        if "производитель" in data.columns:
            data["производитель"] = data["производитель"].replace(
                replacement_dict, regex=True
            )

        # Сохраняем датафрейм под именем таблицы
        all_data[table] = data

    except Exception as e:
        print(f"Ошибка при обработке таблицы {table}: {e}")
        columns_query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{schema_name}' AND table_name = '{table}';
        """
        columns = pd.read_sql(columns_query, engine)["column_name"].tolist()
        data = pd.DataFrame(columns=columns)
        all_data[table] = data

# Шаг 4: Поиск совпадений
results = {}

for table_name, df in all_data.items():
    if (
        "артикул" in df.columns
        and "производитель" in df.columns
        and "количество" in df.columns
    ):
        # Очистка столбцов в текущем датафрейме
        df["artikul_clean"] = df["артикул"].apply(clean_string)
        df["proizvoditel_clean"] = df["производитель"].apply(clean_string)

        # Объединение с full_statistic по очищенным колонкам
        merged_df = df.merge(
            full_statistic_df,
            left_on=["artikul_clean", "proizvoditel_clean"],
            right_on=["artikul_clean", "proizvoditel_clean"],
            how="inner",
        )

        # Фильтрация по количеству >= min_stock
        filtered_df = merged_df[merged_df["количество"] >= merged_df["min_stock"]]

        # Сохраняем результат
        results[table_name] = filtered_df

# Показать информацию о совпадениях
for table_name, data in results.items():
    print(f"Совпадающие данные из таблицы {table_name}:")
    print(data.head())

print("Завершено выполнение скрипта.")
