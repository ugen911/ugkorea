import pandas as pd
import re
from ugkorea.db.database import get_db_engine
from datetime import datetime, timedelta
from ugkorea.reglament_task.dicts import replacement_dict 


# Определение функции
def process_data():
    # Получаем подключение к базе данных
    engine = get_db_engine()

    # Определяем схему
    schema_name = "prices"

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
            if re.search(r"[a-z]$", artikul):
                new_artikul = artikul[:-1]
                new_row = row.copy()
                new_row["artikul"] = new_artikul
                new_rows.append(new_row)
        return pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)

    full_statistic_df = create_doubles(full_statistic_df)

    # Очистка столбцов в full_statistic
    def clean_string(s):
        return re.sub(r"[\s.-]", "", str(s)).lower()

    full_statistic_df["artikul_clean"] = full_statistic_df["artikul"].apply(
        clean_string
    )
    full_statistic_df["proizvoditel_clean"] = full_statistic_df["proizvoditel"].apply(
        clean_string
    )
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

    all_data = {}

    # Цикл по каждой таблице
    for table in tables:
        print(f"Обработка таблицы: {table}")

        date_query = f"""
            SELECT MAX(дата) AS last_date
            FROM {schema_name}.{table};
        """
        try:
            last_date_result = pd.read_sql(date_query, engine)
            last_date = last_date_result["last_date"].iloc[0]

            if last_date and pd.to_datetime(last_date) >= date_limit:
                data_query = f"""
                    SELECT *
                    FROM {schema_name}.{table}
                    WHERE дата = '{last_date}';
                """
                data = pd.read_sql(data_query, engine)
            else:
                columns_query = f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = '{schema_name}' AND table_name = '{table}';
                """
                columns = pd.read_sql(columns_query, engine)["column_name"].tolist()
                data = pd.DataFrame(columns=columns)

            if "производитель" in data.columns:
                data["производитель"] = data["производитель"].replace(
                    replacement_dict, regex=True
                )

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
            df["artikul_clean"] = df["артикул"].apply(clean_string)
            df["proizvoditel_clean"] = df["производитель"].apply(clean_string)

            merged_df = df.merge(
                full_statistic_df,
                left_on=["artikul_clean", "proizvoditel_clean"],
                right_on=["artikul_clean", "proizvoditel_clean"],
                how="inner",
            )

            filtered_df = merged_df[merged_df["количество"] >= merged_df["min_stock"]]
            results[table_name] = filtered_df

    # Шаг 5: Объединяем все датафреймы из results в один
    combined_df = pd.concat(results.values(), ignore_index=True)

    # Шаг 6: Фильтрация, чтобы оставить только строки с минимальной ценой для каждого kod
    def get_min_price_group(df):
        df_sorted = df.sort_values(by=["цена", "количество"], ascending=[True, False])
        return df_sorted.groupby("kod").first().reset_index()

    final_df = get_min_price_group(combined_df)

    # Шаг 7: Переименование колонок и отбор необходимых
    final_df = final_df.rename(
        columns={"количество": "stock", "цена": "price", "поставщик": "sklad"}
    )
    final_df = final_df[["kod", "stock", "price", "sklad"]]

    return final_df


# Функция для загрузки данных из bergapi
def load_bergapi_data():
    # Получаем подключение к базе данных
    engine = get_db_engine()

    # Определяем схему и таблицу
    schema_name = "api"
    table_name = "bergapi"

    # Получаем текущую дату и лимит в 7 дней
    current_date = datetime.now()
    date_limit = current_date - timedelta(days=7)

    # Получаем последнюю дату из колонки date_checked, которая не дальше 7 дней от текущей
    date_query = f"""
        SELECT MAX(date_checked) AS last_date
        FROM {schema_name}.{table_name}
        WHERE date_checked >= '{date_limit.strftime('%Y-%m-%d')}';
    """
    last_date_result = pd.read_sql(date_query, engine)
    last_date = last_date_result["last_date"].iloc[0]

    # Проверяем, что дата найдена и корректна
    if last_date:
        # Извлекаем все данные за последнюю дату
        data_query = f"""
            SELECT kod, price, quantity AS stock, 'bergapi' AS sklad
            FROM {schema_name}.{table_name}
            WHERE date_checked = '{last_date}';
        """
        bergapi_df = pd.read_sql(data_query, engine)

        # Возвращаем датафрейм с данными
        return bergapi_df
    else:
        # Если данных нет, возвращаем пустой датафрейм с необходимыми колонками
        return pd.DataFrame(columns=["kod", "price", "stock", "sklad"])


def main():
    # Получаем данные из функций
    bergapi_data = load_bergapi_data()
    process_data_result = process_data()

    # Конкатенируем данные с условием: если kod есть в process_data_result, то берем только его данные
    # Если kod нет в process_data_result, берем данные из bergapi_data
    combined_df = pd.concat(
        [
            process_data_result,
            bergapi_data[~bergapi_data["kod"].isin(process_data_result["kod"])],
        ]
    )

    # Сохраняем результат в базу данных
    engine = get_db_engine()
    combined_df.to_sql(
        "deliveryminprice", engine, schema="public", if_exists="replace", index=False
    )

    print("Данные успешно сохранены в таблицу deliveryminprice.")


# Проверка на запуск скрипта
if __name__ == "__main__":
    main()
