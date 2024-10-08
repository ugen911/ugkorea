import pandas as pd
import re
from ugkorea.db.database import get_db_engine
from datetime import datetime, timedelta
from ugkorea.reglament_task.dicts import replacement_dict
from sqlalchemy import text

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

    full_statistic_df["artikul_clean"] = full_statistic_df["artikul"].apply(clean_string)
    full_statistic_df["proizvoditel_clean"] = full_statistic_df["proizvoditel"].apply(clean_string)
    full_statistic_df["proizvoditel"] = full_statistic_df["proizvoditel"].replace(replacement_dict, regex=True)

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
                data["производитель"] = data["производитель"].replace(replacement_dict, regex=True)

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
            "артикул" in df.columns and
            "производитель" in df.columns and
            "количество" in df.columns
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

    # Шаг 7: Переименование колонок и отбор необходимых, включая proizvoditel_clean
    final_df = final_df.rename(
        columns={"количество": "stock", "цена": "price", "поставщик": "sklad"}
    )
    final_df = final_df[["kod", "stock", "price", "sklad", "proizvoditel_clean"]]

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



def manage_deliverypriceintime(engine):
    # Загрузка данных из deliverypriceintime
    current_date = datetime.now()
    one_year_ago = current_date - timedelta(days=365)

    # Удаляем записи старше одного года
    delete_query = text(
        f"""
        DELETE FROM analitic.deliverypriceintime
        WHERE date_processed < '{one_year_ago.strftime('%Y-%m-%d')}';
    """
    )
    with engine.connect() as conn:
        conn.execute(delete_query)  # Используем text() для выполнения SQL-запроса

    # Проверяем, есть ли данные за текущую дату
    check_query = f"""
        SELECT 1 FROM analitic.deliverypriceintime
        WHERE date_processed = '{current_date.strftime('%Y-%m-%d')}'
        LIMIT 1;
    """
    existing_data = pd.read_sql(check_query, engine)

    # Если данных нет, продолжаем добавление
    if existing_data.empty:
        print("Данные за текущую дату отсутствуют. Добавляем данные.")
        return False
    else:
        print("Данные за текущую дату уже существуют. Добавление не требуется.")
        return True


def main():
    # Получаем данные из функций
    bergapi_data = load_bergapi_data()
    process_data_result = process_data()

    # Конкатенируем данные с условием:
    # Если kod есть в process_data_result, но производитель "hyundaikia", выбираем минимальную цену
    # из обоих датафреймов.
    # Для всех остальных брендов используем логику по умолчанию.
    combined_df = pd.concat(
        [
            process_data_result[process_data_result["proizvoditel_clean"] != "hyundaikia"],
            bergapi_data[
                ~bergapi_data["kod"].isin(process_data_result["kod"])
            ],
            process_data_result[process_data_result["proizvoditel_clean"] == "hyundaikia"]
            .merge(
                bergapi_data[bergapi_data["kod"].isin(process_data_result["kod"])],
                on="kod",
                suffixes=("_process", "_bergapi"),
            )
            .assign(
                price=lambda x: x[["price_process", "price_bergapi"]].min(axis=1),
                stock=lambda x: x[["stock_process", "stock_bergapi"]].max(axis=1),
            )
            [["kod", "price", "stock", "sklad_process"]]
            .rename(columns={"sklad_process": "sklad"}),
        ],
        ignore_index=True,
    )

    # Убираем колонку proizvoditel_clean перед загрузкой в deliveryminprice
    combined_df = combined_df.drop(columns=["proizvoditel_clean"])

    # Сохраняем результат в базу данных в таблицу deliveryminprice (без колонки date_processed)
    engine = get_db_engine()
    combined_df.to_sql(
        "deliveryminprice", engine, schema="public", if_exists="replace", index=False
    )

    # Добавляем колонку с сегодняшней датой только для таблицы deliverypriceintime
    combined_df["date_processed"] = datetime.now().strftime('%Y-%m-%d')

    # Проверяем и удаляем старые записи, затем добавляем новые данные, если их еще нет
    if not manage_deliverypriceintime(engine):
        combined_df.to_sql(
            "deliverypriceintime", engine, schema="analitic", if_exists="append", index=False
        )

    print("Данные успешно сохранены в таблицы deliveryminprice и deliverypriceintime.")


# Проверка на запуск скрипта
if __name__ == "__main__":
    main()
