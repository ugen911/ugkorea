import pandas as pd
from sqlalchemy import inspect, text
import os
import re
from datetime import datetime, timedelta


def get_or_create_call_log_table(engine):
    """
    Проверяет наличие таблицы call_log в схеме public.
    Если таблицы нет, создает её с уточненными типами данных.
    Возвращает данные из таблицы call_log.
    """
    schema = "public"
    table_name = "call_log"
    inspector = inspect(engine)

    # Проверка наличия таблицы
    if table_name not in inspector.get_table_names(schema=schema):
        # SQL-запрос для создания таблицы call_log
        create_table_query = f"""
        CREATE TABLE {schema}.{table_name} (
            "Дата вызова" DATE,
            "Время вызова" TIME,
            "Имя вызывающего" TEXT,
            "Номер вызывающего" VARCHAR(20),
            "Имя вызываемого" TEXT,
            "Номер вызываемого" VARCHAR(20),
            "Первый ответивший" TEXT,
            "Тип" TEXT,
            "Статус" TEXT,
            "Длительность" INTERVAL,
            "Входящая линия" VARCHAR(20),
            "Группа" INTEGER,
            "Анализ разговора" TEXT,
            "Ссылка на запись разговоров" TEXT
        );
        """
        with engine.connect() as connection:
            connection.execute(text(create_table_query))
            connection.commit()  # Фиксация изменений
        print(f"Таблица '{table_name}' была создана в схеме '{schema}'.")

    # Извлечение данных из таблицы call_log
    with engine.connect() as connection:
        query = f"SELECT * FROM {schema}.{table_name};"
        result = pd.read_sql(query, connection)

    return result


def find_and_load_latest_call_report():
    """
    Находит папку Downloads на компьютере Windows, ищет файл с именем
    'Отчет по вызовам в домене.xlsx' или его версии с номером (1), (2), ...,
    загружает последнюю версию файла в DataFrame.
    """
    downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")
    file_pattern = re.compile(r"Отчет по вызовам в домене(?: \((\d+)\))?\.xlsx")

    # Найти все подходящие файлы
    matching_files = [
        (filename, int(match.group(1)) if match.group(1) else 0)
        for filename in os.listdir(downloads_path)
        if (match := file_pattern.match(filename))
    ]

    if not matching_files:
        print("Файлы с именем 'Отчет по вызовам в домене.xlsx' не найдены.")
        return None

    # Найти последнюю версию файла
    latest_file = sorted(matching_files, key=lambda x: x[1], reverse=True)[0][0]
    latest_file_path = os.path.join(downloads_path, latest_file)
    print(f"Найден файл: {latest_file_path}")

    # Загрузка данных с указанием типов колонок
    dtype_mapping = {
        "Имя вызывающего": "string",
        "Номер вызывающего": "string",
        "Имя вызываемого": "string",
        "Номер вызываемого": "string",
        "Первый ответивший": "string",
        "Тип": "string",
        "Статус": "string",
        "Входящая линия": "string",
        "Группа": "Int64",  # Для числовых данных с возможными пропусками
        "Анализ разговора": "string",
        "Ссылка на запись разговоров": "string",
    }

    # Загрузка файла
    data = pd.read_excel(
        latest_file_path,
        dtype=dtype_mapping,
        parse_dates=["Дата вызова"],  # Конвертация в datetime
    )

    # Конвертация дополнительных колонок
    data["Время вызова"] = pd.to_datetime(
        data["Время вызова"], format="%H:%M:%S"
    ).dt.time
    data["Длительность"] = pd.to_timedelta(data["Длительность"])

    print("Данные успешно загружены.")
    return data


def update_call_log_table(engine, df, call_log_data):
    """
    Обновляет данные в таблице call_log:
    - Заменяет данные за совпадающие даты.
    - Добавляет данные за новые даты.
    - Удаляет данные старше 5 лет перед загрузкой.
    """
    schema = "public"
    table_name = "call_log"

    if df is None or df.empty:
        print("Нет данных для обновления: файл отсутствует или пустой.")
        return

    # Удаление данных старше 5 лет
    five_years_ago = datetime.now() - timedelta(days=5 * 365)
    cutoff_date = pd.to_datetime(five_years_ago.date())

    print(f"Удаление данных старше: {cutoff_date}")
    call_log_data = call_log_data[
        pd.to_datetime(call_log_data["Дата вызова"]) >= cutoff_date
    ]
    df = df[pd.to_datetime(df["Дата вызова"]) >= cutoff_date]

    # Получаем уникальные даты из обоих датафреймов
    df_dates = df["Дата вызова"].unique()
    call_log_dates = call_log_data["Дата вызова"].unique()

    # Найти совпадающие даты и новые даты
    common_dates = set(df_dates).intersection(set(call_log_dates))
    new_dates = set(df_dates) - set(call_log_dates)

    # Обновить данные за совпадающие даты
    if common_dates:
        call_log_data = call_log_data[~call_log_data["Дата вызова"].isin(common_dates)]
        updated_data = df[df["Дата вызова"].isin(common_dates)]
        if not updated_data.empty:
            # Удаляем полностью пустые или NaN колонки
            call_log_data = call_log_data.dropna(axis=1, how="all")
            updated_data = updated_data.dropna(axis=1, how="all")
            call_log_data = pd.concat([call_log_data, updated_data], ignore_index=True)

    # Добавить данные за новые даты
    if new_dates:
        new_data = df[df["Дата вызова"].isin(new_dates)]
        if not new_data.empty:
            # Удаляем полностью пустые или NaN колонки
            call_log_data = call_log_data.dropna(axis=1, how="all")
            new_data = new_data.dropna(axis=1, how="all")
            call_log_data = pd.concat([call_log_data, new_data], ignore_index=True)

    # Убедиться, что "Длительность" в формате timedelta
    if not pd.api.types.is_timedelta64_dtype(call_log_data["Длительность"]):
        call_log_data["Длительность"] = pd.to_timedelta(
            call_log_data["Длительность"], errors="coerce"
        )

    # Конвертировать "Длительность" в строку "hh:mm:ss" для загрузки в базу данных
    call_log_data["Длительность"] = (
        call_log_data["Длительность"]
        .dt.total_seconds()
        .apply(
            lambda x: (
                f"{int(x // 3600):02}:{int((x % 3600) // 60):02}:{int(x % 60):02}"
                if not pd.isna(x)
                else None
            )
        )
    )

    # Убедиться, что типы данных корректны
    call_log_data["Время вызова"] = pd.to_datetime(
        call_log_data["Время вызова"], format="%H:%M:%S", errors="coerce"
    ).dt.time

    # Загрузка обновленных данных в базу данных
    with engine.connect() as connection:
        call_log_data.to_sql(
            table_name,
            connection,
            schema=schema,
            if_exists="replace",  # Заменить данные в таблице
            index=False,
        )
    print("Данные успешно обновлены в таблице call_log.")

    # Получаем данные из таблицы call_log или создаем таблицу, если её нет


def update_call_journal(engine):
    call_log_data = get_or_create_call_log_table(engine)
    df = find_and_load_latest_call_report()
    if df is not None:
        update_call_log_table(engine, df, call_log_data)
    else:
        print("Нет данных для обновления.")


