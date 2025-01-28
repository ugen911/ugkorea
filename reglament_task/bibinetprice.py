import os
import pandas as pd
from ugkorea.db.database import get_db_engine


def export_price_to_yandexdisk_if_exists():
    # Путь к папке на Яндекс.Диске
    yandex_dir = "C:\\Users\\evgen\\YandexDisk\\ЮК\\"

    # Проверяем, существует ли указанный путь
    if not os.path.exists(yandex_dir):
        return  # Завершаем, если пути нет

    # 1) Подключаемся к базе
    engine = get_db_engine()

    # 2) Загружаем таблицы
    df_nomenklaturaold = pd.read_sql(
        "SELECT kod, artikul, proizvoditel, naimenovanie, edizm FROM nomenklaturaold",
        engine,
    )
    df_priceold = pd.read_sql("SELECT kod, tsenarozn FROM priceold", engine)
    df_stockold = pd.read_sql("SELECT kod, osnsklad FROM stockold", engine)
    df_photoadress = pd.read_sql("SELECT kod, adress FROM photoadress", engine)
    df_nomenklaturaprimenjaemost = pd.read_sql(
        "SELECT kod, model FROM nomenklaturaprimenjaemost", engine
    )

    # 3) Обработка и группировка (применяемость)
    df_nomenklaturaprimenjaemost_grouped = (
        df_nomenklaturaprimenjaemost.groupby("kod")["model"]
        .apply(lambda x: ", ".join(filter(None, x)))
        .reset_index()
    )

    # 4) Очистка лишних пробелов по ключу kod
    for df_ in [
        df_nomenklaturaold,
        df_priceold,
        df_stockold,
        df_photoadress,
        df_nomenklaturaprimenjaemost_grouped,
    ]:
        df_["kod"] = df_["kod"].str.strip()

    # 5) Преобразование цен в float
    df_priceold["tsenarozn"] = df_priceold["tsenarozn"].astype(float)

    # 6) Объединение всех датафреймов в один
    df_merged = (
        df_nomenklaturaold.merge(df_priceold, on="kod", how="left")
        .merge(df_stockold, on="kod", how="left")
        .merge(df_photoadress, on="kod", how="left")
        .merge(df_nomenklaturaprimenjaemost_grouped, on="kod", how="left")
    )

    # 7) Создаём колонку «Наличие» и фильтруем только «В наличии»
    df_merged["Наличие"] = df_merged["osnsklad"].apply(
        lambda x: "В наличии" if (pd.notnull(x) and x > 0) else "Отсутствует"
    )
    df_instock = df_merged[df_merged["Наличие"] == "В наличии"].copy()

    # 8) Делим на «б/у» и «не б/у» по столбцу naimenovanie
    #    (Важно: используем исходное поле 'naimenovanie', т.к. переименования пойдут отдельно)
    mask_bu = df_instock["naimenovanie"].str.lower().str.contains("б/у", na=False)
    df_bu = df_instock[mask_bu].copy()  # только б/у
    df_non_bu = df_instock[~mask_bu].copy()  # без б/у

    # ===================================================
    #       ЧАСТЬ 1. Формирование bibinet.csv (без б/у)
    # ===================================================
    #    1.1) Создаём / переименовываем колонки, как было раньше
    df_non_bu.rename(
        columns={
            "naimenovanie": "Наименование товара *",
            "artikul": "OEM код / Код производителя *",
            "proizvoditel": "Производитель",
            "tsenarozn": "Стоимость",
            "model": "Примечание (марка, модель, кузов, двигатель, применимость)",
            "adress": "Фотографии",
            "osnsklad": "Количество",
        },
        inplace=True,
    )

    #    1.2) Добавляем пустые колонки «Марка», «Модель», «Кузов» и т.д.
    df_non_bu["Марка"] = ""
    df_non_bu["Модель"] = ""
    df_non_bu["Кузов"] = ""
    df_non_bu["Двигатель"] = ""
    df_non_bu["Перед/Зад"] = ""
    df_non_bu["Право/Лево"] = ""
    df_non_bu["Верх/Низ"] = ""

    #    1.3) Убираем запятую в конце «Фотографии», если есть
    df_non_bu["Фотографии"] = df_non_bu["Фотографии"].fillna("").str.rstrip(",")

    #    1.4) Определяем порядок столбцов
    cols_non_bu = [
        "Наименование товара *",
        "OEM код / Код производителя *",
        "Марка",
        "Модель",
        "Кузов",
        "Двигатель",
        "Перед/Зад",
        "Право/Лево",
        "Верх/Низ",
        "Производитель",
        "Стоимость",
        "Примечание (марка, модель, кузов, двигатель, применимость)",
        "Фотографии",
        "Количество",
        "Наличие",
    ]
    df_non_bu = df_non_bu[cols_non_bu]

    # ===================================================
    #       ЧАСТЬ 2. Формирование bibinetrepair.csv (б/у)
    # ===================================================
    #  В df_bu оставляем и переименовываем столбцы по новому ТЗ:
    #    - Оставляем kod => переименовываем в «Инвентарный номер»
    #    - naimenovanie => «Наименование»
    #    - tsenarozn => «Стоимость»
    #    - adress => «Фотографии»
    #    - model => «Применимость»
    #    - Добавляем пустые: Марка, Модель, Кузов, Номер оптики, Двигатель, Перед/Зад,
    #      Право/Лево, Верх/Низ, Цвет, Год, Примечание
    #    - В «Наличие» ставим «В наличии» (принудительно)

    df_bu.rename(
        columns={
            "kod": "Инвентарный номер",
            "naimenovanie": "Наименование",
            "tsenarozn": "Стоимость",
            "adress": "Фотографии",
            "model": "Применимость",
            # «Наличие» мы позже выставим вручную, чтобы точно соответствовать требованиям
        },
        inplace=True,
    )

    #    2.1) Добавляем пустые колонки
    df_bu["Марка"] = ""
    df_bu["Модель"] = ""
    df_bu["Кузов"] = ""
    df_bu["Номер оптики"] = ""
    df_bu["Двигатель"] = ""
    df_bu["Перед/Зад"] = ""
    df_bu["Право/Лево"] = ""
    df_bu["Верх/Низ"] = ""
    df_bu["Цвет"] = ""
    df_bu["Год"] = ""
    df_bu["Примечание"] = ""  # пустая

    #    2.2) В «Наличие» ставим «В наличии» для всех
    df_bu["Наличие"] = "В наличии"

    #    2.3) Убираем запятую в конце «Фотографии», если есть
    df_bu["Фотографии"] = df_bu["Фотографии"].fillna("").str.rstrip(",")

    #    2.4) Формируем порядок столбцов для b/у
    cols_bu = [
        "Инвентарный номер",
        "Наименование",
        "Марка",
        "Модель",
        "Кузов",
        "Номер оптики",
        "Двигатель",
        "Перед/Зад",
        "Право/Лево",
        "Верх/Низ",
        "Цвет",
        "Год",
        "Стоимость",
        "Примечание",
        "Применимость",
        "Фотографии",
        "Наличие",
    ]
    df_bu = df_bu[cols_bu]

    # 9) Сохраняем оба результата в CSV в одной папке
    csv_path_non_bu = os.path.join(yandex_dir, "bibinet.csv")
    csv_path_bu = os.path.join(yandex_dir, "bibinetrepair.csv")

    # С кодировкой Windows-1251 и разделителем ";"
    df_non_bu.to_csv(csv_path_non_bu, index=False, sep=";", encoding="windows-1251")
    df_bu.to_csv(csv_path_bu, index=False, sep=";", encoding="windows-1251")


# Вызов функции при запуске скрипта
if __name__ == "__main__":
    export_price_to_yandexdisk_if_exists()
