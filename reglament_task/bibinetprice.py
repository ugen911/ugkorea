import os
import pandas as pd
from ugkorea.db.database import get_db_engine


def export_price_to_yandexdisk_if_exists():
    # Путь к папке на Яндекс.Диске
    yandex_dir = "C:\\Users\\evgen\\YandexDisk\\ЮК\\"

    # Проверяем, существует ли указанный путь
    if not os.path.exists(yandex_dir):
        # Если путь не найден, завершаем функцию
        return

    # Получаем подключение к базе данных
    engine = get_db_engine()

    # Загружаем данные в DataFrame
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

    # Группировка применяемости по 'kod'
    df_nomenklaturaprimenjaemost_grouped = (
        df_nomenklaturaprimenjaemost.groupby("kod")["model"]
        .apply(lambda x: ", ".join(filter(None, x)))
        .reset_index()
    )

    # Очистка данных от лишних пробелов
    df_nomenklaturaold["kod"] = df_nomenklaturaold["kod"].str.strip()
    df_priceold["kod"] = df_priceold["kod"].str.strip()
    df_stockold["kod"] = df_stockold["kod"].str.strip()
    df_photoadress["kod"] = df_photoadress["kod"].str.strip()
    df_nomenklaturaprimenjaemost_grouped["kod"] = df_nomenklaturaprimenjaemost_grouped[
        "kod"
    ].str.strip()

    # Преобразование tsenarozn во float
    df_priceold["tsenarozn"] = df_priceold["tsenarozn"].astype(float)

    # Объединение DataFrame
    df_final = df_nomenklaturaold.merge(df_priceold, on="kod", how="left")
    df_final = df_final.merge(df_stockold, on="kod", how="left")
    df_final = df_final.merge(df_photoadress, on="kod", how="left")
    df_final = df_final.merge(
        df_nomenklaturaprimenjaemost_grouped, on="kod", how="left"
    )

    # Создание колонки "Наличие" на основе osnsklad
    df_final["Наличие"] = df_final["osnsklad"].apply(
        lambda x: "В наличии" if x and x > 0 else "Отсутствует"
    )

    # Оставляем только те записи, где 'В наличии'
    df_final = df_final[df_final["Наличие"] == "В наличии"]

    # Добавляем пустые колонки
    df_final["Марка"] = ""
    df_final["Модель"] = ""
    df_final["Кузов"] = ""
    df_final["Двигатель"] = ""
    df_final["Перед/Зад"] = ""
    df_final["Право/Лево"] = ""
    df_final["Верх/Низ"] = ""

    # Переименовываем нужные колонки
    df_final.rename(
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


    # Формируем список нужных колонок в правильном порядке
    cols_to_keep = [
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

    # Оставляем только нужные колонки
    df_final = df_final[cols_to_keep]

    # Разделяем на 2 набора:
    # 1) Без "б/у" (bibinet.csv)
    # 2) Только "б/у" (bibinetrepair.csv)

    # Создаём маску, где в "Наименование товара *" есть "б/у" (в любом регистре)
    mask_bu = (
        df_final["Наименование товара *"].str.lower().str.contains("б/у", na=False)
    )

    df_non_bu = df_final[~mask_bu]  # товары, не содержащие "б/у"
    df_bu = df_final[mask_bu]  # товары, содержащие "б/у"

    # Сохраняем результат в CSV (кодировка Windows-1251, разделитель ";")
    # 1) bibinet.csv – без б/у
    csv_path_main = os.path.join(yandex_dir, "bibinet.csv")
    df_non_bu.to_csv(csv_path_main, index=False, sep=";", encoding="windows-1251")

    # 2) bibinetrepair.csv – только б/у
    csv_path_repair = os.path.join(yandex_dir, "bibinetrepair.csv")
    df_bu.to_csv(csv_path_repair, index=False, sep=";", encoding="windows-1251")


# Вызов функции при запуске скрипта
if __name__ == "__main__":
    export_price_to_yandexdisk_if_exists()
