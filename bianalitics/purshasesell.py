from ugkorea.db.database import get_db_engine
from ugkorea.bianalitics.generalloaddata import load_foranalitics_data
from ugkorea.bianalitics.korrectirovki import process_corrections_and_supplies
from ugkorea.bianalitics.getgroupdoc import get_saildocument
import pandas as pd

# Подключение к базе данных
engine = get_db_engine()

# Загрузка данных
nomenklatura, postuplenija_df, prodaja, korrektirovki_df, zakaz_naryad = (
    load_foranalitics_data(engine=engine)
)

# Обработка поступлений
postuplenija = process_corrections_and_supplies(korrektirovki_df, postuplenija_df)
saildoc = get_saildocument(prodaja)

# Сохранение файлов в указанный путь без первой строки (имён колонок)
postuplenija.to_excel(
    r"C:\Users\evgen\YandexDisk\ЮК\postuplenija.xlsx", index=False, header=False
)

prodaja.to_excel(
    r"C:\Users\evgen\YandexDisk\ЮК\prodaja.xlsx", index=False, header=False
)

saildoc.to_excel(
    r"C:\Users\evgen\YandexDisk\ЮК\saildoc.xlsx", index=False, header=False
)

zakaz_naryad.to_excel(
    r"C:\Users\evgen\YandexDisk\ЮК\zakaz_naryad.xlsx", index=False, header=False
)


nomenklatura.to_excel(
    r"C:\Users\evgen\YandexDisk\ЮК\nomenklatura.xlsx", index=False, header=False
)