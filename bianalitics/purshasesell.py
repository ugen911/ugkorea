from ugkorea.db.database import get_db_engine
from ugkorea.bianalitics.generalloaddata import load_foranalitics_data
from ugkorea.bianalitics.korrectirovki import process_corrections_and_supplies
from ugkorea.bianalitics.getgroupdoc import get_saildocument
from ugkorea.bianalitics.call_log import update_call_journal, fetch_and_filter_call_log
import pandas as pd

# Подключение к базе данных
engine = get_db_engine()

# Загрузка данных
(
    vyrabotka_df,
    filtered_df,
    postuplenija_df,
    prodaja_df,
    korrektirovki_df,
    zakaz_naryad,
) = load_foranalitics_data(engine=engine)

# Обработка поступлений
postuplenija = process_corrections_and_supplies(korrektirovki_df, postuplenija_df)
saildoc = get_saildocument(prodaja_df)

update_call_journal(engine=engine)
call_log = fetch_and_filter_call_log(engine)

# Сохранение файлов в указанный путь без первой строки (имён колонок)
postuplenija.to_excel(
    r"C:\Users\evgen\YandexDisk\ЮК\postuplenija.xlsx", index=False, header=False
)

prodaja_df.to_excel(
    r"C:\Users\evgen\YandexDisk\ЮК\prodaja.xlsx", index=False, header=False
)

saildoc.to_excel(
    r"C:\Users\evgen\YandexDisk\ЮК\saildoc.xlsx", index=False, header=False
)

zakaz_naryad.to_excel(
    r"C:\Users\evgen\YandexDisk\ЮК\zakaz_naryad.xlsx", index=False, header=False
)

filtered_df.to_excel(
    r"C:\Users\evgen\YandexDisk\ЮК\nomenklatura.xlsx", index=False, header=False
)

call_log.to_excel(
    r"C:\Users\evgen\YandexDisk\ЮК\zvonki.xlsx", index=False, header=False
)

vyrabotka_df.to_excel(r"C:\Users\evgen\YandexDisk\ЮК\vyrabotka.xlsx", index=False, header=False
)
