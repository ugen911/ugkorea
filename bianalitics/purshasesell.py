from ugkorea.db.database import get_db_engine
from ugkorea.bianalitics.generalloaddata import load_foranalitics_data
from ugkorea.bianalitics.korrectirovki import process_corrections_and_supplies
import pandas as pd

engine = get_db_engine()

filtered_df, postuplenija_df, prodaja_df, korrektirovki_df = load_foranalitics_data(
    engine=engine
)

korrektirovki = process_corrections_and_supplies(korrektirovki_df, postuplenija_df)

korrektirovki.to_excel('korrect.xlsx')

postuplenija_df.to_excel("postuplenija_df.xlsx")
