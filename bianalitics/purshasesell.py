from ugkorea.db.database import get_db_engine
from ugkorea.bianalitics.generalloaddata import load_foranalitics_data
from ugkorea.bianalitics.korrectirovki import process_corrections_and_supplies
import pandas as pd

engine = get_db_engine()

nomenklatura, postuplenija_df, prodaja, korrektirovki_df = load_foranalitics_data(
    engine=engine
)

postuplenija = process_corrections_and_supplies(korrektirovki_df, postuplenija_df)

postuplenija.to_excel("korrect.xlsx")

postuplenija_df.to_excel("postuplenija_df.xlsx")
