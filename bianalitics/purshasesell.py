from ugkorea.db.database import get_db_engine
from ugkorea.bianalitics.generalloaddata import load_foranalitics_data
import pandas as pd

engine = get_db_engine()

filtered_df, postuplenija_df, prodaja_df, korrektirovki_df = load_foranalitics_data(
    engine=engine
)


print(prodaja_df.to_excel("prodaja_df.xlsx"))
print(prodaja_df.info())
print(korrektirovki_df.to_excel("korrektirovki_df.xlsx"))
print(korrektirovki_df.info())
