from ugkorea.db.database import get_db_engine
from ugkorea.bianalitics.generalloaddata import load_foranalitics_data
import pandas as pd

engine = get_db_engine()

filtered_df, postuplenija_df, prodaja_df = load_foranalitics_data(engine=engine)


print(prodaja_df.to_excel("prodaja_df.xlsx"))
print(prodaja_df.info())
