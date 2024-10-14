from ugkorea.reprising.basefiltering import prepare_filtered_data
from ugkorea.db.database import get_db_engine

engine = get_db_engine()
filtered_df, priceendmonth, salespivot, stockendmonth, suppliespivot, postuplenija= prepare_filtered_data(engine)
filtered_df.to_csv("filtered_df.csv")
priceendmonth.to_csv("priceendmonth.csv")
salespivot.to_csv("salespivot.csv")
stockendmonth.to_csv("stockendmonth.csv")
suppliespivot.to_csv("suppliespivot.csv")
postuplenija.to_csv("postuplenija.csv")
