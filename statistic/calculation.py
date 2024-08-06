import pandas as pd
from ugkorea.statistic.loaddata import load_and_process_data

# Загрузка данных

nomenklatura_merged, stockendmonth, priceendmonth, postuplenija, prodazhi = load_and_process_data()



priceendmonth.to_csv('priceendmonth.csv')