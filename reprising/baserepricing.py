import logging
from ugkorea.statistic.loaddata import get_final_data, load_and_process_data, perform_abc_xyz_analysis
from ugkorea.statistic.calculation import load_additional_data
from ugkorea.db.database import get_db_engine
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


if __name__ == "__main__":
    engine = get_db_engine()

    # Загрузка данных
    sales_data, nomenklatura_ml = get_final_data(engine)
    nomenklatura_merged, stockendmonth, priceendmonth, postuplenija, prodazhi = load_and_process_data(engine)
    abc_xyz_analysis = perform_abc_xyz_analysis(engine)

    # Объединение данных
    merged_data = pd.merge(nomenklatura_ml, nomenklatura_merged[['kod', 'naimenovanie', 'artikul', 'edizm', 'datasozdanija', 'osnsklad', 'tsenarozn']], on='kod', how='left')
    union_data = pd.merge(merged_data, abc_xyz_analysis, on='kod', how='left')
