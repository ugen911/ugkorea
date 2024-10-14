from ugkorea.db.database import get_db_engine
from ugkorea.reprising.loaddata import load_forreprice_data
from ugkorea.reprising.partofservice import load_and_process_data as prodazhi
from ugkorea.reprising.partofservice import calculate_median_service_percent
from ugkorea.reprising.getkonkurent import autocoreec_data
from ugkorea.statistic.loaddata import perform_abc_xyz_analysis


engine = get_db_engine()


# Загружаем данные с ценами и остатками статистикой
(   
    filtered_df,
    priceendmonth_filtered,
    salespivot_filtered,
    stockendmonth_filtered,
    suppliespivot_filtered,
    postuplenija_filtered,
) = load_forreprice_data(engine)


# Доля медианных продаж каждого kod в сервисе
df = prodazhi(engine)
sales_share_df = calculate_median_service_percent(df)

# Загружаем данные цены автокорейца
konkurent = autocoreec_data(engine)

# ABC анализ 

abc = perform_abc_xyz_analysis(engine)

filtered_df = filtered_df.merge(sales_share_df ,on='kod', how='left')
filtered_df = filtered_df.merge(konkurent, on='kod', how='left')
filtered_df = filtered_df.merge(abc, on='kod', how='left')
