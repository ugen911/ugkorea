from ugkorea.db.database import get_db_engine
from ugkorea.reprising.loaddata import load_forreprice_data
from ugkorea.reprising.partofservice import load_and_process_data as prodazhi
from ugkorea.reprising.partofservice import calculate_median_service_percent
from ugkorea.reprising.getkonkurent import autocoreec_data
from ugkorea.statistic.loaddata import perform_abc_xyz_analysis
from ugkorea.reprising.lastpostuplenie import (
    load_postuplenija_data,
    calculate_last_postuplenija,
)
from ugkorea.reprising.getmedianprice import get_median_price_by_kod
from ugkorea.reprising.loaddata import load_brands_and_text

def prepare_filtered_data(engine):
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

    # Вычисляем последнюю дату поступления
    postuplenija_df = load_postuplenija_data(engine)
    last_postuplenija_df = calculate_last_postuplenija(postuplenija_df)

    # Получаем медианную цену с проценки
    medianprice = get_median_price_by_kod(engine)

    # Объединяем данные
    filtered_df = filtered_df.merge(sales_share_df, on='kod', how='left')
    filtered_df = filtered_df.merge(konkurent, on='kod', how='left')
    filtered_df = filtered_df.merge(abc, on='kod', how='left')
    filtered_df = filtered_df.merge(last_postuplenija_df, on="kod", how="left")
    filtered_df = filtered_df.merge(medianprice, on="kod", how="left")

    producers_to_exclude, phrases_to_exclude = load_brands_and_text()

    # Убираем строки, где 'naimenovanie' содержит указанные фразы
    
    phrases_to_exclude
    pattern = '|'.join(phrases_to_exclude)
    filtered_df = filtered_df[~filtered_df['naimenovanie'].str.contains(pattern, case=False, na=False)]

    # Убираем строки, где 'proizvoditel' равен одному из указанных производителей
    

    filtered_df = filtered_df[~filtered_df['proizvoditel'].str.contains('|'.join(producers_to_exclude), case=False, na=False)]

    return filtered_df, priceendmonth_filtered, salespivot_filtered, stockendmonth_filtered, suppliespivot_filtered, postuplenija_filtered
