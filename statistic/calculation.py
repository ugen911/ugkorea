import logging
from ugkorea.statistic.loaddata import get_final_data, load_and_process_data, perform_abc_xyz_analysis
from ugkorea.db.database import get_db_engine
import pandas as pd
import numpy as np
from datetime import datetime
# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

engine = get_db_engine()

# Загрузка данных с использованием функций
sales_data, nomenklatura_ml = get_final_data(engine)
nomenklatura_merged, stockendmonth, priceendmonth, postuplenija, prodazhi = load_and_process_data(engine)
abc_xyz_analysis = perform_abc_xyz_analysis(engine)

# Объединение таблиц по полю 'kod'
merged_data = pd.merge(nomenklatura_ml, nomenklatura_merged[['kod', 'naimenovanie', 'artikul', 'edizm', 'datasozdanija', 'osnsklad', 'tsenarozn']], on='kod', how='left')
union_data = pd.merge(merged_data, abc_xyz_analysis, on='kod', how='left')

# Объединение с sales_data по полю 'kod' и добавление колонок 'gruppa_analogov' и 'type_detail'
sales_data = pd.merge(sales_data, union_data[['kod', 'gruppa_analogov', 'type_detail']], on='kod', how='left')

# Извлечение года и месяца из колонки 'year_month'
sales_data['year'] = sales_data['year_month'].dt.year
sales_data['month'] = sales_data['year_month'].dt.month




def analyze_sales_by_type(sales_data, union_data):
    # Определение всех type_detail, которые содержат хотя бы один kod с XYZ равным X или Y
    relevant_type_details = union_data[union_data['XYZ'].isin(['X', 'Y'])]['type_detail'].unique()
    
    # Фильтрация sales_data, чтобы оставить все записи, где type_detail совпадает с найденными выше
    filtered_sales_data = sales_data[sales_data['type_detail'].isin(relevant_type_details)]

    # Группировка по type_detail, year и month и суммирование total_sales и balance, добавляем расчет средней
    grouped = filtered_sales_data.groupby(['type_detail', 'year', 'month']).agg(
        total_sales_sum=('total_sales', 'sum'),
        balance_sum=('balance', 'sum'),
        mean_sales_sum=('total_sales', 'mean')
    ).reset_index()

    # Фильтрация групп, где 90% строк имеют total_sales_sum и balance_sum одновременно равные нулю
    def filter_groups(group):
        zero_sales_and_balance = (group['total_sales_sum'] == 0) & (group['balance_sum'] == 0)
        return zero_sales_and_balance.mean() < 0.9

    valid_groups = grouped.groupby('type_detail').filter(filter_groups)

    # Рассчет средних продаж по месяцам для каждого type_detail
    monthly_averages = valid_groups.groupby(['type_detail', 'month']).agg(
        avg_monthly_sales=('mean_sales_sum', 'mean')
    ).reset_index()

    # Рассчет медианных продаж и выбор месяцев с максимальными и минимальными средними продажами
    result = monthly_averages.groupby('type_detail').apply(lambda g: pd.Series({
        'mean_type_sales': g['avg_monthly_sales'].mean(),
        'top_type_sales': ', '.join(sorted(g.nlargest(3, 'avg_monthly_sales')['month'].apply(lambda x: f"{int(x):02d}"))),
        'low_type_sales': ', '.join(sorted(g.nsmallest(3, 'avg_monthly_sales')['month'].apply(lambda x: f"{int(x):02d}")))
    })).reset_index()

    # Округление mean_type_sales по заданным правилам
    result['mean_type_sales'] = result['mean_type_sales'].apply(lambda x: np.ceil(x) if x > int(x) else int(x))

    # Объединение данных result с union_data на основе поля 'type_detail'
    updated_union_data = pd.merge(union_data, result, on='type_detail', how='left')

    return updated_union_data

# Вызов функции с вашими данными
updated_union_data = analyze_sales_by_type(sales_data, union_data)

# Вывод результатов
print(updated_union_data.head())





