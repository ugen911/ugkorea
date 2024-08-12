import logging
from ugkorea.statistic.loaddata import get_final_data, load_and_process_data, perform_abc_xyz_analysis
from ugkorea.db.database import get_db_engine
import pandas as pd

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

    # Группировка по type_detail, year и month и суммирование total_sales и balance
    grouped = filtered_sales_data.groupby(['type_detail', 'year', 'month']).agg(
        total_sales_sum=('total_sales', 'sum'),
        balance_sum=('balance', 'sum')
    ).reset_index()


    return grouped

# Вызов функции с вашими данными
result = analyze_sales_by_type(sales_data, union_data)

# Вывод результатов
print(result)
union_data.to_csv('analiz.csv')