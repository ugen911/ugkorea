
import logging
from ugkorea.statistic.loaddata import get_final_data, load_and_process_data, perform_abc_xyz_analysis
from ugkorea.db.database import get_db_engine
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

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


def calculate_sales_metrics(sales_data: pd.DataFrame, reference_date: datetime = datetime(2024, 8, 12)) -> pd.DataFrame:
    # Convert reference date to a Period object
    current_period = pd.Period(reference_date.strftime('%Y-%m'), freq='M')
    
    # Calculate relevant periods directly as Period objects
    twelve_months_ago = current_period - 12
    three_months_ago = current_period - 3

    # Periods for August, September, and October of previous years
    periods_last_year = [pd.Period(f"{reference_date.year - 1}-{month:02d}", freq='M') for month in range(8, 11)]
    periods_two_years_ago = [period - 12 for period in periods_last_year]
    periods_three_years_ago = [period - 24 for period in periods_last_year]

    # Initialize an empty DataFrame to store the results
    metrics = []

    # Group by 'kod' and calculate the required sums in a single pass
    for kod, group in sales_data.groupby('kod'):
        total_sales_last_12 = group[(group['year_month'] > twelve_months_ago) & (group['year_month'] <= current_period)]['total_sales'].sum()
        total_sales_last_3 = group[(group['year_month'] > three_months_ago) & (group['year_month'] <= current_period)]['total_sales'].sum()

        sum_sales_last_year = group[group['year_month'].isin(periods_last_year)]['total_sales'].sum()
        sum_sales_two_years_ago = group[group['year_month'].isin(periods_two_years_ago)]['total_sales'].sum()
        sum_sales_three_years_ago = group[group['year_month'].isin(periods_three_years_ago)]['total_sales'].sum()

        metrics.append({
            'kod': kod,
            'total_sales_last_12_months': total_sales_last_12,
            'total_sales_last_3_months': total_sales_last_3,
            'sum_sales_last_year': sum_sales_last_year,
            'sum_sales_two_years_ago': sum_sales_two_years_ago,
            'sum_sales_three_years_ago': sum_sales_three_years_ago,
        })

    # Convert the results into a DataFrame
    sales_metrics = pd.DataFrame(metrics)
    
    return sales_metrics

# Use the function to calculate metrics
sales_metrics = calculate_sales_metrics(sales_data)

# Display the results
print(sales_metrics.head())
