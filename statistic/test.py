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

# Объединение с sales_data по полю 'kod' и добавление колонок 'gruppa_analogov' и 'type_detail'
sales_data = pd.merge(sales_data, union_data[['kod', 'gruppa_analogov', 'type_detail']], on='kod', how='left')

# Извлечение года и месяца из колонки 'year_month'
sales_data['year'] = sales_data['year_month'].dt.year
sales_data['month'] = sales_data['year_month'].dt.month

# Создание колонки с датой на основе year и month
sales_data['date'] = pd.to_datetime(sales_data[['year', 'month']].assign(day=1))

def process_sales_analysis(sales_data, union_data):
    current_date = datetime.now()
    one_year_ago = current_date - timedelta(days=365)
    three_months_ago = current_date - timedelta(days=90)
    months_to_check = [8, 9, 10]
    current_year = current_date.year

    # Filter for relevant sales data based on the XYZ analysis and type details
    relevant_sales_data = sales_data[sales_data['type_detail'].isin(union_data[union_data['XYZ'].isin(['X', 'Y'])]['type_detail'].unique())]

    # Group by 'type_detail' and calculate required metrics in one step
    grouped = relevant_sales_data.groupby(['type_detail', 'year', 'month']).agg(
        total_sales_sum=('total_sales', 'sum'),
        balance_sum=('balance', 'sum'),
        mean_sales_sum=('total_sales', 'mean')
    ).reset_index()

    # Filter out groups with insufficient data
    valid_groups = grouped.groupby('type_detail').filter(
        lambda g: ((g['total_sales_sum'] != 0) | (g['balance_sum'] != 0)).mean() > 0.1
    )

    # Calculate monthly averages and identify top and low sales months
    monthly_averages = valid_groups.groupby(['type_detail', 'month']).agg(
        avg_monthly_sales=('mean_sales_sum', 'mean')
    ).reset_index()

    # Calculate top and low sales months in one step
    top_low_sales = monthly_averages.groupby('type_detail').agg(
        mean_type_sales=('avg_monthly_sales', 'mean'),
        top_type_sales_1=('month', lambda x: sorted(x.nlargest(3))[0] if len(x) > 0 else None),
        top_type_sales_2=('month', lambda x: sorted(x.nlargest(3))[1] if len(x) > 1 else None),
        top_type_sales_3=('month', lambda x: sorted(x.nlargest(3))[2] if len(x) > 2 else None),
        low_type_sales_1=('month', lambda x: sorted(x.nsmallest(3))[0] if len(x) > 0 else None),
        low_type_sales_2=('month', lambda x: sorted(x.nsmallest(3))[1] if len(x) > 1 else None),
        low_type_sales_3=('month', lambda x: sorted(x.nsmallest(3))[2] if len(x) > 2 else None)
    ).reset_index()

    # Merge back to the union_data
    updated_union_data = pd.merge(union_data, top_low_sales, on='type_detail', how='left')

    # Calculate sales trend comparison
    overall_avg = monthly_averages.groupby('type_detail')['avg_monthly_sales'].mean().reset_index()
    overall_avg.rename(columns={'avg_monthly_sales': 'overall_avg_sales'}, inplace=True)

    monthly_vs_overall_comparison = pd.merge(monthly_averages, overall_avg, on='type_detail', how='left')
    monthly_vs_overall_comparison['sales_deviation'] = monthly_vs_overall_comparison['avg_monthly_sales'] - monthly_vs_overall_comparison['overall_avg_sales']
    monthly_vs_overall_comparison['sales_trend'] = np.where(monthly_vs_overall_comparison['sales_deviation'] > 0, 'Рост', 'Падение')

    monthly_vs_overall_comparison = monthly_vs_overall_comparison[['type_detail', 'month', 'sales_trend', 'sales_deviation']]

    # Calculate total sales for last 12 and 3 months
    sales_summary = sales_data.groupby('kod').agg(
        sales_last_12_months=('total_sales', lambda x: x[(sales_data['date'] >= one_year_ago) & (sales_data['date'] < current_date)].sum()),
        sales_last_3_months=('total_sales', lambda x: x[(sales_data['date'] >= three_months_ago) & (sales_data['date'] < current_date)].sum())
    ).reset_index()

    # Calculate top month sales based on previously identified top sales months
    top_sales_cols = ['top_type_sales_1', 'top_type_sales_2', 'top_type_sales_3']
    for i, col in enumerate(top_sales_cols, start=1):
        sales_summary[f'top_month_sales_{i}'] = updated_union_data.apply(
            lambda row: sales_data[(sales_data['kod'] == row['kod']) & (sales_data['month'] == row[col]) & 
            (sales_data['date'] >= one_year_ago) & (sales_data['date'] < current_date)]['total_sales'].sum() 
            if not pd.isna(row[col]) else None, axis=1
        )

    # Pivot and merge year-over-year sales for August, September, October
    filtered_sales_data = sales_data[(sales_data['month'].isin(months_to_check)) & 
                                     (sales_data['year'].isin([current_year - 1, current_year - 2, current_year - 3]))]

    pivoted_sales = filtered_sales_data.pivot_table(index='kod', columns='year', values='total_sales', aggfunc='sum', fill_value=0).reset_index()

    for year in [current_year - 1, current_year - 2, current_year - 3]:
        if year not in pivoted_sales.columns:
            pivoted_sales[year] = None

    sales_summary = sales_summary.merge(pivoted_sales[['kod', current_year - 1, current_year - 2, current_year - 3]], on='kod', how='left')

    sales_summary.rename(columns={
        current_year - 1: 'sales_aug_sep_oct_2023',
        current_year - 2: 'sales_aug_sep_oct_2022',
        current_year - 3: 'sales_aug_sep_oct_2021'
    }, inplace=True)

    return updated_union_data, monthly_vs_overall_comparison, sales_summary

updated_union_data, monthly_vs_overall_comparison, sales_summary = process_sales_analysis(sales_data, union_data)

print(updated_union_data.head())
print(monthly_vs_overall_comparison.head())
print(sales_summary.head())
