
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



def calculate_sales_metrics(sales_data: pd.DataFrame, union_data: pd.DataFrame) -> pd.DataFrame:
    # Use the current date as the reference date
    reference_date = datetime.now()
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
        # Fill NaN values in 'total_sales' and 'balance' with 0 for sales calculations
        group['total_sales_filled'] = group['total_sales'].fillna(0)
        group['balance_filled'] = group['balance'].fillna(0)

        # Total sales calculations considering all months
        total_sales_last_12 = group[(group['year_month'] > twelve_months_ago) & (group['year_month'] <= current_period)]['total_sales_filled'].sum()
        total_sales_last_3 = group[(group['year_month'] > three_months_ago) & (group['year_month'] <= current_period)]['total_sales_filled'].sum()

        sum_sales_last_year = group[group['year_month'].isin(periods_last_year)]['total_sales_filled'].sum()
        sum_sales_two_years_ago = group[group['year_month'].isin(periods_two_years_ago)]['total_sales_filled'].sum()
        sum_sales_three_years_ago = group[group['year_month'].isin(periods_three_years_ago)]['total_sales_filled'].sum()

        # Filter for the last 12 months and include months where there were sales or balance was > 0
        filtered_last_12_months = group[(group['year_month'] > twelve_months_ago) & (group['year_month'] <= current_period) & 
                                        ((group['balance_filled'] > 0) | (group['total_sales_filled'] > 0))].copy()

        # Calculate mean and standard deviation of total_sales for the filtered months
        if not filtered_last_12_months.empty:
            mean_sales = filtered_last_12_months['total_sales_filled'].sum() / filtered_last_12_months.shape[0]
            std_sales = filtered_last_12_months['total_sales_filled'].std()
        else:
            mean_sales = std_sales = 0

        # Count months without sales over the entire period in sales_data
        if group['total_sales_filled'].sum() == 0:
            # If no sales at all, set months_without_sales to the number of unique months
            months_without_sales = group['year_month'].nunique()
        elif group[(group['balance_filled'] > 0) & ((group['total_sales'].isna()) | (group['total_sales_filled'] <= 0))].empty:
            # If there were sales in every month, set months_without_sales to 0
            months_without_sales = 0
        else:
            # Otherwise, count months without sales where balance > 0 and sales are <= 0 or NaN
            months_without_sales = group[
                (group['balance_filled'] > 0) & ((group['total_sales'].isna()) | (group['total_sales_filled'] <= 0))
            ].shape[0]

        # Calculate the number of months since the last sale
        last_sale_period = group[group['total_sales_filled'] > 0]['year_month'].max()
        if pd.notna(last_sale_period):
            months_since_last_sale = current_period - last_sale_period
            months_since_last_sale = months_since_last_sale.n
        else:
            # If no sales were found, set months_since_last_sale to the total number of months in the observation
            months_since_last_sale = group['year_month'].nunique()

        metrics.append({
            'kod': kod,
            'total_sales_last_12_months': total_sales_last_12,
            'total_sales_last_3_months': total_sales_last_3,
            'sum_sales_last_year': sum_sales_last_year,
            'sum_sales_two_years_ago': sum_sales_two_years_ago,
            'sum_sales_three_years_ago': sum_sales_three_years_ago,
            'mean_sales_last_12_months': mean_sales,
            'std_sales_last_12_months': std_sales,
            'months_without_sales': months_without_sales,
            'months_since_last_sale': months_since_last_sale
        })

    # Convert the results into a DataFrame
    sales_metrics = pd.DataFrame(metrics)
    
    # Merge with union_data
    union_data = union_data.merge(sales_metrics, on='kod', how='left')

    # Calculate 'min_stock' based on the given logic
    def calculate_min_stock(row):
        if pd.isna(row['mean_sales_last_12_months']) or pd.isna(row['std_sales_last_12_months']):
            return 0  # Return 0 if no data for calculations

        if row['ABC'] in ['A', 'A1']:
            return round((row['mean_sales_last_12_months'] * 1 + row['std_sales_last_12_months']) + 0.49)
        elif row['ABC'] == 'B':
            return round((row['mean_sales_last_12_months'] * 1 + row['std_sales_last_12_months']) + 0.49)
        elif row['mean_sales_last_12_months'] == 0:
            return 1
        else:
            return round((row['mean_sales_last_12_months'] + row['std_sales_last_12_months']) + 0.49)

    union_data['min_stock'] = union_data.apply(calculate_min_stock, axis=1)

    # Adjust 'min_stock' based on sales data from the previous year
    def adjust_min_stock(row):
        current_month = current_period.month
        last_year_same_month = pd.Period(f"{current_period.year - 1}-{current_month:02d}", freq='M')
        last_year_month_minus_1 = last_year_same_month - 1
        last_year_month_plus_1 = last_year_same_month + 1

        # Find sales for the specified periods
        relevant_sales = sales_data[
            (sales_data['kod'] == row['kod']) &
            (sales_data['year_month'].isin([last_year_same_month, last_year_month_minus_1, last_year_month_plus_1]))
        ]['total_sales'].max()
        
        if pd.notna(relevant_sales) and relevant_sales > row['min_stock']:
            return relevant_sales
        return row['min_stock']

    union_data['min_stock'] = union_data.apply(adjust_min_stock, axis=1)

    # Further adjust 'min_stock' if 'datasozdanija' is in the current or previous month
    def adjust_min_stock_for_creation_date(row):
        datasozdanija_date = pd.to_datetime(row['datasozdanija'], format='%d.%m.%Y %H:%M:%S', errors='coerce')
        if datasozdanija_date is not pd.NaT:
            creation_period = pd.Period(datasozdanija_date.strftime('%Y-%m'), freq='M')
            if creation_period in [current_period, current_period - 1]:
                # Find the maximum 'min_stock' for the same 'gruppa_analogov'
                max_min_stock = union_data[union_data['gruppa_analogov'] == row['gruppa_analogov']]['min_stock'].max()
                if max_min_stock > row['min_stock']:
                    return max_min_stock
        return row['min_stock']

    union_data['min_stock'] = union_data.apply(adjust_min_stock_for_creation_date, axis=1)
    
    return union_data




union_data = calculate_sales_metrics(sales_data, union_data)
union_data.to_csv('union_data.csv')
print(union_data.head(20))
print(union_data.columns)
