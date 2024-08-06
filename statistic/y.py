from ugkorea.statistic.loaddata import load_and_process_data
import pandas as pd

# Load data from database
nomenklatura, stockendmonth, postuplenija, prodazhi = load_and_process_data()

# Convert date columns to datetime
postuplenija['data'] = pd.to_datetime(postuplenija['data'], format='%d.%m.%Y')
prodazhi['datasales'] = pd.to_datetime(prodazhi['datasales'], format='%d.%m.%Y')
stockendmonth['date'] = pd.to_datetime(stockendmonth['date'])
nomenklatura['datasozdanija'] = pd.to_datetime(nomenklatura['datasozdanija'])

# Extract year and month for aggregation
prodazhi['year_month'] = prodazhi['datasales'].dt.to_period('M')
postuplenija['year_month'] = postuplenija['data'].dt.to_period('M')
stockendmonth['year_month'] = stockendmonth['date'].dt.to_period('M')

# Aggregate sales data by month and product code
sales_monthly = prodazhi.groupby(['kod', 'year_month']).agg({'kolichestvo': 'sum'}).reset_index()
sales_monthly.rename(columns={'kolichestvo': 'total_sales'}, inplace=True)

# Aggregate stock data by month and product code
stock_monthly = stockendmonth.groupby(['kod', 'year_month']).agg({'balance': 'sum'}).reset_index()

# Calculate the balance for the beginning of each month
stock_monthly['prev_year_month'] = stock_monthly['year_month'] - 1

# Merge nomenklatura with stock_monthly to ensure we have the creation date for each product
stock_with_creation = pd.merge(stock_monthly, nomenklatura[['kod', 'datasozdanija']], on='kod', how='left')

# Filter out rows where the product was not yet created
stock_with_creation = stock_with_creation[stock_with_creation['year_month'].dt.to_timestamp() >= stock_with_creation['datasozdanija']]

# Calculate the initial stock for the beginning of the month
initial_stock = stock_with_creation[['kod', 'prev_year_month', 'balance']].rename(columns={'prev_year_month': 'year_month', 'balance': 'initial_balance'})

# Calculate the incoming stock for the current month
incoming_stock = postuplenija.groupby(['kod', 'year_month']).agg({'kolichestvo': 'sum'}).reset_index().rename(columns={'kolichestvo': 'incoming_balance'})

# Merge initial stock and incoming stock
balance_monthly = pd.merge(initial_stock, incoming_stock, on=['kod', 'year_month'], how='outer').fillna(0)
balance_monthly['balance'] = balance_monthly['initial_balance'] + balance_monthly['incoming_balance']

# Merge sales data with balance data
merged_data = pd.merge(sales_monthly, balance_monthly[['kod', 'year_month', 'balance']], on=['kod', 'year_month'], how='left')

# Select only the required columns
result_data = merged_data[['kod', 'year_month', 'total_sales', 'balance']]

# Display the first few rows of the final data for debugging
print("\nResult Data head:")
print(result_data.head())

# Optionally save to a CSV for further inspection
result_data.to_csv('result_sales_data.csv', index=False)
