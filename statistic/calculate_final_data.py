import pandas as pd
from ugkorea.statistic.loaddata import load_and_process_data

def get_final_data():
    # Load data from database
    nomenklatura, stockendmonth, priceendmonth, postuplenija, prodazhi = load_and_process_data()

    # Rename 'month' column to 'date' in stockendmonth
    stockendmonth.rename(columns={'month': 'date'}, inplace=True)

    # Convert date columns to datetime
    postuplenija['data'] = pd.to_datetime(postuplenija['data'], format='%d.%m.%Y')
    prodazhi['datasales'] = pd.to_datetime(prodazhi['datasales'], format='%d.%m.%Y')
    stockendmonth['date'] = pd.to_datetime(stockendmonth['date'])
    nomenklatura['datasozdanija'] = pd.to_datetime(nomenklatura['datasozdanija'])
    priceendmonth['data'] = pd.to_datetime(priceendmonth['data'])

    # Extract year and month for aggregation
    prodazhi['year_month'] = prodazhi['datasales'].dt.to_period('M')
    postuplenija['year_month'] = postuplenija['data'].dt.to_period('M')
    stockendmonth['year_month'] = stockendmonth['date'].dt.to_period('M')
    priceendmonth['year_month'] = priceendmonth['data'].dt.to_period('M')

    # Create a DataFrame with all combinations of products and months
    all_products = nomenklatura['kod'].unique()
    all_months = pd.period_range(start=prodazhi['year_month'].min(), end=prodazhi['year_month'].max(), freq='M')
    all_combinations = pd.MultiIndex.from_product([all_products, all_months], names=['kod', 'year_month']).to_frame(index=False)

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

    # Merge all combinations with merged data to ensure all months and products are included
    result_data = pd.merge(all_combinations, merged_data, on=['kod', 'year_month'], how='left').fillna(0)

    # Merge with nomenklatura to filter by creation date
    result_data = pd.merge(result_data, nomenklatura[['kod', 'datasozdanija']], on='kod', how='left')
    result_data = result_data[result_data['year_month'].dt.to_timestamp() >= result_data['datasozdanija']]

    # Merge with priceendmonth to add price information
    priceendmonth_renamed = priceendmonth[['kod', 'year_month', 'tsena']].rename(columns={'tsena': 'price'})
    final_data = pd.merge(result_data, priceendmonth_renamed, on=['kod', 'year_month'], how='left').fillna({'price': 0})

    # Set price to 0 where total_sales and balance are 0
    final_data.loc[(final_data['total_sales'] == 0) & (final_data['balance'] == 0), 'price'] = 0

    # Select only the required columns
    final_data = final_data[['kod', 'year_month', 'total_sales', 'balance', 'price']]

    return final_data



def prepare_nomenklatura_ml():
    # Загружаем данные из базы данных
    nomenklatura, _, _, _, _ = load_and_process_data()

    # Извлекаем нужные столбцы
    nomenklatura_ml = nomenklatura[['kod', 'proizvoditel', 'gruppa_analogov', 'type_detail']]

    # Удаляем строки с пропущенными значениями в 'proizvoditel'
    nomenklatura_ml = nomenklatura_ml.dropna(subset=['proizvoditel'])

    # Кодируем 'proizvoditel' как категории
    nomenklatura_ml['proizvoditel'] = nomenklatura_ml['proizvoditel'].astype('category').cat.codes

    # Обрабатываем пропущенные значения в 'type_detail'
    nomenklatura_ml['type_detail'] = nomenklatura_ml.apply(
        lambda row: row.name if pd.isna(row['type_detail']) else row['type_detail'], axis=1)

    # Обрабатываем пропущенные значения в 'gruppa_analogov'
    nomenklatura_ml['gruppa_analogov'] = nomenklatura_ml.apply(
        lambda row: row.name if pd.isna(row['gruppa_analogov']) else row['gruppa_analogov'], axis=1)

    # Преобразуем 'gruppa_analogov' и 'type_detail' в категории
    nomenklatura_ml['gruppa_analogov'] = nomenklatura_ml['gruppa_analogov'].astype('category').cat.codes
    nomenklatura_ml['type_detail'] = nomenklatura_ml['type_detail'].astype('category').cat.codes

    return nomenklatura_ml



if __name__ == "__main__":
    final_data = get_final_data()
    print(final_data.head())
    nomenklatura_ml = prepare_nomenklatura_ml()
    print(nomenklatura_ml.head())
    nomenklatura_ml.to_csv('nomenklatura_ml_data.csv', index=False)
    
