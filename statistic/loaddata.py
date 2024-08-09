import pandas as pd

# Функция для удаления пробелов
def trim_whitespace(df):
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    return df

def load_and_process_data(engine):
    # Загрузка данных из таблиц
    nomenklaturaold = pd.read_sql_table('nomenklaturaold', engine)
    nomenklatura = pd.read_sql_table('nomenklatura', engine)
    stockold = pd.read_sql_table('stockold', engine)
    priceold = pd.read_sql_table('priceold', engine)
    prodazhi = pd.read_sql_table('prodazhi', engine)
    postuplenija = pd.read_sql_table('postuplenija', engine)
    stockendmonth = pd.read_sql_table('stockendmonth', engine)
    priceendmonth = pd.read_sql_table('priceendmonth', engine)  # Добавлена загрузка таблицы priceendmonth
    groupanalogiold = pd.read_sql_table('groupanalogiold', engine)
    typedetailgen = pd.read_sql_table('typedetailgen', engine)

    # Удаление пробелов сначала и конца у каждой строковой колонки
    nomenklaturaold = trim_whitespace(nomenklaturaold)
    nomenklatura = trim_whitespace(nomenklatura)
    stockold = trim_whitespace(stockold)
    priceold = trim_whitespace(priceold)
    prodazhi = trim_whitespace(prodazhi)
    postuplenija = trim_whitespace(postuplenija)
    stockendmonth = trim_whitespace(stockendmonth)
    priceendmonth = trim_whitespace(priceendmonth)  # Удаление пробелов в таблице priceendmonth
    groupanalogiold = trim_whitespace(groupanalogiold)
    typedetailgen = trim_whitespace(typedetailgen)

    # Фильтрация и выбор колонок для nomenklaturaold
    nomenklaturaold = nomenklaturaold[nomenklaturaold['pometkaudalenija'] == 'Нет']
    nomenklaturaold = nomenklaturaold[['kod','naimenovanie', 'artikul', 'proizvoditel', 'edizm']]

    # Выбор колонок и преобразование дат для nomenklatura
    nomenklatura = nomenklatura[['kod', 'datasozdanija', 'roditel', 'vidnomenklatury']]
    nomenklatura['datasozdanija'] = nomenklatura['datasozdanija'].replace('', '31.12.2021')
    nomenklatura['datasozdanija'] = pd.to_datetime(nomenklatura['datasozdanija'], format='%d.%m.%Y', errors='coerce')
    nomenklatura['datasozdanija'] = nomenklatura['datasozdanija'].fillna(pd.Timestamp('2021-12-31'))

    # Оставляем только те товары, у которых vidnomenklatury равен 'Товар'
    nomenklatura = nomenklatura[nomenklatura['vidnomenklatury'] == 'Товар']

    # Фильтрация и выбор колонок для prodazhi
    prodazhi = prodazhi[['kod', 'kolichestvo', 'period']]
    prodazhi['datasales'] = pd.to_datetime(prodazhi['period'], format='%d.%m.%Y', errors='coerce')
    prodazhi.drop(columns=['period'], inplace=True)

    # Преобразование kolichestvo в строки, замена запятых на точки, замена 'None' и NaN на 0 и преобразование в float для prodazhi
    prodazhi['kolichestvo'] = prodazhi['kolichestvo'].astype(str).str.replace(',', '.').replace('None', '0').fillna('0').astype(float)

    # Группировка prodazhi по datasales и kod с суммированием kolichestvo
    prodazhi = prodazhi.groupby(['datasales', 'kod']).agg({'kolichestvo': 'sum'}).reset_index()

    # Фильтрация и выбор колонок для postuplenija
    postuplenija = postuplenija[postuplenija['proveden'] == 'Да']
    postuplenija = postuplenija[['kod', 'kolichestvo', 'data']]
    postuplenija['data'] = pd.to_datetime(postuplenija['data']).dt.strftime('%d.%m.%Y')
    postuplenija['data'] = pd.to_datetime(postuplenija['data'], format='%d.%m.%Y')

    # Преобразование kolichestvo в строки, замена запятых на точки, замена 'None' и NaN на 0 и преобразование в float для postuplenija
    postuplenija['kolichestvo'] = postuplenija['kolichestvo'].astype(str).str.replace(',', '.').replace('None', '0').fillna('0').astype(float)

    # Группировка postuplenija по kod и data с суммированием kolichestvo
    postuplenija = postuplenija.groupby(['kod', 'data']).agg({'kolichestvo': 'sum'}).reset_index()

    # Переименование колонки nomenklaturakod в kod в таблице stockendmonth
    stockendmonth.rename(columns={'nomenklaturakod': 'kod'}, inplace=True)
    # Фильтрация и выбор колонок для stockendmonth
    stockendmonth = stockendmonth[['kod', 'month', 'balance']]
    stockendmonth['month'] = pd.to_datetime(stockendmonth['month'], format='%Y-%m', errors='coerce')

    # Переименование колонки nomenklaturakod в kod в таблице priceendmonth
    priceendmonth.rename(columns={'nomenklaturakod': 'kod'}, inplace=True)
    # Фильтрация и выбор колонок для priceendmonth
    priceendmonth = priceendmonth[['kod', 'data', 'tsena']]
    priceendmonth['data'] = pd.to_datetime(priceendmonth['data'], format='%Y-%m', errors='coerce')

    # Переименование колонки kod_1s в kod в таблице groupanalogiold
    groupanalogiold.rename(columns={'kod_1s': 'kod'}, inplace=True)

    # Объединение таблиц
    nomenklatura_merged = pd.merge(nomenklaturaold, nomenklatura, on='kod', how='left')
    nomenklatura_merged = pd.merge(nomenklatura_merged, stockold, on='kod', how='left').fillna(0)
    nomenklatura_merged = pd.merge(nomenklatura_merged, priceold, on='kod', how='left').fillna(0)
    nomenklatura_merged = pd.merge(nomenklatura_merged, groupanalogiold, on='kod', how='left')
    nomenklatura_merged = pd.merge(nomenklatura_merged, typedetailgen, on='kod', how='left')

    # Оставляем только те строки, которые есть в nomenklatura
    valid_kods = nomenklatura['kod']
    nomenklatura_merged = nomenklatura_merged[nomenklatura_merged['kod'].isin(valid_kods)]
    stockendmonth = stockendmonth[stockendmonth['kod'].isin(valid_kods)]
    priceendmonth = priceendmonth[priceendmonth['kod'].isin(valid_kods)]
    postuplenija = postuplenija[postuplenija['kod'].isin(valid_kods)]
    prodazhi = prodazhi[prodazhi['kod'].isin(valid_kods)]

    return nomenklatura_merged, stockendmonth, priceendmonth, postuplenija, prodazhi

def get_final_data(engine):
    # Load data from database
    nomenklatura, stockendmonth, priceendmonth, postuplenija, prodazhi = load_and_process_data(engine)

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

    # Извлекаем нужные столбцы
    nomenklatura_ml = nomenklatura[['kod', 'proizvoditel', 'gruppa_analogov', 'type_detail']]

    # Удаляем строки с пропущенными значениями в 'proizvoditel'
    nomenklatura_ml = nomenklatura_ml.dropna(subset=['proizvoditel'])


    # Обрабатываем пропущенные значения в 'type_detail'
    nomenklatura_ml['type_detail'] = nomenklatura_ml.apply(
        lambda row: row.name if pd.isna(row['type_detail']) else row['type_detail'], axis=1)

    # Обрабатываем пропущенные значения в 'gruppa_analogov'
    nomenklatura_ml['gruppa_analogov'] = nomenklatura_ml.apply(
        lambda row: row.name if pd.isna(row['gruppa_analogov']) else row['gruppa_analogov'], axis=1)


    return final_data, nomenklatura_ml



