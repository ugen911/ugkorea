import pandas as pd
import numpy as np

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
    # Загружаем данные из базы данных
    nomenklatura, stockendmonth, priceendmonth, postuplenija, prodazhi = load_and_process_data(engine)

    # Переименовываем колонку 'month' в 'date' в stockendmonth
    stockendmonth.rename(columns={'month': 'date'}, inplace=True)

    # Преобразуем колонки с датами в datetime
    postuplenija['data'] = pd.to_datetime(postuplenija['data'], format='%d.%m.%Y')
    prodazhi['datasales'] = pd.to_datetime(prodazhi['datasales'], format='%d.%m.%Y')
    stockendmonth['date'] = pd.to_datetime(stockendmonth['date'])
    nomenklatura['datasozdanija'] = pd.to_datetime(nomenklatura['datasozdanija'])
    priceendmonth['data'] = pd.to_datetime(priceendmonth['data'])

    # Извлекаем год и месяц для агрегации
    prodazhi['year_month'] = prodazhi['datasales'].dt.to_period('M')
    postuplenija['year_month'] = postuplenija['data'].dt.to_period('M')
    stockendmonth['year_month'] = stockendmonth['date'].dt.to_period('M')
    priceendmonth['year_month'] = priceendmonth['data'].dt.to_period('M')

    # Создаем DataFrame со всеми комбинациями товаров и месяцев
    all_products = nomenklatura['kod'].unique()
    all_months = pd.period_range(start=prodazhi['year_month'].min(), end=prodazhi['year_month'].max(), freq='M')
    all_combinations = pd.MultiIndex.from_product([all_products, all_months], names=['kod', 'year_month']).to_frame(index=False)

    # Агрегируем данные о продажах по месяцам и коду товара
    sales_monthly = prodazhi.groupby(['kod', 'year_month']).agg({'kolichestvo': 'sum'}).reset_index()
    sales_monthly.rename(columns={'kolichestvo': 'total_sales'}, inplace=True)

    # Объединяем данные о продажах с all_combinations, чтобы гарантировать наличие всех месяцев и продуктов
    merged_data = pd.merge(all_combinations, sales_monthly, on=['kod', 'year_month'], how='left').fillna(0)

    # Объединяем с номенклатурой, чтобы фильтровать по дате создания
    merged_data = pd.merge(merged_data, nomenklatura[['kod', 'datasozdanija']], on='kod', how='left')
    merged_data = merged_data[merged_data['year_month'].dt.to_timestamp() >= merged_data['datasozdanija']]

    # Объединяем с priceendmonth для добавления информации о цене
    priceendmonth_renamed = priceendmonth[['kod', 'year_month', 'tsena']].rename(columns={'tsena': 'price'})
    final_data = pd.merge(merged_data, priceendmonth_renamed, on=['kod', 'year_month'], how='left').fillna({'price': 0})

    # Присоединяем stockendmonth в конце
    stockendmonth_renamed = stockendmonth[['kod', 'year_month', 'balance']]
    final_data = pd.merge(final_data, stockendmonth_renamed, on=['kod', 'year_month'], how='left')

    # Заполняем отсутствующие значения в колонке 'balance' нулями
    final_data['balance'] = final_data['balance'].fillna(0)

    # Обработка условий для balance и total_sales
    final_data.loc[(final_data['total_sales'] == 0) & (final_data['balance'] == 0), ['balance', 'total_sales', 'price']] = None

    # Оставляем только нужные колонки
    final_data = final_data[['kod', 'year_month', 'total_sales', 'balance', 'price']]

    # Обрабатываем номенклатуру для ML
    nomenklatura_ml = nomenklatura[['kod', 'proizvoditel', 'gruppa_analogov', 'type_detail']].copy()

    # Обработка пропущенных значений в 'type_detail' и 'gruppa_analogov'
    nomenklatura_ml['type_detail'] = nomenklatura_ml.apply(
        lambda row: row.name if pd.isna(row['type_detail']) else row['type_detail'], axis=1)
    nomenklatura_ml['gruppa_analogov'] = nomenklatura_ml.apply(
        lambda row: row.name if pd.isna(row['gruppa_analogov']) else row['gruppa_analogov'], axis=1)

    return final_data, nomenklatura_ml



def perform_abc_xyz_analysis(engine):
    
    # Вспомогательная функция для очистки и преобразования колонки summa
    def clean_and_convert_summa(df):
        # Удаляем пробелы, которые используются как разделители тысяч, и заменяем запятые на точки
        df['summa'] = df['summa'].astype(str).str.replace('\s', '', regex=True).str.replace(',', '.')
        # Преобразуем в числовой тип данных
        df['summa'] = pd.to_numeric(df['summa'], errors='coerce').fillna(0)
        return df

    # Загрузка данных из таблиц
    prodazhi_df = pd.read_sql_table('prodazhi', con=engine)
    nomenklatura_df = pd.read_sql_table('nomenklatura', con=engine)

    # Удаление пробелов и нежелательных символов в колонке kod
    prodazhi_df['kod'] = prodazhi_df['kod'].str.strip()
    nomenklatura_df['kod'] = nomenklatura_df['kod'].str.strip()

    # Фильтрация nomenklatura по vidnomenklatury == "Товар"
    filtered_nomenklatura = nomenklatura_df[nomenklatura_df['vidnomenklatury'] == "Товар"][['kod']]

    # Фильтрация prodazhi на основе kod из отфильтрованной nomenklatura
    filtered_prodazhi = prodazhi_df[prodazhi_df['kod'].isin(filtered_nomenklatura['kod'])].copy()

    # Преобразование kolichestvo и summa
    filtered_prodazhi['kolichestvo'] = pd.to_numeric(filtered_prodazhi['kolichestvo'], errors='coerce').fillna(0)
    filtered_prodazhi = clean_and_convert_summa(filtered_prodazhi)

    # Преобразование period в дату
    filtered_prodazhi['period'] = pd.to_datetime(filtered_prodazhi['period'], format='%d.%m.%Y')

    # Фильтрация данных за последние 13 месяцев от текущей даты
    current_date = pd.Timestamp.now()
    last_13_months = current_date - pd.DateOffset(months=13)
    last_13_months_data = filtered_prodazhi[filtered_prodazhi['period'] >= last_13_months]

    # ABC анализ по сумме
    total_summa = last_13_months_data.groupby('kod')['summa'].sum()
    total_summa_sorted = total_summa.sort_values(ascending=False)
    cumulative_sum = total_summa_sorted.cumsum()
    total_cumulative_sum = cumulative_sum.iloc[-1]

    # Определение ABC категорий
    abc_labels = pd.cut(cumulative_sum, 
                        bins=[0, 0.05 * total_cumulative_sum, 0.8 * total_cumulative_sum, 0.95 * total_cumulative_sum, total_cumulative_sum], 
                        labels=['A1', 'A', 'B', 'C'])

    # XYZ анализ по количеству в процентном соотношении
    total_kolichestvo = last_13_months_data.groupby('kod')['kolichestvo'].sum()
    total_kolichestvo_sorted = total_kolichestvo.sort_values(ascending=False)
    cumulative_kolichestvo = total_kolichestvo_sorted.cumsum()
    total_cumulative_kolichestvo = cumulative_kolichestvo.iloc[-1]

    # Определение XYZ категорий аналогично ABC анализу
    xyz_labels = pd.cut(cumulative_kolichestvo, 
                        bins=[0, 0.05 * total_cumulative_kolichestvo, 0.8 * total_cumulative_kolichestvo, 0.95 * total_cumulative_kolichestvo, total_cumulative_kolichestvo], 
                        labels=['X1', 'X', 'Y', 'Z'])

    # Объединение ABC и XYZ анализов в один DataFrame
    abc_xyz_analysis = pd.DataFrame({
        'ABC': abc_labels,
        'XYZ': xyz_labels
    }, index=total_summa_sorted.index)

    # Проверка и заполнение категорий для товаров, которые были проданы хотя бы раз
    sold_items = last_13_months_data['kod'].unique()
    for item in sold_items:
        if item not in abc_xyz_analysis.index:
            abc_xyz_analysis.loc[item] = {'ABC': 'C', 'XYZ': 'Z'}

    return abc_xyz_analysis





