import pandas as pd
from sklearn.metrics import mean_squared_error, mean_absolute_error, mean_absolute_percentage_error
from ugkorea.statistic.loaddata import load_and_process_data

# Загрузка данных
print("Загрузка данных...")
nomenklatura, stockendmonth, postuplenija, prodazhi = load_and_process_data()
print("Данные загружены.")

# Преобразование столбцов с датами в datetime
nomenklatura['datasozdanija'] = pd.to_datetime(nomenklatura['datasozdanija'])
postuplenija['data'] = pd.to_datetime(postuplenija['data'])
prodazhi['datasales'] = pd.to_datetime(prodazhi['datasales'])
stockendmonth['date'] = pd.to_datetime(stockendmonth['date'])

# Разделение данных на тренировочные и тестовые части
split_date = pd.Timestamp.now() - pd.DateOffset(months=3)
train_prodazhi = prodazhi[prodazhi['datasales'] < split_date].copy()
test_prodazhi = prodazhi[prodazhi['datasales'] >= split_date].copy()

# Фильтрация данных на основе даты создания товара
def filter_data_by_creation_date(group, date_col):
    creation_date = group['datasozdanija'].iloc[0]
    return group[group[date_col] >= creation_date]

filtered_stockendmonth_groups = []
for name, group in stockendmonth.merge(nomenklatura[['kod', 'datasozdanija']], on='kod').groupby('kod'):
    filtered_stockendmonth_groups.append(filter_data_by_creation_date(group, 'date'))
filtered_stockendmonth = pd.concat(filtered_stockendmonth_groups).reset_index(drop=True)

filtered_postuplenija_groups = []
for name, group in postuplenija.merge(nomenklatura[['kod', 'datasozdanija']], on='kod').groupby('kod'):
    filtered_postuplenija_groups.append(filter_data_by_creation_date(group, 'data'))
filtered_postuplenija = pd.concat(filtered_postuplenija_groups).reset_index(drop=True)

# Анализ продаж по месяцам и годам с учетом производителя и типа детали
train_prodazhi['year_month'] = train_prodazhi['datasales'].dt.to_period('M')
monthly_sales = train_prodazhi.groupby(['kod', 'year_month']).agg({'kolichestvo': 'sum'}).reset_index()
monthly_sales = monthly_sales.merge(nomenklatura[['kod', 'proizvoditel', 'type_detail', 'gruppa_analogov']], on='kod')

# Анализ поступлений товара на склад
postuplenija['year_month'] = postuplenija['data'].dt.to_period('M')
monthly_receptions = postuplenija.groupby(['kod', 'year_month']).agg({'kolichestvo': 'sum'}).reset_index()

# Определение закономерностей изменения спроса
sales_trends = monthly_sales.groupby(['year_month', 'proizvoditel', 'type_detail']).agg({'kolichestvo': 'sum'}).reset_index()
sales_trends['year_month'] = sales_trends['year_month'].dt.to_timestamp()

# Фиксированное время на доставку (12 дней)
fixed_delivery_time = 12 / 30  # 12 дней в месяцах

# Прогнозирование продаж на следующие 3 месяца, используя все данные
def predict_sales(kod, product_sales, analog_sales, current_month):
    if product_sales.empty:
        avg_sales_per_month = 0
    else:
        avg_sales_per_month = product_sales['kolichestvo'].mean() + analog_sales
    
    if product_sales.empty or sales_trends.empty:
        return 0

    sales_trend = sales_trends[(sales_trends['year_month'] == current_month) & 
                               (sales_trends['proizvoditel'] == product_sales['proizvoditel'].iloc[0]) & 
                               (sales_trends['type_detail'] == product_sales['type_detail'].iloc[0])]
    if sales_trend.empty:
        trend_change = 0
    else:
        trend_change = sales_trend['kolichestvo'].pct_change().fillna(0).mean()
    
    forecasted_sales = avg_sales_per_month * (1 + trend_change)
    return forecasted_sales * 3

# Используем все данные для прогнозирования будущих продаж
full_prodazhi = prodazhi.copy()
full_prodazhi['year_month'] = full_prodazhi['datasales'].dt.to_period('M')
monthly_sales_full = full_prodazhi.groupby(['kod', 'year_month']).agg({'kolichestvo': 'sum'}).reset_index()
monthly_sales_full = monthly_sales_full.merge(nomenklatura[['kod', 'proizvoditel', 'type_detail', 'gruppa_analogov']], on='kod')

predicted_sales = []
current_month = pd.Timestamp.now().to_period('M').to_timestamp()
for kod in nomenklatura['kod']:
    product_sales = monthly_sales_full[monthly_sales_full['kod'] == kod]
    if nomenklatura.loc[nomenklatura['kod'] == kod, 'gruppa_analogov'].notna().any():
        analog_kods = nomenklatura[nomenklatura['gruppa_analogov'] == nomenklatura.loc[nomenklatura['kod'] == kod, 'gruppa_analogov'].values[0]]['kod'].values
        analog_sales = monthly_sales_full[monthly_sales_full['kod'].isin(analog_kods)]['kolichestvo'].sum() if len(analog_kods) > 0 else 0
    else:
        analog_sales = 0
    predicted_sales.append(predict_sales(kod, product_sales, analog_sales, current_month))
nomenklatura['predicted_sales'] = predicted_sales

# Расчет необходимых запасов для каждого товара
nomenklatura['required_inventory'] = nomenklatura['predicted_sales']

# Подсчет годовых продаж для каждого товара
current_year = pd.Timestamp.now().year
annual_sales = prodazhi[prodazhi['datasales'].dt.year == current_year].groupby('kod')['kolichestvo'].sum().reset_index()
annual_sales.rename(columns={'kolichestvo': 'annual_sales'}, inplace=True)

# Объединение данных
result = nomenklatura[['kod', 'required_inventory', 'predicted_sales']].merge(annual_sales, on='kod', how='left')

# Сохранение результата в файл
result.to_csv('required_inventory_and_annual_sales.csv', index=False)

# Вывод результатов
print("Необходимые запасы для текущего месяца и годовые продажи сохранены в файл 'required_inventory_and_annual_sales.csv'.")
print(result.head())

# Анализ предсказанных остатков и фактических продаж за следующие 3 месяца
print("Анализ предсказанных остатков и фактических продаж за следующие 3 месяца...")

# Суммирование продаж за последние 3 месяца по каждой позиции
test_prodazhi['year_month'] = test_prodazhi['datasales'].dt.to_period('M')
future_sales_summary = test_prodazhi.groupby(['kod', 'year_month']).agg({'kolichestvo': 'sum'}).reset_index()
future_sales_summary = future_sales_summary.groupby('kod')['kolichestvo'].sum().reset_index()
future_sales_summary.rename(columns={'kolichestvo': 'future_3_months_sales'}, inplace=True)

# Слияние предсказанных данных с фактическими продажами
merged_data = pd.merge(result, future_sales_summary, on='kod', how='left')

# Рассчет метрик ошибки
merged_data['future_3_months_sales'] = merged_data['future_3_months_sales'].fillna(0)  # Заполняем пропуски нулями
rmse = mean_squared_error(merged_data['predicted_sales'], merged_data['future_3_months_sales']) ** 0.5
mae = mean_absolute_error(merged_data['predicted_sales'], merged_data['future_3_months_sales'])
mape = mean_absolute_percentage_error(merged_data['predicted_sales'], merged_data['future_3_months_sales'])

print(f"RMSE: {rmse}")
print(f"MAE: {mae}")
print(f"MAPE: {mape}")
