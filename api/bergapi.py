import pandas as pd
import requests
from ugkorea.api.pricefor import get_final_df

API_KEY = '5b3ce90b93007ae32610223081e42b78e4a5761fb117a384c956568a348f4e11'
BASE_URL = 'https://api.berg.ru/v1.0'

def get_stock_info(item):
    url = f"{BASE_URL}/ordering/get_stock.json"
    params = {
        'key': API_KEY,
        'items[0][resource_article]': item['resource_article'],
        'items[0][brand_name]': item['brand_name']
    }
    
    response = requests.get(url, params=params)
    return response.json()

# Получение датафрейма с помощью функции get_final_df
df = get_final_df()

# Проверка, что датафрейм не пуст
if not df.empty:
    # Убедимся, что индексы корректны
    df.reset_index(drop=True, inplace=True)
    
    # Извлечение первой строки
    first_item = {
        'resource_article': df.loc[0, 'artikul'],
        'brand_name': df.loc[0, 'proizvoditel']
    }
    
    # Получение данных по первой строке
    stock_info = get_stock_info(first_item)
    
    # Вывод данных на экран
    print(stock_info)
else:
    print("Dataframe is empty")
