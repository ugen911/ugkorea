import pandas as pd
import requests
from ugkorea.api.pricefor import get_final_df
from ugkorea.api.config import API_KEY_BERG, BASE_URL_BERG


# Переменная для включения/отключения режима отладки
DEBUG_MODE = False  # True для включения отладки, False для полной обработки

API_KEY = API_KEY_BERG
BASE_URL = BASE_URL_BERG

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
    
    results = []
    
    # Определяем количество итераций в зависимости от режима отладки
    max_iterations = 20 if DEBUG_MODE else len(df)
    
    for i in range(max_iterations):
        item = {
            'resource_article': df.loc[i, 'artikul'],
            'brand_name': df.loc[i, 'proizvoditel']
        }
        
        # Получение данных по текущему элементу
        stock_info = get_stock_info(item)
        
        if 'resources' in stock_info and len(stock_info['resources']) > 0:
            offers = stock_info['resources'][0]['offers']
            
            # Фильтруем предложения по average_period <= 15 и ищем минимальную цену
            valid_offers = [offer for offer in offers if offer['average_period'] <= 15]
            
            if valid_offers:
                min_price_offer = min(valid_offers, key=lambda x: x['price'])
                result = {
                    'article': stock_info['resources'][0]['article'],
                    'brand': stock_info['resources'][0]['brand']['name'],
                    'warehouse_name': min_price_offer['warehouse']['name'],
                    'price': min_price_offer['price'],
                    'quantity': min_price_offer['quantity'],
                    'average_period': min_price_offer['average_period']
                }
                results.append(result)
    
    # Преобразование списка результатов в DataFrame
    final_df = pd.DataFrame(results)
    
    # Сохранение итогового DataFrame в файл
    final_df.to_csv('final_stock_info.csv', index=False)  # Сохранение в CSV
    # final_df.to_excel('final_stock_info.xlsx', index=False)  # Сохранение в Excel
    
    print("Dataframe saved successfully.")
else:
    print("Dataframe is empty")
