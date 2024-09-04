import pandas as pd
import requests
from tqdm import tqdm
import random
from ugkorea.api.pricefor import get_final_df

def get_article_prices_and_stock(api_key, agreement_code, delivery_address_code, debug_mode=False):
    # Получение исходного DataFrame
    df = get_final_df()

    # Приведение DataFrame к нужной структуре
    df = df[['kod', 'artikul', 'proizvoditel']]
    df['stock'] = 0.0
    df['price'] = 0.0
    df['article_info'] = None  # Добавляем столбец для хранения информации об артикуле

    # Очистка данных
    df['artikul'] = df['artikul'].str.strip().str.upper()
    df['proizvoditel'] = df['proizvoditel'].str.strip().str.upper()

    # Если включен debug_mode, выбираем случайные 10 позиций
    if debug_mode:
        df = df.sample(10)

    # Авторизация
    auth_url = "https://api.shate-m.ru/api/v1/auth/loginByapiKey"
    auth_data = {
        'apikey': api_key
    }
    print("Авторизация...")
    auth_response = requests.post(auth_url, data=auth_data)
    
    if auth_response.status_code != 200:
        raise Exception(f"Authorization failed: {auth_response.text}")
    
    access_token = auth_response.json().get("access_token")
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    print("Авторизация успешна. Начинается процесс получения данных о запасах и ценах...")

    # Подготовка списка артикулов для запроса
    articles = []
    for index, row in df.iterrows():
        articles.append({
            "ArticleCode": row['artikul'],
            "TradeMarkName": row['proizvoditel']
        })

    # Запрос на получение информации по артикулу
    search_url = "https://api.shate-m.ru/api/v1/articles/search"
    search_data = {
        "Keys": articles,
        "Include": "Contents,extended_info,trademark"
    }
    
    search_response = requests.post(search_url, headers=headers, json=search_data)
    
    if search_response.status_code != 200 or not search_response.json():
        raise Exception(f"Ошибка при поиске артикулов: {search_response.text}")

    # Обработка ответа
    article_info_list = search_response.json()

    for article_info in article_info_list:
        article_id = article_info['article']['id']
        artikul = article_info['article']['code']
        proizvoditel = article_info['article']['tradeMarkName']

        # Поиск в исходном DataFrame по артикулу и производителю
        match_index = df[(df['artikul'] == artikul) & (df['proizvoditel'] == proizvoditel)].index
        
        if match_index.empty:
            print(f"Артикул {artikul} от производителя {proizvoditel} не найден в исходных данных.")
            continue

        # Запрос цен и запасов
        prices_url = "https://api.shate-m.ru/api/v1/prices/search"
        prices_data = {
            "ArticleId": article_id,
            "IncludeAnalogs": False,
            "AgreementCode": agreement_code,
            "DeliveryAddressCode": delivery_address_code
        }
        
        prices_response = requests.post(prices_url, headers=headers, json=prices_data)
        
        if prices_response.status_code == 200 and prices_response.json():
            price_info = prices_response.json()[0]
            stock_info = price_info['quantity'].get('available', 0.0)
            price_value = price_info['price'].get('value', 0.0)
            
            df.at[match_index, 'stock'] = stock_info
            df.at[match_index, 'price'] = price_value
            df.at[match_index, 'article_info'] = article_info
        else:
            print(f"Ошибка при получении цены для артикула {artikul}: {prices_response.json()}")
    
    return df

# Пример использования функции
api_key = "4f4297fe-d81e-4ecc-9413-c5104ff6f840"
agreement_code = "RSAGR64561"  # Код договора
delivery_address_code = "Д1"    # Код адреса доставки
debug_mode = True

try:
    final_df = get_article_prices_and_stock(api_key, agreement_code, delivery_address_code, debug_mode=debug_mode)
    print(final_df)
except Exception as e:
    print(str(e))
