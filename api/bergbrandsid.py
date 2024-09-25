import re
import requests
import pandas as pd
from ugkorea.api.config import BASE_URL_BERG, API_KEY_BERG
from ugkorea.api.pricefor import get_final_df

# Основная функция для получения данных, нормализации и слияния
def get_price_with_brand_id(api_url=BASE_URL_BERG, api_key=API_KEY_BERG):
    def get_brand_reference(api_url, api_key):
        url = f"{api_url}/references/brands.json"
        params = {
            'key': api_key  # Передаем API-ключ как параметр запроса
        }

        try:
            response = requests.get(url, params=params, timeout=10)
        except requests.exceptions.ConnectTimeout:
            print("Connection timeout occurred while fetching brand references.")
            return None
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching brand references: {e}")
            return None

        if response.status_code != 200:
            print(f"Error: Received status code {response.status_code} when fetching brand references.")
            return None

        if not response.text:
            print("Error: Empty response when fetching brand references.")
            return None

        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError:
            print(f"Error decoding JSON when fetching brand references, response text: {response.text}")
            return None

        if 'errors' in data:
            for error in data['errors']:
                print(f"API Error - Code: {error['code']}, Text: {error['text']}")
            return None

        try:
            if isinstance(data, dict):
                brand_list = data.get('brands', [])
                if isinstance(brand_list, list):
                    brands_df = pd.DataFrame(brand_list, columns=['id', 'name'])
                    return brands_df
                else:
                    print("Error: 'brands' is not a list.")
                    return None
            else:
                print("Error: Data is not a dict.")
                return None
        except Exception as e:
            print(f"An error occurred while creating DataFrame: {e}")
            return None

    def normalize_and_merge(brands_df, price_df):
        # Маппинг брендов с фиксированными ID
        fixed_brands = {
            "knecht": 27,
            "parts mall": 676,
            "kyb": 399,
            "reinz": 499,
            "hyundaikia": 730,
            "dongil": 840,
            "sangsin": 637,
            "startvolt": 1138181,
            "lesjofors": 6,
            "lynx": 709,
            "vernet": 307,
            "maruichi": 729,
            "asakashi": 30278,
            "jd": 1083935,
            "viktor reinz": 499,
            "asam": 1059030,
            "gm": 711,
            "chonwoo": 1195889,
            "kashiyama": 592,
            "lemforder": 19,
            "cheon-woo": 1195889,
            "стартвольт": 1138181,
            "elwis": 330,
            "rhee jin": 794438,
            "geunyoung": 1193789,
        }

        # Приведение к нижнему регистру для поиска совпадений без учета регистра
        brands_df['name_clean'] = brands_df['name'].str.lower()
        price_df['proizvoditel_clean'] = price_df['proizvoditel'].str.lower()

        # Сначала присваиваем ID для полных совпадений между name из API и proizvoditel из get_final_df
        merged_df = price_df.merge(brands_df[['id', 'name_clean']], left_on='proizvoditel_clean', right_on='name_clean', how='left')
        price_df['brand_id'] = merged_df['id']

        # Присваиваем ID для брендов, которые не нашли соответствие
        price_df['brand_id'] = price_df.apply(
            lambda row: fixed_brands.get(row['proizvoditel_clean'], row['brand_id']),
            axis=1
        )

        result_df = price_df[price_df['brand_id'].notna()]

        # Список брендов без ID
        brands_without_id = price_df[price_df['brand_id'].isna()]['proizvoditel_clean'].unique()

        if len(brands_without_id) > 0:
            print("Бренды без ID:")
            for brand in brands_without_id:
                print(brand)
        else:
            print("Все бренды получили свой ID.")
        
        return result_df

    # Получаем данные о брендах и ценах
    brands_df = get_brand_reference(api_url, api_key)
    price_df = get_final_df()

    # Проверяем, что данные получены корректно, и выполняем нормализацию и слияние
    if brands_df is not None and price_df is not None:
        return normalize_and_merge(brands_df, price_df)
    else:
        print("Failed to fetch or create DataFrame.")
        return None
