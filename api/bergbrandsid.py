import re
import requests
import pandas as pd
from ugkorea.api.config import BASE_URL_BERG, API_KEY_BERG
from ugkorea.api.pricefor import get_final_df

# Функция для получения данных о брендах из API и их преобразования в DataFrame
def get_brand_reference(api_url, api_key):
    url = f"{api_url}/references/brands.json"
    params = {
        'key': api_key  # Передаем API-ключ как параметр запроса
    }
    
    try:
        # Отправляем GET-запрос с указанными параметрами
        response = requests.get(url, params=params, timeout=10)
    except requests.exceptions.ConnectTimeout:
        print("Connection timeout occurred while fetching brand references.")
        return None
    except requests.exceptions.RequestException as e:
        # Обрабатываем любые другие ошибки запроса
        print(f"An error occurred while fetching brand references: {e}")
        return None

    # Проверяем статус ответа
    if response.status_code != 200:
        print(f"Error: Received status code {response.status_code} when fetching brand references.")
        return None

    # Проверяем, что ответ не пустой
    if not response.text:
        print("Error: Empty response when fetching brand references.")
        return None
    
    try:
        # Пытаемся декодировать JSON из ответа
        data = response.json()
    except requests.exceptions.JSONDecodeError:
        print(f"Error decoding JSON when fetching brand references, response text: {response.text}")
        return None

    # Проверяем на наличие ошибок в ответе
    if 'errors' in data:
        for error in data['errors']:
            print(f"API Error - Code: {error['code']}, Text: {error['text']}")
        return None

    # Преобразуем данные в DataFrame
    try:
        # Получаем список брендов, если это словарь
        if isinstance(data, dict):
            brand_list = data.get('brands', [])  # Замените 'brands' на нужный ключ, если он другой
            
            if isinstance(brand_list, list):
                # Преобразуем список словарей в DataFrame
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
    # Словарь для сопоставления синонимов брендов
    brand_synonyms = {
        "asam": "asam-sa",
        "dongil": "dongil super star",
        "hyundaikia": "hyundai kia",
        "hyundai kia": "hyundai kia",
        "hyundai-kia": "hyundai kia",
        "hyundai/kia": "hyundai kia",
        "kashiyama": "mk kashiyama",
        "kyb": "kayaba",
        "lemforder": "lemfoerder",
        "lesjofors": "lesjoefors",
        "lynx": "lynx auto",
        "lynx auto": "lynx auto",
        "lynxauto": "lynx auto",
        "parts mall": "parts-mall",
        "parts-mall": "parts-mall",
        "partsmall": "parts-mall",
        "pmc": "parts-mall",
        "reinz": "victor reinz",
        "sangsin": "sangsin brake",
        "valeo phc": "phc",
        "vernet": "vernet-calorstat",
        "vernet-calorstat": "vernet-calorstat",
        "just drive": "jd",
        "startvolt": "стартвольт",
        "elwis royal": "elwis"
    }

    # Функция нормализации названия бренда с учётом синонимов
    def normalize_brand_name(name):
        name = re.sub(r'\W+', '', name).lower()  # Убираем спецсимволы и приводим к нижнему регистру
        return brand_synonyms.get(name, name)  # Возвращаем синоним или оригинальное значение

    # Приведение данных к единому формату с учётом синонимов
    brands_df['name_clean'] = brands_df['name'].apply(lambda x: normalize_brand_name(x))
    price_df['proizvoditel_clean'] = price_df['proizvoditel'].apply(lambda x: normalize_brand_name(x))

    # Объединение данных по нормализованному названию брендов
    merged_df = price_df.merge(brands_df[['id', 'name_clean']], left_on='proizvoditel_clean', right_on='name_clean', how='left')

    # В результате объединения создаётся новый DataFrame, в который добавляется столбец 'id' бренда
    price_df['brand_id'] = merged_df['id']

    # Фильтрация строк, где brand_id не пустое (найдено соответствие)
    result_df = price_df[price_df['brand_id'].notna()]

    # Возвращаем итоговый DataFrame только с заполненными brand_id
    return result_df


# Тестируем подключение к API и выводим данные в виде DataFrame
if __name__ == "__main__":
    brands_df = get_brand_reference(BASE_URL_BERG, API_KEY_BERG)
    price_df = get_final_df()
    price_with_id = normalize_and_merge(brands_df, price_df)
    if brands_df is not None:
        print("DataFrame created successfully:")
        print(price_with_id.head(20))  # Вывод первых нескольких строк для проверки
    else:
        print("Failed to fetch or create DataFrame.")
