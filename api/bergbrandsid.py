import requests
import pandas as pd
from ugkorea.api.config import BASE_URL_BERG, API_KEY_BERG

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

# Тестируем подключение к API и выводим данные в виде DataFrame
if __name__ == "__main__":
    brands_df = get_brand_reference(BASE_URL_BERG, API_KEY_BERG)
    if brands_df is not None:
        print("DataFrame created successfully:")
        print(brands_df.head())  # Вывод первых нескольких строк для проверки
    else:
        print("Failed to fetch or create DataFrame.")
