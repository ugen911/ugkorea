import time
import os
import pandas as pd
import requests
from ugkorea.api.pricefor import get_final_df
from ugkorea.db.database import get_db_engine
from ugkorea.api.config import BASE_URL_BERG, API_KEY_BERG
from datetime import datetime
from tqdm import tqdm

DEBUG_MODE = False  # True для включения отладки, False для полной обработки

def process_and_save_data():
    API_KEY = API_KEY_BERG
    BASE_URL = BASE_URL_BERG

    def get_stock_info(item):
        url = f"{BASE_URL}/ordering/get_stock.json"
        params = {
            'key': API_KEY,
            'items[0][resource_article]': item['resource_article'],
            'items[0][brand_name]': item['brand_name']
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)  # Увеличиваем время ожидания до 10 секунд
        except requests.exceptions.ConnectTimeout:
            print(f"Connection timeout occurred for item: {item}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return None
        
        # Проверяем статус ответа
        if response.status_code != 200:
            print(f"Error: Received status code {response.status_code} for item: {item}")
            return None
        
        # Проверяем, что тело ответа не пустое
        if not response.text:
            print(f"Error: Empty response for item: {item}")
            return None
        
        # Попробуем разобрать JSON
        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError:
            print(f"Error decoding JSON for item: {item}, response text: {response.text}")
            return None
        
        # Проверяем наличие ошибок в ответе
        if 'errors' in data:
            for error in data['errors']:
                print(f"API Error for item: {item} - Code: {error['code']}, Text: {error['text']}")
            return None
        
        return data

    def save_progress_to_csv(df, filename='output/partial_result.csv'):
        """Сохраняет промежуточные результаты в CSV-файл."""
        if not os.path.exists('output'):
            os.makedirs('output')
        df.to_csv(filename, index=False)


    def remove_file(filename='output/partial_result.csv'):
        """Удаляет указанный файл."""
        if os.path.exists(filename):
            os.remove(filename)
            print(f"File {filename} has been removed.")

    # Получение датафрейма с помощью функции get_final_df
    df = get_final_df()

    # Проверка, что датафрейм не пуст
    if not df.empty:
        # Убедимся, что индексы корректны
        df.reset_index(drop=True, inplace=True)
        
        results = []
        
        # Определяем количество итераций в зависимости от режима отладки
        max_iterations = 20 if DEBUG_MODE else len(df)
        
        try:
            # Используем tqdm для отслеживания прогресса
            for i in tqdm(range(max_iterations), desc="Processing items"):
                # Получение данных для текущей строки
                kod = df.loc[i, 'kod']
                artikul = df.loc[i, 'artikul']
                proizvoditel = df.loc[i, 'proizvoditel']
                
                item = {
                    'resource_article': artikul,
                    'brand_name': proizvoditel
                }
                
                # Получение данных по текущему элементу
                stock_info = get_stock_info(item)
                
                if stock_info and 'resources' in stock_info and len(stock_info['resources']) > 0:
                    offers = stock_info['resources'][0]['offers']
                    
                    # Фильтруем предложения по условиям average_period <= 15 и reliability > 80
                    valid_offers = [
                        offer for offer in offers 
                        if offer['average_period'] <= 15 and offer['reliability'] > 80
                    ]
                    
                    if valid_offers:
                        min_price_offer = min(valid_offers, key=lambda x: x['price'])
                        result = {
                            'kod': kod,
                            'artikul': artikul,
                            'proizvoditel': proizvoditel,
                            'warehouse_name': min_price_offer['warehouse']['name'],
                            'price': min_price_offer['price'],
                            'quantity': min_price_offer['quantity'],
                            'average_period': min_price_offer['average_period'],
                            'reliability': min_price_offer['reliability']
                        }
                        results.append(result)
                
                # Периодически сохраняем промежуточные результаты
                if (i + 1) % 100 == 0:  # Сохранение каждые 100 итераций
                    partial_df = pd.DataFrame(results)
                    save_progress_to_csv(partial_df)

        except Exception as e:
            print(f"An error occurred: {e}")
            # Сохраняем результаты перед завершением работы в случае ошибки
            if results:
                final_df = pd.DataFrame(results)
                save_progress_to_csv(final_df, filename='output/final_result_on_error.csv')
            raise  # Перебрасываем исключение после сохранения

        # Преобразование списка результатов в DataFrame
        final_df = pd.DataFrame(results)
        
        # Добавляем колонки с меткой "berg" и текущей датой
        final_df['source'] = 'berg'
        final_df['date_checked'] = datetime.today().strftime('%Y-%m-%d')
        
        # Проверяем на дубликаты по `kod` и оставляем строки с минимальной ценой больше 0
        final_df = final_df[final_df['price'] > 0]
        final_df = final_df.loc[final_df.groupby('kod')['price'].idxmin()]

        # Размещаем колонку `kod` слева всего датафрейма
        final_df = final_df[['kod', 'artikul', 'proizvoditel', 'warehouse_name', 'price', 'quantity', 'average_period', 'reliability', 'source', 'date_checked']]
        

        # Получение движка для базы данных
        engine = get_db_engine()
        
        # Загрузка датафрейма в базу данных в схему `prices` как таблицу `bergapi`
        final_df.to_sql('bergapi', con=engine, schema='prices', if_exists='replace', index=False)
        
        print("Dataframe saved to the database successfully.")
        
        # Удаляем промежуточный файл после завершения работы
        remove_file()
    else:
        print("Dataframe is empty")


if __name__ == "__main__":
    process_and_save_data()
