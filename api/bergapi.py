import concurrent.futures
import time
import os
import pandas as pd
import requests
from ugkorea.api.bergbrandsid import get_price_with_brand_id as get_final_df
from ugkorea.db.database import get_db_engine
from ugkorea.api.config import BASE_URL_BERG, API_KEY_BERG
from datetime import datetime
from tqdm import tqdm

DEBUG_MODE = False  # True для отладки


def get_stock_info(item, base_url, api_key):
    """
    Вынесем функцию запроса наружу, чтобы удобнее было вызывать в ThreadPoolExecutor.
    """
    url = f"{base_url}/ordering/get_stock.json"
    params = {
        "key": api_key,
        "items[0][resource_article]": item["resource_article"],
        "items[0][brand_id]": item["brand_id"],
        "analogs": 0,
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()  # выбросит исключение, если код не 200
        data = response.json()
        # Если есть ошибки в ответе
        if "errors" in data:
            for error in data["errors"]:
                print(
                    f"API Error for item: {item} - Code: {error['code']}, Text: {error['text']}"
                )
            return None
        return data
    except requests.exceptions.RequestException as e:
        # Любые сетевые ошибки или timeout
        print(f"Request failed for item {item} with error: {e}")
        return None


def process_and_save_data():
    API_KEY = API_KEY_BERG
    BASE_URL = BASE_URL_BERG

    def save_progress_to_csv(df, filename="output/partial_result.csv"):
        if not os.path.exists("output"):
            os.makedirs("output")
        df.to_csv(filename, index=False)

    def remove_file(filename="output/partial_result.csv"):
        if os.path.exists(filename):
            os.remove(filename)
            print(f"File {filename} has been removed.")

    df = get_final_df()
    if df.empty:
        print("Dataframe is empty")
        return

    # Сброс индекса на всякий случай
    df.reset_index(drop=True, inplace=True)

    # Определяем, сколько итераций делаем (в отладочном режиме уменьшаем)
    max_iterations = 100 if DEBUG_MODE else len(df)

    # Готовим список всех «items» для дальнейшей параллельной обработки
    items = []
    for i in range(max_iterations):
        item = {
            "kod": df.loc[i, "kod"],
            "artikul": df.loc[i, "artikul"],
            "proizvoditel": df.loc[i, "proizvoditel"],
            "brand_id": df.loc[i, "brand_id"],
        }
        items.append(item)

    results = []

    # Запускаем пул потоков – можно регулировать max_workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Создаём задачи
        future_to_item = {}
        for it in items:
            # Передаём сам словарь it, а также BASE_URL, API_KEY
            future = executor.submit(
                get_stock_info,
                {"resource_article": it["artikul"], "brand_id": it["brand_id"]},
                BASE_URL,
                API_KEY,
            )
            future_to_item[future] = it

        # Используем tqdm, чтобы видеть прогресс по мере завершения запросов
        for future in tqdm(
            concurrent.futures.as_completed(future_to_item),
            total=len(future_to_item),
            desc="Processing items",
        ):
            original_item = future_to_item[future]
            stock_info = future.result()

            if not stock_info:
                continue
            if "resources" not in stock_info or not stock_info["resources"]:
                continue

            resource = stock_info["resources"][0]
            brand = resource["brand"]
            brand_name = brand["name"]

            # Фильтруем предложения
            valid_offers = [
                offer
                for offer in resource["offers"]
                if (
                    offer["average_period"] <= 15
                    and offer["reliability"] > 80
                    # проверяем соответствие brand_id
                    and resource["brand"]["id"] == original_item["brand_id"]
                )
            ]
            if not valid_offers:
                continue

            # Ищем минимальную цену
            min_price_offer = min(valid_offers, key=lambda x: x["price"])
            result = {
                "kod": original_item["kod"],
                "artikul": original_item["artikul"],
                "proizvoditel": original_item["proizvoditel"],
                "brand_id": original_item["brand_id"],
                "brand_name": brand_name,
                "warehouse_name": min_price_offer["warehouse"]["name"],
                "price": min_price_offer["price"],
                "quantity": min_price_offer["quantity"],
                "average_period": min_price_offer["average_period"],
                "reliability": min_price_offer["reliability"],
            }
            results.append(result)

            # Пример периодического сохранения
            if len(results) % 100 == 0:
                partial_df = pd.DataFrame(results)
                save_progress_to_csv(partial_df, filename="output/partial_result.csv")

    # После выхода из with все потоки завершены, собираем финальный датафрейм
    final_df = pd.DataFrame(results)
    if final_df.empty:
        print("No valid offers found.")
        return

    final_df["source"] = "berg"
    final_df["date_checked"] = datetime.today().strftime("%Y-%m-%d")

    # Фильтруем по цене > 0 и берём минимальную цену для каждого kod
    final_df = final_df[final_df["price"] > 0]
    final_df = final_df.loc[final_df.groupby("kod")["price"].idxmin()]

    final_df = final_df[
        [
            "kod",
            "artikul",
            "proizvoditel",
            "brand_id",
            "brand_name",
            "warehouse_name",
            "price",
            "quantity",
            "average_period",
            "reliability",
            "source",
            "date_checked",
        ]
    ]

    # Сохраняем в БД
    engine = get_db_engine()
    final_df.to_sql(
        "bergapi", con=engine, schema="api", if_exists="replace", index=False
    )
    print("Dataframe saved to the database successfully.")

    # Удаляем промежуточный файл
    remove_file()


if __name__ == "__main__":
    process_and_save_data()
