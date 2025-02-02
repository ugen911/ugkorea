import time
import os
import pandas as pd
import requests
from ugkorea.api.bergbrandsid import get_price_with_brand_id as get_final_df
from ugkorea.db.database import get_db_engine
from ugkorea.api.config import BASE_URL_BERG, API_KEY_BERG
from datetime import datetime
from tqdm import tqdm

DEBUG_MODE = False  # True для отладки (при отладке обрабатывается 1000 позиций)


def get_stock_info(item, base_url, api_key):
    """
    Выполняет запрос к API для получения информации о наличии товара.
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

    # Определяем, сколько итераций делаем:
    # Если DEBUG_MODE включен, то обрабатывается 1000 позиций, иначе все позиции.
    max_iterations = 1000 if DEBUG_MODE else len(df)

    # Готовим список всех товаров для последовательной обработки
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
    failed_items = (
        []
    )  # Список для товаров, по которым не удалось получить корректный результат

    # Обрабатываем товары последовательно — следующий запрос отправляется только после получения ответа предыдущего
    for it in tqdm(items, desc="Processing items"):
        stock_info = get_stock_info(
            {"resource_article": it["artikul"], "brand_id": it["brand_id"]},
            BASE_URL,
            API_KEY,
        )

        if not stock_info:
            failed_items.append(
                {**it, "reason": "API вернул пустой ответ или произошла ошибка сети"}
            )
            continue

        if "resources" not in stock_info or not stock_info["resources"]:
            failed_items.append({**it, "reason": "В ответе отсутствуют ресурсы"})
            continue

        resource = stock_info["resources"][0]
        brand = resource["brand"]
        brand_name = brand["name"]

        # Фильтруем предложения по условиям:
        # average_period <= 15, reliability > 80 и совпадение brand_id
        valid_offers = [
            offer
            for offer in resource["offers"]
            if (
                offer["average_period"] <= 15
                and offer["reliability"] > 80
                and resource["brand"]["id"] == it["brand_id"]
            )
        ]
        if not valid_offers:
            # Берем только предложения с нужным brand_id
            offers_matching_brand = [
                offer
                for offer in resource["offers"]
                if resource["brand"]["id"] == it["brand_id"]
            ]
            if not offers_matching_brand:
                reason_text = "Предложения с нужным brand_id отсутствуют в ответе."
            else:
                # Сортируем по цене и берем топ-3
                sorted_offers = sorted(offers_matching_brand, key=lambda x: x["price"])
                top_offers = sorted_offers[:3]
                additional_data = []
                for offer in top_offers:
                    warehouse_name = offer["warehouse"]["name"]
                    price = offer["price"]
                    quantity = offer["quantity"]
                    average_period = offer["average_period"]
                    reliability = offer["reliability"]
                    additional_data.append(
                        f"Warehouse: {warehouse_name}, Brand: {brand_name}, Artikul: {it['artikul']}, Price: {price}, Quantity: {quantity}, AvgPeriod: {average_period}, Reliability: {reliability}"
                    )
                additional_str = " | ".join(additional_data)
                reason_text = f"Не найдено валидных предложений по условиям (цена/срок/надежность). Доп. данные: {additional_str}"
            failed_items.append({**it, "reason": reason_text})
            continue

        # Если найдены валидные предложения — выбираем то, у которого минимальная цена
        min_price_offer = min(valid_offers, key=lambda x: x["price"])
        result = {
            "kod": it["kod"],
            "artikul": it["artikul"],
            "proizvoditel": it["proizvoditel"],
            "brand_id": it["brand_id"],
            "brand_name": brand_name,
            "warehouse_name": min_price_offer["warehouse"]["name"],
            "price": min_price_offer["price"],
            "quantity": min_price_offer["quantity"],
            "average_period": min_price_offer["average_period"],
            "reliability": min_price_offer["reliability"],
        }
        results.append(result)

        # Пример периодического сохранения промежуточных результатов
        if len(results) % 100 == 0:
            partial_df = pd.DataFrame(results)
            save_progress_to_csv(partial_df, filename="output/partial_result.csv")

    # Формируем финальный DataFrame для успешных результатов
    final_df = pd.DataFrame(results)
    if final_df.empty:
        print("No valid offers found.")
    else:
        final_df["source"] = "berg"
        final_df["date_checked"] = datetime.today().strftime("%Y-%m-%d")

        # Фильтруем по цене > 0 и оставляем минимальное предложение для каждого товара (по полю kod)
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

        # Сохраняем успешные результаты в БД
        engine = get_db_engine()
        final_df.to_sql(
            "bergapi", con=engine, schema="api", if_exists="replace", index=False
        )
        print("Dataframe saved to the database successfully.")

    # Сохраняем неудачные позиции в Excel с указанием причины
    if failed_items:
        failed_df = pd.DataFrame(failed_items)
        failed_filename = "output/failed_items.xlsx"
        if not os.path.exists("output"):
            os.makedirs("output")
        failed_df.to_excel(failed_filename, index=False)
        print(f"Failed items saved to {failed_filename}.")

    # Удаляем промежуточный файл
    remove_file()


if __name__ == "__main__":
    process_and_save_data()
