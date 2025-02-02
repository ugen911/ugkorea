import time
import os
import pandas as pd
import requests
from ugkorea.api.bergbrandsid import get_price_with_brand_id as get_final_df
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
            "key": API_KEY,
            "items[0][resource_article]": item["resource_article"],
            "items[0][brand_id]": item["brand_id"],
            "analogs": 0,  # Добавляем параметр analogs=0
        }

        try:
            response = requests.get(
                url, params=params, timeout=10
            )  # Увеличиваем время ожидания до 10 секунд
        except requests.exceptions.ConnectTimeout:
            return None, "Connection timeout"
        except requests.exceptions.RequestException as e:
            return None, f"Request error: {e}"

        if response.status_code != 200:
            return None, f"HTTP Error {response.status_code}"

        if not response.text:
            return None, "Empty response"

        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError:
            return None, f"JSON Decode Error: {response.text}"

        if "errors" in data:
            error_messages = ", ".join(
                [f"Code: {err['code']}, Text: {err['text']}" for err in data["errors"]]
            )
            return None, f"API Error: {error_messages}"

        return data, None

    def save_progress_to_csv(df, filename):
        if not os.path.exists("output"):
            os.makedirs("output")
        df.to_csv(filename, index=False)

    def save_failed_items(df, filename):
        if not df.empty:
            df.to_excel(filename, index=False)

    def save_api_responses(data_list, filename):
        with open(filename, "w", encoding="utf-8") as f:
            for data in data_list:
                f.write(str(data))
                f.write("\n" + "-" * 100 + "\n" * 10)  # Разделение по 10 строк

    df = get_final_df()
    if df.empty:
        print("Dataframe is empty")
        return

    df.reset_index(drop=True, inplace=True)
    results = []
    failed_items = []
    api_responses = []

    max_iterations = 500 if DEBUG_MODE else len(df)

    try:
        for i in tqdm(range(max_iterations), desc="Processing items"):
            kod = df.loc[i, "kod"]
            artikul = df.loc[i, "artikul"]
            proizvoditel = df.loc[i, "proizvoditel"]
            brand_id = df.loc[i, "brand_id"]

            item = {"resource_article": artikul, "brand_id": brand_id}
            stock_info, error = get_stock_info(item)

            if error:
                failed_items.append(
                    {**item, "kod": kod, "proizvoditel": proizvoditel, "reason": error}
                )
                api_responses.append({"item": item, "response": stock_info})
                continue

            if "resources" not in stock_info or not stock_info["resources"]:
                failed_items.append(
                    {
                        **item,
                        "kod": kod,
                        "proizvoditel": proizvoditel,
                        "reason": "No resources found",
                    }
                )
                api_responses.append({"item": item, "response": stock_info})
                continue

            valid_offers = []

            for resource in stock_info["resources"]:
                if resource["brand"]["id"] == brand_id:
                    valid_offers.extend(
                        [
                            offer
                            for offer in resource["offers"]
                            if offer["average_period"] <= 20
                            and offer["reliability"] >= 80
                        ]
                    )

            if not valid_offers:
                failed_items.append(
                    {
                        **item,
                        "kod": kod,
                        "proizvoditel": proizvoditel,
                        "reason": "No valid offers",
                    }
                )
                api_responses.append({"item": item, "response": stock_info})
                continue

            min_price_offer = min(valid_offers, key=lambda x: x["price"])
            results.append(
                {
                    "kod": kod,
                    "artikul": artikul,
                    "proizvoditel": proizvoditel,
                    "brand_id": brand_id,
                    "brand_name": resource["brand"]["name"],
                    "warehouse_name": min_price_offer["warehouse"]["name"],
                    "price": min_price_offer["price"],
                    "quantity": min_price_offer["quantity"],
                    "average_period": min_price_offer["average_period"],
                    "reliability": min_price_offer["reliability"],
                }
            )

    except Exception as e:
        print(f"An error occurred: {e}")
        save_failed_items(pd.DataFrame(failed_items), "output/failed_items.xlsx")
        save_api_responses(api_responses, "output/api_responses.txt")
        raise

    final_df = pd.DataFrame(results)
    final_df["source"] = "berg"
    final_df["date_checked"] = datetime.today().strftime("%Y-%m-%d")
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

    engine = get_db_engine()
    final_df.to_sql(
        "bergapi", con=engine, schema="api", if_exists="replace", index=False
    )
    print("Dataframe saved to the database successfully.")

    save_failed_items(pd.DataFrame(failed_items), "output/failed_items.xlsx")
    save_api_responses(api_responses, "output/api_responses.txt")

    print("Failed items and API responses saved.")


if __name__ == "__main__":
    process_and_save_data()
