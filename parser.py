import logging
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

# Настройка логирования
logging.basicConfig(filename='errors.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.binary_location = "C:/Program Files/Google/chrome.exe"  # Укажите путь к вашему Chrome

    service = Service('C:/Users/evgen/repo/ugkorea/drivers/chromedriver.exe')  # Укажите путь к вашему ChromeDriver

    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'})
    
    return driver

def get_categories(driver):
    driver.get("https://krasnoyarsk.avtomoe.com/catalog")
    time.sleep(3)  # Добавлена задержка для загрузки страницы
    categories = driver.find_elements(By.CSS_SELECTOR, "a.catalogItem")
    category_links = [category.get_attribute("href") for category in categories]
    logging.info(f'Found {len(category_links)} categories.')
    return category_links[:2]  # Ограничиваем количество категорий для отладки

def get_product_links_from_category(driver, category_link):
    driver.get(category_link)
    time.sleep(3)  # Добавлена задержка для загрузки страницы
    product_links = []
    page_number = 1

    while True:
        logging.info(f'Processing page {page_number} of category: {category_link}')
        products = driver.find_elements(By.CSS_SELECTOR, "a.js-product-buy")
        product_links.extend([product.get_attribute("href") for product in products])
        logging.info(f'Found {len(products)} products on page {page_number}')
        
        try:
            next_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Следующие')]")
            next_button.click()
            time.sleep(3)  # Добавлена задержка для загрузки страницы
            page_number += 1
        except Exception as e:
            logging.info(f'No more pages in category: {category_link}')
            break
    
    return product_links

def get_product_details(driver, product_link):
    try:
        driver.get(product_link)
        time.sleep(3)  # Добавлена задержка для загрузки страницы
        product_details = {}

        product_details['link'] = product_link
        product_details['catalog_number'] = driver.find_element(By.CSS_SELECTOR, "div.type").text
        product_details['manufacturer'] = driver.find_element(By.CSS_SELECTOR, "div.value").text
        product_details['availability'] = driver.find_element(By.CSS_SELECTOR, "span.available").text
        product_details['price'] = driver.find_element(By.CSS_SELECTOR, "span.td-price").text
        return product_details
    except Exception as e:
        logging.error(f'Error getting product details: {e}')
        return None

def main():
    logging.info('Starting the scraping process...')
    driver = setup_driver()

    category_links = get_categories(driver)
    all_product_details = []
    common_tags = set()

    for category_link in category_links:
        logging.info(f'Processing category: {category_link}')
        product_links = get_product_links_from_category(driver, category_link)

        for product_link in product_links:
            logging.info(f'Processing product: {product_link}')
            details = get_product_details(driver, product_link)
            if details:
                all_product_details.append(details)
        
        # Собираем общие теги для анализа
        page_source = driver.page_source
        for line in page_source.split('\n'):
            common_tags.add(line.strip())

    driver.quit()

    # Записываем общие теги в файл
    with open('page_details.html', 'w', encoding='utf-8') as f:
        for tag in common_tags:
            f.write(f"{tag}\n")

    if all_product_details:
        df = pd.DataFrame(all_product_details)
        df.to_csv('product_details.csv', index=False)
        logging.info('Scraping completed successfully.')
        print(df.head(20))
    else:
        logging.info('No product details were scraped.')

if __name__ == "__main__":
    main()
