import logging
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

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

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.catalogItem"))
        )
    except Exception as e:
        logging.error(f'Ошибка ожидания категорий: {e}')
    
    current_url = driver.current_url
    logging.info(f'Текущий URL: {current_url}')
    
    logging.info(f'Длина исходного кода страницы: {len(driver.page_source)}')
    
    categories = driver.find_elements(By.CSS_SELECTOR, "a.catalogItem")
    logging.info(f'Найдено элементов категорий: {len(categories)}')
    
    for category in categories:
        logging.info(f'Категория: {category.get_attribute("href")}')
    
    category_links = [category.get_attribute("href") for category in categories]
    logging.info(f'Найдено {len(category_links)} категорий.')
    return category_links

def get_product_links_from_category(driver, category_link):
    driver.get(category_link)
    time.sleep(3)  # Добавлена задержка для загрузки страницы
    product_links = []

    while True:
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "article.product-teaser"))
            )
        except Exception as e:
            logging.error(f'Ошибка ожидания продуктов: {e}')
        
        products = driver.find_elements(By.CSS_SELECTOR, "article.product-teaser a")
        product_links.extend([product.get_attribute("href") for product in products if product.get_attribute("href")])
        logging.info(f'Найдено {len(products)} продуктов на текущей странице.')

        # Проверка наличия кнопки "Следующие" и ее доступности
        try:
            next_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Следующие')]")
            if "disabled" in next_button.get_attribute("class"):
                break
            else:
                next_button.click()
                time.sleep(3)  # Добавлена задержка для загрузки следующей страницы
        except Exception as e:
            logging.error(f'Ошибка перехода на следующую страницу: {e}')
            break

    logging.info(f'Всего продуктов найдено в категории {category_link}: {len(product_links)}')

    return product_links

def get_product_details(driver, product_link):
    driver.get(product_link)
    time.sleep(3)  # Добавлена задержка для загрузки страницы

    product_details = {}
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    product_details['link'] = product_link
    product_details['name'] = soup.find('h1').text.strip() if soup.find('h1') else 'N/A'
    product_details['price'] = soup.select_one('.td-price').text.strip() if soup.select_one('.td-price') else 'N/A'
    product_details['availability'] = soup.select_one('.available').text.strip() if soup.select_one('.available') else 'N/A'
    product_details['catalog_number'] = soup.find('div', class_='title', text='Каталожный номер').find_next_sibling('div', class_='value').text.strip() if soup.find('div', class_='title', text='Каталожный номер') else 'N/A'
    product_details['manufacturer'] = soup.find('div', class_='title', text='Производитель').find_next_sibling('div', class_='value').text.strip() if soup.find('div', class_='title', text='Производитель') else 'N/A'
    product_details['stock'] = soup.select('td span')[1].text.strip() if len(soup.select('td span')) > 1 else 'N/A'

    return product_details

def main():
    logging.info('Запуск процесса скрапинга...')
    driver = setup_driver()

    category_links = get_categories(driver)
    logging.info(f'Найдено {len(category_links)} категорий.')
    
    all_product_details = []

    # Ограничение на первые две категории
    for category_link in category_links[:2]:  # Обработка первых двух категорий
        logging.info(f'Обработка категории: {category_link}')
        product_links = get_product_links_from_category(driver, category_link)
        
        for product_link in product_links:
            logging.info(f'Обработка продукта: {product_link}')
            product_details = get_product_details(driver, product_link)
            all_product_details.append(product_details)

    if all_product_details:
        df = pd.DataFrame(all_product_details)
        df = df[['link', 'name', 'catalog_number', 'manufacturer', 'availability', 'stock', 'price']]  # Удалена колонка description
        df.to_csv('product_details.csv', index=False)
        logging.info('Детали продуктов сохранены в product_details.csv')
        print(df.head(20))
    else:
        logging.info('Детали продуктов не были скрапированы.')

    driver.quit()
    logging.info('Процесс скрапинга завершен.')

if __name__ == "__main__":
    main()
