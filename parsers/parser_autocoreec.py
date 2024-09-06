###Парсер avtomoe


import logging
import time
import random
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from ugkorea.db.database import get_db_engine
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, NoSuchElementException, TimeoutException
from bs4 import BeautifulSoup
import psutil

# Настройка логирования
logging.basicConfig(filename='errors.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def setup_driver():
    chrome_options = Options()
    chrome_prefs = {
        "profile.default_content_settings": {"cookies": 2},
        "profile.managed_default_content_settings": {"cookies": 2}
    }
    chrome_options.add_experimental_option("prefs", chrome_prefs)
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--log-level=3")
    chrome_options.binary_location = "C:/Program Files/Google/chrome.exe"  # Укажите путь к вашему Chrome

    # Формируем путь к chromedriver
    driver_path = os.path.expanduser('~/repo/ugkorea/drivers/chromedriver.exe')
    
    service = Service(driver_path)  # Укажите путь к вашему ChromeDriver

    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'})
    
    return driver

def get_categories(driver):
    driver.get("https://krasnoyarsk.avtomoe.com/catalog")
    print("Загружается страница каталога...")
    logging.info("Загружается страница каталога...")

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.catalogItem"))
        )
    except Exception as e:
        logging.error(f'Ошибка ожидания категорий: {e}')
    
    categories = driver.find_elements(By.CSS_SELECTOR, "a.catalogItem")
    
    category_links = [category.get_attribute("href") for category in categories]
    return category_links

def get_product_links_from_page(driver):
    product_links = []
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "article.product-teaser"))
        )
        products = driver.find_elements(By.CSS_SELECTOR, "article.product-teaser")

        for product in products:
            try:
                if "В наличии" in product.text:
                    product_links.append(product.find_element(By.CSS_SELECTOR, "a").get_attribute("href"))
            except StaleElementReferenceException:
                logging.error('StaleElementReferenceException поймано. Повторная попытка...')
                # Повторная попытка нахождения продукта
                WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "article.product-teaser"))
                )
                products = driver.find_elements(By.CSS_SELECTOR, "article.product-teaser")
                for product in products:
                    if "В наличии" in product.text:
                        product_links.append(product.find_element(By.CSS_SELECTOR, "a").get_attribute("href"))
    except TimeoutException as e:
        logging.error(f'Ошибка ожидания продуктов: {e}')
    logging.info(f'Найдено {len(product_links)} товаров на странице.')
    return product_links

def go_to_next_page(driver):
    try:
        next_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Следующие')]"))
        )
        if next_button.is_displayed() and next_button.is_enabled():
            driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
            time.sleep(1)  # Небольшая задержка перед кликом
            driver.execute_script("arguments[0].click();", next_button)  # Использование JavaScript для клика
            time.sleep(3)  # Добавлена задержка для загрузки следующей страницы
            # Проверка, загружены ли новые продукты
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "article.product-teaser"))
            )
            new_products = driver.find_elements(By.CSS_SELECTOR, "article.product-teaser")
            if len(new_products) > 0:
                logging.info(f'Перешли на следующую страницу, найдено {len(new_products)} новых товаров.')
                return True
    except StaleElementReferenceException:
        logging.error('StaleElementReferenceException поймано в go_to_next_page. Повторная попытка...')
        # Повторная попытка нахождения кнопки "Следующие"
        return go_to_next_page(driver)
    except NoSuchElementException:
        logging.info('Кнопка "Следующие" не найдена, возможно это последняя страница.')
    except Exception as e:
        logging.error(f'Ошибка перехода на следующую страницу: {e}')
    return False

def get_all_product_links(driver, category_link, debug_mode=False):
    driver.get(category_link)
    time.sleep(5)  # Добавлена задержка для загрузки страницы
    product_links = set()  # Используем множество для хранения уникальных ссылок
    page_count = 0  # Переменная для подсчета количества страниц

    while True:
        new_product_links = get_product_links_from_page(driver)
        if not new_product_links and page_count == 0:
            logging.info(f'На первой странице категории {category_link} не найдено товаров.')
            break

        product_links.update(new_product_links)  # Добавляем ссылки во множество
        page_count += 1

        if debug_mode and page_count >= 10:
            break

        if not go_to_next_page(driver):
            break

    logging.info(f'Категория: {category_link} - Всего страниц: {page_count}, Всего товаров: {len(product_links)}')
    print(f'Категория: {category_link} - Всего страниц: {page_count}, Всего товаров: {len(product_links)}')
    return list(product_links)  # Преобразуем множество обратно в список

def get_product_details(driver, product_link):
    driver.get(product_link)
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "h1"))
        )
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".td-price"))
        )
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".available"))
        )
    except StaleElementReferenceException:
        logging.error('StaleElementReferenceException поймано в get_product_details. Повторная попытка...')
        return get_product_details(driver, product_link)
    except Exception as e:
        logging.error(f'Ошибка ожидания элементов страницы продукта: {e}')
        return {
            'link': product_link,
            'name': 'N/A',
            'price': 'N/A',
            'availability': 'N/A',
            'catalog_number': 'N/A',
            'manufacturer': 'N/A',
            'stock': 'N/A'
        }

    soup = BeautifulSoup(driver.page_source, 'html.parser')

    product_details = {
        'link': product_link,
        'name': soup.find('h1').text.strip() if soup.find('h1') else 'N/A',
        'price': soup.select_one('.td-price').text.strip() if soup.select_one('.td-price') else 'N/A',
        'availability': soup.select_one('.available').text.strip() if soup.select_one('.available') else 'N/A',
        'catalog_number': soup.find('div', class_='title', text='Каталожный номер').find_next_sibling('div', class_='value').text.strip() if soup.find('div', class_='title', text='Каталожный номер') else 'N/A',
        'manufacturer': soup.find('div', class_='title', text='Производитель').find_next_sibling('div', class_='value').text.strip() if soup.find('div', class_='title', text='Производитель') else 'N/A',
        'stock': soup.select('td span')[1].text.strip() if len(soup.select('td span')) > 1 else 'N/A'
    }

    return product_details

def process_category(category_link, driver, debug_mode=False):
    try:
        logging.info(f'Обработка категории: {category_link}')
        print(f'Обработка категории: {category_link}')
        product_links = get_all_product_links(driver, category_link, debug_mode)
        return product_links
    except Exception as e:
        logging.error(f'Ошибка обработки категории {category_link}: {e}')
        return []

def kill_chrome_processes():
    for process in psutil.process_iter():
        try:
            if process.name() == "chrome.exe" or process.name() == "chromedriver.exe":
                process.kill()
        except psutil.NoSuchProcess:
            pass

def main(debug_mode=False):
    logging.info('Запуск процесса скрапинга...')
    print('Запуск процесса скрапинга...')
    drivers = [setup_driver() for _ in range(5)]  # Создаем пул из 5 драйверов

    try:
        driver = drivers[0]  # Используем первый драйвер для получения категорий
        category_links = get_categories(driver)
        total_categories = len(category_links)
        print(f'Найдено {total_categories} категорий.')
        logging.info(f'Найдено {total_categories} категорий.')

        if debug_mode:
            category_links = random.sample(category_links, 3)
            logging.info(f'Режим отладки: выбрано 3 случайные категории.')
            print(f'Режим отладки: выбрано 3 случайные категории.')

        all_product_details = []
        all_product_links = set()  # Используем множество для хранения уникальных ссылок

        print('Начинаем обработку категорий...')
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(process_category, category_link, drivers[i % len(drivers)], debug_mode): category_link for i, category_link in enumerate(category_links)}
            for future in as_completed(futures):
                category_link = futures[future]
                try:
                    product_links = future.result()
                    if debug_mode:
                        product_links = random.sample(product_links, min(10, len(product_links)))
                    all_product_links.update(product_links)  # Добавляем ссылки во множество
                except Exception as e:
                    logging.error(f'Ошибка обработки категории {category_link}: {e}')
        print('Обработка категорий завершена.')

        logging.info(f'Всего ссылок на продукты: {len(all_product_links)}')
        print(f'Всего ссылок на продукты: {len(all_product_links)}')

        print('Начинаем обработку товаров...')
        total_products = len(all_product_links)
        processed_products = 0

        for product_link in all_product_links:
            product_details = get_product_details(driver, product_link)
            if not all(value == 'N/A' for key, value in product_details.items() if key != 'link'):
                all_product_details.append(product_details)
            processed_products += 1
            print(f'Обработан продукт {processed_products} из {total_products}: {product_details}')
            logging.info(f'Скрапирован продукт: {product_details}')
            
        if all_product_details:
            df = pd.DataFrame(all_product_details)
            df = df[['link', 'name', 'catalog_number', 'manufacturer', 'availability', 'stock', 'price']]
            engine = get_db_engine()
            df.to_sql('autocoreec', engine, schema='konkurents', if_exists='replace', index=False)
            logging.info('Детали продуктов сохранены в product_details.csv')
            print('Детали продуктов сохранены в autocoreec схема api')
            print(df.head(20))
        else:
            logging.info('Детали продуктов не были скрапированы.')
            print('Детали продуктов не были скрапированы.')

    except Exception as e:
        logging.error(f'Ошибка во время выполнения скрипта: {e}')
    finally:
        for driver in drivers:
            driver.quit()
        kill_chrome_processes()
        logging.info('Процесс скрапинга завершен.')
        print('Процесс скрапинга завершен.')

if __name__ == "__main__":
    main(debug_mode=False)  # Установите debug_mode=True для режима отладки