import time
import json
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
from tqdm import tqdm
from selenium_stealth import stealth

# --- Configuration ---
TEST_MODE = True

CATEGORY_URLS = [
    'https://shop.super-pharm.co.il/care/hair-care/c/15170000',
    'https://shop.super-pharm.co.il/care/oral-hygiene/c/15160000',
    # ... etc ...
    'https://shop.super-pharm.co.il/care/facial-skin-care/skincare-devices/c/15231800'
]
OUTPUT_FILE = "superpharm_products_final.jsonl"
TEST_OUTPUT_FILE = "superpharm_test_output.jsonl"
DELAY_BETWEEN_PAGES = 2.0
MAX_CLICKS_FAILSAFE = 200

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)
processed_product_ids = set()

def setup_driver():
    """Configures an advanced stealth-enabled, headless WebDriver."""
    logger.info("Setting up ADVANCED stealth-enabled Chrome WebDriver...")
    chrome_options = Options()
    
    # --- Core Options ---
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--window-size=1920,1080")
    
    # --- Anti-Bot-Detection Options from your Shufersal script ---
    chrome_options.add_argument("--lang=he-IL,he")
    chrome_options.add_experimental_option('prefs', {'intl.accept_languages': 'he-IL,he'})
    
    # --- Options from your original script ---
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    # Apply selenium-stealth for maximum effect
    stealth(driver,
            languages=["he-IL", "he", "en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True)
            
    driver.set_page_load_timeout(45)
    return driver

def scrape_category(driver, url, pbar, output_file):
    try:
        category_name = url.split('/')[-2]
        pbar.set_description(f"Navigating to {category_name}")
        driver.get(url)
        
        wait = WebDriverWait(driver, 20)

        try:
            wait.until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))).click()
        except TimeoutException:
            pass 

        try:
            pbar.set_description(f"Waiting for products to load for {category_name}")
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a.item-box-link")))
        except TimeoutException:
            logger.warning(f"No products found on page {url} after waiting. This could be an 'Access Denied' block. Skipping category.")
            # Let's save a debug screenshot to confirm
            driver.save_screenshot(f"debug_screenshot_{category_name}.png")
            logger.warning(f"Saved screenshot to debug_screenshot_{category_name}.png to check for blocks.")
            return

        pbar.set_description(f"Loading all products for {category_name}")
        click_count = 0
        while click_count < MAX_CLICKS_FAILSAFE:
            try:
                load_more_button = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "button.btn-more")))
                driver.execute_script("arguments[0].click();", load_more_button)
                click_count += 1
                time.sleep(DELAY_BETWEEN_PAGES)
            except Exception:
                break

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        product_links = soup.find_all('a', class_='item-box-link')
        pbar.set_description(f"Parsing {len(product_links)} products from {category_name}")
        
        with open(output_file, 'a', encoding='utf-8') as f:
            for link_container in product_links:
                try:
                    product_container = link_container.find('div', class_='item-box')
                    if not product_container: continue

                    product_id = product_container.get('data-id')
                    if not product_id or product_id in processed_product_ids:
                        continue

                    add_to_basket_div = product_container.find('div', class_='add-to-basket')
                    ean = add_to_basket_div.get('data-ean') if add_to_basket_div else ""
                    
                    product_data = {
                        'productId': product_id,
                        'ean': ean,
                        'brand': product_container.get('data-brand', ''),
                        'name': product_container.get('data-name', ''),
                        'price': product_container.get('data-price', ''),
                        'productUrl': link_container.get('href', ''),
                        'scrapedFrom': url
                    }
                    f.write(json.dumps(product_data, ensure_ascii=False) + '\n')
                    processed_product_ids.add(product_id)
                except Exception as e:
                    logger.warning(f"Could not parse a product on page {url}. Skipping. Error: {e}")
                    continue
    except Exception as e:
        logger.error(f"A critical error occurred while scraping {url}: {e}")
        driver.save_screenshot(f"error_screenshot_{url.split('/')[-2]}.png")

def main():
    driver = setup_driver()
    if not driver: return

    if TEST_MODE:
        logger.info("--- RUNNING IN TEST MODE ---")
        urls_to_scrape = CATEGORY_URLS[:1]
        output_file_to_use = TEST_OUTPUT_FILE
        logger.info(f"Will process 1 category and save to '{output_file_to_use}'")
    else:
        logger.info("--- RUNNING IN FULL PRODUCTION MODE ---")
        urls_to_scrape = CATEGORY_URLS
        output_file_to_use = OUTPUT_FILE
        logger.info(f"Will process all {len(urls_to_scrape)} categories and save to '{output_file_to_use}'")

    with open(output_file_to_use, 'w', encoding='utf-8') as f:
        pass
    
    try:
        with tqdm(urls_to_scrape, desc="Scraping Categories", unit="category") as pbar:
            for url in pbar:
                scrape_category(driver, url, pbar, output_file_to_use)
    finally:
        driver.quit()
        logger.info(f"\n--- SCRAPING FINISHED ---")
        logger.info(f"Found and saved a total of {len(processed_product_ids)} unique products.")
        logger.info(f"Data saved to {output_file_to_use}")

if __name__ == "__main__":
    main()