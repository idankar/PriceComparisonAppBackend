import time
import json
import logging
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
from selenium_stealth import stealth
from tqdm import tqdm # Import tqdm

# --- Configuration ---
CATEGORY_URLS = [
    'https://shop.super-pharm.co.il/care/hair-care/c/15170000',
    'https://shop.super-pharm.co.il/care/oral-hygiene/c/15160000',
    'https://shop.super-pharm.co.il/care/deodorants/c/15150000',
    'https://shop.super-pharm.co.il/care/shaving-and-hair-removal/c/15140000',
    'https://shop.super-pharm.co.il/care/bath-and-hygiene/c/15120000',
    'https://shop.super-pharm.co.il/care/feminine-hygiene-products/c/15210000',
    'https://shop.super-pharm.co.il/care/sun-protection/c/15100000',
    'https://shop.super-pharm.co.il/care/for-children/c/15130000',
    'https://shop.super-pharm.co.il/care/facial-skin-care/c/15230000',
    'https://shop.super-pharm.co.il/care/body-care/c/15220000',
    'https://shop.super-pharm.co.il/care/eye-care/c/15260000',
    'https://shop.super-pharm.co.il/care/beard-care/c/15250000',
    'https://shop.super-pharm.co.il/cosmetics/perfumes/c/20110000',
    'https://shop.super-pharm.co.il/cosmetics/cosmetics-brands-care-products/c/20230000',
    'https://shop.super-pharm.co.il/cosmetics/facial-makeup/c/20180000',
    'https://shop.super-pharm.co.il/cosmetics/eye-makeup/c/20170000',
    'https://shop.super-pharm.co.il/cosmetics/lip-makeup/c/20190000',
    'https://shop.super-pharm.co.il/cosmetics/eyebrows-makeup/c/20100000',
    'https://shop.super-pharm.co.il/cosmetics/makeup-tool/c/20140000',
    'https://shop.super-pharm.co.il/cosmetics/makeup-kits/c/20200000',
    'https://shop.super-pharm.co.il/cosmetics/facial-spray/c/20220000',
    'https://shop.super-pharm.co.il/cosmetics/nail-care/c/20130000',
    'https://shop.super-pharm.co.il/cosmetics/pallets/c/20210000',
    'https://shop.super-pharm.co.il/infants-and-toddlers/baby-care/c/25130000',
    'https://shop.super-pharm.co.il/infants-and-toddlers/baby-wash/c/25200000',
    'https://shop.super-pharm.co.il/infants-and-toddlers/nursing-and-feeding/c/25120000',
    'https://shop.super-pharm.co.il/infants-and-toddlers/pacifiers-and-teethers/c/25140000',
    'https://shop.super-pharm.co.il/infants-and-toddlers/diapering/c/25110000',
    'https://shop.super-pharm.co.il/health/medicines/c/30140000',
    'https://shop.super-pharm.co.il/health/supplements/c/30300000',
    'https://shop.super-pharm.co.il/health/first-aid/c/30100000',
    'https://shop.super-pharm.co.il/health/measuring-and-testing/c/30220000',
    'https://shop.super-pharm.co.il/health/sexual-wellness-and-sex-toys/c/30210000',
    'https://shop.super-pharm.co.il/health/medical-devices/c/30230000',
    'https://shop.super-pharm.co.il/health/lice/c/30200000',
    'https://shop.super-pharm.co.il/health/respiratory-therapy/c/30190000',
    'https://shop.super-pharm.co.il/health/natural-health/c/30170000',
    'https://shop.super-pharm.co.il/health/orthopedics/c/30160000',
    'https://shop.super-pharm.co.il/health/incontinence/c/30250000',
    'https://shop.super-pharm.co.il/optics/contact-lenses/c/65120000',
    'https://shop.super-pharm.co.il/optics/sunglasses/c/65110000',
    'https://shop.super-pharm.co.il/optics/cleaning-storage-and-accessories/c/65130000',
    'https://shop.super-pharm.co.il/care/dermocosmetics/body-care-dermocosmetics/c/15271200',
    'https://shop.super-pharm.co.il/care/dermocosmetics/facial-skin-care-dermocosmetics/c/15271000',
    'https://shop.super-pharm.co.il/care/dermocosmetics/hair-care-dermocosmetics/c/15271300',
    'https://shop.super-pharm.co.il/care/dermocosmetics/kids-care-dermocosmetics/c/15271100',
    'https://shop.super-pharm.co.il/care/dermocosmetics/sun-protection-dermocosmetics/c/15271400',
    'https://shop.super-pharm.co.il/care/dermocosmetics/deodorants-dermocosmetics/c/15271500',
    'https://shop.super-pharm.co.il/care/facial-skin-care/skincare-devices/c/15231800'
]
# --- End of Configuration ---

OUTPUT_FILE = "superpharm_products_final.jsonl"
REQUEST_TIMEOUT = 30
DELAY_BETWEEN_PAGES = 2.0
MAX_CLICKS_FAILSAFE = 150

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)
processed_product_ids = set()

def setup_driver():
    """Configures a stealth-enabled, headless Selenium WebDriver."""
    logger.info("Setting up stealth-enabled Chrome WebDriver...")
    chrome_options = ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    try:
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        stealth(driver, languages=["en-US", "en"], vendor="Google Inc.", platform="Win32", webgl_vendor="Intel Inc.", renderer="Intel Iris OpenGL Engine", fix_hairline=True)
        driver.set_page_load_timeout(REQUEST_TIMEOUT)
        return driver
    except Exception as e:
        logger.error(f"Failed to setup WebDriver: {e}")
        return None

def scrape_category(driver, url, pbar):
    try:
        pbar.set_description(f"Navigating to {url.split('/')[-1]}")
        driver.get(url)
        
        try:
            cookie_accept_button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))
            cookie_accept_button.click()
            time.sleep(1)
        except TimeoutException:
            pass # No cookie banner found

        click_count = 0
        pbar.set_description(f"Loading all products for {url.split('/')[-1]}")
        while click_count < MAX_CLICKS_FAILSAFE:
            try:
                load_more_button = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "button.btn-more")))
                driver.execute_script("arguments[0].click();", load_more_button)
                click_count += 1
                time.sleep(DELAY_BETWEEN_PAGES)
            except TimeoutException:
                break # No more 'Load More' buttons found
            except Exception:
                break

        soup = BeautifulSoup(driver.page_source, 'html.parser')

        ean_map = {}
        try:
            data_script = soup.find('script', string=lambda t: t and 'impressions' in t and 'productDetails' in t)
            if data_script:
                match = re.search(r'"impressions"\s*:\s*(\[.*?\])', data_script.string, re.DOTALL)
                if match:
                    products_list = json.loads(match.group(1))
                    ean_map = {
                        prod.get('productDetails', {}).get('code'): prod.get('productDetails', {}).get('ean')
                        for prod in products_list if prod.get('productDetails')
                    }
        except Exception as e:
            logger.warning(f"Could not parse EAN data for {url}: {e}")
        
        product_links = soup.find_all('a', class_='item-box-link')
        pbar.set_description(f"Parsing {len(product_links)} products from {url.split('/')[-1]}")
        
        with open(OUTPUT_FILE, 'a', encoding='utf-8') as f:
            for link_container in product_links:
                try:
                    product_container = link_container.find('div', class_='item-box')
                    if not product_container: continue

                    product_id = product_container.get('data-id')
                    if not product_id or product_id in processed_product_ids: continue

                    ean = ean_map.get(product_id, '')
                    
                    product_data = {
                        'productId': product_id,
                        'ean': ean,
                        'brand': product_container.get('data-brand', ''),
                        'name': product_container.get('data-name', ''),
                        'price': product_container.get('data-price', ''),
                        'imageUrl': product_container.select_one('.item-image img')['src'] if product_container.select_one('.item-image img') else '',
                        'productUrl': link_container.get('href', ''),
                        'scrapedFrom': url
                    }
                    f.write(json.dumps(product_data, ensure_ascii=False) + '\n')
                    processed_product_ids.add(product_id)
                except Exception:
                    continue
    except Exception as e:
        logger.error(f"Critical error scraping {url}: {e}")

def main():
    driver = setup_driver()
    if not driver: return
    
    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            logger.info(f"Output file {OUTPUT_FILE} cleared for new session.")
        
        # Wrap the CATEGORY_URLS list with tqdm for a progress bar
        with tqdm(CATEGORY_URLS, desc="Scraping Categories", unit="category") as pbar:
            for url in pbar:
                scrape_category(driver, url, pbar)

    finally:
        logger.info(f"Scraping finished. Found a total of {len(processed_product_ids)} unique products.")
        if driver:
            driver.quit()

if __name__ == "__main__":
    main()