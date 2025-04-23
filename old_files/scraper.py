#!/usr/bin/env python3
# src/scraper.py - Website scraper for product screenshots

import sys
import os
import time
from datetime import datetime
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Add the project root to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(config.LOGS_DIR, "scraper.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def scroll_page(driver, pause=1.5, max_scrolls=5):
    """Scroll the page to load dynamic content"""
    last_height = driver.execute_script("return document.body.scrollHeight")
    for _ in range(max_scrolls):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

def scrape_shufersal(query):
    """
    Scrape Shufersal website for product data and screenshots
    
    Args:
        query (str): Search query for products
        
    Returns:
        dict: Paths to saved screenshots and extracted product data
    """
    logger.info(f"🔍 Searching for: {query}")
    
    # Get query-specific paths
    paths = config.get_query_paths(query)
    
    options = Options()
    # options.add_argument("--headless")  # Uncomment to run headless
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)
    driver.set_window_size(1920, 1080)
    driver.get("https://www.shufersal.co.il/online/he/")

    screenshots = []
    results = []
    
    try:
        # Wait for search box to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "js-site-search-input"))
        )
        search_box = driver.find_element(By.ID, "js-site-search-input")
        search_box.clear()
        search_box.send_keys(query)
        search_box.send_keys(Keys.RETURN)

        # Wait for results to load
        time.sleep(5)
        scroll_page(driver, pause=2, max_scrolls=5)

        # Take multiple screenshots to capture more products
        for i in range(3):  # Take 3 screenshots
            driver.execute_script(f"window.scrollTo(0, {i * 800});")  # Scroll down for each screenshot
            time.sleep(1)
            
            # Save screenshot
            run_id = paths["run_id"]
            filename = f"screenshot_{run_id}_page_{i}.png"
            filepath = os.path.join(paths["screenshots_dir"], filename)
            driver.save_screenshot(filepath)
            screenshots.append(filepath)
            logger.info(f"📸 Screenshot saved to: {filepath}")

        # Extract product information
        whole_spans = driver.find_elements(By.CLASS_NAME, "whole")
        
        for i, whole in enumerate(whole_spans):
            try:
                parent = whole.find_element(By.XPATH, "..")
                fraction = parent.find_element(By.CLASS_NAME, "fraction").text.strip()
                price = float(f"{whole.text.strip()}.{fraction}")

                container = parent
                for _ in range(4):
                    container = container.find_element(By.XPATH, "..")

                title = None
                for c in container.find_elements(By.XPATH, ".//div"):
                    t = c.text.strip()
                    if len(t) > 6 and "₪" not in t and not t.startswith("הוסף"):
                        title = t
                        break

                if title:
                    results.append({"title": title, "price_ils": price})
            except Exception as e:
                logger.warning(f"Error extracting product info: {str(e)}")
                continue

    except Exception as e:
        logger.error(f"Error during scraping: {str(e)}")
    finally:
        driver.quit()

    return {
        "screenshots": screenshots,
        "results": results,
        "paths": paths
    }

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("⚠️  Missing query. Usage: python scraper.py 'מעדן'")
        sys.exit(1)

    query = sys.argv[1]
    scrape_result = scrape_shufersal(query)
    
    if scrape_result["results"]:
        logger.info("\n🛒 Results:")
        for item in scrape_result["results"]:
            logger.info(f"{item['title']}: ₪{item['price_ils']}")
    else:
        logger.warning("⚠️ No products found.")
    
    logger.info(f"✅ Saved {len(scrape_result['screenshots'])} screenshots for query: {query}")