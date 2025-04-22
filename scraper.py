from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import sys
import os
from datetime import datetime

# === Screenshot directory ===
SCREENSHOT_DIR = "screenshots"
os.makedirs(SCREENSHOT_DIR, exist_ok=True)

def scroll_page(driver, pause=1.5, max_scrolls=5):
    last_height = driver.execute_script("return document.body.scrollHeight")
    for _ in range(max_scrolls):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

def scrape_shufersal_with_selenium(query):
    print(f"\n🔍 Searching for: {query}")

    options = Options()
    # options.add_argument("--headless")  # Uncomment to run headless
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)
    driver.set_window_size(1920, 1080)
    driver.get("https://www.shufersal.co.il/online/he/")

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "js-site-search-input"))
        )
        search_box = driver.find_element(By.ID, "js-site-search-input")
        search_box.clear()
        search_box.send_keys(query)
        search_box.send_keys(Keys.RETURN)

        time.sleep(5)
        scroll_page(driver, pause=2, max_scrolls=5)

        # Save screenshot
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_query = "".join(c if c.isalnum() else "_" for c in query)
        filename = f"{safe_query}_{timestamp}.png"
        filepath = os.path.join(SCREENSHOT_DIR, filename)
        driver.save_screenshot(filepath)
        print(f"📸 Screenshot saved to: {filepath}")

        # Optional: print some sample prices
        whole_spans = driver.find_elements(By.CLASS_NAME, "whole")
        results = []

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
            except Exception:
                continue

        return results

    finally:
        driver.quit()

# === Entry point ===
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("⚠️  Missing query. Usage: python scraper.py 'מעדן'")
        sys.exit(1)

    query = sys.argv[1]
    results = scrape_shufersal_with_selenium(query)

    if results:
        print("\n🛒 Results:")
        for item in results:
            print(f"{item['title']}: ₪{item['price_ils']}")
    else:
        print("⚠️ No products found.")
