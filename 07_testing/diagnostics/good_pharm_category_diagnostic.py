#!/usr/bin/env python3
"""
Good Pharm Category Name Diagnostic Script
Checks how to extract category names from Good Pharm website.
"""

import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

def extract_category_info(url):
    """Extract all possible category information from a Good Pharm page"""

    # Setup Chrome options
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        print(f"🔍 Loading: {url}")
        driver.get(url)
        time.sleep(5)  # Wait for page to fully load

        # Extract page title
        title = driver.title
        print(f"📄 Page Title: {title}")

        # Look for breadcrumbs
        try:
            breadcrumb_elements = driver.find_elements(By.CSS_SELECTOR, ".breadcrumb, .breadcrumbs, .woocommerce-breadcrumb, nav ol, nav ul")
            for i, breadcrumb in enumerate(breadcrumb_elements):
                print(f"🍞 Breadcrumb {i+1}: {breadcrumb.text}")
        except:
            print("❌ No breadcrumbs found")

        # Look for headings
        for heading_tag in ['h1', 'h2', 'h3']:
            try:
                headings = driver.find_elements(By.TAG_NAME, heading_tag)
                for i, heading in enumerate(headings):
                    if heading.text.strip():
                        print(f"📢 {heading_tag.upper()} {i+1}: {heading.text}")
            except:
                pass

        # Look for active/selected filters
        filter_selectors = [
            "[class*='active']", "[class*='selected']", "[class*='current']",
            ".wpf_filter", ".product-filter", ".shop-filter"
        ]

        for selector in filter_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for i, element in enumerate(elements):
                    text = element.text.strip()
                    if text and len(text) < 100:  # Avoid huge blocks of text
                        print(f"🎯 Filter/Active element ({selector}) {i+1}: {text}")
            except:
                pass

        # Look for specific category-related text in Hebrew
        hebrew_categories = ['אופטיקה', 'דנטלית', 'חד פעמי', 'חשמל ואלקטרוניקה', 'טואלטיקה', 'כלי בית']

        for category in hebrew_categories:
            try:
                elements = driver.find_elements(By.XPATH, f"//*[contains(text(), '{category}')]")
                if elements:
                    print(f"✅ Found category '{category}' in {len(elements)} elements")
                    for i, element in enumerate(elements[:3]):  # Show first 3
                        print(f"   Element {i+1}: {element.tag_name} - {element.text[:50]}")
            except:
                pass

        # Check for URL parameters that might indicate category
        current_url = driver.current_url
        print(f"🌐 Current URL: {current_url}")

        # Look for any data attributes on body or main containers
        try:
            body = driver.find_element(By.TAG_NAME, "body")
            for attr in body.get_property("attributes"):
                attr_name = attr['name']
                if 'category' in attr_name.lower() or 'cat' in attr_name.lower():
                    print(f"🏷️ Body attribute: {attr_name} = {attr['value']}")
        except:
            pass

    except Exception as e:
        print(f"❌ Error: {e}")

    finally:
        driver.quit()

if __name__ == "__main__":
    # Test a few different category URLs
    test_urls = [
        "https://goodpharm.co.il/shop?wpf_filter_cat_0=45",  # אופטיקה
        "https://goodpharm.co.il/shop?wpf_filter_cat_0=46",  # חד פעמי
        "https://goodpharm.co.il/shop?wpf_filter_cat_0=47"   # unknown
    ]

    for url in test_urls:
        print(f"\n{'='*80}")
        extract_category_info(url)
        print(f"{'='*80}\n")