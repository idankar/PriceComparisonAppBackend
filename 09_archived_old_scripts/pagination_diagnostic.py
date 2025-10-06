#!/usr/bin/env python3
"""
Diagnostic script to inspect Super-Pharm pagination elements
"""
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc

# Test URL - a category that should have multiple pages
TEST_URL = "https://shop.super-pharm.co.il/care/hair-care/c/15170000"

print("Setting up browser...")
options = uc.ChromeOptions()
options.add_argument("--start-maximized")

try:
    driver = uc.Chrome(options=options, use_subprocess=False)
except:
    print("Falling back to regular Selenium...")
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager

    regular_options = webdriver.ChromeOptions()
    regular_options.add_argument("--start-maximized")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=regular_options)

print(f"\nNavigating to: {TEST_URL}")
driver.get(TEST_URL)

# Wait for products to load
WebDriverWait(driver, 20).until(
    EC.presence_of_element_located((By.CSS_SELECTOR, "div.add-to-basket[data-ean]"))
)
print("✅ Page loaded, products found")

# Count products
products = driver.find_elements(By.CSS_SELECTOR, "div.add-to-basket[data-ean]")
print(f"\n📦 Found {len(products)} products on first load")

# Scroll to bottom
print("\n📜 Scrolling to bottom...")
driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
time.sleep(3)

# Check for product count again after scroll
products_after_scroll = driver.find_elements(By.CSS_SELECTOR, "div.add-to-basket[data-ean]")
print(f"📦 Found {len(products_after_scroll)} products after scroll")

# Look for ALL pagination-related elements
print("\n🔍 Searching for pagination elements...")

# Try multiple selectors
selectors_to_try = [
    ("a#nextHiddenLink", "Next Hidden Link (original)"),
    ("a[id*='next']", "Any anchor with 'next' in ID"),
    ("a[class*='next']", "Any anchor with 'next' in class"),
    ("a[rel='next']", "Anchor with rel='next'"),
    ("button[class*='next']", "Button with 'next' in class"),
    ("[class*='pagination']", "Any element with 'pagination' in class"),
    ("a[href*='page=']", "Any anchor with 'page=' in href"),
    ("nav", "Any nav element"),
]

for selector, description in selectors_to_try:
    elements = driver.find_elements(By.CSS_SELECTOR, selector)
    if elements:
        print(f"\n✅ Found {len(elements)} elements for: {description}")
        print(f"   Selector: {selector}")
        for i, elem in enumerate(elements[:3]):  # Show first 3
            try:
                tag = elem.tag_name
                elem_id = elem.get_attribute('id') or 'N/A'
                elem_class = elem.get_attribute('class') or 'N/A'
                elem_href = elem.get_attribute('href') or 'N/A'
                elem_text = elem.text[:50] if elem.text else 'N/A'
                print(f"   [{i+1}] Tag:{tag}, ID:{elem_id}, Class:{elem_class}, Href:{elem_href}, Text:{elem_text}")
            except:
                print(f"   [{i+1}] Could not extract element info")
    else:
        print(f"❌ No elements found for: {description}")

# Try JavaScript to find pagination
print("\n🔍 Checking for dynamically loaded pagination...")
has_more_button = driver.execute_script("""
    const buttons = document.querySelectorAll('button');
    for (let btn of buttons) {
        if (btn.textContent.includes('עוד') || btn.textContent.includes('more') || btn.textContent.includes('הבא')) {
            return btn.outerHTML;
        }
    }
    return null;
""")

if has_more_button:
    print(f"✅ Found load-more/next button: {has_more_button[:200]}")
else:
    print("❌ No load-more/next button found")

# Check page source for pagination clues
print("\n🔍 Checking page source for pagination hints...")
page_source = driver.page_source
pagination_keywords = ['page=', 'pagination', 'nextPage', 'loadMore', 'show-more']
for keyword in pagination_keywords:
    if keyword in page_source:
        print(f"✅ Found '{keyword}' in page source")
    else:
        print(f"❌ No '{keyword}' in page source")

print("\n✅ Diagnostic complete. Press Enter to close browser...")
input()
driver.quit()
