#!/usr/bin/env python3
"""Quick pagination debug script"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time

options = Options()
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36')

driver = webdriver.Chrome(options=options)
driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

driver.get("https://www.april.co.il/women-perfume")
time.sleep(5)  # Wait for Cloudflare

print("\n=== PAGINATION DEBUG ===\n")

# Look for pagination
try:
    pagination = driver.find_element(By.CSS_SELECTOR, 'ul.pagination')
    print(f"✓ Found pagination element")
    print(f"Pagination HTML:\n{pagination.get_attribute('outerHTML')[:500]}\n")

    # Find all links
    links = pagination.find_elements(By.CSS_SELECTOR, 'a')
    print(f"Found {len(links)} pagination links:\n")

    for idx, link in enumerate(links):
        onclick = link.get_attribute('onclick')
        href = link.get_attribute('href')
        text = link.text
        parent_class = link.find_element(By.XPATH, '..').get_attribute('class')

        print(f"Link {idx}:")
        print(f"  Text: '{text}'")
        print(f"  Onclick: {onclick}")
        print(f"  Href: {href}")
        print(f"  Parent class: {parent_class}")
        print()

except Exception as e:
    print(f"✗ Pagination not found: {e}")

driver.quit()
