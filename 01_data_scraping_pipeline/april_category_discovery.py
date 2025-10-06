#!/usr/bin/env python3
"""
April.co.il Category Discovery
Discovers all product categories on the site
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
import json

def setup_driver():
    """Configure Chrome driver"""
    options = Options()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36')

    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def discover_categories():
    """Discover all product categories"""
    driver = setup_driver()
    categories = []

    try:
        print("Navigating to april.co.il...")
        driver.get("https://www.april.co.il/")
        time.sleep(5)  # Wait for Cloudflare

        print("Extracting categories from navigation...")

        # Find all navigation links that lead to category pages
        # Looking for links in main navigation
        nav_links = driver.find_elements(By.CSS_SELECTOR, 'nav a, .menu a, header a')

        seen_urls = set()

        for link in nav_links:
            try:
                href = link.get_attribute('href')
                text = link.text.strip()

                if not href or not text:
                    continue

                # Filter for product category links
                # Exclude non-category pages
                exclude_keywords = ['contact', 'about', 'branches', 'shipping', 'returns',
                                   'login', 'register', 'cart', 'checkout', 'search',
                                   'facebook', 'instagram', 'whatsapp', 'mailto', 'tel:',
                                   'javascript:', '#', 'stores', 'gift']

                if any(keyword in href.lower() for keyword in exclude_keywords):
                    continue

                # Look for category-like URLs
                if 'april.co.il' in href and href not in seen_urls:
                    # Extract category path
                    path = href.replace('https://www.april.co.il/', '').replace('http://www.april.co.il/', '')

                    # Filter out empty paths and homepage
                    if path and path != '' and '?' not in path and not path.startswith('htmls/'):
                        seen_urls.add(href)
                        categories.append({
                            'name': text,
                            'url': path,
                            'full_url': href
                        })
                        print(f"  Found: {text} -> {path}")

            except Exception as e:
                continue

        # Also look for category links in the mobile menu
        try:
            mobile_menu_links = driver.find_elements(By.CSS_SELECTOR, '.offcanvas a, [class*="mobile"] a')
            for link in mobile_menu_links:
                try:
                    href = link.get_attribute('href')
                    text = link.text.strip()

                    if not href or not text or href in seen_urls:
                        continue

                    exclude_keywords = ['contact', 'about', 'branches', 'shipping', 'returns',
                                       'login', 'register', 'cart', 'checkout', 'search',
                                       'facebook', 'instagram', 'whatsapp', 'mailto', 'tel:',
                                       'javascript:', '#', 'stores', 'gift']

                    if any(keyword in href.lower() for keyword in exclude_keywords):
                        continue

                    if 'april.co.il' in href:
                        path = href.replace('https://www.april.co.il/', '').replace('http://www.april.co.il/', '')
                        if path and path != '' and '?' not in path and not path.startswith('htmls/'):
                            seen_urls.add(href)
                            categories.append({
                                'name': text,
                                'url': path,
                                'full_url': href
                            })
                            print(f"  Found (mobile): {text} -> {path}")

                except Exception as e:
                    continue
        except:
            pass

        # Save to file
        with open('april_categories.json', 'w', encoding='utf-8') as f:
            json.dump(categories, f, indent=2, ensure_ascii=False)

        print(f"\n✓ Discovered {len(categories)} categories")
        print(f"✓ Saved to april_categories.json")

        return categories

    finally:
        driver.quit()

if __name__ == "__main__":
    categories = discover_categories()

    print("\n=== CATEGORY LIST ===")
    for cat in categories:
        print(f"{cat['name']}: {cat['url']}")
