#!/usr/bin/env python3
"""
April.co.il Diagnostic Script
Comprehensive analysis to determine scraping strategy
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import json

def setup_driver():
    """Configure Chrome driver to bypass Cloudflare"""
    options = Options()

    # Anti-detection measures
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')

    # Use a realistic user agent
    options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

    driver = webdriver.Chrome(options=options)

    # Override navigator.webdriver flag
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    return driver

def wait_for_cloudflare(driver, timeout=30):
    """Wait for Cloudflare challenge to complete"""
    print("Waiting for Cloudflare challenge...")
    try:
        # Wait for the challenge to disappear
        WebDriverWait(driver, timeout).until_not(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.main-wrapper"))
        )
        print("✓ Cloudflare challenge passed")
        return True
    except TimeoutException:
        # Check if we're already on the main page
        if "Just a moment" not in driver.page_source:
            print("✓ Already past Cloudflare")
            return True
        print("✗ Cloudflare challenge timeout")
        return False

def analyze_homepage(driver):
    """Analyze the homepage structure"""
    print("\n=== HOMEPAGE ANALYSIS ===")

    driver.get("https://www.april.co.il/")

    if not wait_for_cloudflare(driver):
        return None

    time.sleep(3)  # Let page fully load

    # Save page source for analysis
    with open('april_homepage.html', 'w', encoding='utf-8') as f:
        f.write(driver.page_source)

    print(f"Page title: {driver.title}")
    print(f"Current URL: {driver.current_url}")

    # Look for category navigation
    try:
        categories = driver.find_elements(By.CSS_SELECTOR, "nav a, .category a, .menu a, [class*='category'] a, [class*='menu'] a")
        print(f"\nFound {len(categories)} potential category links")

        category_links = []
        for cat in categories[:10]:  # Limit to first 10 for inspection
            try:
                text = cat.text.strip()
                href = cat.get_attribute('href')
                if text and href and 'category' in href.lower() or 'product' in href.lower():
                    category_links.append({'text': text, 'href': href})
                    print(f"  - {text}: {href}")
            except:
                pass

        return category_links
    except Exception as e:
        print(f"Error finding categories: {e}")
        return []

def analyze_category_page(driver, url=None):
    """Deep dive into a category listing page"""
    print("\n=== CATEGORY LISTING PAGE ANALYSIS ===")

    # If no URL provided, try to find one
    if not url:
        try:
            # Try common category URL patterns
            test_urls = [
                "https://www.april.co.il/category/",
                "https://www.april.co.il/products/",
                "https://www.april.co.il/shop/",
            ]

            # Or try to click first category link
            category_link = driver.find_element(By.CSS_SELECTOR, "[href*='category'], [href*='product']")
            url = category_link.get_attribute('href')
        except:
            print("Could not find category page")
            return None

    driver.get(url)
    time.sleep(3)

    # Save page source
    with open('april_category_page.html', 'w', encoding='utf-8') as f:
        f.write(driver.page_source)

    print(f"Analyzing: {driver.current_url}")

    # Find product cards/items
    product_selectors = [
        ".product",
        ".product-item",
        ".product-card",
        "[class*='product']",
        ".item",
        "[data-product-id]",
        "[data-product]"
    ]

    products = []
    for selector in product_selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            if elements:
                print(f"\nFound {len(elements)} products using selector: {selector}")
                products = elements
                break
        except:
            continue

    if not products:
        print("⚠ No products found on category page")
        return None

    # Analyze first few products in detail
    print(f"\n=== ANALYZING FIRST 3 PRODUCTS ===")
    extracted_data = []

    for idx, product in enumerate(products[:3], 1):
        print(f"\n--- Product {idx} ---")
        data = {}

        # Try to extract various fields
        fields_to_check = {
            'name': ['h2', 'h3', '.title', '.product-name', '.name', '[class*="name"]', '[class*="title"]'],
            'price': ['.price', '.product-price', '[class*="price"]', '[data-price]'],
            'image': ['img'],
            'link': ['a', '[href*="product"]'],
            'brand': ['.brand', '[class*="brand"]'],
            'barcode': ['.barcode', '[data-barcode]', '[class*="barcode"]', '.ean', '.upc'],
            'description': ['.description', '.desc', '[class*="desc"]'],
            'sku': ['.sku', '[data-sku]', '[class*="sku"]'],
        }

        for field, selectors in fields_to_check.items():
            for sel in selectors:
                try:
                    if field == 'image':
                        elem = product.find_element(By.CSS_SELECTOR, sel)
                        value = elem.get_attribute('src') or elem.get_attribute('data-src')
                    elif field == 'link':
                        elem = product.find_element(By.CSS_SELECTOR, sel)
                        value = elem.get_attribute('href')
                    else:
                        elem = product.find_element(By.CSS_SELECTOR, sel)
                        value = elem.text.strip()

                    if value:
                        data[field] = value
                        data[f'{field}_selector'] = sel
                        print(f"  {field}: {value[:100] if isinstance(value, str) else value}")
                        break
                except:
                    continue

        # Get full HTML for manual inspection
        data['html_snippet'] = product.get_attribute('outerHTML')[:500]
        extracted_data.append(data)

    # Save extracted data
    with open('april_category_data.json', 'w', encoding='utf-8') as f:
        json.dump(extracted_data, f, indent=2, ensure_ascii=False)

    return extracted_data

def analyze_product_detail_page(driver, product_url=None):
    """Deep dive into an individual product detail page"""
    print("\n=== PRODUCT DETAIL PAGE ANALYSIS ===")

    if not product_url:
        print("No product URL provided")
        return None

    driver.get(product_url)
    time.sleep(3)

    # Save page source
    with open('april_product_detail.html', 'w', encoding='utf-8') as f:
        f.write(driver.page_source)

    print(f"Analyzing: {driver.current_url}")

    # Extract all possible fields
    detail_data = {}

    fields_to_check = {
        'name': ['h1', 'h2', '.product-name', '.title', '[class*="name"]', '[class*="title"]'],
        'price': ['.price', '.product-price', '[class*="price"]', '[data-price]'],
        'brand': ['.brand', '[class*="brand"]', '.manufacturer'],
        'barcode': ['.barcode', '[data-barcode]', '[class*="barcode"]', '.ean', '.upc', '[class*="ean"]'],
        'description': ['.description', '.desc', '[class*="desc"]', '.product-description'],
        'long_description': ['.full-description', '.details', '[class*="detail"]'],
        'sku': ['.sku', '[data-sku]', '[class*="sku"]'],
        'category': ['.category', '.breadcrumb', '[class*="category"]'],
        'images': ['img', '.product-image', '[class*="image"]'],
        'specifications': ['.specs', '.specifications', 'table', '[class*="spec"]'],
        'stock': ['.stock', '.availability', '[class*="stock"]', '[class*="available"]'],
    }

    for field, selectors in fields_to_check.items():
        for sel in selectors:
            try:
                if field == 'images':
                    elems = driver.find_elements(By.CSS_SELECTOR, sel)
                    images = []
                    for elem in elems:
                        src = elem.get_attribute('src') or elem.get_attribute('data-src')
                        if src and 'product' in src.lower():
                            images.append(src)
                    if images:
                        detail_data[field] = images
                        detail_data[f'{field}_selector'] = sel
                        print(f"  {field}: Found {len(images)} images")
                        break
                else:
                    elem = driver.find_element(By.CSS_SELECTOR, sel)
                    value = elem.text.strip()
                    if value:
                        detail_data[field] = value
                        detail_data[f'{field}_selector'] = sel
                        print(f"  {field}: {value[:100]}")
                        break
            except:
                continue

    # Look for barcode specifically in various formats
    print("\n=== BARCODE SPECIFIC SEARCH ===")
    barcode_searches = [
        "ean", "upc", "barcode", "קוד פריט", "מק\"ט", "ברקוד"
    ]

    page_text = driver.page_source.lower()
    for term in barcode_searches:
        if term in page_text:
            print(f"  Found '{term}' in page source")
            # Try to extract the value near this term
            try:
                # Look for elements containing this text
                elements = driver.find_elements(By.XPATH, f"//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{term.lower()}')]")
                for elem in elements[:3]:
                    print(f"    Context: {elem.text[:200]}")
            except:
                pass

    # Save extracted data
    with open('april_product_detail_data.json', 'w', encoding='utf-8') as f:
        json.dump(detail_data, f, indent=2, ensure_ascii=False)

    return detail_data

def analyze_pagination(driver):
    """Determine pagination mechanism"""
    print("\n=== PAGINATION ANALYSIS ===")

    # Look for pagination elements
    pagination_selectors = [
        '.pagination',
        '.pager',
        '[class*="pagination"]',
        '[class*="pager"]',
        'nav[aria-label*="page"]',
        '.page-numbers',
        '[class*="page"]'
    ]

    for selector in pagination_selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            if elements:
                print(f"Found pagination using: {selector}")
                for elem in elements[:3]:
                    print(f"  HTML: {elem.get_attribute('outerHTML')[:200]}")

                # Look for next button
                next_buttons = elem.find_elements(By.CSS_SELECTOR, "a, button")
                for btn in next_buttons:
                    text = btn.text.strip().lower()
                    if 'next' in text or '›' in text or '»' in text or 'הבא' in text:
                        print(f"  Next button: {btn.get_attribute('outerHTML')[:200]}")

        except Exception as e:
            continue

    # Check for infinite scroll
    print("\nChecking for infinite scroll...")
    initial_height = driver.execute_script("return document.body.scrollHeight")
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)
    new_height = driver.execute_script("return document.body.scrollHeight")

    if new_height > initial_height:
        print("  ✓ Infinite scroll detected")
    else:
        print("  ✗ No infinite scroll detected")

def main():
    print("="*60)
    print("APRIL.CO.IL RECONNAISSANCE MISSION")
    print("="*60)

    driver = None
    try:
        driver = setup_driver()

        # Step 1: Analyze homepage and find categories
        category_links = analyze_homepage(driver)

        if not category_links:
            print("\n⚠ Could not find category links. Checking common URLs...")
            category_links = [
                {'text': 'Test Category', 'href': 'https://www.april.co.il/category/test'},
            ]

        # Step 2: Analyze category page
        category_data = None
        if category_links:
            category_data = analyze_category_page(driver, category_links[0]['href'])

        # Step 3: Analyze product detail page
        if category_data and len(category_data) > 0 and 'link' in category_data[0]:
            product_url = category_data[0]['link']
            analyze_product_detail_page(driver, product_url)

        # Step 4: Analyze pagination
        if category_links:
            driver.get(category_links[0]['href'])
            time.sleep(2)
            analyze_pagination(driver)

        print("\n" + "="*60)
        print("RECONNAISSANCE COMPLETE")
        print("="*60)
        print("\nGenerated files:")
        print("  - april_homepage.html")
        print("  - april_category_page.html")
        print("  - april_category_data.json")
        print("  - april_product_detail.html")
        print("  - april_product_detail_data.json")

    except Exception as e:
        print(f"\n✗ Error during analysis: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if driver:
            print("\nClosing browser in 5 seconds...")
            time.sleep(5)
            driver.quit()

if __name__ == "__main__":
    main()
