#!/usr/bin/env python3
"""
April.co.il Deep Analysis - Phase 2
Focused analysis on actual product listing and detail pages
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time
import json
import re

def setup_driver():
    """Configure Chrome driver"""
    options = Options()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def wait_for_page_load(driver, timeout=10):
    """Wait for page to fully load"""
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script('return document.readyState') == 'complete'
        )
        time.sleep(2)  # Additional buffer
        return True
    except:
        return False

def analyze_product_listing_page(driver):
    """Analyze a category page with actual products"""
    print("\n" + "="*60)
    print("PHASE 1: PRODUCT LISTING PAGE ANALYSIS")
    print("="*60)

    # Navigate to women's perfume category (known to have products)
    url = "https://www.april.co.il/women-perfume"
    print(f"\nNavigating to: {url}")

    driver.get(url)
    wait_for_page_load(driver)

    # Save HTML for manual inspection
    with open('april_listing_full.html', 'w', encoding='utf-8') as f:
        f.write(driver.page_source)

    print(f"Current URL: {driver.current_url}")
    print(f"Page Title: {driver.title}")

    # Try multiple selectors to find product items
    product_selectors = [
        '.product-item',
        '.product',
        '.item-product',
        '[data-product-id]',
        '.card.product',
        '.product-card',
        'div[class*="product"]',
        'article',
        '.item',
    ]

    products = []
    successful_selector = None

    for selector in product_selectors:
        try:
            elems = driver.find_elements(By.CSS_SELECTOR, selector)
            if elems and len(elems) > 3:  # Make sure we have actual products
                products = elems
                successful_selector = selector
                print(f"\n✓ Found {len(products)} products using: {selector}")
                break
        except:
            continue

    if not products:
        print("\n✗ No products found. Let me inspect the page structure...")
        # Get all elements with 'product' in class name
        all_divs = driver.find_elements(By.CSS_SELECTOR, 'div')
        product_divs = [d for d in all_divs if 'product' in d.get_attribute('class').lower()]
        print(f"Found {len(product_divs)} divs with 'product' in class name")
        if product_divs:
            print(f"Sample: {product_divs[0].get_attribute('outerHTML')[:300]}")
        return None

    # Detailed analysis of first 5 products
    print(f"\n{'='*60}")
    print("EXTRACTING DATA FROM LISTING PAGE")
    print(f"{'='*60}")

    listing_data = []

    for idx, product in enumerate(products[:5], 1):
        print(f"\n--- Product {idx} ---")
        data = {
            'index': idx,
            'selector_used': successful_selector
        }

        # Product Name
        name_selectors = ['h1', 'h2', 'h3', 'h4', '.name', '.title', '.product-name',
                         '[class*="name"]', '[class*="title"]', 'a']
        for sel in name_selectors:
            try:
                elem = product.find_element(By.CSS_SELECTOR, sel)
                text = elem.text.strip()
                if text and len(text) > 2:
                    data['name'] = text
                    data['name_selector'] = sel
                    print(f"  Name: {text}")
                    break
            except:
                continue

        # Price
        price_selectors = ['.price', '[class*="price"]', '.amount', '[data-price]',
                          'span[class*="price"]', '.cost']
        for sel in price_selectors:
            try:
                elem = product.find_element(By.CSS_SELECTOR, sel)
                text = elem.text.strip()
                if text and ('₪' in text or re.search(r'\d', text)):
                    data['price'] = text
                    data['price_selector'] = sel
                    print(f"  Price: {text}")
                    break
            except:
                continue

        # Product URL
        link_selectors = ['a', '[href*="product"]', '[href*="item"]']
        for sel in link_selectors:
            try:
                elem = product.find_element(By.CSS_SELECTOR, sel)
                href = elem.get_attribute('href')
                if href and ('product' in href.lower() or 'item' in href.lower() or len(href) > 30):
                    data['product_url'] = href
                    data['url_selector'] = sel
                    print(f"  URL: {href}")
                    break
            except:
                continue

        # Image
        img_selectors = ['img', 'img[src*="product"]', '[data-src]']
        for sel in img_selectors:
            try:
                elem = product.find_element(By.CSS_SELECTOR, sel)
                src = elem.get_attribute('src') or elem.get_attribute('data-src')
                if src:
                    data['image_url'] = src
                    data['image_selector'] = sel
                    print(f"  Image: {src[:80]}...")
                    break
            except:
                continue

        # Brand (on listing page)
        brand_selectors = ['.brand', '[class*="brand"]', '.manufacturer',
                          '[data-brand]', 'span.brand']
        for sel in brand_selectors:
            try:
                elem = product.find_element(By.CSS_SELECTOR, sel)
                text = elem.text.strip()
                if text:
                    data['brand'] = text
                    data['brand_selector'] = sel
                    print(f"  Brand: {text}")
                    break
            except:
                continue

        # Barcode (unlikely but check)
        barcode_selectors = ['.barcode', '[data-barcode]', '.ean', '[data-ean]']
        for sel in barcode_selectors:
            try:
                elem = product.find_element(By.CSS_SELECTOR, sel)
                text = elem.text.strip() or elem.get_attribute('data-barcode') or elem.get_attribute('data-ean')
                if text:
                    data['barcode_on_listing'] = text
                    data['barcode_selector'] = sel
                    print(f"  ⭐ BARCODE FOUND ON LISTING: {text}")
                    break
            except:
                continue

        # SKU
        sku_selectors = ['.sku', '[data-sku]', '[data-product-id]']
        for sel in sku_selectors:
            try:
                elem = product.find_element(By.CSS_SELECTOR, sel)
                text = elem.text.strip() or elem.get_attribute('data-sku') or elem.get_attribute('data-product-id')
                if text:
                    data['sku'] = text
                    data['sku_selector'] = sel
                    print(f"  SKU: {text}")
                    break
            except:
                continue

        # Store HTML snippet
        data['html_snippet'] = product.get_attribute('outerHTML')[:800]

        listing_data.append(data)

    # Save listing data
    with open('april_listing_data_detailed.json', 'w', encoding='utf-8') as f:
        json.dump(listing_data, f, indent=2, ensure_ascii=False)

    print(f"\n✓ Extracted data from {len(listing_data)} products on listing page")

    return listing_data

def analyze_product_detail_page(driver, product_url):
    """Deep dive into individual product page"""
    print("\n" + "="*60)
    print("PHASE 2: PRODUCT DETAIL PAGE ANALYSIS")
    print("="*60)

    print(f"\nNavigating to: {product_url}")
    driver.get(product_url)
    wait_for_page_load(driver)

    # Save HTML
    with open('april_product_detail_full.html', 'w', encoding='utf-8') as f:
        f.write(driver.page_source)

    print(f"Current URL: {driver.current_url}")
    print(f"Page Title: {driver.title}")

    detail_data = {}

    # Extract all fields
    print(f"\n{'='*60}")
    print("EXTRACTING DATA FROM DETAIL PAGE")
    print(f"{'='*60}\n")

    # Product Name
    name_selectors = ['h1', '.product-name', '.product-title', 'h1.title']
    for sel in name_selectors:
        try:
            elem = driver.find_element(By.CSS_SELECTOR, sel)
            text = elem.text.strip()
            if text:
                detail_data['name'] = text
                detail_data['name_selector'] = sel
                print(f"Name: {text}")
                break
        except:
            continue

    # Price
    price_selectors = ['.price', '[class*="price"]', '.amount', 'span.price', '.product-price']
    for sel in price_selectors:
        try:
            elem = driver.find_element(By.CSS_SELECTOR, sel)
            text = elem.text.strip()
            if text and ('₪' in text or re.search(r'\d', text)):
                detail_data['price'] = text
                detail_data['price_selector'] = sel
                print(f"Price: {text}")
                break
        except:
            continue

    # Brand
    brand_selectors = ['.brand', '[class*="brand"]', '.manufacturer', 'span.brand',
                       'div.brand', '[itemprop="brand"]']
    for sel in brand_selectors:
        try:
            elem = driver.find_element(By.CSS_SELECTOR, sel)
            text = elem.text.strip()
            if text:
                detail_data['brand'] = text
                detail_data['brand_selector'] = sel
                print(f"Brand: {text}")
                break
        except:
            continue

    # BARCODE - Critical field
    print("\n🔍 BARCODE SEARCH (Critical):")
    barcode_selectors = [
        '.barcode', '[data-barcode]', '.ean', '[data-ean]', '.upc',
        '[class*="barcode"]', '[class*="ean"]', 'span.sku', '.product-code'
    ]
    for sel in barcode_selectors:
        try:
            elem = driver.find_element(By.CSS_SELECTOR, sel)
            text = elem.text.strip() or elem.get_attribute('data-barcode') or elem.get_attribute('data-ean')
            if text and re.search(r'\d{8,13}', text):  # Barcode pattern
                detail_data['barcode'] = text
                detail_data['barcode_selector'] = sel
                print(f"  ⭐ BARCODE FOUND: {text} (selector: {sel})")
                break
        except:
            continue

    # Search in page source
    page_source = driver.page_source
    barcode_patterns = [
        r'barcode["\s:]+(\d{8,13})',
        r'ean["\s:]+(\d{8,13})',
        r'upc["\s:]+(\d{8,13})',
        r'קוד פריט["\s:]+(\d{8,13})',
        r'ברקוד["\s:]+(\d{8,13})'
    ]

    for pattern in barcode_patterns:
        match = re.search(pattern, page_source, re.IGNORECASE)
        if match:
            print(f"  ⭐ BARCODE in source: {match.group(1)} (pattern: {pattern})")
            if 'barcode' not in detail_data:
                detail_data['barcode'] = match.group(1)
                detail_data['barcode_source'] = 'page_source_regex'
            break

    if 'barcode' not in detail_data:
        print("  ✗ NO BARCODE FOUND")

    # Description
    desc_selectors = ['.description', '.product-description', '[class*="description"]',
                     '.desc', 'div.description', '[itemprop="description"]']
    for sel in desc_selectors:
        try:
            elem = driver.find_element(By.CSS_SELECTOR, sel)
            text = elem.text.strip()
            if text and len(text) > 10:
                detail_data['description'] = text[:500]  # Limit length
                detail_data['description_selector'] = sel
                print(f"Description: {text[:100]}...")
                break
        except:
            continue

    # SKU
    sku_selectors = ['.sku', '[data-sku]', '.product-sku', 'span.sku']
    for sel in sku_selectors:
        try:
            elem = driver.find_element(By.CSS_SELECTOR, sel)
            text = elem.text.strip() or elem.get_attribute('data-sku')
            if text:
                detail_data['sku'] = text
                detail_data['sku_selector'] = sel
                print(f"SKU: {text}")
                break
        except:
            continue

    # Images
    image_selectors = ['img.product-image', 'img[src*="product"]', '.product-images img',
                      '[class*="image"] img', '.gallery img']
    images = []
    for sel in image_selectors:
        try:
            elems = driver.find_elements(By.CSS_SELECTOR, sel)
            for elem in elems:
                src = elem.get_attribute('src') or elem.get_attribute('data-src')
                if src and src not in images:
                    images.append(src)
            if images:
                detail_data['images'] = images
                detail_data['images_selector'] = sel
                print(f"Images: Found {len(images)}")
                break
        except:
            continue

    # Category/Breadcrumb
    breadcrumb_selectors = ['.breadcrumb', 'nav[aria-label="breadcrumb"]', '.breadcrumbs',
                           '[class*="breadcrumb"]']
    for sel in breadcrumb_selectors:
        try:
            elem = driver.find_element(By.CSS_SELECTOR, sel)
            text = elem.text.strip()
            if text:
                detail_data['category'] = text
                detail_data['category_selector'] = sel
                print(f"Category: {text}")
                break
        except:
            continue

    # Save detail data
    with open('april_product_detail_data_detailed.json', 'w', encoding='utf-8') as f:
        json.dump(detail_data, f, indent=2, ensure_ascii=False)

    print(f"\n✓ Detail page analysis complete")

    return detail_data

def analyze_pagination(driver):
    """Analyze pagination mechanism"""
    print("\n" + "="*60)
    print("PHASE 3: PAGINATION ANALYSIS")
    print("="*60)

    # Go back to listing page
    driver.get("https://www.april.co.il/women-perfume")
    wait_for_page_load(driver)

    pagination_info = {}

    # Look for pagination elements
    pagination_selectors = [
        '.pagination', '.pager', 'nav[aria-label*="pagination"]',
        '[class*="pagination"]', '.page-numbers', '[class*="pager"]'
    ]

    found_pagination = False
    for selector in pagination_selectors:
        try:
            elem = driver.find_element(By.CSS_SELECTOR, selector)
            print(f"\n✓ Found pagination element: {selector}")
            print(f"  HTML: {elem.get_attribute('outerHTML')[:300]}...")

            # Look for next button
            next_selectors = ['a.next', 'button.next', '[aria-label*="next"]',
                            'a:contains("Next")', '[class*="next"]']
            for next_sel in next_selectors:
                try:
                    next_btn = elem.find_element(By.CSS_SELECTOR, next_sel)
                    print(f"  Next button: {next_sel}")
                    print(f"    HTML: {next_btn.get_attribute('outerHTML')}")
                    pagination_info['next_button_selector'] = next_sel
                    found_pagination = True
                    break
                except:
                    continue

            if found_pagination:
                pagination_info['pagination_type'] = 'standard'
                pagination_info['pagination_selector'] = selector
                break

        except:
            continue

    if not found_pagination:
        # Check for "Load More" button
        load_more_selectors = ['button[class*="load"]', 'a[class*="load"]',
                              'button:contains("Load")', '.load-more']
        for selector in load_more_selectors:
            try:
                elem = driver.find_element(By.CSS_SELECTOR, selector)
                print(f"\n✓ Found 'Load More' button: {selector}")
                pagination_info['pagination_type'] = 'load_more'
                pagination_info['load_more_selector'] = selector
                found_pagination = True
                break
            except:
                continue

    if not found_pagination:
        # Test for infinite scroll
        print("\n🔍 Testing for infinite scroll...")
        initial_height = driver.execute_script("return document.body.scrollHeight")
        initial_product_count = len(driver.find_elements(By.CSS_SELECTOR, '.item, .product'))

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)

        new_height = driver.execute_script("return document.body.scrollHeight")
        new_product_count = len(driver.find_elements(By.CSS_SELECTOR, '.item, .product'))

        if new_height > initial_height or new_product_count > initial_product_count:
            print("  ✓ Infinite scroll detected")
            pagination_info['pagination_type'] = 'infinite_scroll'
        else:
            print("  ✗ No pagination detected")
            pagination_info['pagination_type'] = 'none_or_all_on_one_page'

    with open('april_pagination_info.json', 'w', encoding='utf-8') as f:
        json.dump(pagination_info, f, indent=2)

    return pagination_info

def main():
    print("\n" + "="*70)
    print(" APRIL.CO.IL DEEP RECONNAISSANCE - SCRAPING STRATEGY ANALYSIS")
    print("="*70)

    driver = None
    try:
        driver = setup_driver()

        # Phase 1: Analyze listing page
        listing_data = analyze_product_listing_page(driver)

        if not listing_data:
            print("\n✗ Failed to extract listing data")
            return

        # Phase 2: Analyze detail page (use first product)
        if listing_data and 'product_url' in listing_data[0]:
            product_url = listing_data[0]['product_url']
            detail_data = analyze_product_detail_page(driver, product_url)
        else:
            print("\n⚠ No product URL found, skipping detail page analysis")
            detail_data = None

        # Phase 3: Analyze pagination
        pagination_info = analyze_pagination(driver)

        # Generate Summary Report
        print("\n" + "="*70)
        print(" SUMMARY REPORT")
        print("="*70)

        print("\n1. TECHNOLOGY STACK:")
        print("   - Anti-bot protection: Cloudflare (JavaScript challenge)")
        print("   - Required tool: Selenium/Playwright (requests won't work)")

        print("\n2. LISTING PAGE FIELDS:")
        if listing_data:
            available_fields = [k for k in listing_data[0].keys() if not k.endswith('_selector') and k != 'html_snippet' and k != 'index' and k != 'selector_used']
            print(f"   Available fields: {', '.join(available_fields)}")
            if 'barcode_on_listing' in available_fields:
                print("   ⭐ BARCODE available on listing page!")

        print("\n3. DETAIL PAGE FIELDS:")
        if detail_data:
            detail_fields = [k for k in detail_data.keys() if not k.endswith('_selector') and not k.endswith('_source')]
            print(f"   Available fields: {', '.join(detail_fields)}")
            if 'barcode' in detail_data:
                print(f"   ⭐ BARCODE found: {detail_data['barcode']}")
            else:
                print("   ✗ BARCODE NOT FOUND on detail page")

        print("\n4. PAGINATION:")
        if pagination_info:
            print(f"   Type: {pagination_info.get('pagination_type', 'unknown')}")

        print("\n5. RECOMMENDATION:")
        # Determine if barcode is available
        barcode_on_listing = any('barcode_on_listing' in item for item in listing_data)
        barcode_on_detail = detail_data and 'barcode' in detail_data

        if barcode_on_listing:
            print("   📋 SCENARIO A: Single-Pass Scraper")
            print("      Reason: Barcode available on listing pages")
        elif barcode_on_detail:
            print("   📋 SCENARIO B: Two-Phase Strategy (Scraper + Backfill)")
            print("      Reason: Barcode only available on detail pages")
        else:
            print("   ⚠ SCENARIO C: Investigate Alternative Data Sources")
            print("      Reason: Barcode not found on listing or detail pages")
            print("      Recommendation: Check for API endpoints or alternative methods")

        print("\n" + "="*70)
        print(" FILES GENERATED:")
        print("="*70)
        print("   - april_listing_full.html")
        print("   - april_listing_data_detailed.json")
        print("   - april_product_detail_full.html")
        print("   - april_product_detail_data_detailed.json")
        print("   - april_pagination_info.json")
        print("="*70)

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if driver:
            print("\nClosing browser in 5 seconds...")
            time.sleep(5)
            driver.quit()

if __name__ == "__main__":
    main()
