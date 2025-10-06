#!/usr/bin/env python3
"""
Diagnostic script to investigate price extraction on Super-Pharm product detail pages.
Target: Product 7290111602979 (Product Code: 592015)
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

# Product that failed price extraction
PRODUCT_URL = "https://shop.super-pharm.co.il/food-and-drinks/crackers-and-rice-cakes/rice-cakes/%D7%95%D7%95%D7%9C%D7%A0%D7%A1-%D7%A4%D7%A8%D7%99%D7%9B%D7%99%D7%95%D7%AA-%D7%AA%D7%99%D7%A8%D7%A1-%D7%93%D7%A7%D7%95%D7%AA-%D7%9C%D7%9E%D7%A8%D7%99%D7%97%D7%94/p/592015"
BARCODE = "7290111602979"
PRODUCT_CODE = "592015"

def diagnose_detail_page():
    """Diagnose price extraction on product detail page."""

    print("=" * 80)
    print("SUPER-PHARM PRODUCT DETAIL PAGE PRICE DIAGNOSTIC")
    print("=" * 80)
    print(f"\n📦 Product: {BARCODE}")
    print(f"🔗 URL: {PRODUCT_URL}\n")

    # Initialize driver
    options = webdriver.ChromeOptions()
    # Don't use headless - we want to see what's happening
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

    # Set window size for screenshot
    options.add_argument('--window-size=1920,1080')

    driver = webdriver.Chrome(options=options)

    try:
        print("🚀 Loading product detail page...")
        driver.get(PRODUCT_URL)

        # Wait for page to load
        print("⏳ Waiting for page to render...")
        time.sleep(8)

        print(f"📄 Page title: {driver.title}")
        print(f"📄 Current URL: {driver.current_url}\n")

        # Take screenshot
        screenshot_path = "/tmp/super_pharm_detail_page.png"
        driver.save_screenshot(screenshot_path)
        print(f"📸 Screenshot saved: {screenshot_path}\n")

        # Save full HTML
        html_path = "/tmp/super_pharm_detail_page.html"
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        print(f"💾 HTML saved: {html_path}\n")

        print("=" * 80)
        print("SEARCHING FOR PRICE ELEMENTS")
        print("=" * 80)

        # Try various price selectors
        price_selectors = [
            # Current selector from _scrape_online_price
            ("CSS: span.price", By.CSS_SELECTOR, "span.price"),

            # Common price selectors
            ("CSS: .price", By.CSS_SELECTOR, ".price"),
            ("CSS: [class*='price']", By.CSS_SELECTOR, "[class*='price']"),
            ("CSS: .product-price", By.CSS_SELECTOR, ".product-price"),
            ("CSS: .item-price", By.CSS_SELECTOR, ".item-price"),
            ("CSS: span.shekels", By.CSS_SELECTOR, "span.shekels"),
            ("CSS: span.money-sign", By.CSS_SELECTOR, "span.money-sign"),
            ("CSS: .price-row", By.CSS_SELECTOR, ".price-row"),
            ("CSS: div[class*='price']", By.CSS_SELECTOR, "div[class*='price']"),

            # XPath selectors
            ("XPath: Contains ₪", By.XPATH, "//*[contains(text(), '₪')]"),
            ("XPath: price class", By.XPATH, "//*[contains(@class, 'price')]"),
            ("XPath: shekels class", By.XPATH, "//*[contains(@class, 'shekels')]"),
        ]

        found_prices = []

        for name, by_type, selector in price_selectors:
            try:
                elements = driver.find_elements(by_type, selector)
                if elements:
                    print(f"\n✅ {name}: FOUND {len(elements)} element(s)")
                    for i, elem in enumerate(elements[:3], 1):  # Show first 3
                        text = elem.text.strip()
                        classes = elem.get_attribute('class')
                        tag = elem.tag_name

                        # Try to get inner HTML for more context
                        try:
                            inner_html = driver.execute_script("return arguments[0].innerHTML;", elem)
                            inner_html = inner_html[:100] if inner_html else ""
                        except:
                            inner_html = ""

                        print(f"   Element {i}:")
                        print(f"      Tag: {tag}")
                        print(f"      Class: {classes}")
                        print(f"      Text: '{text}'")
                        if inner_html:
                            print(f"      HTML: {inner_html}")

                        # Check if this looks like a price
                        if text and ('₪' in text or any(char.isdigit() for char in text)):
                            found_prices.append({
                                'selector': name,
                                'text': text,
                                'classes': classes,
                                'html': inner_html
                            })
                else:
                    print(f"\n❌ {name}: NOT FOUND")
            except Exception as e:
                print(f"\n❌ {name}: ERROR - {str(e)[:100]}")

        # Try to find price using JavaScript
        print("\n" + "=" * 80)
        print("JAVASCRIPT PRICE SEARCH")
        print("=" * 80)

        try:
            # Search for elements containing shekel sign
            js_prices = driver.execute_script("""
                const elements = Array.from(document.querySelectorAll('*'));
                const priceElements = elements.filter(el => {
                    const text = el.textContent;
                    return text && text.includes('₪') && el.children.length <= 3;
                });
                return priceElements.slice(0, 10).map(el => ({
                    tag: el.tagName,
                    class: el.className,
                    text: el.textContent.trim().substring(0, 50),
                    innerHTML: el.innerHTML.substring(0, 100)
                }));
            """)

            if js_prices:
                print(f"\n✅ JavaScript found {len(js_prices)} elements with ₪:")
                for i, elem in enumerate(js_prices, 1):
                    print(f"\n   Element {i}:")
                    print(f"      Tag: {elem['tag']}")
                    print(f"      Class: {elem['class']}")
                    print(f"      Text: {elem['text']}")
                    print(f"      HTML: {elem['innerHTML']}")
            else:
                print("\n❌ JavaScript found no elements with ₪")
        except Exception as e:
            print(f"\n❌ JavaScript search failed: {e}")

        # Summary
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)

        if found_prices:
            print(f"\n✅ Found {len(found_prices)} potential price elements:")
            for i, price in enumerate(found_prices, 1):
                print(f"\n   {i}. Selector: {price['selector']}")
                print(f"      Text: {price['text']}")
                print(f"      Classes: {price['classes']}")
        else:
            print("\n❌ NO PRICE ELEMENTS FOUND")
            print("   This suggests either:")
            print("   - The product has no price displayed on the detail page")
            print("   - The page requires additional interaction (scrolling, clicking)")
            print("   - The page uses heavy JavaScript that hasn't loaded yet")

    finally:
        print(f"\n⏸️  Keeping browser open for 5 seconds for manual inspection...")
        time.sleep(5)
        driver.quit()
        print("✅ Browser closed\n")

if __name__ == "__main__":
    diagnose_detail_page()
