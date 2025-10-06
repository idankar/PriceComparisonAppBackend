#!/usr/bin/env python3
"""
Diagnostic script to inspect Super-Pharm product container HTML structure
to identify the correct selector for product URLs.
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def inspect_product_html():
    """Inspect the HTML structure of product containers on Super-Pharm."""

    # Test URL - same category as the scraper test
    test_url = "https://shop.super-pharm.co.il/food-and-drinks/crackers-and-rice-cakes/c/70190000"

    print("🚀 Starting HTML structure inspection...")
    print(f"📄 Target URL: {test_url}\n")

    # Initialize driver
    options = webdriver.ChromeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

    driver = webdriver.Chrome(options=options)

    try:
        # Load the page
        print("⏳ Loading page...")
        driver.get(test_url)

        # Wait longer for JavaScript to render
        print("⏳ Waiting for page to render...")
        time.sleep(8)

        # Check what's on the page
        print(f"📄 Page title: {driver.title}")
        print(f"📄 Current URL: {driver.current_url}")

        # Try to find products without waiting
        try:
            # Look for any div elements that might be products
            all_divs = driver.find_elements(By.CSS_SELECTOR, "div")
            print(f"📊 Total divs on page: {len(all_divs)}")
        except:
            pass

        print("✅ Page loaded (proceeding without wait)\n")

        # Use THE EXACT SAME APPROACH AS THE SCRAPER
        # Find add-to-basket divs first, then navigate to product containers
        print("🔍 Using same selector as scraper: div.add-to-basket[data-ean][data-product-code]")
        add_to_basket_elements = driver.find_elements(By.CSS_SELECTOR, "div.add-to-basket[data-ean][data-product-code]")

        if not add_to_basket_elements:
            print("❌ No add-to-basket elements found")
            # Save page HTML for debugging
            with open('/tmp/super_pharm_product_page.html', 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            print(f"💾 Page HTML saved to: /tmp/super_pharm_product_page.html")
            return

        print(f"✅ Found {len(add_to_basket_elements)} add-to-basket elements\n")

        # Get product containers by navigating up from add-to-basket divs
        products = []
        for element in add_to_basket_elements[:3]:  # Only inspect first 3
            try:
                product_container = element.find_element(By.XPATH, "../..")  # Go up 2 levels
                products.append((element, product_container))
            except Exception as e:
                print(f"❌ Could not navigate to product container: {e}")
                continue

        if not products:
            print("❌ No product containers found")
            return

        print(f"✅ Successfully navigated to {len(products)} product containers\n")

        # Inspect first 3 products
        print("=" * 80)
        print("PRODUCT CONTAINER HTML ANALYSIS")
        print("=" * 80)

        for i, (add_to_basket, product_container) in enumerate(products, 1):
            print(f"\n{'='*80}")
            print(f"PRODUCT {i}")
            print(f"{'='*80}\n")

            # Show EAN and product code
            ean = add_to_basket.get_attribute('data-ean')
            product_code = add_to_basket.get_attribute('data-product-code')
            print(f"📊 EAN: {ean}")
            print(f"📊 Product Code: {product_code}\n")

            # Get outer HTML
            outer_html = product_container.get_attribute('outerHTML')
            print("📦 FULL HTML:")
            print("-" * 80)
            print(outer_html[:2000])  # First 2000 chars
            if len(outer_html) > 2000:
                print(f"\n... [truncated, total length: {len(outer_html)} chars] ...\n")
            print("-" * 80)

            # Try different approaches to find links
            print("\n🔍 LINK SEARCH ATTEMPTS:")
            print("-" * 80)

            # Attempt 1: Direct a[href]
            try:
                link = product_container.find_element(By.CSS_SELECTOR, "a[href]")
                print(f"✅ Direct a[href]: FOUND")
                print(f"   URL: {link.get_attribute('href')}")
            except Exception as e:
                print(f"❌ Direct a[href]: NOT FOUND")
                print(f"   Error: {str(e)[:100]}")

            # Attempt 2: Any anchor tag
            try:
                links = product_container.find_elements(By.TAG_NAME, "a")
                print(f"\n✅ Tag name 'a': FOUND {len(links)} anchor tag(s)")
                for idx, link in enumerate(links[:5], 1):
                    href = link.get_attribute('href')
                    text = link.text.strip()[:50]
                    classes = link.get_attribute('class')
                    print(f"   Link {idx}: {href}")
                    print(f"           Text: '{text}'")
                    print(f"           Classes: {classes}")
            except Exception as e:
                print(f"\n❌ Tag name 'a': NOT FOUND")
                print(f"   Error: {str(e)[:100]}")

            # Attempt 3: XPath for any link
            try:
                links = product_container.find_elements(By.XPATH, ".//a")
                print(f"\n✅ XPath './/a': FOUND {len(links)} link(s)")
                for idx, link in enumerate(links[:5], 1):
                    href = link.get_attribute('href')
                    print(f"   Link {idx}: {href}")
            except Exception as e:
                print(f"\n❌ XPath './/a': NOT FOUND")
                print(f"   Error: {str(e)[:100]}")

            # Attempt 4: Look for data attributes that might contain URLs
            try:
                all_attrs = driver.execute_script(
                    "return Object.entries(arguments[0].dataset).filter(([k,v]) => v.includes('http') || v.includes('/p/'))",
                    product_container
                )
                if all_attrs:
                    print(f"\n✅ Data attributes with URLs: FOUND {len(all_attrs)}")
                    for key, value in all_attrs:
                        print(f"   data-{key}: {value}")
                else:
                    print(f"\n❌ Data attributes with URLs: NONE")
            except Exception as e:
                print(f"\n❌ Data attributes: ERROR")
                print(f"   Error: {str(e)[:100]}")

            # Attempt 5: Check for onclick or data-url attributes
            try:
                onclick = product_container.get_attribute('onclick')
                data_url = product_container.get_attribute('data-url')
                data_link = product_container.get_attribute('data-link')
                data_href = product_container.get_attribute('data-href')

                print(f"\n🔍 Special attributes on container:")
                if onclick:
                    print(f"   onclick: {onclick[:100]}")
                if data_url:
                    print(f"   data-url: {data_url}")
                if data_link:
                    print(f"   data-link: {data_link}")
                if data_href:
                    print(f"   data-href: {data_href}")
                if not any([onclick, data_url, data_link, data_href]):
                    print(f"   None found")
            except Exception as e:
                print(f"\n❌ Special attributes: ERROR")
                print(f"   Error: {str(e)[:100]}")

            print("\n" + "=" * 80)

        # Save full HTML for manual inspection
        with open('/tmp/super_pharm_product_page.html', 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        print(f"\n💾 Full page HTML saved to: /tmp/super_pharm_product_page.html")

    finally:
        driver.quit()
        print("\n✅ Browser closed")

if __name__ == "__main__":
    inspect_product_html()
