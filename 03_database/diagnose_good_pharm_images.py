#!/usr/bin/env python3
"""
Diagnostic script to test Good Pharm image extraction
"""
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

def test_image_extraction():
    """Test image extraction from Good Pharm listing page"""

    print("🔍 Starting Good Pharm Image Extraction Diagnostic")
    print("=" * 70)

    # Setup driver
    options = webdriver.ChromeOptions()
    # Run headless
    options.add_argument("--headless=new")
    options.add_argument("--start-maximized")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        url = "https://goodpharm.co.il/shop?wpf_filter_cat_0=44"
        print(f"\n📍 Loading URL: {url}")
        driver.get(url)

        # Wait for products to load
        print("⏳ Waiting for products to load...")
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "li.product"))
        )
        time.sleep(3)  # Extra wait for images

        # Get all product containers
        products = driver.find_elements(By.CSS_SELECTOR, "li.product")
        print(f"\n✅ Found {len(products)} product containers")

        # Test different image selectors on first 3 products
        for i, product in enumerate(products[:3], 1):
            print(f"\n{'='*70}")
            print(f"🔬 PRODUCT {i}")
            print(f"{'='*70}")

            # Get product name for context
            try:
                name_elem = product.find_element(By.CSS_SELECTOR, "h2.woocommerce-loop-product__title")
                print(f"📦 Product Name: {name_elem.text[:60]}")
            except:
                print("📦 Product Name: [Could not extract]")

            # Get outer HTML to see structure
            outer_html = product.get_attribute('outerHTML')
            print(f"\n📄 HTML Length: {len(outer_html)} characters")

            # Test various image selectors
            selectors = [
                "img",
                "img.attachment-woocommerce_thumbnail",
                "img.wp-post-image",
                "a.woocommerce-LoopProduct-link img",
                "img[src*='.jpg']",
                "img[class*='woocommerce']"
            ]

            print(f"\n🎯 Testing Image Selectors:")
            for selector in selectors:
                try:
                    imgs = product.find_elements(By.CSS_SELECTOR, selector)
                    if imgs:
                        img = imgs[0]
                        src = img.get_attribute('src')
                        data_src = img.get_attribute('data-src')
                        classes = img.get_attribute('class')

                        print(f"\n  ✅ Selector: {selector}")
                        print(f"     Found: {len(imgs)} image(s)")
                        print(f"     Classes: {classes}")
                        print(f"     src: {src[:80] if src else 'None'}...")
                        print(f"     data-src: {data_src[:80] if data_src else 'None'}...")
                    else:
                        print(f"\n  ❌ Selector: {selector} - No images found")
                except Exception as e:
                    print(f"\n  ⚠️  Selector: {selector} - Error: {e}")

            # Show a sample of the HTML
            if len(outer_html) < 1000:
                print(f"\n📋 Full HTML:\n{outer_html}")
            else:
                print(f"\n📋 HTML Sample (first 800 chars):\n{outer_html[:800]}...")

        print(f"\n{'='*70}")
        print("✅ Diagnostic Complete")
        print(f"{'='*70}")

    except Exception as e:
        print(f"\n❌ Error during diagnostic: {e}")
    finally:
        driver.quit()
        print("\n🌐 Browser closed")

if __name__ == "__main__":
    test_image_extraction()
