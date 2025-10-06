#!/usr/bin/env python3
"""
Super-Pharm HTML Inspector - See what the browser actually sees

This script loads a Super-Pharm category page using Selenium and inspects
the actual HTML structure that the scraper would see, focusing on image elements.
"""

import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

def inspect_super_pharm_images():
    """Inspect how images are actually loaded on Super-Pharm"""

    print("="*80)
    print("🔍 SUPER-PHARM HTML INSPECTOR")
    print("="*80)
    print()

    # Setup browser
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--start-maximized")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    # Load a category page (nail polish - one with missing images)
    test_url = "https://shop.super-pharm.co.il/cosmetics/nail-care/c/20130000"
    print(f"📄 Loading: {test_url}")
    driver.get(test_url)

    # Wait for products to load
    print("⏳ Waiting for products to load...")
    time.sleep(8)  # Give it time to load dynamically

    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.add-to-basket[data-ean]"))
        )
        print("✅ Products loaded!\n")
    except:
        print("❌ Products didn't load in time\n")

    # Find product elements
    product_elements = driver.find_elements(By.CSS_SELECTOR, "div.add-to-basket[data-ean]")
    print(f"📦 Found {len(product_elements)} product containers\n")

    # Inspect first 5 products in detail
    print("="*80)
    print("DETAILED INSPECTION OF FIRST 5 PRODUCTS")
    print("="*80)

    for i, element in enumerate(product_elements[:5], 1):
        try:
            ean = element.get_attribute('data-ean')
            product_code = element.get_attribute('data-product-code')

            print(f"\n🔍 PRODUCT {i}:")
            print(f"   EAN/Barcode: {ean}")
            print(f"   Product Code: {product_code}")

            # Navigate up to product container
            product_container = element.find_element(By.XPATH, "../..")

            # Find ALL img elements in the container
            img_elements = product_container.find_elements(By.TAG_NAME, "img")
            print(f"   Found {len(img_elements)} <img> elements")

            for img_idx, img in enumerate(img_elements, 1):
                print(f"\n   📷 IMAGE {img_idx}:")

                # Get ALL attributes
                attrs_to_check = [
                    'src', 'data-src', 'data-lazy-src', 'data-original',
                    'data-srcset', 'srcset', 'alt', 'title', 'class',
                    'loading', 'data-zoom-image', 'data-image'
                ]

                for attr in attrs_to_check:
                    value = img.get_attribute(attr)
                    if value:
                        print(f"      {attr}: {value[:120]}")

                # Check if it's a picture element
                try:
                    parent = img.find_element(By.XPATH, "..")
                    if parent.tag_name == 'picture':
                        print(f"      ⚠️  Inside <picture> element")
                        sources = parent.find_elements(By.TAG_NAME, 'source')
                        for src_idx, source in enumerate(sources, 1):
                            srcset = source.get_attribute('srcset')
                            media = source.get_attribute('media')
                            print(f"         <source {src_idx}> srcset: {srcset}")
                            print(f"         <source {src_idx}> media: {media}")
                except:
                    pass

            # Try to get product name
            try:
                name_elem = product_container.find_element(By.CSS_SELECTOR, ".name, .product-name, [class*='name']")
                name = name_elem.text.strip()
                print(f"\n   📝 Product Name: {name}")
            except:
                print(f"\n   📝 Product Name: (not found)")

            print("-"*80)

        except Exception as e:
            print(f"\n   ❌ Error inspecting product {i}: {e}")
            continue

    print("\n")
    print("="*80)
    print("IMAGE EXTRACTION SUMMARY")
    print("="*80)

    # Try the current scraper's approach
    successful_current = 0
    failed_current = 0

    # Try enhanced approach
    successful_enhanced = 0
    failed_enhanced = 0

    for element in product_elements[:20]:
        try:
            product_container = element.find_element(By.XPATH, "../..")
            img = product_container.find_element(By.TAG_NAME, "img")

            # Current scraper approach
            current_url = img.get_attribute('src') or img.get_attribute('data-src')
            if current_url and 'placeholder' not in current_url.lower():
                successful_current += 1
            else:
                failed_current += 1

            # Enhanced approach
            enhanced_url = (
                img.get_attribute('src') or
                img.get_attribute('data-src') or
                img.get_attribute('data-lazy-src') or
                img.get_attribute('data-original') or
                img.get_attribute('data-srcset') or
                (img.get_attribute('srcset') or '').split(',')[0].strip().split(' ')[0]
            )

            if enhanced_url and 'placeholder' not in enhanced_url.lower():
                successful_enhanced += 1
            else:
                failed_enhanced += 1

        except:
            failed_current += 1
            failed_enhanced += 1

    total_tested = successful_current + failed_current

    print(f"\nTested first {total_tested} products:")
    print(f"  Current scraper method: {successful_current}/{total_tested} images captured ({successful_current/total_tested*100:.1f}%)")
    print(f"  Enhanced method:        {successful_enhanced}/{total_tested} images captured ({successful_enhanced/total_tested*100:.1f}%)")
    print()

    if successful_enhanced > successful_current:
        improvement = successful_enhanced - successful_current
        print(f"🎯 Enhanced method captures {improvement} more images ({improvement/total_tested*100:.1f}% improvement)")
    else:
        print("🟡 No significant improvement with enhanced method")
        print("   The issue may be elsewhere (e.g., products genuinely without images)")

    print()
    driver.quit()
    print("✅ Inspection complete")

if __name__ == "__main__":
    inspect_super_pharm_images()
