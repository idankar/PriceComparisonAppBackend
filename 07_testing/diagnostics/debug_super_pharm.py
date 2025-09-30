#!/usr/bin/env python3
"""
Debug script to examine Super-Pharm page structure
"""
import time
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By

def debug_super_pharm():
    print("🚀 Starting debug session...")
    options = uc.ChromeOptions()
    options.add_argument("--start-maximized")

    driver = uc.Chrome(options=options, use_subprocess=False)

    try:
        # Navigate to a known category page
        url = "https://shop.super-pharm.co.il/cosmetics/makeup/c/30010000"
        print(f"📍 Navigating to: {url}")
        driver.get(url)

        # Wait for page to load
        time.sleep(5)

        # Take screenshot
        driver.save_screenshot("super_pharm_debug.png")
        print("📸 Screenshot saved as 'super_pharm_debug.png'")

        # Check page title
        print(f"🏷️  Page title: {driver.title}")

        # Look for product-related elements with various selectors
        selectors_to_try = [
            "li[class*='product']",
            "div[class*='product']",
            "article[class*='product']",
            "[data-product-id]",
            "[data-product-sku]",
            ".product",
            "li.product-item",
            "div.product-item",
            "li[class*='item']",
            "div[class*='item']",
            "[class*='card']",
            "[class*='tile']"
        ]

        print("\n🔍 Searching for product elements...")
        for selector in selectors_to_try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            print(f"  {selector}: {len(elements)} elements")
            if elements and len(elements) < 20:  # Show details for manageable numbers
                for i, elem in enumerate(elements[:5]):
                    try:
                        text = elem.text[:100] + "..." if len(elem.text) > 100 else elem.text
                        classes = elem.get_attribute("class")
                        print(f"    [{i}] Classes: {classes}")
                        print(f"        Text: {text}")
                    except:
                        print(f"    [{i}] Could not extract text")

        # Check for prices specifically
        print("\n💰 Looking for price elements...")
        price_elements = driver.find_elements(By.XPATH, "//*[contains(text(), '₪')]")
        print(f"  Found {len(price_elements)} elements containing '₪'")

        for i, elem in enumerate(price_elements[:5]):
            try:
                text = elem.text
                tag_name = elem.tag_name
                classes = elem.get_attribute("class")
                print(f"    [{i}] {tag_name}.{classes}: '{text}'")
            except:
                continue

        # Let's also check the page source size
        page_source = driver.page_source
        print(f"\n📄 Page source length: {len(page_source)} characters")

        # Save page source for manual inspection
        with open("super_pharm_page_source.html", "w", encoding="utf-8") as f:
            f.write(page_source)
        print("💾 Page source saved as 'super_pharm_page_source.html'")

        input("Press Enter to continue...")

    finally:
        driver.quit()
        print("✅ Debug session complete")

if __name__ == "__main__":
    debug_super_pharm()