#!/usr/bin/env python3
"""
Download Super-Pharm page HTML for reverse engineering
"""
import time
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

def download_super_pharm_html():
    print("🚀 Starting HTML download session...")

    # Try undetected-chromedriver first, fallback to regular Selenium
    driver = None
    try:
        options = uc.ChromeOptions()
        options.add_argument("--start-maximized")
        driver = uc.Chrome(options=options, use_subprocess=False)
        print("✅ Using undetected-chromedriver")
    except Exception as e:
        print(f"⚠️ Undetected-chromedriver failed: {e}")
        print("🔄 Falling back to regular Selenium...")

        try:
            regular_options = webdriver.ChromeOptions()
            regular_options.add_argument("--start-maximized")
            regular_options.add_argument("--disable-blink-features=AutomationControlled")
            regular_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            regular_options.add_experimental_option('useAutomationExtension', False)
            regular_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=regular_options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            print("✅ Using regular Selenium WebDriver")
        except Exception as e2:
            print(f"❌ Both drivers failed: {e2}")
            return

    try:
        # Navigate to the cereals category
        url = "https://shop.super-pharm.co.il/food-and-drinks/cereals-and-snacks/c/70140000"
        print(f"📍 Navigating to: {url}")
        driver.get(url)

        # Wait for page to load
        print("⏳ Waiting for page to load...")
        time.sleep(10)  # Give it extra time to fully load

        # Check page title
        print(f"🏷️  Page title: {driver.title}")
        print(f"🌐 Current URL: {driver.current_url}")

        # Get the full page source
        page_source = driver.page_source
        print(f"📄 Page source length: {len(page_source)} characters")

        # Save the HTML
        with open("super_pharm_full_page.html", "w", encoding="utf-8") as f:
            f.write(page_source)
        print("💾 Full page HTML saved as 'super_pharm_full_page.html'")

        # Also save a cleaned version with better formatting
        from bs4 import BeautifulSoup
        try:
            soup = BeautifulSoup(page_source, 'html.parser')
            pretty_html = soup.prettify()
            with open("super_pharm_pretty.html", "w", encoding="utf-8") as f:
                f.write(pretty_html)
            print("💾 Pretty-formatted HTML saved as 'super_pharm_pretty.html'")
        except ImportError:
            print("⚠️ BeautifulSoup not available for pretty formatting")

        # Look for key elements to understand the structure
        print("\n🔍 Analyzing page structure...")

        # Check for the "פרטים נוספים" button
        from selenium.webdriver.common.by import By

        buttons = driver.find_elements(By.XPATH, "//*[contains(text(), 'פרטים נוספים')]")
        print(f"🔘 Found {len(buttons)} 'פרטים נוספים' buttons")

        for i, button in enumerate(buttons[:3]):
            try:
                tag = button.tag_name
                classes = button.get_attribute("class")
                text = button.text
                print(f"  Button {i+1}: <{tag}> class='{classes}' text='{text}'")
            except:
                continue

        # Look for product containers
        print("\n🛍️ Looking for product containers...")
        product_patterns = [
            "div[data-testid*='product']",
            "[class*='product']",
            "[class*='Product']",
            "[class*='item']",
            "[class*='Item']",
            "[class*='card']",
            "[class*='Card']",
            "[class*='tile']",
            "[class*='Tile']"
        ]

        for pattern in product_patterns:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, pattern)
                if elements:
                    print(f"  {pattern}: {len(elements)} elements")
                    # Sample first few elements
                    for i, elem in enumerate(elements[:3]):
                        try:
                            classes = elem.get_attribute("class")
                            text_preview = elem.text[:100].replace('\n', ' ') if elem.text else "No text"
                            print(f"    [{i}] class='{classes}' text='{text_preview}...'")
                        except:
                            continue
            except:
                continue

        # Look for images that might be product images
        print("\n🖼️ Analyzing images...")
        images = driver.find_elements(By.TAG_NAME, "img")
        print(f"Found {len(images)} images total")

        product_images = []
        for img in images[:10]:  # Check first 10
            try:
                src = img.get_attribute("src")
                alt = img.get_attribute("alt")
                if src and not any(skip in src for skip in ['logo', 'icon', 'user-offline', 'placeholder']):
                    product_images.append({"src": src, "alt": alt})
            except:
                continue

        print(f"Found {len(product_images)} potential product images:")
        for i, img in enumerate(product_images[:5]):
            print(f"  [{i}] {img['src']}")
            print(f"      Alt: {img['alt']}")

        print(f"\n✅ Analysis complete! Check the saved HTML files for detailed structure.")

        input("Press Enter to close browser...")

    finally:
        driver.quit()
        print("✅ Browser closed")

if __name__ == "__main__":
    download_super_pharm_html()