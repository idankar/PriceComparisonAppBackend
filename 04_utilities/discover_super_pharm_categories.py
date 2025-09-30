#!/usr/bin/env python3
"""
Discover all Super-Pharm product categories
"""
import time
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import json

def discover_categories():
    print("🔍 Starting Super-Pharm category discovery...")

    # Setup driver
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
        # Navigate to homepage
        base_url = "https://shop.super-pharm.co.il/"
        print(f"📍 Navigating to: {base_url}")
        driver.get(base_url)

        # Wait for page to load
        time.sleep(8)
        print(f"🏷️  Page title: {driver.title}")

        # Take screenshot
        driver.save_screenshot("super_pharm_homepage.png")
        print("📸 Homepage screenshot saved")

        # Look for category navigation elements
        print("\n🔍 Searching for category links...")

        category_selectors = [
            # Common navigation patterns
            "nav a[href*='/c/']",
            ".navigation a[href*='/c/']",
            ".menu a[href*='/c/']",
            ".categories a[href*='/c/']",
            "[class*='nav'] a[href*='/c/']",
            "[class*='menu'] a[href*='/c/']",
            "[class*='category'] a[href*='/c/']",

            # Department/category specific
            "a.department-name[href]",
            "a.deaprtment-name[href]",  # Keep typo from original
            "[data-cy='departments-menu'] a",
            "[data-testid*='category'] a",

            # Generic category patterns
            "a[href*='food-and-drinks']",
            "a[href*='cosmetics']",
            "a[href*='health']",
            "a[href*='care']",
            "a[href*='baby']",

            # Menu/dropdown patterns
            ".dropdown-menu a[href*='/c/']",
            ".mega-menu a[href*='/c/']",
            "[role='menu'] a[href*='/c/']"
        ]

        all_categories = {}

        for selector in category_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    print(f"  ✅ {selector}: {len(elements)} links")

                    for elem in elements:
                        try:
                            href = elem.get_attribute('href')
                            text = elem.text.strip()
                            title = elem.get_attribute('title')

                            if href and ('/c/' in href or 'category' in href.lower()):
                                name = text or title or href.split('/')[-1]
                                if name and len(name) > 1:
                                    all_categories[href] = {
                                        'name': name,
                                        'url': href,
                                        'selector': selector
                                    }
                        except:
                            continue

            except Exception as e:
                print(f"  ❌ {selector}: {e}")

        print(f"\n📋 Total unique categories found: {len(all_categories)}")

        # Display categories
        categories_list = list(all_categories.values())
        for i, cat in enumerate(categories_list[:20], 1):  # Show first 20
            print(f"  {i:2d}. {cat['name']} -> {cat['url']}")

        if len(categories_list) > 20:
            print(f"  ... and {len(categories_list) - 20} more")

        # Save to file
        with open("super_pharm_categories.json", "w", encoding="utf-8") as f:
            json.dump(categories_list, f, ensure_ascii=False, indent=2)
        print(f"\n💾 Categories saved to 'super_pharm_categories.json'")

        # Also look for sitemap or category index pages
        print("\n🗺️ Looking for sitemap or category index...")
        sitemap_urls = [
            "https://shop.super-pharm.co.il/sitemap",
            "https://shop.super-pharm.co.il/categories",
            "https://shop.super-pharm.co.il/all-categories"
        ]

        for sitemap_url in sitemap_urls:
            try:
                print(f"  📍 Checking: {sitemap_url}")
                driver.get(sitemap_url)
                time.sleep(3)

                if "404" not in driver.title and "not found" not in driver.title.lower():
                    print(f"    ✅ Found valid page: {driver.title}")
                    # Look for additional categories
                    more_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/c/']")
                    if more_links:
                        print(f"    📦 Found {len(more_links)} additional category links")
                else:
                    print(f"    ❌ Page not found")
            except:
                print(f"    ❌ Error accessing {sitemap_url}")

        input("\nPress Enter to close browser and continue...")

    finally:
        if driver:
            driver.quit()
        print("✅ Category discovery complete")

if __name__ == "__main__":
    discover_categories()