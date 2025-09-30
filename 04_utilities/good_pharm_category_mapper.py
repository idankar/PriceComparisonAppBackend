#!/usr/bin/env python3
"""
Good Pharm Category Mapper
Maps category IDs to their Hebrew names based on product analysis.
"""

import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

# All Good Pharm category URLs from the scraper
CATEGORY_URLS = [
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=44",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=45",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=46",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=47",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=48",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=49",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=50",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=51",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=52",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=53",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=54",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=55",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=56",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=64",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=76",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=80"
]

def analyze_category(url):
    """Analyze a Good Pharm category page to determine its category name"""

    # Setup Chrome options
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        category_id = url.split('=')[-1]
        print(f"\n🔍 Category {category_id}: {url}")

        driver.get(url)
        time.sleep(3)

        # Get first few product names to analyze category
        product_names = []
        try:
            # Wait for products to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "li.product"))
            )

            # Get product names from h2 elements (like we saw in diagnostic)
            h2_elements = driver.find_elements(By.TAG_NAME, "h2")
            for h2 in h2_elements[:8]:  # First 8 products
                text = h2.text.strip()
                if text.startswith("GOOD PHARM"):
                    product_names.append(text)

        except TimeoutException:
            print(f"   ⚠️ No products found in category {category_id}")

        # Analyze product names to determine category
        category_name = determine_category_from_products(product_names, category_id)

        print(f"   📦 Found {len(product_names)} products")
        print(f"   🏷️  Determined category: {category_name}")

        if product_names:
            print(f"   📝 Sample products:")
            for i, name in enumerate(product_names[:3], 1):
                print(f"      {i}. {name[:60]}...")

        return category_id, category_name

    except Exception as e:
        print(f"   ❌ Error analyzing category {category_id}: {e}")
        return category_id, "לא ידוע"

    finally:
        driver.quit()

def determine_category_from_products(product_names, category_id):
    """Determine category name based on product analysis"""

    if not product_names:
        return "קטגוריה ריקה"

    # Join all product names for keyword analysis
    all_text = ' '.join(product_names).lower()

    # Category determination based on keywords
    if any(word in all_text for word in ['משקפי', 'קריאה', 'שמש', 'עדשות']):
        return "אופטיקה"
    elif any(word in all_text for word in ['קיסמי', 'שיניים', 'דנטלי', 'מברשת שיניים']):
        return "דנטלית"
    elif any(word in all_text for word in ['נייר', 'כוסות', 'חד פעמי', 'צלחות']):
        return "חד פעמי"
    elif any(word in all_text for word in ['אוזניות', 'חיבור', 'דיגיטלי', 'מטען', 'כבל']):
        return "חשמל ואלקטרוניקה"
    elif any(word in all_text for word in ['שמפו', 'סבון', 'מברשת', 'פילינג', 'חמאת']):
        return "טואלטיקה"
    elif any(word in all_text for word in ['כלי', 'מטבח', 'תבנית', 'סיר', 'כוסות']):
        return "כלי בית"
    elif any(word in all_text for word in ['מגבוני', 'חיתולי', 'תינוק', 'ילדים']):
        return "תינוקות וילדים"
    elif any(word in all_text for word in ['ויטמינים', 'תוספי', 'בריאות']):
        return "תוספי תזונה"
    elif any(word in all_text for word in ['תיק', 'נסיעות', 'מזוודה']):
        return "נסיעות ופנוי"
    elif any(word in all_text for word in ['פלסטרים', 'תחבושת', 'רפואי']):
        return "עזרה ראשונה"
    elif any(word in all_text for word in ['מגן', 'אלסטי', 'תמיכה']):
        return "אורתופדיה"
    elif any(word in all_text for word in ['מדחום', 'מדידה']):
        return "מכשור רפואי"
    else:
        # Fallback - try to identify by category ID patterns we've seen
        category_fallbacks = {
            "44": "מוצרי בית כלליים",
            "45": "אופטיקה",
            "46": "דנטלית",
            "47": "חד פעמי",
            "48": "אביזרי נסיעות",
            "49": "טואלטיקה",
            "50": "מוצרי ילדים",
            "51": "מכשור רפואי",
            "52": "תוספי תזונה",
            "53": "עזרה ראשונה",
            "54": "אורתופדיה",
            "55": "מוצרי יופי",
            "56": "אביזרי אמבט",
            "64": "קטגוריה מיוחדת",
            "76": "מוצרי ספורט",
            "80": "מוצרי חורף"
        }

        return category_fallbacks.get(category_id, f"קטגוריה {category_id}")

def main():
    """Main function to analyze all categories"""
    print("🚀 Good Pharm Category Mapping Analysis")
    print("=" * 60)

    category_mapping = {}

    for url in CATEGORY_URLS:
        try:
            category_id, category_name = analyze_category(url)
            category_mapping[category_id] = category_name
            time.sleep(2)  # Be respectful to the server
        except Exception as e:
            print(f"❌ Failed to analyze {url}: {e}")

    print(f"\n📊 FINAL CATEGORY MAPPING:")
    print("=" * 60)

    for category_id, category_name in sorted(category_mapping.items()):
        print(f"    {category_id}: '{category_name}'")

    # Generate Python dictionary format for easy copying
    print(f"\n💻 Python Dictionary Format:")
    print("CATEGORY_ID_TO_NAME = {")
    for category_id, category_name in sorted(category_mapping.items()):
        print(f'    "{category_id}": "{category_name}",')
    print("}")

if __name__ == "__main__":
    main()