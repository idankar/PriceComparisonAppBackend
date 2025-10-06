#!/usr/bin/env python3
"""
Test script to investigate if Super-Pharm barcode search is viable
"""

import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Test barcodes
test_cases = [
    {
        'barcode': '074764386045',
        'expected_product': 'ארדל PRESS ON מיני ריסים',
        'has_url': True,
        'url': 'https://shop.super-pharm.co.il/cosmetics/eye-makeup/true-eyelashes/PRESS-ON-%D7%9E%D7%99%D7%A0%D7%99-%D7%A8%D7%99%D7%A1%D7%99%D7%9D-WISPIES-NATURAL/p/688206'
    },
    {
        'barcode': '4001638096591',
        'expected_product': 'וולדה קרם אמבט קלנדולה לתינוק',
        'has_url': False,
        'url': None
    },
    {
        'barcode': '7290016692150',
        'expected_product': 'מאמי קר סבון טבעי',
        'has_url': True,
        'url': 'https://shop.super-pharm.co.il/infants-and-toddlers/nursing-and-feeding/brushes-and-soap-bottles-and-baby/%D7%A1%D7%91%D7%95%D7%9F-%D7%98%D7%91%D7%A2%D7%99-%D7%9E%D7%99%D7%95%D7%97%D7%93-%D7%9C%D7%A0%D7%99%D7%A7%D7%95%D7%99-%D7%91%D7%A7%D7%91%D7%95%D7%A7%D7%99%D7%9D-%D7%9E%D7%95%D7%A6%D7%A6%D7%99%D7%9D-%D7%95%D7%9E%D7%A9%D7%90%D7%91%D7%95%D7%AA-%D7%97%D7%9C%D7%91-%D7%90%D7%A8%D7%99%D7%96%D7%AA-%D7%97%D7%A1%D7%9B%D7%95%D7%9F/p/559907'
    },
    {
        'barcode': '7290120150843',
        'expected_product': 'לייף קליפסים רחבים',
        'has_url': False,
        'url': None
    },
    {
        'barcode': '0680196962551',
        'expected_product': 'חן אלקבץ סופיה קורל ליפסטיק',
        'has_url': False,
        'url': None
    }
]

def test_barcode_search(driver, barcode, expected_product):
    """Test a single barcode search"""
    search_url = f"https://shop.super-pharm.co.il/search?text={barcode}"

    print(f"\n{'='*80}")
    print(f"Testing barcode: {barcode}")
    print(f"Expected: {expected_product}")
    print(f"{'='*80}")

    try:
        driver.get(search_url)
        time.sleep(3)  # Wait for page load

        current_url = driver.current_url
        page_title = driver.title

        print(f"  Search URL: {search_url}")
        print(f"  Current URL: {current_url}")
        print(f"  Page Title: {page_title}")

        # Check if redirected to product page
        if '/p/' in current_url and current_url != search_url:
            print(f"  ✅ REDIRECT: Yes - redirected to product page")
            print(f"  Product URL: {current_url}")
            return {'status': 'redirect', 'url': current_url}

        # Check for "no results" message
        no_results_found = False
        try:
            no_results_texts = [
                'לא נמצאו תוצאות',
                'No results',
                'לא נמצא',
                'אין תוצאות'
            ]

            page_source = driver.page_source
            for text in no_results_texts:
                if text in page_source:
                    print(f"  ❌ NO RESULTS: Found '{text}' in page")
                    no_results_found = True
                    break
        except:
            pass

        # Check for search results
        try:
            # Look for product links
            product_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/p/']")
            print(f"  Found {len(product_links)} links with /p/ in href")

            if product_links:
                # Check if they're in a results container
                try:
                    results_containers = driver.find_elements(By.CSS_SELECTOR,
                        ".search-results, .product-grid, [class*='searchResult'], [class*='productList']")
                    print(f"  Found {len(results_containers)} potential results containers")

                    if results_containers:
                        first_link = product_links[0].get_attribute('href')
                        link_text = product_links[0].text[:50] if product_links[0].text else "No text"
                        print(f"  First product link: {first_link}")
                        print(f"  Link text: {link_text}")
                        return {'status': 'results', 'url': first_link, 'count': len(product_links)}
                    else:
                        print(f"  ⚠️ Product links found but NO results container")
                        print(f"  These are likely navigation/featured products")
                        first_link = product_links[0].get_attribute('href')
                        print(f"  First link (probably wrong): {first_link}")
                except Exception as e:
                    print(f"  Error checking containers: {e}")
        except Exception as e:
            print(f"  Error finding product links: {e}")

        if no_results_found:
            return {'status': 'no_results', 'url': None}
        else:
            return {'status': 'unknown', 'url': None}

    except Exception as e:
        print(f"  ❌ ERROR: {e}")
        return {'status': 'error', 'error': str(e)}

def main():
    print("="*80)
    print("SUPER-PHARM BARCODE SEARCH VIABILITY TEST")
    print("="*80)

    # Setup driver
    print("\nInitializing Chrome driver...")
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless=new")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        results = []

        for test_case in test_cases:
            result = test_barcode_search(driver, test_case['barcode'], test_case['expected_product'])
            results.append({
                'barcode': test_case['barcode'],
                'expected': test_case['expected_product'],
                'has_url_in_db': test_case['has_url'],
                'result': result
            })
            time.sleep(2)

        # Summary
        print(f"\n\n{'='*80}")
        print("SUMMARY")
        print(f"{'='*80}")

        redirect_count = sum(1 for r in results if r['result']['status'] == 'redirect')
        results_count = sum(1 for r in results if r['result']['status'] == 'results')
        no_results_count = sum(1 for r in results if r['result']['status'] == 'no_results')

        print(f"  Total tests: {len(results)}")
        print(f"  Redirects to product page: {redirect_count}")
        print(f"  Search results page: {results_count}")
        print(f"  No results: {no_results_count}")

        print(f"\n  Detailed Results:")
        for r in results:
            status_emoji = {
                'redirect': '✅',
                'results': '✅',
                'no_results': '❌',
                'unknown': '❓',
                'error': '💥'
            }.get(r['result']['status'], '❓')

            print(f"    {status_emoji} {r['barcode']}: {r['result']['status']}")
            if r['has_url_in_db']:
                print(f"       (Has URL in DB)")

        print(f"\n  Conclusion:")
        if redirect_count + results_count >= len(results) * 0.5:
            print(f"    ✅ Barcode search is VIABLE - {redirect_count + results_count}/{len(results)} successful")
        else:
            print(f"    ❌ Barcode search is NOT VIABLE - only {redirect_count + results_count}/{len(results)} successful")
            print(f"    Alternative approach needed for products without URLs")

        print(f"{'='*80}\n")

    finally:
        driver.quit()
        print("Browser closed")

if __name__ == "__main__":
    main()
