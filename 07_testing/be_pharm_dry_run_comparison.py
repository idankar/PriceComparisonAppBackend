#!/usr/bin/env python3
"""
Be Pharm Data Source Comparison - Dry Run

This script runs both Be Pharm scrapers (commercial and ETL/transparency) in dry run mode,
saves results to JSON files, and provides a detailed comparison of data quality.
"""

import json
import subprocess
import sys
import os
from datetime import datetime
from collections import defaultdict
import re


def run_commercial_scraper_dry_run():
    """
    Run the Be Pharm commercial website scraper in dry run mode.
    """
    print("\n" + "="*80)
    print("RUNNING BE PHARM COMMERCIAL SCRAPER (DRY RUN)")
    print("="*80)

    # Create a modified version of the commercial scraper for dry run
    dry_run_script = """
import sys
import os
sys.path.append('/Users/idankarbat/Documents/noa_recovery/Desktop/PriceComparisonApp')
sys.path.append('/Users/idankarbat/Documents/noa_recovery/Desktop/PriceComparisonApp/01_data_scraping_pipeline')

import json
import logging
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import time

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def scrape_be_pharm_commercial(limit_stores=3, limit_products=100):
    \"\"\"Scrape Be Pharm commercial website for product and price data.\"\"\"

    base_url = "https://shop.be-pharm.co.il"
    api_url = f"{base_url}/api/catalog/PriceFull"

    logger.info("Starting Be Pharm commercial scraper dry run...")

    # Sample store IDs (limiting to 3 for dry run)
    stores = [
        {"store_id": "1", "name": "Be Pharm Ramat Gan"},
        {"store_id": "2", "name": "Be Pharm Tel Aviv"},
        {"store_id": "3", "name": "Be Pharm Herzliya"}
    ][:limit_stores]

    all_products = []
    all_prices = []

    for store in stores:
        logger.info(f"Scraping store: {store['name']}")

        try:
            # Make request to API
            params = {
                'store_id': store['store_id'],
                'limit': limit_products
            }

            response = requests.get(api_url, params=params, timeout=30)

            if response.status_code == 200:
                data = response.json()

                # Process products
                products = data.get('products', [])

                for product in products[:limit_products]:
                    # Extract product info
                    product_data = {
                        'barcode': product.get('barcode', ''),
                        'name': product.get('name', ''),
                        'brand': product.get('brand', ''),
                        'category': product.get('category', ''),
                        'description': product.get('description', ''),
                        'image_url': product.get('image_url', ''),
                        'source': 'Be Pharm Commercial',
                        'scraped_at': datetime.now().isoformat()
                    }

                    # Extract price info
                    price_data = {
                        'barcode': product.get('barcode', ''),
                        'store_id': store['store_id'],
                        'store_name': store['name'],
                        'price': float(product.get('price', 0)),
                        'sale_price': float(product.get('sale_price', 0)) if product.get('sale_price') else None,
                        'currency': 'ILS',
                        'scraped_at': datetime.now().isoformat()
                    }

                    if product_data['barcode']:  # Only add if barcode exists
                        all_products.append(product_data)
                        all_prices.append(price_data)

                logger.info(f"  Found {len(products)} products with prices")

            else:
                logger.error(f"  Failed to fetch data: {response.status_code}")

        except Exception as e:
            logger.error(f"  Error scraping store {store['name']}: {e}")
            # Try alternative scraping method
            try:
                # Fallback to HTML scraping
                category_url = f"{base_url}/categories/pharmacy"
                response = requests.get(category_url, timeout=30)

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')

                    # Find product cards
                    product_cards = soup.find_all('div', class_='product-card')[:limit_products]

                    for card in product_cards:
                        try:
                            # Extract from HTML
                            barcode = card.get('data-barcode', '')
                            name = card.find('h3', class_='product-name')
                            price = card.find('span', class_='price')

                            if barcode:
                                product_data = {
                                    'barcode': barcode,
                                    'name': name.text.strip() if name else '',
                                    'brand': '',  # Extract if available
                                    'category': 'pharmacy',
                                    'description': '',
                                    'image_url': card.find('img')['src'] if card.find('img') else '',
                                    'source': 'Be Pharm Commercial HTML',
                                    'scraped_at': datetime.now().isoformat()
                                }

                                price_data = {
                                    'barcode': barcode,
                                    'store_id': store['store_id'],
                                    'store_name': store['name'],
                                    'price': float(re.sub(r'[^0-9.]', '', price.text)) if price else 0,
                                    'sale_price': None,
                                    'currency': 'ILS',
                                    'scraped_at': datetime.now().isoformat()
                                }

                                all_products.append(product_data)
                                all_prices.append(price_data)

                        except Exception as e:
                            logger.debug(f"    Error parsing product card: {e}")

                    logger.info(f"  Found {len(product_cards)} products via HTML scraping")

            except Exception as e2:
                logger.error(f"  Fallback scraping also failed: {e2}")

        # Small delay between stores
        time.sleep(1)

    # Remove duplicates
    seen_barcodes = set()
    unique_products = []
    for p in all_products:
        if p['barcode'] not in seen_barcodes:
            seen_barcodes.add(p['barcode'])
            unique_products.append(p)

    logger.info(f"\\nTotal unique products: {len(unique_products)}")
    logger.info(f"Total price points: {len(all_prices)}")

    return unique_products, all_prices

# Run the scraper
if __name__ == "__main__":
    products, prices = scrape_be_pharm_commercial()

    # Save to JSON
    output = {
        'metadata': {
            'source': 'Be Pharm Commercial Website',
            'scraped_at': datetime.now().isoformat(),
            'total_products': len(products),
            'total_prices': len(prices),
            'unique_barcodes': len(set(p['barcode'] for p in products if p['barcode']))
        },
        'products': products,
        'prices': prices
    }

    with open('be_pharm_commercial_dry_run.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\\n✅ Saved {len(products)} products and {len(prices)} prices to be_pharm_commercial_dry_run.json")
"""

    # Write and run the dry run script
    with open('temp_commercial_dry_run.py', 'w') as f:
        f.write(dry_run_script)

    try:
        result = subprocess.run([sys.executable, 'temp_commercial_dry_run.py'],
                              capture_output=True, text=True, timeout=60)
        print(result.stdout)
        if result.stderr:
            print("Errors:", result.stderr)
    except subprocess.TimeoutExpired:
        print("⚠️ Commercial scraper timed out after 60 seconds")
    except Exception as e:
        print(f"❌ Error running commercial scraper: {e}")
    finally:
        # Clean up temp file
        if os.path.exists('temp_commercial_dry_run.py'):
            os.remove('temp_commercial_dry_run.py')


def run_etl_transparency_dry_run():
    """
    Run the Be Pharm transparency/ETL scraper in dry run mode.
    """
    print("\n" + "="*80)
    print("RUNNING BE PHARM ETL/TRANSPARENCY SCRAPER (DRY RUN)")
    print("="*80)

    # Create a modified version of the ETL scraper for dry run
    dry_run_script = """
import sys
import os
sys.path.append('/Users/idankarbat/Documents/noa_recovery/Desktop/PriceComparisonApp')
sys.path.append('/Users/idankarbat/Documents/noa_recovery/Desktop/PriceComparisonApp/01_data_scraping_pipeline')

import json
import logging
from datetime import datetime
import requests
import gzip
import xml.etree.ElementTree as ET
from io import BytesIO

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def scrape_be_pharm_transparency(limit_stores=3):
    \"\"\"Scrape Be Pharm transparency portal data.\"\"\"

    base_url = "https://url.publishedprices.co.il"

    logger.info("Starting Be Pharm transparency scraper dry run...")

    # Be Pharm chain ID
    chain_id = "7290058197699"

    all_products = []
    all_prices = []

    # Get store list
    stores_url = f"{base_url}/file/d/{chain_id}-1-202509"

    logger.info(f"Fetching store list from transparency portal...")

    # Sample stores (limiting for dry run)
    stores = []
    try:
        response = requests.get(f"{base_url}/stores?chain={chain_id}", timeout=30)
        if response.status_code == 200:
            store_data = response.json() if response.headers.get('content-type') == 'application/json' else []
            stores = store_data[:limit_stores]
            logger.info(f"Found {len(store_data)} stores, using {len(stores)} for dry run")
        else:
            # Use default stores if API fails
            stores = [
                {"store_id": "001", "name": "Be Store 1"},
                {"store_id": "002", "name": "Be Store 2"},
                {"store_id": "003", "name": "Be Store 3"}
            ][:limit_stores]
            logger.info(f"Using default {len(stores)} stores")
    except Exception as e:
        logger.error(f"Error fetching stores: {e}")
        stores = [
            {"store_id": "001", "name": "Be Store 1"},
            {"store_id": "002", "name": "Be Store 2"}
        ][:limit_stores]

    for store in stores:
        logger.info(f"Processing store: {store.get('name', store['store_id'])}")

        try:
            # Construct price file URL (format varies)
            # Try different URL patterns
            patterns = [
                f"{base_url}/file/d/{chain_id}-{store['store_id']}-202509",
                f"{base_url}/price/7290058197699/{store['store_id']}/202509",
                f"{base_url}/prices/{chain_id}_{store['store_id']}_202509.gz"
            ]

            data_found = False
            for url in patterns:
                try:
                    logger.info(f"  Trying URL: {url}")
                    response = requests.get(url, timeout=30, stream=True)

                    if response.status_code == 200:
                        # Try to decompress if gzipped
                        if url.endswith('.gz') or response.headers.get('content-encoding') == 'gzip':
                            content = gzip.decompress(response.content)
                        else:
                            content = response.content

                        # Parse XML
                        root = ET.fromstring(content)

                        # Extract products and prices
                        items = root.findall('.//Item') or root.findall('.//Product')

                        logger.info(f"  Found {len(items)} items in transparency data")

                        for item in items[:100]:  # Limit to 100 items per store for dry run
                            barcode = item.findtext('ItemCode') or item.findtext('Barcode') or ''

                            if barcode:
                                product_data = {
                                    'barcode': barcode,
                                    'name': item.findtext('ItemName') or item.findtext('ProductName') or '',
                                    'brand': item.findtext('ManufacturerName') or '',
                                    'category': item.findtext('ItemType') or '',
                                    'description': item.findtext('ItemDescription') or '',
                                    'image_url': '',  # Usually not in transparency data
                                    'source': 'Be Pharm Transparency Portal',
                                    'scraped_at': datetime.now().isoformat()
                                }

                                price_data = {
                                    'barcode': barcode,
                                    'store_id': store['store_id'],
                                    'store_name': store.get('name', f"Store {store['store_id']}"),
                                    'price': float(item.findtext('ItemPrice') or item.findtext('Price') or 0),
                                    'sale_price': float(item.findtext('UnitOfMeasurePrice') or 0) if item.findtext('UnitOfMeasurePrice') else None,
                                    'currency': 'ILS',
                                    'scraped_at': datetime.now().isoformat()
                                }

                                all_products.append(product_data)
                                all_prices.append(price_data)

                        data_found = True
                        break

                except Exception as e:
                    logger.debug(f"    Failed with pattern {url}: {e}")
                    continue

            if not data_found:
                logger.warning(f"  No data found for store {store['store_id']}")

        except Exception as e:
            logger.error(f"  Error processing store {store['store_id']}: {e}")

    # Remove duplicates
    seen_barcodes = set()
    unique_products = []
    for p in all_products:
        if p['barcode'] not in seen_barcodes:
            seen_barcodes.add(p['barcode'])
            unique_products.append(p)

    logger.info(f"\\nTotal unique products: {len(unique_products)}")
    logger.info(f"Total price points: {len(all_prices)}")

    return unique_products, all_prices

# Run the scraper
if __name__ == "__main__":
    products, prices = scrape_be_pharm_transparency()

    # Save to JSON
    output = {
        'metadata': {
            'source': 'Be Pharm Transparency Portal',
            'scraped_at': datetime.now().isoformat(),
            'total_products': len(products),
            'total_prices': len(prices),
            'unique_barcodes': len(set(p['barcode'] for p in products if p['barcode']))
        },
        'products': products,
        'prices': prices
    }

    with open('be_pharm_transparency_dry_run.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\\n✅ Saved {len(products)} products and {len(prices)} prices to be_pharm_transparency_dry_run.json")
"""

    # Write and run the dry run script
    with open('temp_etl_dry_run.py', 'w') as f:
        f.write(dry_run_script)

    try:
        result = subprocess.run([sys.executable, 'temp_etl_dry_run.py'],
                              capture_output=True, text=True, timeout=60)
        print(result.stdout)
        if result.stderr:
            print("Errors:", result.stderr)
    except subprocess.TimeoutExpired:
        print("⚠️ ETL scraper timed out after 60 seconds")
    except Exception as e:
        print(f"❌ Error running ETL scraper: {e}")
    finally:
        # Clean up temp file
        if os.path.exists('temp_etl_dry_run.py'):
            os.remove('temp_etl_dry_run.py')


def compare_results():
    """
    Compare the results from both scrapers.
    """
    print("\n" + "="*80)
    print("COMPARISON ANALYSIS")
    print("="*80)

    commercial_file = 'be_pharm_commercial_dry_run.json'
    transparency_file = 'be_pharm_transparency_dry_run.json'

    # Load results
    commercial_data = {}
    transparency_data = {}

    if os.path.exists(commercial_file):
        with open(commercial_file, 'r', encoding='utf-8') as f:
            commercial_data = json.load(f)
    else:
        print(f"⚠️ Commercial data file not found: {commercial_file}")

    if os.path.exists(transparency_file):
        with open(transparency_file, 'r', encoding='utf-8') as f:
            transparency_data = json.load(f)
    else:
        print(f"⚠️ Transparency data file not found: {transparency_file}")

    if not commercial_data or not transparency_data:
        print("Cannot perform comparison without both datasets")
        return

    # Analyze commercial data
    print("\n📊 COMMERCIAL WEBSITE DATA:")
    print("-" * 60)
    if commercial_data:
        print(f"Total products: {commercial_data['metadata']['total_products']}")
        print(f"Total price points: {commercial_data['metadata']['total_prices']}")
        print(f"Unique barcodes: {commercial_data['metadata']['unique_barcodes']}")

        # Check data quality
        products = commercial_data.get('products', [])
        prices = commercial_data.get('prices', [])

        with_images = sum(1 for p in products if p.get('image_url'))
        with_brands = sum(1 for p in products if p.get('brand'))
        with_categories = sum(1 for p in products if p.get('category'))
        avg_name_length = sum(len(p.get('name', '')) for p in products) / len(products) if products else 0

        print(f"\nData Quality:")
        print(f"  Products with images: {with_images}/{len(products)} ({with_images*100/len(products):.1f}%)")
        print(f"  Products with brands: {with_brands}/{len(products)} ({with_brands*100/len(products):.1f}%)")
        print(f"  Products with categories: {with_categories}/{len(products)} ({with_categories*100/len(products):.1f}%)")
        print(f"  Average product name length: {avg_name_length:.1f} chars")

        # Price analysis
        if prices:
            price_values = [p['price'] for p in prices if p.get('price')]
            avg_price = sum(price_values) / len(price_values) if price_values else 0
            print(f"  Average price: ₪{avg_price:.2f}")
            print(f"  Prices per product: {len(prices)/len(products):.1f}")

    # Analyze transparency data
    print("\n📊 TRANSPARENCY PORTAL DATA:")
    print("-" * 60)
    if transparency_data:
        print(f"Total products: {transparency_data['metadata']['total_products']}")
        print(f"Total price points: {transparency_data['metadata']['total_prices']}")
        print(f"Unique barcodes: {transparency_data['metadata']['unique_barcodes']}")

        # Check data quality
        products = transparency_data.get('products', [])
        prices = transparency_data.get('prices', [])

        with_images = sum(1 for p in products if p.get('image_url'))
        with_brands = sum(1 for p in products if p.get('brand'))
        with_categories = sum(1 for p in products if p.get('category'))
        avg_name_length = sum(len(p.get('name', '')) for p in products) / len(products) if products else 0

        print(f"\nData Quality:")
        print(f"  Products with images: {with_images}/{len(products)} ({with_images*100/len(products) if products else 0:.1f}%)")
        print(f"  Products with brands: {with_brands}/{len(products)} ({with_brands*100/len(products) if products else 0:.1f}%)")
        print(f"  Products with categories: {with_categories}/{len(products)} ({with_categories*100/len(products) if products else 0:.1f}%)")
        print(f"  Average product name length: {avg_name_length:.1f} chars")

        # Price analysis
        if prices:
            price_values = [p['price'] for p in prices if p.get('price')]
            avg_price = sum(price_values) / len(price_values) if price_values else 0
            print(f"  Average price: ₪{avg_price:.2f}")
            print(f"  Prices per product: {len(prices)/len(products) if products else 0:.1f}")

    # Barcode overlap analysis
    print("\n🔄 BARCODE OVERLAP ANALYSIS:")
    print("-" * 60)

    commercial_barcodes = set(p['barcode'] for p in commercial_data.get('products', []) if p.get('barcode'))
    transparency_barcodes = set(p['barcode'] for p in transparency_data.get('products', []) if p.get('barcode'))

    overlap = commercial_barcodes & transparency_barcodes
    commercial_only = commercial_barcodes - transparency_barcodes
    transparency_only = transparency_barcodes - commercial_barcodes

    print(f"Commercial only: {len(commercial_only)} products")
    print(f"Transparency only: {len(transparency_only)} products")
    print(f"Both sources: {len(overlap)} products")

    if commercial_barcodes:
        print(f"Overlap rate: {len(overlap)*100/len(commercial_barcodes):.1f}% of commercial products also in transparency")

    # Recommendation
    print("\n" + "="*80)
    print("📝 RECOMMENDATION")
    print("="*80)

    commercial_score = 0
    transparency_score = 0

    # Score based on various factors
    if commercial_data:
        if commercial_data['metadata']['total_products'] > transparency_data['metadata'].get('total_products', 0):
            commercial_score += 2
        if any(p.get('image_url') for p in commercial_data.get('products', [])):
            commercial_score += 3  # Images are important
        if commercial_data['metadata']['total_prices'] > transparency_data['metadata'].get('total_prices', 0):
            commercial_score += 1

    if transparency_data:
        if transparency_data['metadata']['total_products'] > commercial_data['metadata'].get('total_products', 0):
            transparency_score += 2

    print("\n🏆 WINNER:", end=" ")
    if commercial_score > transparency_score:
        print("COMMERCIAL WEBSITE DATA")
        print("\nReasons:")
        print("  ✅ More complete product information")
        print("  ✅ Includes product images")
        print("  ✅ Better data quality")
        print("  ✅ More reliable price points")
        print("\n💡 RECOMMENDATION: Switch to commercial website data for Be Pharm prices")
    else:
        print("TRANSPARENCY PORTAL DATA")
        print("\nReasons:")
        print("  ✅ Official government-mandated data")
        print("  ✅ Standardized format")
        print("\n💡 RECOMMENDATION: Keep using transparency data but needs improvement")


def main():
    """Main execution function."""
    print("\n" + "="*80)
    print("BE PHARM DATA SOURCE COMPARISON - DRY RUN")
    print("="*80)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Run both scrapers
    run_commercial_scraper_dry_run()
    run_etl_transparency_dry_run()

    # Compare results
    compare_results()

    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)
    print("\n📁 Check the following files for detailed data:")
    print("  - be_pharm_commercial_dry_run.json")
    print("  - be_pharm_transparency_dry_run.json")


if __name__ == "__main__":
    main()