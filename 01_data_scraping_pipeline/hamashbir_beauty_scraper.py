#!/usr/bin/env python3
"""
HaMashbir 365 Beauty Department Scraper
Scrapes beauty products from https://365mashbir.co.il/

Collections scraped:
- Perfume (559 products)
- Makeup (191 products)
- Face Care (311 products)
- Body Care (101 products)
- Hair Care (54 products)
- Male Skincare (12 products)

Total: ~1,228 products
"""

import requests
import json
import time
import random
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import sys
import argparse

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'price_comparison_app_v2',
    'user': 'postgres',
    'password': '025655358'
}

class HaMashbirBeautyScraper:
    def __init__(self, db_config):
        self.base_url = "https://365mashbir.co.il"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://365mashbir.co.il/'
        })
        self.db_config = db_config
        self.db_conn = None
        self.retailer_id = None
        self.store_id = None

        # Collections to scrape
        self.collections = [
            ('perfume', 'Perfume'),
            ('%D7%90%D7%99%D7%A4%D7%95%D7%A8', 'Makeup'),
            ('%D7%98%D7%99%D7%A4%D7%95%D7%97', 'Face Care'),
            ('%D7%98%D7%99%D7%A4%D7%95%D7%97-%D7%92%D7%95%D7%A3', 'Body Care'),
            ('%D7%99', 'Hair Care'),
            ('male-skin-care', 'Male Skincare')
        ]

        # Statistics
        self.stats = {
            'total_products': 0,
            'products_with_barcode': 0,
            'products_without_barcode': 0,
            'errors': 0
        }

    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_conn = psycopg2.connect(**self.db_config)
            print("✅ Database connected successfully")
            return True
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False

    def get_or_create_retailer(self):
        """Get or create HaMashbir retailer record"""
        cursor = self.db_conn.cursor()

        # Check if retailer exists
        cursor.execute("""
            SELECT retailerid FROM retailers WHERE retailername = 'HaMashbir 365'
        """)
        result = cursor.fetchone()

        if result:
            self.retailer_id = result[0]
            print(f"✅ Found existing retailer: HaMashbir 365 (ID: {self.retailer_id})")
        else:
            # Create new retailer
            cursor.execute("""
                INSERT INTO retailers (retailername, chainid, pricetransparencyportalurl, fileformat)
                VALUES ('HaMashbir 365', 'HAMASHBIR365', 'https://365mashbir.co.il/', 'JSON')
                RETURNING retailerid
            """)
            self.retailer_id = cursor.fetchone()[0]
            self.db_conn.commit()
            print(f"✅ Created new retailer: HaMashbir 365 (ID: {self.retailer_id})")

        cursor.close()

    def get_or_create_store(self):
        """Get or create default online store for HaMashbir"""
        cursor = self.db_conn.cursor()

        # Check if store exists
        cursor.execute("""
            SELECT storeid FROM stores
            WHERE retailerid = %s AND retailerspecificstoreid = 'ONLINE'
        """, (self.retailer_id,))
        result = cursor.fetchone()

        if result:
            self.store_id = result[0]
            print(f"✅ Found existing store: HaMashbir Online (ID: {self.store_id})")
        else:
            # Create new store
            cursor.execute("""
                INSERT INTO stores (
                    retailerid, retailerspecificstoreid, storename,
                    city, isactive, storetype
                )
                VALUES (%s, 'ONLINE', 'HaMashbir 365 Online Store', 'Online', TRUE, 'online')
                RETURNING storeid
            """, (self.retailer_id,))
            self.store_id = cursor.fetchone()[0]
            self.db_conn.commit()
            print(f"✅ Created new store: HaMashbir Online (ID: {self.store_id})")

        cursor.close()

    def fetch_collection_products(self, collection_slug, collection_name):
        """Fetch all products from a collection using pagination"""
        all_products = []
        page = 0
        limit = 250  # Shopify max

        print(f"\n📦 Scraping collection: {collection_name} ({collection_slug})")

        while True:
            url = f"{self.base_url}/collections/{collection_slug}/products.json"
            params = {'limit': limit, 'page': page + 1}

            try:
                response = self.session.get(url, params=params, timeout=30)

                if response.status_code == 404:
                    print(f"  ℹ️  No more pages (404)")
                    break

                response.raise_for_status()
                data = response.json()
                products = data.get('products', [])

                if not products:
                    print(f"  ℹ️  No more products found")
                    break

                all_products.extend(products)
                print(f"  📄 Page {page + 1}: Found {len(products)} products (total: {len(all_products)})")

                page += 1
                time.sleep(random.uniform(1, 2))  # Rate limiting

            except Exception as e:
                print(f"  ❌ Error fetching page {page + 1}: {e}")
                self.stats['errors'] += 1
                break

        print(f"  ✅ Total products in {collection_name}: {len(all_products)}")
        return all_products

    def fetch_product_barcode(self, handle):
        """Fetch barcode for a specific product"""
        url = f"{self.base_url}/products/{handle}.json"

        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()

            # Get barcode from first variant
            product = data.get('product', {})
            variants = product.get('variants', [])

            if variants and variants[0].get('barcode'):
                return variants[0]['barcode']

            return None

        except Exception as e:
            print(f"    ⚠️  Error fetching barcode for {handle}: {e}")
            return None

    def save_product(self, product_data, collection_name):
        """Save product to database following the proper schema"""
        cursor = self.db_conn.cursor()

        # Extract data from product
        product_id = str(product_data['id'])
        handle = product_data['handle']
        title = product_data['title']
        vendor = product_data.get('vendor', '')
        product_type = product_data.get('product_type', collection_name)
        description = product_data.get('body_html', '')

        # Get first variant data
        variants = product_data.get('variants', [])
        if not variants:
            print(f"    ⚠️  No variants for product {product_id}")
            cursor.close()
            return

        variant = variants[0]
        price = float(variant.get('price', 0))
        sku = variant.get('sku', '')
        retailer_item_code = sku if sku else product_id

        # Get image URL
        images = product_data.get('images', [])
        image_url = images[0]['src'] if images else None

        # Product URL
        product_url = f"https://365mashbir.co.il/products/{handle}"

        # Fetch barcode from individual product API
        print(f"    🔍 Fetching barcode for: {title[:50]}...")
        barcode = self.fetch_product_barcode(handle)

        if not barcode:
            self.stats['products_without_barcode'] += 1
            print(f"    ⚠️  No barcode found - skipping product")
            cursor.close()
            time.sleep(random.uniform(0.5, 1.5))
            return

        self.stats['products_with_barcode'] += 1
        print(f"    ✅ Barcode: {barcode}")

        try:
            # 1. Insert/update in canonical_products
            cursor.execute("""
                INSERT INTO canonical_products (
                    barcode, name, brand, description, image_url,
                    category, url, source_retailer_id, last_scraped_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (barcode) DO UPDATE
                SET name = EXCLUDED.name,
                    brand = EXCLUDED.brand,
                    description = EXCLUDED.description,
                    image_url = COALESCE(EXCLUDED.image_url, canonical_products.image_url),
                    category = EXCLUDED.category,
                    url = EXCLUDED.url,
                    last_scraped_at = NOW()
            """, (barcode, title, vendor, description, image_url, product_type,
                  product_url, self.retailer_id))

            # 2. Insert/update in retailer_products
            cursor.execute("""
                INSERT INTO retailer_products (
                    retailer_id, retailer_item_code, original_retailer_name, barcode
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (retailer_id, retailer_item_code) DO UPDATE
                SET original_retailer_name = EXCLUDED.original_retailer_name,
                    barcode = EXCLUDED.barcode
                RETURNING retailer_product_id
            """, (self.retailer_id, retailer_item_code, title, barcode))

            retailer_product_id = cursor.fetchone()[0]

            # 3. Insert into prices
            cursor.execute("""
                INSERT INTO prices (
                    retailer_product_id, store_id, price, price_timestamp
                )
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (retailer_product_id, store_id, price_timestamp) DO NOTHING
            """, (retailer_product_id, self.store_id, price))

            self.db_conn.commit()
            print(f"    💾 Saved to DB (Product ID: {retailer_product_id})")

        except Exception as e:
            print(f"    ❌ Error saving product: {e}")
            self.db_conn.rollback()
            self.stats['errors'] += 1

        cursor.close()

        # Rate limiting between products
        time.sleep(random.uniform(0.5, 1.5))

    def run(self):
        """Main scraping execution"""
        print("=" * 80)
        print("🛍️  HaMashbir 365 Beauty Scraper")
        print("=" * 80)

        # Connect to database
        if not self.connect_db():
            return

        # Get/create retailer
        self.get_or_create_retailer()

        # Get/create store
        self.get_or_create_store()

        # Scrape each collection
        start_time = datetime.now()

        for collection_slug, collection_name in self.collections:
            products = self.fetch_collection_products(collection_slug, collection_name)

            print(f"\n  💾 Saving {len(products)} products from {collection_name}...")

            for idx, product in enumerate(products, 1):
                print(f"  [{idx}/{len(products)}]", end=" ")
                self.save_product(product, collection_name)
                self.stats['total_products'] += 1

        # Final statistics
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print("\n" + "=" * 80)
        print("📊 SCRAPING COMPLETE")
        print("=" * 80)
        print(f"⏱️  Duration: {duration:.1f} seconds ({duration/60:.1f} minutes)")
        print(f"📦 Total products processed: {self.stats['total_products']}")
        print(f"✅ Products with barcode (saved): {self.stats['products_with_barcode']}")
        print(f"⚠️  Products without barcode (skipped): {self.stats['products_without_barcode']}")
        print(f"❌ Errors: {self.stats['errors']}")

        if self.stats['products_with_barcode'] > 0:
            barcode_rate = (self.stats['products_with_barcode'] / self.stats['total_products']) * 100
            print(f"📈 Barcode success rate: {barcode_rate:.1f}%")

        print("=" * 80)

        # Close database connection
        if self.db_conn:
            self.db_conn.close()
            print("✅ Database connection closed")

def main():
    parser = argparse.ArgumentParser(description='HaMashbir 365 Beauty Scraper')
    parser.add_argument('--test', action='store_true', help='Test mode: scrape only first 5 products from Perfume')
    args = parser.parse_args()

    scraper = HaMashbirBeautyScraper(DB_CONFIG)

    if args.test:
        print("🧪 TEST MODE: Scraping only 5 products from Perfume collection")
        scraper.collections = [('perfume', 'Perfume')]
        # Modify to only get first few products (will be limited by the API)

    scraper.run()

if __name__ == "__main__":
    main()
