#!/usr/bin/env python3
"""
Super-Pharm Portal ETL - Working version with correct XML parsing
"""

import os
import sys
import gzip
import logging
import argparse
import psycopg2
import re
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'price_comparison_app_v2',
    'user': 'postgres',
    'password': '***REMOVED***'
}

SUPER_PHARM_CHAIN_ID = '7290172900007'

class SuperPharmETL:
    def __init__(self, days_back: int = 7):
        self.days_back = days_back
        self.conn = None
        self.cur = None
        self.retailer_id = 52  # Super-Pharm's ID
        self.stats = {
            'files_processed': 0,
            'products_processed': 0,
            'products_created': 0,
            'prices_inserted': 0,
            'stores_created': 0,
            'errors': 0
        }

    def connect_db(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.cur = self.conn.cursor()
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            sys.exit(1)

    def get_or_create_store(self, store_id: str) -> int:
        """Get or create store"""
        try:
            self.cur.execute("""
                SELECT storeid FROM stores
                WHERE retailerid = %s AND retailerspecificstoreid = %s
            """, (self.retailer_id, store_id))

            result = self.cur.fetchone()
            if result:
                return result[0]

            # Create new store
            self.cur.execute("""
                INSERT INTO stores (retailerid, retailerspecificstoreid, storename, isactive)
                VALUES (%s, %s, %s, true)
                RETURNING storeid
            """, (self.retailer_id, store_id, f"Super-Pharm Store {store_id}"))

            new_store_id = self.cur.fetchone()[0]
            self.stats['stores_created'] += 1
            return new_store_id

        except Exception as e:
            logger.error(f"Error with store {store_id}: {e}")
            raise

    def parse_price_file(self, content: bytes, store_id: str) -> List[Dict]:
        """Parse Super-Pharm's new XML format"""
        products = []

        try:
            root = ET.fromstring(content)

            # Try different paths for products
            paths = [
                './/Details/Line',  # New format
                './/Product',       # Old format
                './/Item',         # Alternative
                './/Products/Product'
            ]

            items = None
            for path in paths:
                items = root.findall(path)
                if items:
                    logger.debug(f"Found {len(items)} items using path: {path}")
                    break

            if not items:
                logger.warning("No products found in file")
                return products

            for item in items:
                try:
                    # Extract data - try multiple field names
                    barcode = (item.findtext('ItemCode') or
                              item.findtext('Barcode') or
                              item.findtext('ProductCode') or '').strip()

                    if not barcode or not barcode.isdigit():
                        continue

                    name = (item.findtext('ItemName') or
                           item.findtext('ProductName') or
                           item.findtext('ManufacturerItemDescription') or '').strip()

                    # Price might be in different fields
                    price_str = (item.findtext('ItemPrice') or
                                item.findtext('Price') or
                                item.findtext('UnitPrice') or '0').strip()

                    # If no direct price, calculate from quantity
                    if price_str == '0' or not price_str:
                        qty = item.findtext('Quantity', '0').strip()
                        unit = item.findtext('UnitOfMeasure', '0').strip()
                        if qty and unit and float(qty) > 0:
                            try:
                                price_str = str(float(unit) / float(qty))
                            except:
                                pass

                    try:
                        price = float(price_str)
                    except:
                        continue

                    if price > 0 and name:
                        products.append({
                            'barcode': barcode,
                            'name': name,
                            'price': price,
                            'item_code': barcode  # Use barcode as item code
                        })

                except Exception as e:
                    logger.debug(f"Error parsing item: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error parsing XML: {e}")

        return products

    def process_file(self, url: str, filename: str) -> int:
        """Download and process a price file"""
        try:
            # Extract store ID from filename
            match = re.search(r'-(\d{3})-', filename)
            if not match:
                return 0

            store_id = match.group(1)
            db_store_id = self.get_or_create_store(store_id)

            # Download file
            logger.info(f"Downloading {filename}...")
            response = requests.get(url, timeout=60)
            response.raise_for_status()

            # Decompress
            content = gzip.decompress(response.content)

            # Parse products
            products = self.parse_price_file(content, store_id)
            logger.info(f"  Found {len(products)} products")

            # Process each product
            for product in products:
                try:
                    # Upsert canonical product
                    self.cur.execute("""
                        INSERT INTO canonical_products (barcode, name, is_active)
                        VALUES (%s, %s, false)
                        ON CONFLICT (barcode) DO NOTHING
                    """, (product['barcode'], product['name']))

                    if self.cur.rowcount > 0:
                        self.stats['products_created'] += 1

                    # Upsert retailer product
                    self.cur.execute("""
                        INSERT INTO retailer_products (
                            retailer_id, retailer_item_code,
                            original_retailer_name, barcode
                        ) VALUES (%s, %s, %s, %s)
                        ON CONFLICT (retailer_id, retailer_item_code)
                        DO UPDATE SET
                            original_retailer_name = EXCLUDED.original_retailer_name,
                            barcode = EXCLUDED.barcode
                        RETURNING retailer_product_id
                    """, (self.retailer_id, product['item_code'],
                          product['name'], product['barcode']))

                    retailer_product_id = self.cur.fetchone()[0]

                    # Insert price
                    self.cur.execute("""
                        INSERT INTO prices (
                            retailer_product_id, store_id, price,
                            price_timestamp, scraped_at
                        ) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (retailer_product_id, db_store_id, product['price'],
                          datetime.now(), datetime.now()))

                    if self.cur.rowcount > 0:
                        self.stats['prices_inserted'] += 1

                    self.stats['products_processed'] += 1

                except Exception as e:
                    logger.debug(f"Error processing product: {e}")
                    self.stats['errors'] += 1

            # Commit after each file
            self.conn.commit()
            self.stats['files_processed'] += 1

            return len(products)

        except Exception as e:
            logger.error(f"Error processing {filename}: {e}")
            self.conn.rollback()
            return 0

    def discover_files(self) -> List[Dict]:
        """Discover available files from Super-Pharm portal"""
        files = []

        try:
            base_url = "https://prices.super-pharm.co.il"

            # Get files from multiple pages
            for page in range(1, 20):  # Check first 20 pages
                url = f"{base_url}/?page={page}" if page > 1 else base_url

                response = requests.get(url, timeout=30)
                soup = BeautifulSoup(response.content, 'html.parser')

                page_files = []
                for link in soup.find_all('a', href=True):
                    href = link['href']

                    # Look for price files
                    if ('.gz' in href and
                        ('Price' in href or 'price' in href) and
                        'Download' in href):

                        # Extract filename
                        match = re.search(r'(Price[^?]+\.gz)', href)
                        if match:
                            filename = match.group(1)

                            # Check date
                            date_match = re.search(r'(\d{8})', filename)
                            if date_match:
                                file_date = datetime.strptime(date_match.group(1), '%Y%m%d')
                                if (datetime.now() - file_date).days <= self.days_back:
                                    # Build full URL
                                    full_url = base_url + href if href.startswith('/') else href

                                    files.append({
                                        'url': full_url,
                                        'filename': filename
                                    })
                                    page_files.append(filename)

                if page_files:
                    logger.info(f"Page {page}: Found {len(page_files)} files")
                else:
                    break  # No more files

        except Exception as e:
            logger.error(f"Error discovering files: {e}")

        # Remove duplicates
        seen = set()
        unique_files = []
        for f in files:
            if f['filename'] not in seen:
                seen.add(f['filename'])
                unique_files.append(f)

        return unique_files

    def run(self):
        """Main ETL execution"""
        logger.info("="*60)
        logger.info("SUPER-PHARM ETL - WORKING VERSION")
        logger.info("="*60)
        logger.info(f"Processing last {self.days_back} days")

        self.connect_db()

        try:
            # Discover files
            files = self.discover_files()
            logger.info(f"Found {len(files)} files to process")

            if not files:
                logger.warning("No files found!")
                return

            # Process each file
            for i, file_info in enumerate(files, 1):
                logger.info(f"\n[{i}/{len(files)}] Processing {file_info['filename']}")
                products_count = self.process_file(file_info['url'], file_info['filename'])

                if i % 5 == 0:
                    self._print_stats()

            # Final stats
            logger.info("\n" + "="*60)
            logger.info("ETL COMPLETE")
            logger.info("="*60)
            self._print_stats()

        except Exception as e:
            logger.error(f"ETL failed: {e}")
            self.conn.rollback()

        finally:
            if self.conn:
                self.conn.close()

    def _print_stats(self):
        """Print current statistics"""
        for key, value in self.stats.items():
            logger.info(f"  {key}: {value:,}")

def main():
    parser = argparse.ArgumentParser(description='Super-Pharm Working ETL')
    parser.add_argument('--days', type=int, default=1,
                       help='Number of days back to process')
    args = parser.parse_args()

    etl = SuperPharmETL(days_back=args.days)
    etl.run()

if __name__ == "__main__":
    main()