#!/usr/bin/env python3
"""
Clean version of Good Pharm ETL with proper error handling and reduced warnings.
"""

import os
import re
import sys
import json
import gzip
import logging
import tempfile
import warnings
from datetime import datetime, timedelta
from typing import Dict, List
import xml.etree.ElementTree as ET

import requests
import psycopg2
from psycopg2.extras import Json, RealDictCursor
from bs4 import BeautifulSoup

# Suppress SSL warnings
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GoodPharmETL:
    def __init__(self, days_back: int = 30):
        """Initialize Good Pharm ETL pipeline"""
        self.days_back = days_back

        # Database connection with RealDictCursor
        self.conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="price_comparison_app_v2",
            user="postgres",
            password="025655358"
        )
        # Use RealDictCursor for dictionary-style access
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)

        self.retailer_id = 97  # Good Pharm retailer ID
        self.chain_id = '7290058197699'

        # Statistics
        self.stats = {
            'products': 0,
            'prices': 0,
            'stores': set(),
            'errors': 0
        }

        # Create temp directory for downloads
        self.temp_dir = tempfile.mkdtemp(prefix='good_pharm_etl_')
        logger.info(f"Created temp directory: {self.temp_dir}")

    def extract_store_id(self, store_str):
        """Extract numeric store ID from Good Pharm store string"""
        if not store_str:
            return None

        # If it's already just a number, return it
        if isinstance(store_str, (int, float)):
            return str(int(store_str))

        store_str = str(store_str)
        if store_str.isdigit():
            return store_str

        # Extract first sequence of digits
        match = re.match(r'(\d+)', store_str.strip())
        return match.group(1) if match else None

    def get_good_pharm_files(self) -> List[Dict]:
        """Get list of files from Good Pharm transparency portal"""
        files = []
        base_url = 'https://goodpharm.binaprojects.com/'

        try:
            # Set date range
            from_date = (datetime.now() - timedelta(days=self.days_back)).strftime('%d/%m/%Y')
            to_date = datetime.now().strftime('%d/%m/%Y')
            logger.info(f"Fetching Good Pharm files from {from_date} to {to_date}")

            # Request files
            params = {
                'sType': '1',
                'sFName': '',
                'sSName': '0',
                'sFromDate': from_date,
                'sToDate': to_date,
                'iCheck': 'false'
            }

            response = requests.post(
                base_url + 'MainIO_Hok.aspx',
                data=params,
                timeout=30,
                verify=False
            )
            response.raise_for_status()

            # Parse response
            try:
                data = response.json()
                for item in data[:100]:  # Limit to 100 files for testing
                    filename = item.get('FileNm', '')
                    if filename:
                        store_id = self.extract_store_id(item.get('Store'))

                        files.append({
                            'name': filename,
                            'url': base_url + 'Download.aspx?FileNm=' + filename,
                            'store_id': store_id,
                            'type': 'price'
                        })

            except json.JSONDecodeError:
                logger.error("Failed to parse Good Pharm response as JSON")

            logger.info(f"Found {len(files)} Good Pharm files")

        except Exception as e:
            logger.error(f"Error fetching Good Pharm files: {e}")

        return files

    def download_file(self, url: str, filename: str) -> str:
        """Download file to temp directory"""
        filepath = os.path.join(self.temp_dir, filename)

        try:
            response = requests.get(url, timeout=60, verify=False)
            response.raise_for_status()

            # Check if response is JSON with redirect
            try:
                data = response.json()
                if isinstance(data, list) and len(data) > 0 and 'SPath' in data[0]:
                    actual_url = data[0]['SPath']
                    response = requests.get(actual_url, timeout=60, verify=False)
                    response.raise_for_status()
            except:
                pass

            with open(filepath, 'wb') as f:
                f.write(response.content)

            return filepath

        except Exception as e:
            logger.error(f"Error downloading {filename}: {e}")
            return None

    def parse_xml_file(self, filepath: str, store_id: str = None) -> List[Dict]:
        """Parse XML file"""
        products = []

        try:
            # Read file content
            with open(filepath, 'rb') as f:
                content = f.read()

            # Try to decompress if gzipped
            if content[:2] == b'\x1f\x8b':
                content = gzip.decompress(content)

            # Parse XML
            root = ET.fromstring(content)

            # Find items
            items = root.findall('.//Item')

            for item in items:
                product = {}

                if store_id:
                    product['store_id'] = store_id

                for child in item:
                    tag = child.tag
                    text = child.text

                    if text:
                        if tag in ['ItemCode', 'ItemId']:
                            product['item_code'] = text.strip()
                            # Check for barcode
                            if text.strip().isdigit() and 8 <= len(text.strip()) <= 13:
                                product['barcode'] = text.strip()
                        elif tag in ['ItemName', 'ItemNm', 'ManufacturerItemDescription']:
                            product['name'] = text.strip()
                        elif tag in ['ItemPrice', 'Price']:
                            try:
                                product['price'] = float(text.strip())
                            except:
                                pass
                        elif tag in ['ManufacturerName']:
                            product['manufacturer'] = text.strip()

                if product.get('name') or product.get('item_code'):
                    products.append(product)

        except Exception as e:
            logger.error(f"Error parsing {filepath}: {e}")

        return products

    def process_products(self, products: List[Dict]):
        """Process and insert products into database"""
        for product in products:
            try:
                # Extract barcode
                barcode = product.get('barcode')

                # Product info
                product_name = product.get('name', product.get('item_code', 'Unknown'))
                brand = product.get('manufacturer', '')

                # Insert product
                self.cursor.execute("""
                    INSERT INTO products (canonical_name, brand, attributes, image_source)
                    VALUES (%(name)s, %(brand)s, %(attrs)s, 'transparency')
                    ON CONFLICT (lower(canonical_name), lower(brand)) DO UPDATE
                    SET attributes = COALESCE(products.attributes, EXCLUDED.attributes),
                        updated_at = NOW()
                    RETURNING product_id
                """, {
                    'name': product_name,
                    'brand': brand,
                    'attrs': Json({'barcode': barcode}) if barcode else Json({})
                })

                result = self.cursor.fetchone()
                if not result:
                    continue

                product_id = result['product_id']

                # Insert retailer product
                self.cursor.execute("""
                    INSERT INTO retailer_products (
                        retailer_id, product_id, retailer_item_code,
                        original_retailer_name
                    ) VALUES (%(retailer_id)s, %(product_id)s, %(item_code)s, %(name)s)
                    ON CONFLICT (retailer_id, retailer_item_code) DO UPDATE
                    SET product_id = EXCLUDED.product_id,
                        original_retailer_name = EXCLUDED.original_retailer_name
                    RETURNING retailer_product_id
                """, {
                    'retailer_id': self.retailer_id,
                    'product_id': product_id,
                    'item_code': product.get('item_code', ''),
                    'name': product.get('name', '')
                })

                result = self.cursor.fetchone()
                if not result:
                    continue

                retailer_product_id = result['retailer_product_id']
                self.stats['products'] += 1

                # Insert price if available
                if product.get('price') and product.get('store_id'):
                    store_id = product['store_id']

                    # Get actual store ID from database
                    self.cursor.execute("""
                        SELECT storeid FROM stores
                        WHERE retailerid = %(retailer_id)s
                        AND retailerspecificstoreid = %(store_id)s
                    """, {
                        'retailer_id': self.retailer_id,
                        'store_id': str(store_id)
                    })

                    store_result = self.cursor.fetchone()
                    if store_result:
                        actual_store_id = store_result['storeid']

                        # Insert price
                        self.cursor.execute("""
                            INSERT INTO prices (
                                retailer_product_id, store_id, price,
                                price_timestamp, scraped_at
                            ) VALUES (%(rp_id)s, %(store_id)s, %(price)s, %(timestamp)s, %(scraped)s)
                            ON CONFLICT (retailer_product_id, store_id, price_timestamp)
                            DO UPDATE SET
                                price = EXCLUDED.price,
                                scraped_at = EXCLUDED.scraped_at
                        """, {
                            'rp_id': retailer_product_id,
                            'store_id': actual_store_id,
                            'price': product['price'],
                            'timestamp': datetime.now(),
                            'scraped': datetime.now()
                        })

                        self.stats['prices'] += 1
                        self.stats['stores'].add(store_id)

            except Exception as e:
                logger.debug(f"Error processing product: {e}")
                self.stats['errors'] += 1
                self.conn.rollback()
                continue

        # Commit batch
        try:
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error committing batch: {e}")
            self.conn.rollback()

    def run(self):
        """Run the ETL process"""
        logger.info("Starting Good Pharm ETL...")

        # Get files
        files = self.get_good_pharm_files()

        if not files:
            logger.warning("No files found")
            return

        # Process files
        for i, file_info in enumerate(files, 1):
            if i % 10 == 0:
                logger.info(f"Progress: {i}/{len(files)} files, "
                          f"{self.stats['products']} products, "
                          f"{self.stats['prices']} prices")

            # Download file
            filepath = self.download_file(file_info['url'], file_info['name'])
            if not filepath:
                continue

            # Parse file
            products = self.parse_xml_file(filepath, file_info.get('store_id'))

            # Process products
            if products:
                self.process_products(products)

            # Clean up
            try:
                os.remove(filepath)
            except:
                pass

        # Final summary
        logger.info(f"\nFINAL SUMMARY:")
        logger.info(f"  Products: {self.stats['products']:,}")
        logger.info(f"  Prices: {self.stats['prices']:,}")
        logger.info(f"  Stores: {len(self.stats['stores'])}")
        logger.info(f"  Errors: {self.stats['errors']:,}")

    def cleanup(self):
        """Clean up resources"""
        try:
            import shutil
            shutil.rmtree(self.temp_dir)
        except:
            pass

        if self.conn:
            self.conn.close()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Good Pharm ETL Pipeline')
    parser.add_argument('--days-back', type=int, default=30,
                       help='Number of days of historical data (default: 30)')
    args = parser.parse_args()

    etl = GoodPharmETL(days_back=args.days_back)

    try:
        etl.run()
    finally:
        etl.cleanup()