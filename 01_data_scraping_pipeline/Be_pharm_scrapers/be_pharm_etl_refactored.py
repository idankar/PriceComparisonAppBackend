#!/usr/bin/env python3
"""
Be Pharm ETL Pipeline - Refactored Version
Downloads files from Shufersal portal and filters for Be Pharm (ChainId 7290027600007, SubChainId 005)
Uses batch processing for optimal performance
"""

import os
import re
import sys
import gzip
import logging
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import xml.etree.ElementTree as ET

import requests
import psycopg2
from psycopg2.extras import execute_values, Json
from urllib3.exceptions import InsecureRequestWarning
from bs4 import BeautifulSoup

# Suppress SSL warnings
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('be_pharm_etl_refactored.log')
    ]
)
logger = logging.getLogger(__name__)


class BePharmETL:
    def __init__(self, days_back: int = 30):
        """Initialize Be Pharm ETL with database connection and configuration"""

        # Be Pharm specific identifiers
        self.BE_CHAIN_ID = '7290027600007'
        self.RETAILER_ID = 150

        # Configuration
        self.days_back = days_back
        self.batch_size = 1000  # Batch size for database operations

        # Database connection
        try:
            self.conn = psycopg2.connect(
                host="localhost",
                port=5432,
                database="price_comparison_app_v2",
                user="postgres",
                password="***REMOVED***"
            )
            self.cursor = self.conn.cursor()
            logger.info("Connected to database successfully")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

        # Create temp directory for downloads
        self.temp_dir = tempfile.mkdtemp(prefix='be_pharm_etl_')
        logger.info(f"Created temp directory: {self.temp_dir}")

        # Statistics tracking
        self.stats = {
            'files_downloaded': 0,
            'files_checked': 0,
            'be_pharm_files': 0,
            'files_discarded': 0,
            'files_skipped': 0,
            'products_processed': 0,
            'prices_inserted': 0,
            'stores_created': 0,
            'batch_inserts': 0,
            'errors': 0
        }

        # Track processed files
        self.processed_files = set()

        # Ensure filesprocessed table exists
        self.ensure_filesprocessed_table()

        # Load previously processed files
        self.load_processed_files()

        # Ensure Be Pharm stores exist
        self.ensure_be_pharm_stores()

    def ensure_filesprocessed_table(self):
        """Verify filesprocessed table exists"""
        try:
            # Table already exists in the database with proper schema
            # Just verify it's accessible
            self.cursor.execute("""
                SELECT COUNT(*) FROM filesprocessed WHERE retailerid = %s LIMIT 1
            """, (self.RETAILER_ID,))
            self.cursor.fetchone()
            logger.info("Verified filesprocessed table exists")
        except Exception as e:
            logger.error(f"Error accessing filesprocessed table: {e}")
            self.conn.rollback()

    def load_processed_files(self):
        """Load list of previously processed files to avoid reprocessing"""
        try:
            self.cursor.execute("""
                SELECT filename
                FROM filesprocessed
                WHERE retailerid = %s
            """, (self.RETAILER_ID,))

            for row in self.cursor.fetchall():
                self.processed_files.add(row[0])

            logger.info(f"Loaded {len(self.processed_files)} previously processed files")
        except Exception as e:
            logger.error(f"Error loading processed files: {e}")

    def ensure_be_pharm_stores(self):
        """Ensure Be Pharm stores exist in database from known store IDs"""
        # Known Be Pharm store IDs from Shufersal portal
        known_stores = [
            ('001', 'BE ראשי'),
            ('026', 'BE בלוך גבעתיים'),
            ('041', 'BE דיזנגוף סנטר'),
            ('112', 'BE קרית מוצקין'),
            ('145', 'BE נתיבות'),
            ('172', 'BE באר יעקב'),
            ('178', 'BE סגולה פתח תקווה'),
            ('201', 'BE בריגה'),
            ('233', 'BE באר שבע'),
            ('242', 'BE ויוה חדרה'),
            ('252', 'BE עיןשמר'),
            ('641', 'BE טייבה'),
            ('676', 'BE נהריה'),
            ('765', 'BE נתניה'),
            ('781', 'BE כפר סבא'),
            ('790', 'BE ראש העין'),
            ('854', 'BE רמת גן'),
        ]

        stores_created = 0
        for store_id, store_name in known_stores:
            try:
                self.cursor.execute("""
                    INSERT INTO stores (retailerid, retailerspecificstoreid, storename, isactive)
                    VALUES (%s, %s, %s, true)
                    ON CONFLICT (retailerid, retailerspecificstoreid)
                    DO UPDATE SET storename = EXCLUDED.storename
                    RETURNING storeid
                """, (self.RETAILER_ID, store_id, store_name))

                if self.cursor.fetchone():
                    stores_created += 1
            except Exception as e:
                logger.error(f"Error creating store {store_id}: {e}")
                self.conn.rollback()

        self.conn.commit()
        if stores_created > 0:
            logger.info(f"Created/updated {stores_created} Be Pharm stores")

    def get_and_download_be_pharm_files(self) -> List[Tuple[str, Dict]]:
        """
        Discover AND immediately download Be Pharm files from Shufersal portal.
        Returns list of (filepath, metadata) tuples for Be Pharm files only.
        This avoids URL expiration issues by downloading immediately upon discovery.
        """
        be_pharm_files = []
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

        # Calculate date cutoff for filtering old files
        cutoff_date = datetime.now() - timedelta(days=self.days_back)
        logger.info(f"Searching for files from last {self.days_back} days (since {cutoff_date.strftime('%Y-%m-%d')})")

        # Shufersal portal base URL (Be Pharm files are here)
        base_url = 'https://prices.shufersal.co.il/'

        # Categories to search (using numeric IDs)
        # 2 = PricesFull, 4 = PromosFull
        categories = [
            ('2', 'PricesFull'),
            ('4', 'PromosFull')
        ]

        for category_id, category_name in categories:
            logger.info(f"Searching category: {category_name}")
            page = 1
            max_pages = 20  # Reasonable limit to avoid infinite loops
            consecutive_empty = 0

            while page <= max_pages:
                try:
                    # Build URL for file listing page
                    url = f"{base_url}FileObject/UpdateCategory"
                    params = {
                        'catID': category_id,
                        'storeId': '0',
                        'page': str(page)
                    }

                    response = session.get(url, params=params, timeout=30, verify=False)

                    if response.status_code != 200:
                        logger.warning(f"Page {page} returned status {response.status_code}")
                        break

                    # Parse HTML response to find file links
                    soup = BeautifulSoup(response.text, 'html.parser')

                    # Find all links to .gz files
                    links = soup.find_all('a', href=True)
                    file_list = []

                    for link in links:
                        href = link.get('href', '')
                        # Look for Azure blob storage URLs with .gz extension
                        if '.gz' in href and ('PriceFull' in href or 'PromoFull' in href):
                            # Extract filename from the URL
                            filename_match = re.search(r'(PriceFull|PromoFull)[\w-]+\.gz', href)
                            if filename_match:
                                filename = filename_match.group(0)
                                # Store full URL as it contains Azure blob signature
                                file_list.append({
                                    'FileName': filename,
                                    'FileUrl': href
                                })

                    if file_list:
                        logger.info(f"Page {page}: Found {len(file_list)} files in HTML")

                    if not file_list:
                        consecutive_empty += 1
                        if consecutive_empty >= 3:
                            logger.info(f"No more files found after page {page}")
                            break
                    else:
                        consecutive_empty = 0

                    # Process each file in the listing
                    page_files_added = 0
                    for file_info in file_list:
                        # Extract filename and URL
                        if isinstance(file_info, dict):
                            filename = file_info.get('FileName', '')
                            file_url = file_info.get('FileUrl', '')
                        else:
                            continue

                        # Only process .gz and .xml data files
                        if not (filename.endswith('.gz') or filename.endswith('.xml')):
                            continue

                        # OPTIMIZATION 1: Skip if already processed
                        if filename in self.processed_files:
                            self.stats['files_skipped'] += 1
                            continue

                        # OPTIMIZATION 2: Quick check if it's a Be Pharm file by filename pattern
                        # Be Pharm files contain the chain ID 7290027600007 in the filename
                        if self.BE_CHAIN_ID not in filename:
                            # Not a Be Pharm file, skip without downloading
                            self.stats['files_discarded'] += 1
                            continue

                        # Extract date from filename if possible
                        date_match = re.search(r'-(\d{8})(\d{4})?', filename)
                        if date_match:
                            try:
                                file_date = datetime.strptime(date_match.group(1), '%Y%m%d')
                                # Skip files older than cutoff
                                if file_date < cutoff_date:
                                    continue
                            except:
                                pass  # If date parsing fails, include the file

                        # URLs from HTML should already be complete Azure blob URLs
                        # Download immediately to avoid URL expiration
                        try:
                            logger.debug(f"Downloading: {filename}")
                            download_response = session.get(file_url, timeout=60, verify=False)
                            download_response.raise_for_status()

                            self.stats['files_downloaded'] += 1

                            # Save to temp directory
                            local_path = os.path.join(self.temp_dir, filename)
                            with open(local_path, 'wb') as f:
                                f.write(download_response.content)

                            # Check if it's a Be Pharm file
                            is_be_pharm, metadata = self.check_be_pharm_file(local_path)

                            if is_be_pharm:
                                logger.info(f"✓ BE PHARM FILE: {filename} (Store: {metadata.get('store_id', 'Unknown')}, SubChain: {metadata.get('subchain_id', 'Unknown')})")
                                self.stats['be_pharm_files'] += 1
                                metadata['filename'] = filename
                                metadata['category'] = category_name
                                be_pharm_files.append((local_path, metadata))
                                page_files_added += 1
                            else:
                                logger.debug(f"✗ Not Be Pharm: {filename}")
                                self.stats['files_discarded'] += 1
                                # Clean up non-Be Pharm file
                                try:
                                    os.remove(local_path)
                                except:
                                    pass

                        except Exception as e:
                            logger.error(f"Error downloading {filename}: {e}")
                            self.stats['errors'] += 1

                    if page_files_added > 0:
                        logger.info(f"Page {page}: Found {page_files_added} potential files")

                    page += 1

                except Exception as e:
                    logger.error(f"Error fetching page {page} of category {category_name}: {e}")
                    break

        logger.info(f"Total Be Pharm files downloaded: {len(be_pharm_files)} (skipped {self.stats['files_skipped']} already processed)")
        logger.info(f"Files discarded (not Be Pharm): {self.stats['files_discarded']}")
        return be_pharm_files

    def get_shufersal_portal_files(self) -> List[Dict]:
        """
        Legacy method kept for compatibility.
        Now replaced by get_and_download_be_pharm_files() which downloads immediately.
        """
        logger.warning("get_shufersal_portal_files is deprecated. Use get_and_download_be_pharm_files instead.")
        return []

    def download_and_check_file(self, file_info: Dict) -> Optional[Tuple[str, Dict]]:
        """
        Download file and check if it's a Be Pharm file by examining ChainId and SubChainId.
        Returns (filepath, metadata) if it's a Be Pharm file, None otherwise.
        """
        session = requests.Session()

        try:
            # Download file
            logger.debug(f"Downloading: {file_info['name']}")
            response = session.get(file_info['url'], timeout=60, verify=False)
            response.raise_for_status()

            self.stats['files_downloaded'] += 1

            # Save to temp directory
            local_path = os.path.join(self.temp_dir, file_info['name'])
            with open(local_path, 'wb') as f:
                f.write(response.content)

            # Check if it's a Be Pharm file
            is_be_pharm, metadata = self.check_be_pharm_file(local_path)

            if is_be_pharm:
                logger.info(f"✓ BE PHARM FILE: {file_info['name']} (Store: {metadata.get('store_id', 'Unknown')}, SubChain: {metadata.get('subchain_id', 'Unknown')})")
                self.stats['be_pharm_files'] += 1
                return local_path, metadata
            else:
                logger.debug(f"✗ Not Be Pharm: {file_info['name']}")
                self.stats['files_discarded'] += 1
                # Clean up non-Be Pharm file
                try:
                    os.remove(local_path)
                except:
                    pass
                return None

        except Exception as e:
            logger.error(f"Error downloading {file_info['name']}: {e}")
            self.stats['errors'] += 1
            return None

    def check_be_pharm_file(self, filepath: str) -> Tuple[bool, Dict]:
        """
        Check if file contains Be Pharm ChainId and SubChainId.
        Returns (is_be_pharm, metadata) tuple.
        """
        metadata = {}

        try:
            # Read file content (handle .gz files)
            if filepath.endswith('.gz'):
                with gzip.open(filepath, 'rt', encoding='utf-8') as f:
                    # Read first 2KB to check identifiers
                    content = f.read(2048)
            else:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read(2048)

            self.stats['files_checked'] += 1

            # Check for Be Pharm ChainId
            if f'<ChainId>{self.BE_CHAIN_ID}</ChainId>' not in content:
                return False, metadata

            # Check for correct SubChainId (5 for Be Pharm)
            subchain_match = re.search(r'<SubChainId>(\d+)</SubChainId>', content)
            if subchain_match:
                subchain_id = subchain_match.group(1)
                metadata['subchain_id'] = subchain_id
                # Be Pharm is ONLY SubChainId 005
                if subchain_id != '005':
                    return False, metadata

            # Extract store ID
            store_match = re.search(r'<StoreId>(\d+)</StoreId>', content)
            if store_match:
                metadata['store_id'] = store_match.group(1)

            # It's a Be Pharm file!
            return True, metadata

        except Exception as e:
            logger.error(f"Error checking file {filepath}: {e}")
            return False, metadata

    def parse_be_pharm_file(self, filepath: str) -> List[Dict]:
        """Parse Be Pharm XML file and extract product data"""
        products = []

        try:
            # Parse XML file
            if filepath.endswith('.gz'):
                with gzip.open(filepath, 'rt', encoding='utf-8') as f:
                    tree = ET.parse(f)
            else:
                tree = ET.parse(filepath)

            root = tree.getroot()

            # Get store ID from XML
            store_elem = root.find('.//StoreId')
            store_id = store_elem.text if store_elem is not None else None

            if not store_id:
                logger.warning(f"No store ID found in {filepath}")
                return products

            # Parse all items
            items = root.findall('.//Item')

            for item in items:
                try:
                    product = {'store_id': store_id}

                    # Extract product fields
                    item_code = item.find('ItemCode')
                    if item_code is not None and item_code.text:
                        product['item_code'] = item_code.text.strip()
                    else:
                        continue  # Skip items without item code

                    # Product name
                    item_name = item.find('ItemName')
                    if item_name is not None and item_name.text:
                        product['name'] = item_name.text.strip()
                    else:
                        item_desc = item.find('ManufacturerItemDescription')
                        if item_desc is not None and item_desc.text:
                            product['name'] = item_desc.text.strip()
                        else:
                            product['name'] = f"Product {product['item_code']}"

                    # Price
                    price_elem = item.find('ItemPrice')
                    if price_elem is not None and price_elem.text:
                        try:
                            product['price'] = float(price_elem.text.strip())
                        except:
                            product['price'] = 0.0
                    else:
                        product['price'] = 0.0

                    # Manufacturer
                    manufacturer = item.find('ManufacturerName')
                    if manufacturer is not None and manufacturer.text:
                        product['manufacturer'] = manufacturer.text.strip()

                    # Barcode (if item code is a valid barcode)
                    if product['item_code'].isdigit() and 8 <= len(product['item_code']) <= 13:
                        product['barcode'] = product['item_code']

                    # Price update date
                    price_date = item.find('PriceUpdateDate')
                    if price_date is not None and price_date.text:
                        product['price_date'] = price_date.text.strip()

                    products.append(product)

                except Exception as e:
                    logger.debug(f"Error parsing item: {e}")
                    continue

            logger.info(f"Parsed {len(products)} products from {os.path.basename(filepath)}")

        except Exception as e:
            logger.error(f"Error parsing file {filepath}: {e}")

        return products

    def process_product_batch(self, products: List[Dict], filename: str):
        """
        Process a batch of products using barcode-first matching strategy.
        This ensures ONE product entry per barcode across all retailers.
        """
        if not products:
            return

        try:
            # Get store IDs mapping
            store_mapping = {}
            unique_stores = set(p['store_id'] for p in products if 'store_id' in p)

            for store_id in unique_stores:
                self.cursor.execute("""
                    SELECT storeid FROM stores
                    WHERE retailerid = %s AND retailerspecificstoreid = %s
                """, (self.RETAILER_ID, store_id))

                result = self.cursor.fetchone()
                if result:
                    store_mapping[store_id] = result[0]
                else:
                    # Create store if it doesn't exist
                    self.cursor.execute("""
                        INSERT INTO stores (retailerid, retailerspecificstoreid, storename, isactive)
                        VALUES (%s, %s, %s, true)
                        ON CONFLICT (retailerid, retailerspecificstoreid) DO NOTHING
                        RETURNING storeid
                    """, (self.RETAILER_ID, store_id, f"Be Pharm Store {store_id}"))

                    result = self.cursor.fetchone()
                    if result:
                        store_mapping[store_id] = result[0]
                        self.stats['stores_created'] += 1

            # BARCODE-ONLY MATCHING STRATEGY
            # For each product, check if a product with this barcode exists
            for product in products:
                if 'item_code' not in product:
                    continue

                barcode = product.get('barcode')
                product_id = None

                # Only process products that have a valid barcode
                if barcode:
                    # Step 1: UPSERT to canonical_products table
                    # Search for existing product by barcode
                    self.cursor.execute("""
                        SELECT id, canonical_name, brand
                        FROM canonical_products
                        WHERE barcode = %s
                        LIMIT 1
                    """, (barcode,))

                    existing_product = self.cursor.fetchone()

                    if existing_product:
                        # Product with this barcode exists - use it!
                        product_id = existing_product[0]
                        logger.debug(f"Found existing product for barcode {barcode}: product_id={product_id}")
                    else:
                        # No existing product with this barcode - create ONE canonical entry (INACTIVE until commercial scraper finds it)
                        self.cursor.execute("""
                            INSERT INTO canonical_products (barcode, canonical_name, brand, category, image_url, is_active)
                            VALUES (%s, %s, %s, %s, %s, FALSE)
                            ON CONFLICT (barcode) DO NOTHING
                            RETURNING id
                        """, (
                            barcode,
                            product.get('name', f"Product {product['item_code']}"),
                            product.get('manufacturer', ''),
                            product.get('category'),
                            product.get('image_url')
                        ))

                        result = self.cursor.fetchone()
                        if result:
                            product_id = result[0]
                            logger.debug(f"Created new product for barcode {barcode}: product_id={product_id}")
                else:
                    # No barcode - skip this product entirely
                    logger.debug(f"Skipping product without barcode: {product.get('name', 'Unknown')} (item_code: {product['item_code']})")
                    continue  # Skip to next product

                # Store product_id for later use
                if product_id:
                    product['product_id'] = product_id

            # Step 2: UPSERT to retailer_products table
            # Prepare batch data for retailer_products (now with product_id)
            retailer_products_data = []
            for product in products:
                if 'item_code' in product and 'product_id' in product:
                    retailer_products_data.append((
                        product['product_id'],
                        self.RETAILER_ID,
                        product['item_code'],
                        product.get('name', '')
                    ))

            # Batch insert/update retailer_products
            if retailer_products_data:
                # Perform the upsert to create retailer-product link
                execute_values(
                    self.cursor,
                    """
                    INSERT INTO retailer_products (product_id, retailer_id, retailer_item_code, original_retailer_name)
                    VALUES %s
                    ON CONFLICT (retailer_id, retailer_item_code)
                    DO UPDATE SET
                        product_id = EXCLUDED.product_id,
                        original_retailer_name = EXCLUDED.original_retailer_name
                    """,
                    retailer_products_data,
                    template="(%s, %s, %s, %s)"
                )

                # THE FIX: Now query for ALL retailer_product_ids from the batch to build complete map
                item_codes_in_batch = [p[2] for p in retailer_products_data]  # p[2] is retailer_item_code
                self.cursor.execute(
                    """
                    SELECT retailer_product_id, retailer_item_code
                    FROM retailer_products
                    WHERE retailer_id = %s AND retailer_item_code = ANY(%s)
                    """,
                    (self.RETAILER_ID, item_codes_in_batch)
                )

                # Build the complete mapping from the query result
                product_id_mapping = {row[1]: row[0] for row in self.cursor.fetchall()}

                # Step 3: INSERT into prices table
                # Prepare batch data for prices
                prices_data = []
                for product in products:
                    if 'item_code' in product and product['item_code'] in product_id_mapping:
                        store_id = store_mapping.get(product.get('store_id'))
                        if store_id:
                            # Parse price timestamp
                            price_timestamp = datetime.now()
                            if 'price_date' in product:
                                try:
                                    price_timestamp = datetime.strptime(
                                        product['price_date'],
                                        '%Y-%m-%d %H:%M'
                                    )
                                except:
                                    pass

                            prices_data.append((
                                product_id_mapping[product['item_code']],
                                store_id,
                                product.get('price', 0.0),
                                price_timestamp
                            ))

                # Batch insert prices
                if prices_data:
                    execute_values(
                        self.cursor,
                        """
                        INSERT INTO prices (retailer_product_id, store_id, price, price_timestamp)
                        VALUES %s
                        ON CONFLICT (retailer_product_id, store_id, price_timestamp, scraped_at)
                        DO UPDATE SET price = EXCLUDED.price
                        """,
                        prices_data,
                        template="(%s, %s, %s, %s)"
                    )

                    self.stats['prices_inserted'] += len(prices_data)

                self.stats['products_processed'] += len(products)
                self.stats['batch_inserts'] += 1

            # Record file as processed
            self.cursor.execute("""
                INSERT INTO filesprocessed (
                    retailerid,
                    filename,
                    filetype,
                    rowsadded,
                    processingstatus,
                    processingendtime
                )
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (retailerid, filename)
                DO UPDATE SET
                    rowsadded = EXCLUDED.rowsadded,
                    processingstatus = EXCLUDED.processingstatus,
                    processingendtime = NOW(),
                    updated_at = NOW()
            """, (
                self.RETAILER_ID,
                filename,
                'XML',
                len(products),
                'SUCCESS'
            ))

            # Commit the batch
            self.conn.commit()
            logger.info(f"Batch processed: {len(products)} products, {len(prices_data) if prices_data else 0} prices")

        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            self.conn.rollback()
            self.stats['errors'] += 1

    def process_promotion_file(self, filepath: str, filename: str):
        """
        Process Be Pharm promotion file
        Be Pharm uses a normalized format with <Promotions> containing <Promotion> blocks
        """
        try:
            logger.info(f"Processing Be Pharm promotion file: {filename}")

            # Read and decompress if needed
            with open(filepath, 'rb') as f:
                content = f.read()

            # Try to decompress if gzipped
            try:
                if filepath.endswith('.gz'):
                    content = gzip.decompress(content)
            except gzip.BadGzipFile:
                pass

            # Parse XML
            root = ET.fromstring(content)

            # Track statistics
            promotions_processed = 0
            links_created = 0

            # Be Pharm uses normalized format: look for <Promotion> elements
            promotions = root.findall('.//Promotion')

            if promotions:
                # Process normalized format (like Good Pharm)
                logger.info(f"Found {len(promotions)} promotions in {filename}")

                for promo in promotions:
                    try:
                        # Extract promotion details
                        promo_id = promo.find('PromotionId')
                        promo_desc = promo.find('PromotionDescription')
                        start_date = promo.find('PromotionStartDate')
                        end_date = promo.find('PromotionEndDate')
                        min_qty = promo.find('MinQty')
                        discounted_price = promo.find('DiscountedPrice')
                        discount_rate = promo.find('DiscountRate')
                        discount_type = promo.find('DiscountType')
                        reward_type = promo.find('RewardType')
                        remarks = promo.find('Remarks')

                        if promo_id is None or promo_id.text is None:
                            continue

                        retailer_promo_code = promo_id.text.strip()

                        # Prepare promotion data
                        promo_data = {
                            'retailer_id': self.RETAILER_ID,
                            'retailer_promotion_code': retailer_promo_code,
                            'description': promo_desc.text.strip() if promo_desc is not None and promo_desc.text else None,
                            'start_date': start_date.text.strip() if start_date is not None and start_date.text else None,
                            'end_date': end_date.text.strip() if end_date is not None and end_date.text else None,
                            'min_quantity': float(min_qty.text.strip()) if min_qty is not None and min_qty.text else None,
                            'discounted_price': float(discounted_price.text.strip()) if discounted_price is not None and discounted_price.text else None,
                            'discount_rate': float(discount_rate.text.strip()) if discount_rate is not None and discount_rate.text else None,
                            'discount_type': int(discount_type.text.strip()) if discount_type is not None and discount_type.text else None,
                            'reward_type': int(reward_type.text.strip()) if reward_type is not None and reward_type.text else None,
                            'remarks': remarks.text.strip() if remarks is not None and remarks.text else None
                        }

                        # Upsert promotion and get promotion_id
                        self.cursor.execute("""
                            INSERT INTO promotions (
                                retailer_id, retailer_promotion_code, description,
                                start_date, end_date, min_quantity, discounted_price,
                                discount_rate, discount_type, reward_type, remarks
                            )
                            VALUES (%(retailer_id)s, %(retailer_promotion_code)s, %(description)s,
                                    %(start_date)s, %(end_date)s, %(min_quantity)s, %(discounted_price)s,
                                    %(discount_rate)s, %(discount_type)s, %(reward_type)s, %(remarks)s)
                            ON CONFLICT (retailer_id, retailer_promotion_code)
                            DO UPDATE SET
                                description = EXCLUDED.description,
                                start_date = EXCLUDED.start_date,
                                end_date = EXCLUDED.end_date,
                                min_quantity = EXCLUDED.min_quantity,
                                discounted_price = EXCLUDED.discounted_price,
                                discount_rate = EXCLUDED.discount_rate,
                                discount_type = EXCLUDED.discount_type,
                                reward_type = EXCLUDED.reward_type,
                                remarks = EXCLUDED.remarks
                            RETURNING promotion_id
                        """, promo_data)

                        result = self.cursor.fetchone()
                        if not result:
                            continue

                        db_promotion_id = result[0]
                        promotions_processed += 1

                        # Find PromotionItems
                        promo_items = promo.find('PromotionItems')
                        if promo_items is not None:
                            items = promo_items.findall('Item')

                            # Batch process item links
                            item_codes = []
                            for item in items:
                                item_code_elem = item.find('ItemCode')
                                if item_code_elem is not None and item_code_elem.text:
                                    item_codes.append(item_code_elem.text.strip())

                            if item_codes:
                                # Find retailer_product_ids for these item codes
                                self.cursor.execute("""
                                    SELECT retailer_product_id, retailer_item_code
                                    FROM retailer_products
                                    WHERE retailer_id = %s AND retailer_item_code = ANY(%s)
                                """, (self.RETAILER_ID, item_codes))

                                product_mappings = {row[1]: row[0] for row in self.cursor.fetchall()}

                                # Prepare batch insert data
                                links_data = []
                                for item_code in item_codes:
                                    if item_code in product_mappings:
                                        links_data.append((db_promotion_id, product_mappings[item_code]))

                                # Batch insert links
                                if links_data:
                                    execute_values(
                                        self.cursor,
                                        """
                                        INSERT INTO promotion_product_links (promotion_id, retailer_product_id)
                                        VALUES %s
                                        ON CONFLICT (promotion_id, retailer_product_id) DO NOTHING
                                        """,
                                        links_data,
                                        template="(%s, %s)"
                                    )
                                    links_created += len(links_data)

                    except Exception as e:
                        logger.error(f"Error processing promotion {retailer_promo_code if 'retailer_promo_code' in locals() else 'unknown'}: {e}")
                        continue

                # Commit the batch
                self.conn.commit()

            else:
                # If no Promotion elements found, try the denormalized format as fallback
                logger.warning(f"No <Promotion> elements found in {filename}, trying denormalized format")
                # Fall back to denormalized processing
                promotions_to_process = {}
                promotion_items = {}
                items = root.findall('.//Item') or root.findall('.//Line')
                logger.info(f"Found {len(items)} items in {filename}")

                for item in items:
                    try:
                        # Extract promotion ID (may be in different locations)
                        promo_id = None
                        promo_desc = None

                        # Try different paths for promotion data
                        if item.find('PromotionId') is not None:
                            promo_id = item.find('PromotionId')
                            promo_desc = item.find('PromotionDescription')
                        elif item.find('PromotionDetails') is not None:
                            promo_details = item.find('PromotionDetails')
                            promo_id = promo_details.find('PromotionId')
                            promo_desc = promo_details.find('PromotionDescription')

                        if promo_id is None or promo_id.text is None:
                            continue

                        retailer_promo_code = promo_id.text.strip()

                        # Extract item code
                        item_code_elem = item.find('ItemCode')
                        if item_code_elem is not None and item_code_elem.text:
                            item_code = item_code_elem.text.strip()

                            # Add item to promotion's item set
                            if retailer_promo_code not in promotion_items:
                                promotion_items[retailer_promo_code] = set()
                            promotion_items[retailer_promo_code].add(item_code)

                        # If we haven't seen this promotion yet, store its details
                        if retailer_promo_code not in promotions_to_process:
                            # Extract additional promotion fields (if available)
                            start_date = item.find('PromotionStartDate')
                            end_date = item.find('PromotionEndDate')
                            min_qty = item.find('MinQty')
                            discounted_price = item.find('DiscountedPrice')
                            discount_rate = item.find('DiscountRate')
                            discount_type = item.find('DiscountType')
                            reward_type = item.find('RewardType')
                            remarks = item.find('Remarks')

                            promotions_to_process[retailer_promo_code] = {
                                'retailer_id': self.RETAILER_ID,
                                'retailer_promotion_code': retailer_promo_code,
                                'description': promo_desc.text.strip() if promo_desc is not None and promo_desc.text else None,
                                'start_date': start_date.text.strip() if start_date is not None and start_date.text else None,
                                'end_date': end_date.text.strip() if end_date is not None and end_date.text else None,
                                'min_quantity': float(min_qty.text.strip()) if min_qty is not None and min_qty.text else None,
                                'discounted_price': float(discounted_price.text.strip()) if discounted_price is not None and discounted_price.text else None,
                                'discount_rate': float(discount_rate.text.strip()) if discount_rate is not None and discount_rate.text else None,
                                'discount_type': int(discount_type.text.strip()) if discount_type is not None and discount_type.text else None,
                                'reward_type': int(reward_type.text.strip()) if reward_type is not None and reward_type.text else None,
                                'remarks': remarks.text.strip() if remarks is not None and remarks.text else None
                            }

                    except Exception as e:
                        logger.debug(f"Error processing promotion item: {e}")
                        continue

                # Stage 2: Batch insert promotions and links for denormalized format
                promo_id_mapping = {}  # retailer_promo_code -> db_promotion_id

                # Insert/update all unique promotions
                for retailer_promo_code, promo_data in promotions_to_process.items():
                    try:
                        self.cursor.execute("""
                        INSERT INTO promotions (
                            retailer_id, retailer_promotion_code, description,
                            start_date, end_date, min_quantity, discounted_price,
                            discount_rate, discount_type, reward_type, remarks
                        )
                        VALUES (%(retailer_id)s, %(retailer_promotion_code)s, %(description)s,
                                %(start_date)s, %(end_date)s, %(min_quantity)s, %(discounted_price)s,
                                %(discount_rate)s, %(discount_type)s, %(reward_type)s, %(remarks)s)
                        ON CONFLICT (retailer_id, retailer_promotion_code)
                        DO UPDATE SET
                            description = EXCLUDED.description,
                            start_date = EXCLUDED.start_date,
                            end_date = EXCLUDED.end_date,
                            min_quantity = EXCLUDED.min_quantity,
                            discounted_price = EXCLUDED.discounted_price,
                            discount_rate = EXCLUDED.discount_rate,
                            discount_type = EXCLUDED.discount_type,
                            reward_type = EXCLUDED.reward_type,
                            remarks = EXCLUDED.remarks
                        RETURNING promotion_id
                        """, promo_data)

                        result = self.cursor.fetchone()
                        if result:
                            promo_id_mapping[retailer_promo_code] = result[0]
                            promotions_processed += 1

                    except Exception as e:
                        logger.error(f"Error inserting promotion {retailer_promo_code}: {e}")

                # Now create all the product links
                all_links = []
                for retailer_promo_code, item_codes in promotion_items.items():
                    if retailer_promo_code in promo_id_mapping:
                        db_promotion_id = promo_id_mapping[retailer_promo_code]

                        # Find retailer_product_ids for these item codes
                        item_codes_list = list(item_codes)
                        self.cursor.execute("""
                            SELECT retailer_product_id, retailer_item_code
                            FROM retailer_products
                            WHERE retailer_id = %s AND retailer_item_code = ANY(%s)
                        """, (self.RETAILER_ID, item_codes_list))

                        for row in self.cursor.fetchall():
                            all_links.append((db_promotion_id, row[0]))

                # Batch insert all links
                if all_links:
                    execute_values(
                        self.cursor,
                        """
                        INSERT INTO promotion_product_links (promotion_id, retailer_product_id)
                        VALUES %s
                        ON CONFLICT (promotion_id, retailer_product_id) DO NOTHING
                        """,
                        all_links,
                        template="(%s, %s)"
                    )
                    links_created = len(all_links)

                # Commit the batch
                self.conn.commit()

            # Classify promotion types based on product count (when promotion_type column is available)
            # TODO: Uncomment when promotion_type column is added to database
            # for retailer_promo_code, db_promotion_id in promo_id_mapping.items():
            #     product_count = len(promotion_items.get(retailer_promo_code, []))
            #     if product_count > 1000:
            #         promotion_type = 'Store-Wide'
            #     elif product_count > 100:
            #         promotion_type = 'Category-Wide'
            #     else:
            #         promotion_type = 'Targeted'
            #
            #     self.cursor.execute("""
            #         UPDATE promotions
            #         SET promotion_type = %s
            #         WHERE promotion_id = %s
            #     """, (promotion_type, db_promotion_id))
            #
            # self.conn.commit()

            # Update stats
            if not hasattr(self.stats, 'promotions_processed'):
                self.stats['promotions_processed'] = 0
                self.stats['promotion_links_created'] = 0

            self.stats['promotions_processed'] += promotions_processed
            self.stats['promotion_links_created'] += links_created

            logger.info(f"Processed {promotions_processed} unique promotions with {links_created} product links from {filename}")

        except Exception as e:
            logger.error(f"Error processing promotion file {filepath}: {e}")
            self.conn.rollback()
            self.stats['errors'] += 1

    def run(self, limit: Optional[int] = None):
        """Main ETL execution

        Args:
            limit: Optional limit on number of BE PHARM files to process (for testing)
        """
        logger.info("="*80)
        logger.info("BE PHARM ETL - REFACTORED VERSION")
        logger.info("="*80)
        logger.info(f"Configuration:")
        logger.info(f"  Chain ID: {self.BE_CHAIN_ID}")
        logger.info(f"  Accepting all SubChain IDs for this chain")
        logger.info(f"  Days to process: {self.days_back}")
        logger.info(f"  Batch size: {self.batch_size}")
        logger.info("="*80)

        try:
            # Step 1: Fetch AND download Be Pharm files immediately to avoid URL expiration
            logger.info("Step 1: Fetching and downloading Be Pharm files from Shufersal portal...")
            be_pharm_files = self.get_and_download_be_pharm_files()

            if not be_pharm_files:
                logger.warning("No Be Pharm files found to process")
                return

            # Step 2: Process downloaded Be Pharm files
            logger.info(f"Step 2: Processing {len(be_pharm_files)} Be Pharm files...")
            if limit:
                logger.info(f"Will stop after processing {limit} Be Pharm files.")

            be_pharm_processed = 0
            for i, (filepath, metadata) in enumerate(be_pharm_files, 1):
                # Check if we've hit the limit for Be Pharm files
                if limit and be_pharm_processed >= limit:
                    logger.info(f"Reached limit of {limit} Be Pharm files. Stopping.")
                    break

                if i % 10 == 0:
                    logger.info(f"Progress: {i}/{len(be_pharm_files)} files processed")

                filename = metadata.get('filename', os.path.basename(filepath))

                # Check if it's a promotion file or price file
                if 'promofull' in filename.lower():
                    # Process promotion file
                    self.process_promotion_file(filepath, filename)
                    be_pharm_processed += 1
                else:
                    # Parse price file
                    products = self.parse_be_pharm_file(filepath)

                    if products:
                        # Process batch of products
                        self.process_product_batch(products, filename)
                        be_pharm_processed += 1

                # Clean up processed file
                try:
                    os.remove(filepath)
                except:
                    pass

            # Step 3: Print summary
            self.print_summary()

        except Exception as e:
            logger.error(f"Fatal error in ETL run: {e}")
            raise
        finally:
            self.cleanup()

    def print_summary(self):
        """Print ETL execution summary"""
        logger.info("="*80)
        logger.info("BE PHARM ETL SUMMARY")
        logger.info("="*80)
        logger.info(f"Files downloaded: {self.stats['files_downloaded']}")
        logger.info(f"Files checked: {self.stats['files_checked']}")
        logger.info(f"Be Pharm files found: {self.stats['be_pharm_files']}")
        logger.info(f"Files discarded (not Be Pharm): {self.stats['files_discarded']}")
        logger.info(f"Files skipped (already processed): {self.stats['files_skipped']}")
        logger.info(f"Products processed: {self.stats['products_processed']}")
        logger.info(f"Prices inserted: {self.stats['prices_inserted']}")
        logger.info(f"Stores created: {self.stats['stores_created']}")
        logger.info(f"Promotions processed: {self.stats.get('promotions_processed', 0)}")
        logger.info(f"Promotion-product links created: {self.stats.get('promotion_links_created', 0)}")
        logger.info(f"Batch inserts performed: {self.stats['batch_inserts']}")
        logger.info(f"Errors encountered: {self.stats['errors']}")

        # Query final database counts
        try:
            self.cursor.execute("""
                SELECT
                    COUNT(DISTINCT rp.retailer_product_id) as products,
                    COUNT(DISTINCT p.price_id) as prices,
                    COUNT(DISTINCT p.store_id) as stores
                FROM retailer_products rp
                LEFT JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
                WHERE rp.retailer_id = %s
            """, (self.RETAILER_ID,))

            result = self.cursor.fetchone()
            if result:
                logger.info("="*80)
                logger.info("DATABASE TOTALS FOR BE PHARM:")
                logger.info(f"  Total products: {result[0]:,}")
                logger.info(f"  Total prices: {result[1]:,}")
                logger.info(f"  Stores with data: {result[2]}")
        except:
            pass

        logger.info("="*80)

    def cleanup(self):
        """Clean up resources"""
        # Remove temp directory
        try:
            import shutil
            shutil.rmtree(self.temp_dir)
            logger.info(f"Cleaned up temp directory: {self.temp_dir}")
        except:
            pass

        # Close database connection
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            logger.info("Closed database connection")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Be Pharm ETL - Refactored Version")
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days of historical data to process (default: 30)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of BE PHARM files to process (for testing)"
    )

    args = parser.parse_args()

    try:
        etl = BePharmETL(days_back=args.days)
        etl.run(limit=args.limit)
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        sys.exit(1)