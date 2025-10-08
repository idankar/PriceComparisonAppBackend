#!/usr/bin/env python3
"""
Super-Pharm ETL Pipeline with Barcode-First Matching Strategy
Based on the successful Be Pharm implementation
"""

import os
import re
import sys
import gzip
import logging
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from urllib.parse import urljoin
import xml.etree.ElementTree as ET

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psycopg2
from psycopg2.extras import execute_values, Json
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('super_pharm_barcode_matching.log')
    ]
)
logger = logging.getLogger(__name__)


class SuperPharmBarcodeETL:
    def __init__(self, days_back: int = 30):
        """Initialize Super-Pharm ETL with barcode-first matching strategy

        Args:
            days_back: Number of days of historical data to process (default 30)
        """
        # Super-Pharm identifiers
        self.BASE_URL = "https://prices.super-pharm.co.il/"
        self.RETAILER_ID = 52  # Super-Pharm retailer ID
        self.CHAIN_ID = '7290172900007'

        # Configuration
        self.days_back = days_back
        self.batch_size = 1000

        # Setup session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

        # Database connection
        try:
            self.conn = psycopg2.connect(
                host="localhost",
                port=5432,
                database="price_comparison_app_v2",
                user="postgres",
                password="025655358"
            )
            self.cursor = self.conn.cursor()
            logger.info("Connected to database successfully")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

        # Create temp directory for downloads
        self.temp_dir = tempfile.mkdtemp(prefix='super_pharm_barcode_etl_')
        logger.info(f"Created temp directory: {self.temp_dir}")

        # Statistics tracking
        self.stats = {
            'files_downloaded': 0,
            'files_processed': 0,
            'products_with_barcode': 0,
            'products_without_barcode': 0,
            'products_matched_existing': 0,
            'products_created_new': 0,
            'prices_inserted': 0,
            'stores_created': 0,
            'batch_inserts': 0,
            'errors': 0
        }

        # Track processed files
        self.processed_files = set()

        # Load previously processed files
        self.load_processed_files()

        # Load Super-Pharm stores
        self.ensure_super_pharm_stores()

    def load_processed_files(self):
        """Load list of previously processed files to avoid reprocessing"""
        try:
            self.cursor.execute("""
                SELECT filename
                FROM filesprocessed
                WHERE retailerid = %s
                AND processingstatus = 'SUCCESS'
            """, (self.RETAILER_ID,))

            for row in self.cursor.fetchall():
                self.processed_files.add(row[0])

            logger.info(f"Loaded {len(self.processed_files)} previously processed files")
        except Exception as e:
            logger.error(f"Error loading processed files: {e}")

    def ensure_super_pharm_stores(self):
        """Load stores from the official Super-Pharm stores file if available"""
        stores_file = "/Users/noa/Downloads/StoresFull7290172900007-000-202509180700"

        if os.path.exists(stores_file):
            logger.info(f"Loading stores from official XML: {stores_file}")
            try:
                with open(stores_file, 'rb') as f:
                    content = f.read()
                self.process_store_file(content, "StoresFull7290172900007-000-202509180700")
                logger.info("Successfully loaded stores from official XML")
            except Exception as e:
                logger.error(f"Error loading stores from XML: {e}")
        else:
            logger.info("Stores XML file not found, stores will be created as needed")

    def process_store_file(self, content: bytes, filename: str):
        """Process a store file"""
        try:
            # Parse XML
            root = ET.fromstring(content)
            stores = root.findall('.//Store')

            logger.info(f"Found {len(stores)} stores in {filename}")

            stores_created = 0
            for store in stores:
                # Super-Pharm uses StoreID (capital ID) not StoreId
                store_id = store.find('StoreID')
                store_name = store.find('StoreName')
                address = store.find('Address')
                city = store.find('City')

                if store_id is not None:
                    store_id_val = store_id.text

                    # Insert or update store
                    self.cursor.execute("""
                        INSERT INTO stores (
                            retailerid, retailerspecificstoreid,
                            storename, address, city, isactive
                        ) VALUES (%s, %s, %s, %s, %s, true)
                        ON CONFLICT (retailerid, retailerspecificstoreid) DO UPDATE SET
                            storename = EXCLUDED.storename,
                            address = EXCLUDED.address,
                            city = EXCLUDED.city,
                            updatedat = NOW()
                        RETURNING storeid
                    """, (
                        self.RETAILER_ID,
                        store_id_val,
                        store_name.text if store_name is not None else '',
                        address.text if address is not None else '',
                        city.text if city is not None else ''
                    ))

                    if self.cursor.fetchone():
                        stores_created += 1

            self.conn.commit()
            if stores_created > 0:
                self.stats['stores_created'] = stores_created
                logger.info(f"Created/updated {stores_created} Super-Pharm stores")

        except Exception as e:
            logger.error(f"Error processing store file {filename}: {e}")
            self.stats['errors'] += 1

    def fetch_file_list(self) -> List[Dict]:
        """Fetch list of all files from Super-Pharm portal"""
        all_files = []
        seen_files = set()

        # Calculate date cutoff
        cutoff_date = datetime.now() - timedelta(days=self.days_back)
        logger.info(f"Fetching files from last {self.days_back} days (since {cutoff_date.strftime('%Y-%m-%d')})")

        try:
            page = 1
            consecutive_failures = 0
            max_consecutive_failures = 5
            max_pages = 96  # Super-Pharm has many pages

            while consecutive_failures < max_consecutive_failures and page <= max_pages:
                try:
                    logger.debug(f"Fetching page {page} from {self.BASE_URL}")

                    if page == 1:
                        page_url = self.BASE_URL
                    else:
                        page_url = f"{self.BASE_URL}?page={page}"

                    response = self.session.get(page_url, timeout=60, verify=False)
                    response.raise_for_status()

                    soup = BeautifulSoup(response.content, 'html.parser')
                    file_links = soup.find_all('a', href=True)

                    page_files = []
                    for link in file_links:
                        href = link.get('href', '')
                        # Check if href contains file extensions
                        if any(ext in href for ext in ['.gz', '.zip', '.xml']):
                            file_url = urljoin(self.BASE_URL, href)
                            filename = href.split('/')[-1].split('?')[0]

                            # Skip if already seen or processed
                            if filename in seen_files or filename in self.processed_files:
                                continue

                            # Extract date from filename if possible
                            date_match = re.search(r'-(\d{8})(\d{4})?\.', filename)
                            if date_match:
                                try:
                                    file_date = datetime.strptime(date_match.group(1), '%Y%m%d')
                                    # Skip files older than cutoff
                                    if file_date < cutoff_date:
                                        continue
                                except ValueError:
                                    pass

                            seen_files.add(filename)
                            file_info = {
                                'filename': filename,
                                'url': file_url,
                                'type': self.classify_file_type(filename)
                            }
                            page_files.append(file_info)

                    if not page_files:
                        consecutive_failures += 1
                    else:
                        all_files.extend(page_files)
                        consecutive_failures = 0
                        logger.debug(f"Found {len(page_files)} files on page {page}")

                    page += 1

                except requests.RequestException as e:
                    consecutive_failures += 1
                    logger.error(f"Error fetching page {page}: {e}")
                    page += 1

        except Exception as e:
            logger.error(f"Error during file discovery: {e}")

        logger.info(f"Total files collected: {len(all_files)}")
        return all_files

    def classify_file_type(self, filename: str) -> str:
        """Classify file type based on filename"""
        filename_lower = filename.lower()

        if 'store' in filename_lower:
            return 'store'
        elif 'promo' in filename_lower:
            return 'promotion'
        elif 'price' in filename_lower:
            return 'price'
        else:
            return 'unknown'

    def download_and_extract(self, file_url: str, filename: str) -> Optional[str]:
        """Download and extract file to temp directory"""
        filepath = os.path.join(self.temp_dir, filename)

        try:
            response = self.session.get(file_url, timeout=120, verify=False)
            response.raise_for_status()

            # Save to file
            with open(filepath, 'wb') as f:
                f.write(response.content)

            self.stats['files_downloaded'] += 1
            logger.debug(f"Downloaded: {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"Error downloading {file_url}: {e}")
            self.stats['errors'] += 1
            return None

    def parse_price_file(self, filepath: str) -> List[Dict]:
        """Parse a price file and extract product data"""
        products = []

        try:
            # Read and decompress if needed
            with open(filepath, 'rb') as f:
                content = f.read()

            # Try to decompress if it's gzipped
            try:
                if filepath.endswith('.gz'):
                    content = gzip.decompress(content)
            except gzip.BadGzipFile:
                pass  # Not gzipped

            # Parse XML
            root = ET.fromstring(content)

            # Extract store ID from filename
            filename = os.path.basename(filepath)
            store_match = re.search(r'7290172900007-(\d+)-', filename)
            file_store_id = store_match.group(1) if store_match else None

            if not file_store_id:
                logger.warning(f"Could not extract store ID from filename: {filename}")
                return products

            # Super-Pharm uses Line elements
            items = root.findall('.//Line')

            logger.info(f"Found {len(items)} price items in {filename}")

            for item in items:
                product = {'store_id': file_store_id}

                # Extract item information
                item_code = item.find('ItemCode')
                item_name = item.find('ItemName')
                price_elem = item.find('ItemPrice')
                manufacturer = item.find('ManufacturerName')
                barcode_elem = item.find('Barcode')  # Some files might have explicit barcode

                if item_code is not None and item_code.text:
                    product['item_code'] = item_code.text.strip()

                    # In Super-Pharm files, ItemCode is often the barcode
                    # Check if it's a valid barcode (8-13 digits)
                    if product['item_code'].isdigit() and 8 <= len(product['item_code']) <= 13:
                        product['barcode'] = product['item_code']
                    elif barcode_elem is not None and barcode_elem.text:
                        # Use explicit barcode if available
                        barcode_text = barcode_elem.text.strip()
                        if barcode_text.isdigit() and 8 <= len(barcode_text) <= 13:
                            product['barcode'] = barcode_text

                if item_name is not None and item_name.text:
                    product['name'] = item_name.text.strip()

                if price_elem is not None and price_elem.text:
                    try:
                        product['price'] = float(price_elem.text.strip())
                    except (ValueError, TypeError):
                        pass

                if manufacturer is not None and manufacturer.text:
                    product['manufacturer'] = manufacturer.text.strip()

                # Only add products with essential data
                if product.get('item_code') and product.get('name'):
                    products.append(product)

            logger.info(f"Parsed {len(products)} products from {filename}")

        except Exception as e:
            logger.error(f"Error parsing file {filepath}: {e}")
            self.stats['errors'] += 1

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
                    """, (self.RETAILER_ID, store_id, f"Super-Pharm Store {store_id}"))

                    result = self.cursor.fetchone()
                    if result:
                        store_mapping[store_id] = result[0]
                        self.stats['stores_created'] += 1

            # BARCODE-FIRST MATCHING STRATEGY
            for product in products:
                if 'item_code' not in product:
                    continue

                barcode = product.get('barcode')
                product_id = None

                # Only process products that have a valid barcode
                if barcode:
                    self.stats['products_with_barcode'] += 1

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
                        self.stats['products_matched_existing'] += 1
                        logger.debug(f"Found existing product for barcode {barcode}: product_id={product_id}")
                    else:
                        # No existing product with this barcode - create ONE canonical entry (INACTIVE until commercial scraper finds it)
                        # Use manufacturer name as brand, not retailer name
                        brand = product.get('manufacturer', '')

                        self.cursor.execute("""
                            INSERT INTO canonical_products (barcode, canonical_name, brand, category, image_url, is_active)
                            VALUES (%s, %s, %s, %s, %s, FALSE)
                            ON CONFLICT (barcode) DO NOTHING
                            RETURNING id
                        """, (
                            barcode,
                            product.get('name', f"Product {product['item_code']}"),
                            brand,  # Use actual manufacturer/brand, not 'Super-Pharm'
                            product.get('category'),
                            product.get('image_url')
                        ))

                        result = self.cursor.fetchone()
                        if result:
                            product_id = result[0]
                            self.stats['products_created_new'] += 1
                            logger.debug(f"Created new product for barcode {barcode}: product_id={product_id}")
                else:
                    # No barcode - skip this product entirely as per user directive
                    self.stats['products_without_barcode'] += 1
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
                        if store_id and product.get('price'):
                            prices_data.append((
                                product_id_mapping[product['item_code']],
                                store_id,
                                product['price'],
                                datetime.now()
                            ))

                # Batch insert prices
                if prices_data:
                    execute_values(
                        self.cursor,
                        """
                        INSERT INTO prices (retailer_product_id, store_id, price, price_timestamp)
                        VALUES %s
                        ON CONFLICT (retailer_product_id, store_id, price_timestamp)
                        DO UPDATE SET price = EXCLUDED.price
                        """,
                        prices_data,
                        template="(%s, %s, %s, %s)"
                    )

                    self.stats['prices_inserted'] += len(prices_data)

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
            self.stats['files_processed'] += 1
            logger.info(f"Batch processed: {len(products)} products, {len(prices_data) if prices_data else 0} prices")

        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            self.conn.rollback()
            self.stats['errors'] += 1

    def fix_existing_store_ids(self):
        """Fix existing Super-Pharm prices that have wrong store IDs"""
        logger.info("Checking for Super-Pharm prices with incorrect store IDs...")

        try:
            # Get mapping between retailerspecificstoreid and actual storeid
            self.cursor.execute("""
                SELECT storeid, CAST(retailerspecificstoreid AS INTEGER) as spec_id
                FROM stores
                WHERE retailerid = %s
                AND retailerspecificstoreid ~ '^[0-9]+$'
            """, (self.RETAILER_ID,))

            rows = self.cursor.fetchall()
            store_mapping = {row[1]: row[0] for row in rows} if rows else {}

            # Check if there are any prices with wrong store_ids
            self.cursor.execute("""
                SELECT COUNT(DISTINCT p.store_id) as count
                FROM prices p
                JOIN retailer_products rp ON p.retailer_product_id = rp.retailer_product_id
                WHERE rp.retailer_id = %s
                AND p.store_id < 1000
            """, (self.RETAILER_ID,))

            result = self.cursor.fetchone()
            wrong_count = result[0] if result else 0

            if wrong_count > 0:
                logger.info(f"Found {wrong_count} store IDs that need fixing")

                # Fix each incorrect store_id
                for spec_id, actual_id in store_mapping.items():
                    self.cursor.execute("""
                        UPDATE prices
                        SET store_id = %s
                        WHERE store_id = %s
                        AND retailer_product_id IN (
                            SELECT retailer_product_id
                            FROM retailer_products
                            WHERE retailer_id = %s
                        )
                    """, (actual_id, spec_id, self.RETAILER_ID))

                    if self.cursor.rowcount > 0:
                        logger.info(f"  Fixed {self.cursor.rowcount:,} prices: store_id {spec_id} → {actual_id}")

                self.conn.commit()
                logger.info("Store ID fixes committed")
            else:
                logger.info("All Super-Pharm prices have correct store IDs")

        except Exception as e:
            logger.error(f"Error fixing store IDs: {e}")
            self.conn.rollback()

    def process_promotion_file(self, filepath: str, filename: str):
        """
        Process Super-Pharm promotion file (denormalized format)
        Each promotion is repeated for every item it applies to
        """
        try:
            logger.info(f"Processing promotion file: {filename}")

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

            # Stage 1: De-duplicate promotions in memory
            promotions_to_process = {}  # retailer_promo_code -> promotion details
            promotion_items = {}  # retailer_promo_code -> set of item codes

            # Find all Line elements (denormalized format)
            lines = root.findall('.//Line')
            logger.info(f"Found {len(lines)} promotion lines in {filename}")

            for line in lines:
                try:
                    # Extract promotion ID from Line element (Super-Pharm specific structure)
                    promo_id = line.find('PromotionId')
                    if promo_id is None or promo_id.text is None:
                        continue

                    retailer_promo_code = promo_id.text.strip()

                    # Extract promotion details from PromotionDetails element
                    promo_details = line.find('PromotionDetails')

                    # Extract item code
                    item_code_elem = line.find('ItemCode')
                    if item_code_elem is not None and item_code_elem.text:
                        item_code = item_code_elem.text.strip()

                        # Add item to promotion's item set
                        if retailer_promo_code not in promotion_items:
                            promotion_items[retailer_promo_code] = set()
                        promotion_items[retailer_promo_code].add(item_code)

                    # If we haven't seen this promotion yet, store its details
                    if retailer_promo_code not in promotions_to_process and promo_details is not None:
                        promo_desc = promo_details.find('PromotionDescription')
                        start_date = promo_details.find('PromotionStartDate')
                        end_date = promo_details.find('PromotionEndDate')
                        min_qty = promo_details.find('MinQty')
                        discounted_price = promo_details.find('DiscountedPrice')
                        discount_rate = promo_details.find('DiscountRate')
                        discount_type = promo_details.find('DiscountType')
                        reward_type = line.find('RewardType')  # RewardType is at Line level
                        remarks = promo_details.find('Remarks')

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
                    logger.debug(f"Error processing promotion line: {e}")
                    continue

            # Stage 2: Batch insert promotions and links
            promotions_processed = 0
            links_created = 0
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
        """Main ETL execution"""
        logger.info("="*80)
        logger.info("SUPER-PHARM ETL - BARCODE-FIRST MATCHING VERSION")
        logger.info("="*80)
        logger.info(f"Configuration:")
        logger.info(f"  Portal URL: {self.BASE_URL}")
        logger.info(f"  Chain ID: {self.CHAIN_ID}")
        logger.info(f"  Retailer ID: {self.RETAILER_ID}")
        logger.info(f"  Days to process: {self.days_back}")
        logger.info(f"  Batch size: {self.batch_size}")
        logger.info(f"  Strategy: BARCODE-FIRST MATCHING (products without barcodes will be skipped)")
        logger.info("="*80)

        try:
            # First, fix any existing prices with wrong store IDs
            self.fix_existing_store_ids()

            # Fetch file list
            files = self.fetch_file_list()

            if limit:
                files = files[:limit]
                logger.info(f"Limiting to {limit} files")

            # Separate files by type
            store_files = [f for f in files if f['type'] == 'store']
            price_files = [f for f in files if f['type'] == 'price']
            promo_files = [f for f in files if f['type'] == 'promotion']

            # Process store files first
            logger.info(f"\nProcessing {len(store_files)} store files...")
            for i, file_info in enumerate(store_files, 1):
                if i % 10 == 0:
                    logger.info(f"Progress: {i}/{len(store_files)} store files")

                filepath = self.download_and_extract(file_info['url'], file_info['filename'])
                if filepath:
                    with open(filepath, 'rb') as f:
                        content = f.read()
                    self.process_store_file(content, file_info['filename'])
                    os.remove(filepath)

            # Process price files
            logger.info(f"\nProcessing {len(price_files)} price files...")
            for i, file_info in enumerate(price_files, 1):
                if i % 10 == 0:
                    logger.info(f"Progress: {i}/{len(price_files)} price files")
                    self.print_progress()

                filepath = self.download_and_extract(file_info['url'], file_info['filename'])
                if filepath:
                    products = self.parse_price_file(filepath)
                    if products:
                        self.process_product_batch(products, file_info['filename'])
                    os.remove(filepath)

            # Process promotion files
            logger.info(f"\nProcessing {len(promo_files)} promotion files...")
            for i, file_info in enumerate(promo_files, 1):
                if i % 10 == 0:
                    logger.info(f"Progress: {i}/{len(promo_files)} promotion files")

                filepath = self.download_and_extract(file_info['url'], file_info['filename'])
                if filepath:
                    self.process_promotion_file(filepath, file_info['filename'])
                    os.remove(filepath)

            # Print summary
            self.print_summary()

        except Exception as e:
            logger.error(f"Fatal error in ETL run: {e}")
            raise
        finally:
            self.cleanup()

    def print_progress(self):
        """Print progress statistics"""
        logger.info(f"  Progress Stats:")
        logger.info(f"    Products with barcode: {self.stats['products_with_barcode']}")
        logger.info(f"    Products matched to existing: {self.stats['products_matched_existing']}")
        logger.info(f"    New products created: {self.stats['products_created_new']}")
        logger.info(f"    Products skipped (no barcode): {self.stats['products_without_barcode']}")

    def print_summary(self):
        """Print ETL execution summary"""
        logger.info("="*80)
        logger.info("SUPER-PHARM ETL SUMMARY")
        logger.info("="*80)
        logger.info(f"Files downloaded: {self.stats['files_downloaded']}")
        logger.info(f"Files processed: {self.stats['files_processed']}")
        logger.info(f"Products with barcode: {self.stats['products_with_barcode']}")
        logger.info(f"Products without barcode (skipped): {self.stats['products_without_barcode']}")
        logger.info(f"Products matched to existing: {self.stats['products_matched_existing']}")
        logger.info(f"New products created: {self.stats['products_created_new']}")
        logger.info(f"Prices inserted: {self.stats['prices_inserted']}")
        logger.info(f"Stores created/updated: {self.stats['stores_created']}")
        logger.info(f"Promotions processed: {self.stats.get('promotions_processed', 0)}")
        logger.info(f"Promotion-product links created: {self.stats.get('promotion_links_created', 0)}")
        logger.info(f"Batch inserts performed: {self.stats['batch_inserts']}")
        logger.info(f"Errors encountered: {self.stats['errors']}")

        # Query final database counts
        try:
            # Count shared products between retailers
            self.cursor.execute("""
                WITH product_retailers AS (
                    SELECT DISTINCT p.product_id, rp.retailer_id
                    FROM products p
                    JOIN retailer_products rp ON p.product_id = rp.product_id
                    WHERE p.attributes->>'barcode' IS NOT NULL
                )
                SELECT
                    COUNT(DISTINCT product_id) as shared_products
                FROM product_retailers
                WHERE product_id IN (
                    SELECT product_id
                    FROM product_retailers
                    GROUP BY product_id
                    HAVING COUNT(DISTINCT retailer_id) > 1
                )
            """)

            result = self.cursor.fetchone()
            if result:
                logger.info("="*80)
                logger.info("CROSS-RETAILER MATCHING RESULTS:")
                logger.info(f"  Products shared across retailers: {result[0]:,}")

            # Super-Pharm specific stats
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
                logger.info("DATABASE TOTALS FOR SUPER-PHARM:")
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

    parser = argparse.ArgumentParser(description="Super-Pharm ETL - Barcode-First Matching Version")
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days of historical data to process (default: 30)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of files to process (for testing)"
    )

    args = parser.parse_args()

    try:
        etl = SuperPharmBarcodeETL(days_back=args.days)
        etl.run(limit=args.limit)
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        sys.exit(1)