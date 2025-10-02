#!/usr/bin/env python3
"""
Good Pharm ETL Pipeline with Barcode-First Matching Strategy
Based on the successful Be Pharm implementation
"""

import os
import re
import sys
import json
import gzip
import logging
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import xml.etree.ElementTree as ET

import requests
import psycopg2
from psycopg2.extras import execute_values, Json
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('good_pharm_barcode_matching.log')
    ]
)
logger = logging.getLogger(__name__)


class GoodPharmBarcodeETL:
    def __init__(self, days_back: int = 30):
        """Initialize Good Pharm ETL with barcode-first matching strategy

        Args:
            days_back: Number of days of historical data to process (default 30)
        """
        # Good Pharm identifiers
        self.CHAIN_ID = '7290058108879'
        self.RETAILER_ID = 97  # Good Pharm retailer ID

        # Configuration
        self.days_back = days_back
        self.batch_size = 1000

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
        self.temp_dir = tempfile.mkdtemp(prefix='good_pharm_barcode_etl_')
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

        # Load existing stores
        self.ensure_good_pharm_stores()

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

    def ensure_good_pharm_stores(self):
        """Download and process Good Pharm StoresFull file to populate stores"""
        try:
            logger.info("Downloading Good Pharm StoresFull file...")

            # Good Pharm API endpoint for stores
            base_url = 'https://goodpharm.binaprojects.com/'
            api_url = f"{base_url}MainIO_Hok.aspx?WStore=0&WFileType=StoresFull"

            response = requests.get(api_url, timeout=30, verify=False)

            if response.status_code == 200:
                # Check if it's JSON response with file path
                try:
                    data = response.json()
                    if isinstance(data, list) and len(data) > 0 and 'SPath' in data[0]:
                        stores_url = data[0]['SPath']
                        logger.info(f"Found Good Pharm StoresFull at: {stores_url}")

                        # Download the file
                        response = requests.get(stores_url, timeout=60, verify=False)
                        content = response.content

                        # Try to decompress if gzipped
                        try:
                            content = gzip.decompress(content)
                        except:
                            pass  # Not gzipped

                        # Parse XML
                        root = ET.fromstring(content)
                        stores = root.findall('.//Store')

                        logger.info(f"Found {len(stores)} Good Pharm stores")

                        stores_created = 0
                        for store in stores:
                            store_id_elem = store.find('StoreId')
                            store_name_elem = store.find('StoreName')
                            address_elem = store.find('Address')
                            city_elem = store.find('City')

                            if store_id_elem is not None:
                                store_id = store_id_elem.text
                                store_name = store_name_elem.text if store_name_elem is not None else f'Good Pharm Store {store_id}'
                                address = address_elem.text if address_elem is not None else ''
                                city = city_elem.text if city_elem is not None else ''

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
                                    str(store_id),
                                    store_name,
                                    address,
                                    city
                                ))

                                if self.cursor.fetchone():
                                    stores_created += 1

                        self.conn.commit()
                        if stores_created > 0:
                            logger.info(f"Created/updated {stores_created} Good Pharm stores")
                            self.stats['stores_created'] = stores_created
                except Exception as e:
                    logger.error(f"Error processing stores: {e}")

        except Exception as e:
            logger.warning(f"Could not download Good Pharm stores: {e}")

    def get_good_pharm_files(self) -> List[Dict]:
        """Get list of files from Good Pharm transparency portal"""
        files = []
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

        # Calculate date cutoff
        cutoff_date = datetime.now() - timedelta(days=self.days_back)
        from_date = cutoff_date.strftime('%d/%m/%Y')
        to_date = datetime.now().strftime('%d/%m/%Y')

        logger.info(f"Searching for Good Pharm files from {from_date} to {to_date}")

        base_url = 'https://goodpharm.binaprojects.com/'
        files_url = base_url + 'MainIO_Hok.aspx'

        try:
            # Request files with parameters
            params = {
                'sType': '1',  # File type
                'sFName': '',  # Empty for all files
                'sSName': '0',  # Store ID, 0 for all
                'sFromDate': from_date,
                'sToDate': to_date,
                'iCheck': 'false'
            }

            response = session.post(files_url, data=params, timeout=30, verify=False)
            response.raise_for_status()

            # Parse response
            try:
                # Try JSON first
                data = response.json()
                for item in data:
                    filename = item.get('FileNm', '')
                    if filename and filename not in self.processed_files:
                        # Extract store ID from filename (pattern: ChainID-StoreID-Timestamp)
                        store_match = re.search(r'7290058197699-(\d+)-', filename)
                        store_id = store_match.group(1) if store_match else None

                        files.append({
                            'name': filename,
                            'url': base_url + 'Download.aspx?FileNm=' + filename,
                            'store_id': store_id,
                            'type': 'price'
                        })
            except:
                # Fallback to HTML parsing
                soup = BeautifulSoup(response.text, 'html.parser')

                for link in soup.find_all('a', href=True):
                    filename = link.get_text(strip=True)
                    if filename and filename not in self.processed_files:
                        href = link['href']

                        # Extract store ID (pattern: ChainID-StoreID-Timestamp)
                        store_match = re.search(r'7290058197699-(\d+)-', filename)
                        store_id = store_match.group(1) if store_match else None

                        files.append({
                            'name': filename,
                            'url': base_url + href if not href.startswith('http') else href,
                            'store_id': store_id,
                            'type': 'price'
                        })

            logger.info(f"Found {len(files)} Good Pharm files to process")

        except Exception as e:
            logger.error(f"Error fetching Good Pharm files: {e}")

        return files

    def download_file(self, url: str, filename: str) -> Optional[str]:
        """Download file to temp directory"""
        filepath = os.path.join(self.temp_dir, filename)

        try:
            response = requests.get(url, timeout=60, verify=False)
            response.raise_for_status()

            # Check if response is JSON (Good Pharm pattern)
            try:
                data = response.json()
                if isinstance(data, list) and len(data) > 0 and 'SPath' in data[0]:
                    # Get the actual download URL
                    actual_url = data[0]['SPath']
                    logger.debug(f"Good Pharm redirect to: {actual_url}")
                    response = requests.get(actual_url, stream=True, timeout=60, verify=False)
                    response.raise_for_status()
            except:
                # Not JSON, use original response
                pass

            with open(filepath, 'wb') as f:
                if hasattr(response, 'iter_content'):
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                else:
                    f.write(response.content)

            self.stats['files_downloaded'] += 1
            logger.debug(f"Downloaded: {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"Error downloading {filename}: {e}")
            self.stats['errors'] += 1
            return None

    def parse_good_pharm_file(self, filepath: str, store_id: str = None) -> List[Dict]:
        """Parse Good Pharm XML file and extract product data"""
        products = []

        try:
            # Determine file type and extract content
            with open(filepath, 'rb') as f:
                magic = f.read(2)
                f.seek(0)

                if magic == b'\x1f\x8b':  # GZIP
                    with gzip.open(filepath, 'rt', encoding='utf-8') as gz:
                        content = gz.read()
                elif magic == b'PK':  # ZIP
                    import zipfile
                    with zipfile.ZipFile(filepath, 'r') as zf:
                        xml_files = [n for n in zf.namelist() if n.endswith('.xml')]
                        if xml_files:
                            with zf.open(xml_files[0]) as xml_file:
                                content = xml_file.read().decode('utf-8')
                        else:
                            with zf.open(zf.namelist()[0]) as first_file:
                                content = first_file.read().decode('utf-8')
                else:
                    # Plain text/XML
                    with open(filepath, 'r', encoding='utf-8') as txt:
                        content = txt.read()

            # Parse XML
            root = ET.fromstring(content)

            # Find items - Good Pharm uses various element names
            paths_to_try = ['.//Item', './/Product', './/Line']

            items = []
            for path in paths_to_try:
                items = root.findall(path)
                if items:
                    logger.debug(f"Found {len(items)} items using path: {path}")
                    break

            for item in items:
                product = {}

                # Add store_id from filename if available
                if store_id:
                    product['store_id'] = store_id

                # Parse XML fields
                for child in item:
                    tag = child.tag
                    text = child.text

                    if text:
                        text = text.strip()

                        # Map XML fields to our database fields
                        if tag in ['ItemCode', 'ItemId', 'ProductId', 'Barcode']:
                            product['item_code'] = text
                            # Check if it's a valid barcode (8-13 digits)
                            if text.isdigit() and 8 <= len(text) <= 13:
                                product['barcode'] = text
                        elif tag in ['ItemName', 'ProductName', 'ItemDesc', 'ItemNm', 'ManufacturerItemDescription']:
                            product['name'] = text
                        elif tag in ['ItemPrice', 'Price']:
                            try:
                                product['price'] = float(text)
                            except:
                                pass
                        elif tag in ['ManufacturerName', 'Manufacturer', 'Brand']:
                            product['manufacturer'] = text
                        elif tag in ['StoreId', 'StoreID']:
                            product['store_id'] = text
                        elif tag in ['PriceUpdateDate', 'UpdateDate']:
                            product['price_date'] = text

                # Only add products with at least a name or item code
                if product.get('name') or product.get('item_code'):
                    products.append(product)

            logger.info(f"Parsed {len(products)} products from {os.path.basename(filepath)}")

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
                    """, (self.RETAILER_ID, store_id, f"Good Pharm Store {store_id}"))

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
                            brand,  # Use actual manufacturer/brand, not 'Good Pharm'
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
                            # Parse price timestamp
                            price_timestamp = datetime.now()
                            if 'price_date' in product:
                                try:
                                    # Try different date formats
                                    for fmt in ['%Y-%m-%d %H:%M', '%Y-%m-%d', '%d/%m/%Y']:
                                        try:
                                            price_timestamp = datetime.strptime(product['price_date'], fmt)
                                            break
                                        except:
                                            continue
                                except:
                                    pass

                            prices_data.append((
                                product_id_mapping[product['item_code']],
                                store_id,
                                product['price'],
                                price_timestamp
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

    def process_promotion_file(self, filepath: str, filename: str):
        """
        Process Good Pharm promotion file (normalized format)
        Each promotion appears once with all its items listed inside
        """
        try:
            logger.info(f"Processing promotion file: {filename}")

            # Read file
            with open(filepath, 'rb') as f:
                magic = f.read(2)
                f.seek(0)
                content = f.read()

            # Handle different file formats
            if magic == b'\x1f\x8b':  # GZIP
                content = gzip.decompress(content)
            elif magic == b'PK':  # ZIP
                import zipfile
                with zipfile.ZipFile(filepath, 'r') as zf:
                    xml_files = [n for n in zf.namelist() if n.endswith('.xml')]
                    if xml_files:
                        with zf.open(xml_files[0]) as xml_file:
                            content = xml_file.read()
                    else:
                        # Use first file in zip
                        with zf.open(zf.namelist()[0]) as first_file:
                            content = first_file.read()
            # else: assume it's already plain XML

            # Parse XML
            root = ET.fromstring(content)

            # Track statistics
            promotions_processed = 0
            links_created = 0

            # Find all Promotion elements
            promotions = root.findall('.//Promotion')
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

            # Classify promotion types based on product count (when promotion_type column is available)
            # TODO: Uncomment when promotion_type column is added to database
            # for db_promotion_id, item_count in [(db_promotion_id, len(items.findall('Item')))
            #                                       for db_promotion_id in [db_promotion_id]]:
            #     if item_count > 1000:
            #         promotion_type = 'Store-Wide'
            #     elif item_count > 100:
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

            logger.info(f"Processed {promotions_processed} promotions with {links_created} product links from {filename}")

        except Exception as e:
            logger.error(f"Error processing promotion file {filepath}: {e}")
            self.conn.rollback()
            self.stats['errors'] += 1

    def run(self, limit: Optional[int] = None):
        """Main ETL execution

        Args:
            limit: Optional limit on number of files to process (for testing)
        """
        logger.info("="*80)
        logger.info("GOOD PHARM ETL - BARCODE-FIRST MATCHING VERSION")
        logger.info("="*80)
        logger.info(f"Configuration:")
        logger.info(f"  Chain ID: {self.CHAIN_ID}")
        logger.info(f"  Retailer ID: {self.RETAILER_ID}")
        logger.info(f"  Days to process: {self.days_back}")
        logger.info(f"  Batch size: {self.batch_size}")
        logger.info(f"  Strategy: BARCODE-FIRST MATCHING (products without barcodes will be skipped)")
        logger.info("="*80)

        try:
            # Step 1: Get list of files from Good Pharm portal
            logger.info("Step 1: Fetching file list from Good Pharm portal...")
            files = self.get_good_pharm_files()

            if not files:
                logger.warning("No files found to process")
                return

            # Apply limit if specified
            if limit:
                files = files[:limit]
                logger.info(f"Limiting to first {limit} files for testing.")

            # Step 2: Process each file
            logger.info(f"Step 2: Processing {len(files)} files...")

            for i, file_info in enumerate(files, 1):
                if i % 10 == 0:
                    logger.info(f"Progress: {i}/{len(files)} files processed")
                    self.print_progress()

                # Download file
                filepath = self.download_file(file_info['url'], file_info['name'])
                if not filepath:
                    continue

                # Check if it's a promotion file or price file
                # Good Pharm uses PromoFull and PriceFull naming conventions
                if 'promofull' in file_info['name'].lower():
                    # Process promotion file
                    self.process_promotion_file(filepath, file_info['name'])
                else:
                    # Parse price file (PriceFull or other formats)
                    products = self.parse_good_pharm_file(filepath, file_info.get('store_id'))

                    if products:
                        # Process batch of products with barcode-first matching
                        self.process_product_batch(products, file_info['name'])

                # Clean up downloaded file
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
        logger.info("GOOD PHARM ETL SUMMARY")
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
                    SELECT DISTINCT cp.barcode, rp.retailer_id
                    FROM canonical_products cp
                    JOIN retailer_products rp ON cp.barcode = rp.barcode
                    WHERE cp.barcode IS NOT NULL
                )
                SELECT
                    COUNT(DISTINCT barcode) as shared_products
                FROM product_retailers
                WHERE barcode IN (
                    SELECT barcode
                    FROM product_retailers
                    GROUP BY barcode
                    HAVING COUNT(DISTINCT retailer_id) > 1
                )
            """)

            result = self.cursor.fetchone()
            if result:
                logger.info("="*80)
                logger.info("CROSS-RETAILER MATCHING RESULTS:")
                logger.info(f"  Products shared across retailers: {result[0]:,}")

            # Good Pharm specific stats
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
                logger.info("DATABASE TOTALS FOR GOOD PHARM:")
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

    parser = argparse.ArgumentParser(description="Good Pharm ETL - Barcode-First Matching Version")
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
        etl = GoodPharmBarcodeETL(days_back=args.days)
        etl.run(limit=args.limit)
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        sys.exit(1)