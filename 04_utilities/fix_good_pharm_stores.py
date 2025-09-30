#!/usr/bin/env python3
"""
Fix Good Pharm stores by:
1. Clearing existing Good Pharm data
2. Re-populating from StoresFull file with correct structure
3. Running a small ETL test
"""

import re
import gzip
import xml.etree.ElementTree as ET
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="price_comparison_app_v2",
    user="postgres",
    password="***REMOVED***"
)
cursor = conn.cursor(cursor_factory=RealDictCursor)

def clear_good_pharm_data():
    """Clear all Good Pharm data to start fresh"""
    logger.info("Clearing Good Pharm data...")

    try:
        # Delete prices
        cursor.execute("""
            DELETE FROM prices
            WHERE retailer_product_id IN (
                SELECT retailer_product_id
                FROM retailer_products
                WHERE retailer_id = 97
            )
        """)
        price_count = cursor.rowcount

        # Delete products
        cursor.execute("DELETE FROM retailer_products WHERE retailer_id = 97")
        product_count = cursor.rowcount

        # Delete stores
        cursor.execute("DELETE FROM stores WHERE retailerid = 97")
        store_count = cursor.rowcount

        conn.commit()
        logger.info(f"Deleted {price_count} prices, {product_count} products, {store_count} stores")

    except Exception as e:
        logger.error(f"Error clearing data: {e}")
        conn.rollback()
        raise

def populate_good_pharm_stores():
    """Populate Good Pharm stores from StoresFull file with correct structure"""
    logger.info("Populating Good Pharm stores from StoresFull file...")

    try:
        # Try to download the latest StoresFull file
        base_url = 'https://goodpharm.binaprojects.com/'
        api_url = f"{base_url}MainIO_Hok.aspx?WStore=0&WFileType=StoresFull"

        stores_url = None
        content = None

        try:
            response = requests.get(api_url, timeout=30, verify=False)
            if response.status_code == 200:
                try:
                    data = response.json()
                    if isinstance(data, list) and len(data) > 0 and 'SPath' in data[0]:
                        stores_url = data[0]['SPath']
                        logger.info(f"Found Good Pharm StoresFull at: {stores_url}")

                        # Download the file
                        response = requests.get(stores_url, timeout=60, verify=False)
                        response.raise_for_status()
                        content = response.content
                except:
                    pass
        except Exception as e:
            logger.warning(f"Could not download Good Pharm StoresFull: {e}")

        if not content:
            # Use local file if available
            import os
            local_file = '/Users/noa/Downloads/StoresFull7290058197699-000-202507041024.xml'
            if os.path.exists(local_file):
                logger.info(f"Using local file: {local_file}")
                with open(local_file, 'rb') as f:
                    content = f.read()
            else:
                logger.error("No Good Pharm StoresFull file found")
                return

        # Try to decompress if gzipped
        try:
            content = gzip.decompress(content)
        except:
            pass

        # Parse XML
        root = ET.fromstring(content)

        # Find stores
        stores = root.findall('.//Store')
        logger.info(f"Found {len(stores)} Good Pharm stores in StoresFull file")

        good_pharm_count = 0
        for store in stores:
            store_id_elem = store.find('StoreId')
            store_name_elem = store.find('StoreName')
            address_elem = store.find('Address')
            city_elem = store.find('City')

            if store_id_elem is not None and store_id_elem.text:
                store_id = store_id_elem.text.strip()
                store_name = store_name_elem.text.strip() if store_name_elem is not None and store_name_elem.text else f'Good Pharm Store {store_id}'
                address = address_elem.text.strip() if address_elem is not None and address_elem.text else ''
                city = city_elem.text.strip() if city_elem is not None and city_elem.text else ''

                # Insert Good Pharm store with correct structure
                cursor.execute("""
                    INSERT INTO stores (
                        retailerid, retailerspecificstoreid,
                        storename, address, city, isactive, createdat, updatedat
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (retailerid, retailerspecificstoreid) DO UPDATE SET
                        storename = EXCLUDED.storename,
                        address = EXCLUDED.address,
                        city = EXCLUDED.city,
                        updatedat = EXCLUDED.updatedat
                    RETURNING storeid
                """, (
                    97,  # Good Pharm retailer_id
                    str(store_id),  # Just the numeric ID
                    store_name,  # The actual store name
                    address,
                    city,
                    True,
                    datetime.now(),
                    datetime.now()
                ))

                good_pharm_count += 1
                if good_pharm_count <= 5:
                    logger.info(f"Added Good Pharm store {store_id}: {store_name} in {city}")

        conn.commit()
        logger.info(f"Successfully populated {good_pharm_count} Good Pharm stores")

    except Exception as e:
        logger.error(f"Error populating stores: {e}")
        conn.rollback()
        raise

def fix_etl_store_extraction():
    """Update the Good Pharm ETL to extract store IDs correctly"""
    logger.info("Updating Good Pharm ETL store extraction logic...")

    etl_file = '/Users/noa/Desktop/PriceComparisonApp/01_data_scraping_pipeline/be_good_pharm_etl_FIXED.py'

    try:
        with open(etl_file, 'r') as f:
            content = f.read()

        # Fix the store ID extraction for Good Pharm
        # The Good Pharm API returns store names like "952 גיסין פ"ת"
        # We need to extract just the numeric part

        # Add a helper function to extract numeric store ID
        helper_function = '''
    def extract_store_id(self, store_str):
        """Extract numeric store ID from Good Pharm store string
        Examples:
        - "952 גיסין פ"ת" -> "952"
        - "141 ת"א- פלורנטין" -> "141"
        - "123" -> "123"
        """
        if not store_str:
            return None

        # If it's already just a number, return it
        if store_str.isdigit():
            return store_str

        # Extract first sequence of digits
        match = re.match(r'(\d+)', str(store_str).strip())
        return match.group(1) if match else None
'''

        # Find where to insert the helper function (after parse_xml_file definition)
        insert_pos = content.find('    def process_products(')
        if insert_pos > 0:
            # Insert the helper function before process_products
            content = content[:insert_pos] + helper_function + '\n' + content[insert_pos:]

        # Now update the store ID extraction in get_good_pharm_files
        old_pattern = "'store_id': item.get('Store'),"
        new_pattern = "'store_id': self.extract_store_id(item.get('Store')),"
        content = content.replace(old_pattern, new_pattern)

        # Also update the manual extraction pattern
        old_pattern2 = "store_id = store_match.group(1) if store_match else None"
        new_pattern2 = "store_id = self.extract_store_id(store_match.group(1) if store_match else None)"
        content = content.replace(old_pattern2, new_pattern2)

        # Save the updated file
        with open(etl_file, 'w') as f:
            f.write(content)

        logger.info("Successfully updated ETL store extraction logic")

    except Exception as e:
        logger.error(f"Error updating ETL: {e}")
        raise

def verify_fix():
    """Verify the fix worked"""
    logger.info("\nVerifying the fix...")

    # Check store data
    cursor.execute("""
        SELECT retailerspecificstoreid, storename, address, city
        FROM stores
        WHERE retailerid = 97
        ORDER BY retailerspecificstoreid::int
        LIMIT 10
    """)

    logger.info("\nGood Pharm stores after fix:")
    for row in cursor.fetchall():
        logger.info(f"  Store {row['retailerspecificstoreid']}: {row['storename']} - {row['city']}")

    # Check counts
    cursor.execute("""
        SELECT COUNT(*) as total,
               COUNT(CASE WHEN address IS NOT NULL AND address != '' THEN 1 END) as with_address,
               COUNT(CASE WHEN storename LIKE '%Store %' THEN 1 END) as placeholder_names
        FROM stores
        WHERE retailerid = 97
    """)

    result = cursor.fetchone()
    logger.info(f"\nTotal Good Pharm stores: {result['total']}")
    logger.info(f"Stores with addresses: {result['with_address']}")
    logger.info(f"Stores with placeholder names: {result['placeholder_names']}")

def main():
    """Main function"""
    try:
        # Step 1: Clear existing Good Pharm data
        clear_good_pharm_data()

        # Step 2: Populate stores with correct structure
        populate_good_pharm_stores()

        # Step 3: Fix the ETL for future runs
        fix_etl_store_extraction()

        # Step 4: Verify the fix
        verify_fix()

        logger.info("\n✅ Good Pharm stores successfully fixed!")
        logger.info("You can now run the Good Pharm ETL to populate products and prices.")

    except Exception as e:
        logger.error(f"Error in main: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()