#!/usr/bin/env python3
"""
Update database schema to match the PharmMate documentation
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def update_schema():
    """Update database schema to match documentation"""

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="price_comparison_app_v2",
        user="postgres",
        password="***REMOVED***"
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    try:
        # Check if we need to add missing columns or tables
        logger.info("Checking and updating database schema...")

        # 1. Check and update retailers table
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'retailers'
        """)
        existing_columns = [row[0] for row in cursor.fetchall()]

        if 'chainid' not in existing_columns:
            logger.info("Adding chainid column to retailers table")
            cursor.execute("ALTER TABLE retailers ADD COLUMN IF NOT EXISTS chainid VARCHAR(50)")

        if 'pricetransparencyportalurl' not in existing_columns:
            logger.info("Adding pricetransparencyportalurl column to retailers table")
            cursor.execute("ALTER TABLE retailers ADD COLUMN IF NOT EXISTS pricetransparencyportalurl VARCHAR(255)")

        if 'fileformat' not in existing_columns:
            logger.info("Adding fileformat column to retailers table")
            cursor.execute("ALTER TABLE retailers ADD COLUMN IF NOT EXISTS fileformat VARCHAR(20) DEFAULT 'XML'")

        if 'notes' not in existing_columns:
            logger.info("Adding notes column to retailers table")
            cursor.execute("ALTER TABLE retailers ADD COLUMN IF NOT EXISTS notes TEXT")

        if 'createdat' not in existing_columns:
            logger.info("Adding createdat column to retailers table")
            cursor.execute("ALTER TABLE retailers ADD COLUMN IF NOT EXISTS createdat TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP")

        if 'updatedat' not in existing_columns:
            logger.info("Adding updatedat column to retailers table")
            cursor.execute("ALTER TABLE retailers ADD COLUMN IF NOT EXISTS updatedat TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP")

        # 2. Update retailer records with chain IDs
        logger.info("Updating retailer records with chain IDs...")

        # Super-Pharm - already has chainid
        cursor.execute("""
            UPDATE retailers
            SET pricetransparencyportalurl = 'https://prices.super-pharm.co.il/',
                fileformat = 'XML',
                updatedat = CURRENT_TIMESTAMP
            WHERE retailerid = 52 AND retailername = 'Super-Pharm'
        """)

        # Be Pharm - uses same chainid as Shufersal, so we'll use a different identifier
        cursor.execute("""
            UPDATE retailers
            SET chainid = '7290027600007-BE',
                pricetransparencyportalurl = 'https://prices.shufersal.co.il/',
                fileformat = 'XML',
                notes = 'Uses Shufersal transparency portal',
                updatedat = CURRENT_TIMESTAMP
            WHERE retailerid = 150 AND retailername = 'Be Pharm'
        """)

        # Good Pharm - update chainid to correct value
        cursor.execute("""
            UPDATE retailers
            SET chainid = '7290058108879',
                pricetransparencyportalurl = 'https://goodpharm.binaprojects.com/MainIO_Hok.aspx',
                fileformat = 'XML',
                updatedat = CURRENT_TIMESTAMP
            WHERE retailerid = 97 AND retailername = 'Good Pharm'
        """)

        # 3. Check and update stores table
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'stores'
        """)
        existing_columns = [row[0] for row in cursor.fetchall()]

        if 'retailerspecificstoreid' not in existing_columns:
            logger.info("Adding retailerspecificstoreid column to stores table")
            cursor.execute("ALTER TABLE stores ADD COLUMN IF NOT EXISTS retailerspecificstoreid VARCHAR(50)")

        if 'rawstoredata' not in existing_columns:
            logger.info("Adding rawstoredata column to stores table")
            cursor.execute("ALTER TABLE stores ADD COLUMN IF NOT EXISTS rawstoredata JSONB")

        if 'isactive' not in existing_columns:
            logger.info("Adding isactive column to stores table")
            cursor.execute("ALTER TABLE stores ADD COLUMN IF NOT EXISTS isactive BOOLEAN DEFAULT TRUE")

        if 'createdat' not in existing_columns:
            logger.info("Adding createdat column to stores table")
            cursor.execute("ALTER TABLE stores ADD COLUMN IF NOT EXISTS createdat TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP")

        if 'updatedat' not in existing_columns:
            logger.info("Adding updatedat column to stores table")
            cursor.execute("ALTER TABLE stores ADD COLUMN IF NOT EXISTS updatedat TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP")

        if 'subchainid' not in existing_columns:
            logger.info("Adding subchainid column to stores table")
            cursor.execute("ALTER TABLE stores ADD COLUMN IF NOT EXISTS subchainid TEXT")

        if 'subchainname' not in existing_columns:
            logger.info("Adding subchainname column to stores table")
            cursor.execute("ALTER TABLE stores ADD COLUMN IF NOT EXISTS subchainname VARCHAR(255)")

        if 'storetype' not in existing_columns:
            logger.info("Adding storetype column to stores table")
            cursor.execute("ALTER TABLE stores ADD COLUMN IF NOT EXISTS storetype TEXT")

        if 'lastupdatedfromstoresfile' not in existing_columns:
            logger.info("Adding lastupdatedfromstoresfile column to stores table")
            cursor.execute("ALTER TABLE stores ADD COLUMN IF NOT EXISTS lastupdatedfromstoresfile TEXT")

        # 4. Create missing tables
        logger.info("Creating missing tables if they don't exist...")

        # Create product_groups table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS product_groups (
                group_id SERIAL PRIMARY KEY,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create product_group_links table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS product_group_links (
                product_id INTEGER REFERENCES products(product_id) ON DELETE CASCADE,
                group_id INTEGER REFERENCES product_groups(group_id) ON DELETE CASCADE,
                PRIMARY KEY (product_id, group_id)
            )
        """)

        # Create categories table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS categories (
                category_id SERIAL PRIMARY KEY,
                category_name TEXT NOT NULL,
                parent_category_id INTEGER REFERENCES categories(category_id)
            )
        """)

        # Create barcode_to_canonical_map table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS barcode_to_canonical_map (
                barcode VARCHAR PRIMARY KEY,
                product_id INTEGER REFERENCES products(product_id) ON DELETE CASCADE
            )
        """)

        # Create filesprocessed table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS filesprocessed (
                file_id SERIAL PRIMARY KEY,
                retailer_id INTEGER REFERENCES retailers(retailerid),
                store_id INTEGER REFERENCES stores(storeid),
                filename TEXT NOT NULL,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 5. Ensure all constraints are properly set
        logger.info("Verifying constraints...")

        # Check if unique constraint exists on retailer_products
        cursor.execute("""
            SELECT conname FROM pg_constraint
            WHERE conrelid = 'retailer_products'::regclass
            AND contype = 'u'
        """)
        constraints = [row[0] for row in cursor.fetchall()]

        if not any('retailer_id' in c and 'retailer_item_code' in c for c in constraints):
            logger.info("Adding unique constraint to retailer_products")
            cursor.execute("""
                ALTER TABLE retailer_products
                DROP CONSTRAINT IF EXISTS retailer_products_retailer_id_item_code_key;
                ALTER TABLE retailer_products
                ADD CONSTRAINT retailer_products_retailer_id_item_code_key
                UNIQUE (retailer_id, retailer_item_code)
            """)

        # Check if unique constraint exists on prices
        cursor.execute("""
            SELECT conname FROM pg_constraint
            WHERE conrelid = 'prices'::regclass
            AND contype = 'u'
        """)
        constraints = [row[0] for row in cursor.fetchall()]

        if not any('retailer_product_id' in c and 'store_id' in c and 'price_timestamp' in c for c in constraints):
            logger.info("Adding unique constraint to prices")
            cursor.execute("""
                ALTER TABLE prices
                DROP CONSTRAINT IF EXISTS prices_retailer_product_store_timestamp_key;
                ALTER TABLE prices
                ADD CONSTRAINT prices_retailer_product_store_timestamp_key
                UNIQUE (retailer_product_id, store_id, price_timestamp)
            """)

        # Check products unique constraint
        cursor.execute("""
            SELECT conname FROM pg_constraint
            WHERE conrelid = 'products'::regclass
            AND contype = 'u'
        """)
        constraints = [row[0] for row in cursor.fetchall()]

        if not any('canonical_name' in c and 'brand' in c for c in constraints):
            logger.info("Adding unique constraint to products")
            # Drop existing unique index if it exists
            cursor.execute("DROP INDEX IF EXISTS idx_products_canonical_name;")

            # Create unique index with expression
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_products_canonical_name_brand
                ON products (lower(canonical_name), lower(brand))
            """)

        logger.info("Schema update completed successfully!")

        # Display current schema status
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        tables = cursor.fetchall()
        logger.info(f"Current tables: {[t[0] for t in tables]}")

    except Exception as e:
        logger.error(f"Error updating schema: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    update_schema()