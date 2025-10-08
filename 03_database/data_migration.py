#!/usr/bin/env python3
"""
PharmMate Data Migration Script
================================
This script migrates existing product data to the new barcode-centric schema.
It handles the consolidation of three product tables into one canonical table.

Run AFTER executing schema_migration.sql
"""

import psycopg2
from psycopg2.extras import execute_values
import logging
from datetime import datetime
from typing import Dict, List, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataMigrator:
    def __init__(self):
        """Initialize database connection"""
        self.conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="price_comparison_app_v2",
            user="postgres",
            password="025655358"
        )
        self.cursor = self.conn.cursor()

        self.stats = {
            'total_products': 0,
            'migrated_products': 0,
            'products_with_barcode': 0,
            'products_without_barcode': 0,
            'barcode_conflicts': 0,
            'errors': 0
        }

    def verify_pre_migration_state(self):
        """Check current state before migration"""
        logger.info("="*60)
        logger.info("VERIFYING PRE-MIGRATION STATE")
        logger.info("="*60)

        # Check existing tables
        self.cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name IN ('products', 'canonical_products', 'canonical_products_clean', 'retailer_products')
        """)
        tables = [row[0] for row in self.cursor.fetchall()]
        logger.info(f"Existing tables: {', '.join(tables)}")

        # Count records in each table
        for table in tables:
            if table == 'retailer_products':
                self.cursor.execute(f"SELECT COUNT(*), COUNT(DISTINCT retailer_id) FROM {table}")
                count, retailers = self.cursor.fetchone()
                logger.info(f"  {table}: {count:,} records across {retailers} retailers")
            else:
                self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = self.cursor.fetchone()[0]
                logger.info(f"  {table}: {count:,} records")

    def extract_barcodes_from_products(self) -> Dict[int, str]:
        """Extract barcodes from the products table"""
        logger.info("\nExtracting barcodes from products table...")

        self.cursor.execute("""
            SELECT
                product_id,
                attributes->>'barcode' as barcode,
                canonical_name
            FROM products
            WHERE attributes->>'barcode' IS NOT NULL
            AND attributes->>'barcode' != ''
        """)

        barcode_map = {}
        for product_id, barcode, name in self.cursor.fetchall():
            if barcode and len(barcode) >= 8 and len(barcode) <= 13 and barcode.isdigit():
                barcode_map[product_id] = barcode
            else:
                logger.debug(f"Invalid barcode '{barcode}' for product {product_id}: {name}")

        logger.info(f"  Found {len(barcode_map):,} valid barcodes")
        return barcode_map

    def map_retailer_products_to_barcodes(self, barcode_map: Dict[int, str]):
        """Update retailer_products with barcodes"""
        logger.info("\nMapping retailer_products to barcodes...")

        # Get all retailer products
        self.cursor.execute("""
            SELECT
                retailer_product_id,
                product_id,
                retailer_id,
                retailer_item_code
            FROM retailer_products
            WHERE retailer_id IN (52, 97, 150)  -- Pharmacy chains only
        """)

        retailer_products = self.cursor.fetchall()
        self.stats['total_products'] = len(retailer_products)

        # Prepare batch update data
        updates_with_barcode = []
        updates_without_barcode = []

        for rp_id, product_id, retailer_id, item_code in retailer_products:
            if product_id and product_id in barcode_map:
                # Found barcode via product_id
                updates_with_barcode.append((barcode_map[product_id], rp_id))
            elif item_code and len(item_code) >= 8 and len(item_code) <= 13 and item_code.isdigit():
                # Item code looks like a barcode
                updates_with_barcode.append((item_code, rp_id))
            else:
                updates_without_barcode.append(rp_id)

        # Batch update retailer_products with barcodes
        if updates_with_barcode:
            execute_values(
                self.cursor,
                """
                UPDATE retailer_products
                SET barcode = data.barcode
                FROM (VALUES %s) AS data(barcode, retailer_product_id)
                WHERE retailer_products.retailer_product_id = data.retailer_product_id
                """,
                updates_with_barcode,
                template="(%s, %s)"
            )
            self.conn.commit()
            self.stats['products_with_barcode'] = len(updates_with_barcode)
            logger.info(f"  Updated {len(updates_with_barcode):,} products with barcodes")

        self.stats['products_without_barcode'] = len(updates_without_barcode)
        if updates_without_barcode:
            logger.warning(f"  {len(updates_without_barcode):,} products have no barcode")

    def populate_canonical_products(self):
        """Populate the new canonical_products table"""
        logger.info("\nPopulating canonical_products table...")

        # Check if canonical_products table exists
        self.cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'canonical_products'
            )
        """)

        if not self.cursor.fetchone()[0]:
            logger.error("canonical_products table does not exist! Run schema_migration.sql first.")
            return

        # Get unique barcodes with their best data
        self.cursor.execute("""
            WITH barcode_data AS (
                -- From retailer_products with barcodes
                SELECT DISTINCT ON (rp.barcode)
                    rp.barcode,
                    COALESCE(p.canonical_name, rp.original_retailer_name) as name,
                    p.brand,
                    p.description,
                    p.image_url,
                    rp.retailer_id as source_retailer_id,
                    COALESCE(p.created_at, NOW()) as last_scraped_at
                FROM retailer_products rp
                LEFT JOIN products p ON rp.product_id = p.product_id
                WHERE rp.barcode IS NOT NULL
                AND rp.retailer_id IN (52, 97, 150)
                ORDER BY rp.barcode, p.created_at DESC NULLS LAST
            )
            INSERT INTO canonical_products (barcode, name, brand, description, image_url, source_retailer_id, last_scraped_at)
            SELECT * FROM barcode_data
            ON CONFLICT (barcode) DO UPDATE SET
                name = COALESCE(canonical_products.name, EXCLUDED.name),
                brand = COALESCE(canonical_products.brand, EXCLUDED.brand),
                description = COALESCE(canonical_products.description, EXCLUDED.description),
                image_url = COALESCE(canonical_products.image_url, EXCLUDED.image_url),
                source_retailer_id = COALESCE(canonical_products.source_retailer_id, EXCLUDED.source_retailer_id)
            RETURNING barcode
        """)

        migrated = self.cursor.fetchall()
        self.conn.commit()
        self.stats['migrated_products'] = len(migrated)
        logger.info(f"  Migrated {len(migrated):,} unique products to canonical_products")

    def verify_migration(self):
        """Verify the migration was successful"""
        logger.info("\n" + "="*60)
        logger.info("MIGRATION VERIFICATION")
        logger.info("="*60)

        # Check canonical_products
        self.cursor.execute("SELECT COUNT(*) FROM canonical_products")
        canonical_count = self.cursor.fetchone()[0]
        logger.info(f"Canonical products: {canonical_count:,}")

        # Check retailer_products with barcodes
        self.cursor.execute("""
            SELECT
                r.retailername,
                COUNT(*) as total,
                COUNT(rp.barcode) as with_barcode,
                ROUND(100.0 * COUNT(rp.barcode) / COUNT(*), 2) as pct
            FROM retailer_products rp
            JOIN retailers r ON rp.retailer_id = r.retailerid
            WHERE rp.retailer_id IN (52, 97, 150)
            GROUP BY r.retailername
            ORDER BY total DESC
        """)

        logger.info("\nProducts per retailer:")
        for retailer, total, with_barcode, pct in self.cursor.fetchall():
            logger.info(f"  {retailer}: {total:,} total, {with_barcode:,} with barcode ({pct}%)")

        # Check barcode overlap
        self.cursor.execute("""
            WITH barcode_retailers AS (
                SELECT
                    barcode,
                    COUNT(DISTINCT retailer_id) as retailer_count
                FROM retailer_products
                WHERE barcode IS NOT NULL
                GROUP BY barcode
            )
            SELECT
                retailer_count,
                COUNT(*) as barcode_count
            FROM barcode_retailers
            GROUP BY retailer_count
            ORDER BY retailer_count
        """)

        logger.info("\nBarcode sharing:")
        for retailer_count, barcode_count in self.cursor.fetchall():
            logger.info(f"  {barcode_count:,} barcodes in {retailer_count} retailer(s)")

        # Check prices linkage
        self.cursor.execute("""
            SELECT COUNT(DISTINCT p.price_id)
            FROM prices p
            JOIN retailer_products rp ON p.retailer_product_id = rp.retailer_product_id
            WHERE rp.barcode IS NOT NULL
        """)
        prices_with_barcode = self.cursor.fetchone()[0]
        logger.info(f"\nPrice records linked to barcoded products: {prices_with_barcode:,}")

    def generate_missing_barcode_report(self):
        """Generate report of products without barcodes"""
        logger.info("\nGenerating missing barcode report...")

        self.cursor.execute("""
            SELECT
                r.retailername,
                rp.retailer_item_code,
                rp.original_retailer_name,
                COUNT(DISTINCT p.price_id) as price_records
            FROM retailer_products rp
            JOIN retailers r ON rp.retailer_id = r.retailerid
            LEFT JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
            WHERE rp.barcode IS NULL
            AND rp.retailer_id IN (52, 97, 150)
            GROUP BY r.retailername, rp.retailer_item_code, rp.original_retailer_name
            ORDER BY price_records DESC
            LIMIT 20
        """)

        logger.info("\nTop products without barcodes:")
        for retailer, item_code, name, prices in self.cursor.fetchall():
            logger.info(f"  [{retailer}] {item_code}: {name[:50]} ({prices} prices)")

    def run(self, skip_verification: bool = False):
        """Run the complete migration"""
        try:
            logger.info("Starting data migration...")

            if not skip_verification:
                self.verify_pre_migration_state()

            # Step 1: Extract barcodes from products table
            barcode_map = self.extract_barcodes_from_products()

            # Step 2: Update retailer_products with barcodes
            self.map_retailer_products_to_barcodes(barcode_map)

            # Step 3: Populate canonical_products
            self.populate_canonical_products()

            # Step 4: Verify migration
            self.verify_migration()

            # Step 5: Generate reports
            self.generate_missing_barcode_report()

            # Print summary
            logger.info("\n" + "="*60)
            logger.info("MIGRATION SUMMARY")
            logger.info("="*60)
            for key, value in self.stats.items():
                logger.info(f"{key}: {value:,}")

        except Exception as e:
            logger.error(f"Migration failed: {e}")
            self.conn.rollback()
            raise
        finally:
            self.conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Migrate PharmMate data to new schema")
    parser.add_argument("--skip-verification", action="store_true",
                       help="Skip pre-migration verification")

    args = parser.parse_args()

    migrator = DataMigrator()
    migrator.run(skip_verification=args.skip_verification)