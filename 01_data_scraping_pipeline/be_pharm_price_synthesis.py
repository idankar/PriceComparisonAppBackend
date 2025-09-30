#!/usr/bin/env python3
"""
Be Pharm Price Synthesis Script
================================
This script synthesizes missing price data for Be Pharm products across all stores.

Context:
Be Pharm's daily data files contain only a small subset (~1,800 products per store)
of their total catalog. Over time, we've collected 12,023 unique products.
This script creates synthetic price records to ensure complete coverage.

Strategy:
1. For each product, find its most recent price from any store
2. Use this as the "national price" baseline
3. Back-fill missing (product, store) combinations with this baseline price
4. Mark synthetic prices with a flag for transparency
"""

import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import logging
from typing import Dict, List, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('be_pharm_price_synthesis.log')
    ]
)
logger = logging.getLogger(__name__)


class BePharmPriceSynthesizer:
    def __init__(self, dry_run: bool = False):
        """
        Initialize the price synthesizer

        Args:
            dry_run: If True, analyze but don't insert data
        """
        self.dry_run = dry_run
        self.RETAILER_ID = 150  # Be Pharm

        # Statistics tracking
        self.stats = {
            'total_products': 0,
            'total_stores': 0,
            'existing_combinations': 0,
            'missing_combinations': 0,
            'products_with_baseline': 0,
            'products_without_baseline': 0,
            'synthetic_prices_created': 0,
            'errors': 0
        }

        # Connect to database
        try:
            self.conn = psycopg2.connect(
                host="localhost",
                port=5432,
                database="price_comparison_app_v2",
                user="postgres",
                password="***REMOVED***"
            )
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to database (dry_run={dry_run})")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def analyze_current_coverage(self) -> Dict:
        """Analyze current price coverage for Be Pharm"""
        logger.info("Analyzing current Be Pharm price coverage...")

        # Get total unique products
        self.cursor.execute("""
            SELECT COUNT(DISTINCT product_id)
            FROM retailer_products
            WHERE retailer_id = %s
        """, (self.RETAILER_ID,))
        self.stats['total_products'] = self.cursor.fetchone()[0]

        # Get total stores
        self.cursor.execute("""
            SELECT COUNT(DISTINCT storeid)
            FROM stores
            WHERE retailerid = %s AND isactive = true
        """, (self.RETAILER_ID,))
        self.stats['total_stores'] = self.cursor.fetchone()[0]

        # Get current coverage - simplified query for performance
        self.cursor.execute("""
            SELECT COUNT(DISTINCT (retailer_product_id, store_id))
            FROM prices p
            JOIN retailer_products rp ON p.retailer_product_id = rp.retailer_product_id
            WHERE rp.retailer_id = %s
        """, (self.RETAILER_ID,))
        self.stats['existing_combinations'] = self.cursor.fetchone()[0]

        total_combinations = self.stats['total_products'] * self.stats['total_stores']
        self.stats['missing_combinations'] = total_combinations - self.stats['existing_combinations']

        # Calculate coverage percentage
        coverage_pct = (self.stats['existing_combinations'] / total_combinations * 100) if total_combinations > 0 else 0

        logger.info(f"Total products: {self.stats['total_products']:,}")
        logger.info(f"Total active stores: {self.stats['total_stores']:,}")
        logger.info(f"Total possible combinations: {total_combinations:,}")
        logger.info(f"Existing price records: {self.stats['existing_combinations']:,} ({coverage_pct:.1f}%)")
        logger.info(f"Missing price records: {self.stats['missing_combinations']:,} ({100-coverage_pct:.1f}%)")

        return self.stats

    def find_baseline_prices(self) -> Dict[int, float]:
        """
        Find the most recent price for each product across all stores.
        Uses window function for optimal performance.
        Returns dict mapping retailer_product_id -> baseline_price
        """
        logger.info("Finding baseline prices for all products...")

        # Using window function approach for better performance at scale
        self.cursor.execute("""
            WITH ranked_prices AS (
                SELECT
                    rp.retailer_product_id,
                    p.price,
                    ROW_NUMBER() OVER (
                        PARTITION BY rp.retailer_product_id
                        ORDER BY p.price_timestamp DESC
                    ) as rn
                FROM retailer_products rp
                INNER JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
                WHERE rp.retailer_id = %s
                AND p.price IS NOT NULL
            )
            SELECT retailer_product_id, price
            FROM ranked_prices
            WHERE rn = 1
        """, (self.RETAILER_ID,))

        baseline_prices = {}
        for row in self.cursor.fetchall():
            baseline_prices[row[0]] = float(row[1])

        self.stats['products_with_baseline'] = len(baseline_prices)
        self.stats['products_without_baseline'] = self.stats['total_products'] - len(baseline_prices)

        logger.info(f"Found baseline prices for {len(baseline_prices):,} products")
        logger.info(f"Products without any price: {self.stats['products_without_baseline']:,}")

        # Sample some baseline prices for verification
        if baseline_prices:
            sample = list(baseline_prices.items())[:5]
            logger.info("Sample baseline prices:")
            for prod_id, price in sample:
                logger.info(f"  Product {prod_id}: ₪{price:.2f}")

        return baseline_prices

    def synthesize_prices(self, baseline_prices: Dict[int, float], batch_size: int = 1000):
        """
        Create synthetic price records for missing (product, store) combinations

        Args:
            baseline_prices: Dict mapping retailer_product_id -> price
            batch_size: Number of records to insert per batch
        """
        if not baseline_prices:
            logger.warning("No baseline prices available. Cannot synthesize.")
            return

        logger.info(f"Starting price synthesis (dry_run={self.dry_run})...")

        # Ensure is_synthetic column exists ONCE before any batch inserts
        if not self.dry_run:
            self.add_synthetic_flag_column()

        # Get all active stores
        self.cursor.execute("""
            SELECT storeid, storename
            FROM stores
            WHERE retailerid = %s AND isactive = true
            ORDER BY storeid
        """, (self.RETAILER_ID,))
        stores = self.cursor.fetchall()

        # PRE-FETCH ALL EXISTING COMBINATIONS (Performance optimization)
        logger.info("Fetching existing price combinations...")
        self.cursor.execute("""
            SELECT DISTINCT retailer_product_id, store_id
            FROM prices
            WHERE retailer_product_id IN (
                SELECT retailer_product_id
                FROM retailer_products
                WHERE retailer_id = %s
            )
        """, (self.RETAILER_ID,))
        existing_combinations = {tuple(row) for row in self.cursor.fetchall()}
        logger.info(f"Found {len(existing_combinations):,} existing price combinations")

        # Use current timestamp for all synthetic prices
        synthetic_timestamp = datetime.now()

        # Track progress
        total_to_process = len(baseline_prices) * len(stores)
        processed = 0
        inserted = 0

        logger.info(f"Processing {len(baseline_prices):,} products × {len(stores)} stores = {total_to_process:,} combinations")

        # Prepare batch insert data
        batch_data = []

        for retailer_product_id, baseline_price in baseline_prices.items():
            for store_id, _ in stores:  # store_name not used
                # Fast in-memory check instead of database query
                if (retailer_product_id, store_id) not in existing_combinations:
                    # Missing price - add to batch with is_synthetic flag
                    batch_data.append((
                        retailer_product_id,
                        store_id,
                        baseline_price,
                        synthetic_timestamp,
                        synthetic_timestamp,  # scraped_at
                        None,  # promotion_id
                        True   # is_synthetic flag
                    ))

                    # Insert batch when it reaches batch_size
                    if len(batch_data) >= batch_size:
                        if not self.dry_run:
                            self._insert_price_batch(batch_data)
                        inserted += len(batch_data)
                        logger.info(f"Progress: {processed:,}/{total_to_process:,} checked, {inserted:,} synthesized")
                        batch_data = []

                processed += 1

                # Progress update every 10,000 combinations
                if processed % 10000 == 0:
                    logger.info(f"Progress: {processed:,}/{total_to_process:,} combinations checked")

        # Insert remaining batch
        if batch_data:
            if not self.dry_run:
                self._insert_price_batch(batch_data)
            inserted += len(batch_data)

        self.stats['synthetic_prices_created'] = inserted

        if self.dry_run:
            logger.info(f"DRY RUN: Would have created {inserted:,} synthetic price records")
        else:
            logger.info(f"Successfully created {inserted:,} synthetic price records")

    def _insert_price_batch(self, batch_data: List[Tuple]):
        """Insert a batch of price records with is_synthetic flag"""
        try:
            execute_values(
                self.cursor,
                """
                INSERT INTO prices (
                    retailer_product_id, store_id, price,
                    price_timestamp, scraped_at, promotion_id, is_synthetic
                )
                VALUES %s
                ON CONFLICT (retailer_product_id, store_id, price_timestamp, scraped_at)
                DO NOTHING
                """,
                batch_data,
                template="(%s, %s, %s, %s, %s, %s, %s)"
            )
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error inserting batch: {e}")
            self.conn.rollback()
            self.stats['errors'] += 1

    def add_synthetic_flag_column(self):
        """Add a column to track synthetic prices (optional enhancement)"""
        try:
            logger.info("Checking if is_synthetic column exists...")

            self.cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'prices'
                AND column_name = 'is_synthetic'
            """)

            if not self.cursor.fetchone():
                logger.info("Adding is_synthetic column to prices table...")
                if not self.dry_run:
                    self.cursor.execute("""
                        ALTER TABLE prices
                        ADD COLUMN IF NOT EXISTS is_synthetic BOOLEAN DEFAULT FALSE
                    """)
                    self.conn.commit()
                    logger.info("Added is_synthetic column successfully")
                else:
                    logger.info("DRY RUN: Would add is_synthetic column")
            else:
                logger.info("is_synthetic column already exists")

        except Exception as e:
            logger.error(f"Error adding synthetic flag column: {e}")
            self.conn.rollback()

    def verify_synthesis(self):
        """Verify the results of price synthesis"""
        logger.info("\nVerifying synthesis results...")

        # Re-analyze coverage
        self.cursor.execute("""
            WITH coverage AS (
                SELECT
                    rp.retailer_product_id,
                    s.storeid,
                    MAX(CASE WHEN p.price_id IS NOT NULL THEN 1 ELSE 0 END) as has_price
                FROM retailer_products rp
                CROSS JOIN stores s
                LEFT JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
                    AND p.store_id = s.storeid
                WHERE rp.retailer_id = %s
                AND s.retailerid = %s
                AND s.isactive = true
                GROUP BY rp.retailer_product_id, s.storeid
            )
            SELECT
                COUNT(*) as total_combinations,
                SUM(has_price) as with_prices,
                COUNT(*) - SUM(has_price) as without_prices,
                ROUND(100.0 * SUM(has_price) / COUNT(*), 2) as coverage_pct
            FROM coverage
        """, (self.RETAILER_ID, self.RETAILER_ID))

        result = self.cursor.fetchone()

        logger.info(f"Total combinations: {result[0]:,}")
        logger.info(f"With prices: {result[1]:,}")
        logger.info(f"Without prices: {result[2]:,}")
        logger.info(f"Coverage: {result[3]}%")

        # Check price distribution
        self.cursor.execute("""
            SELECT
                COUNT(DISTINCT price) as unique_prices,
                MIN(price) as min_price,
                AVG(price)::numeric(10,2) as avg_price,
                MAX(price) as max_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price
            FROM prices p
            JOIN retailer_products rp ON p.retailer_product_id = rp.retailer_product_id
            WHERE rp.retailer_id = %s
        """, (self.RETAILER_ID,))

        stats = self.cursor.fetchone()
        logger.info(f"\nPrice statistics:")
        logger.info(f"  Unique prices: {stats[0]:,}")
        logger.info(f"  Min price: ₪{stats[1]:.2f}")
        logger.info(f"  Avg price: ₪{stats[2]:.2f}")
        logger.info(f"  Median price: ₪{stats[4]:.2f}")
        logger.info(f"  Max price: ₪{stats[3]:.2f}")

    def synthesize_prices_sql(self):
        """
        Alternative: Pure SQL approach for maximum performance.
        Generates and executes a single SQL statement to insert all missing prices.
        """
        if self.dry_run:
            logger.info("DRY RUN: Calculating synthesis via SQL...")
        else:
            logger.info("Executing pure SQL synthesis approach...")

        try:
            # First ensure is_synthetic column exists
            self.cursor.execute("""
                ALTER TABLE prices
                ADD COLUMN IF NOT EXISTS is_synthetic BOOLEAN DEFAULT FALSE
            """)

            # Simpler count approach - just calculate mathematically
            self.cursor.execute("""
                SELECT
                    (SELECT COUNT(DISTINCT rp.retailer_product_id)
                     FROM retailer_products rp
                     WHERE rp.retailer_id = %s
                     AND EXISTS (
                         SELECT 1 FROM prices p
                         WHERE p.retailer_product_id = rp.retailer_product_id
                     )) as products_with_prices,
                    (SELECT COUNT(*) FROM stores
                     WHERE retailerid = %s AND isactive = true) as active_stores,
                    (SELECT COUNT(DISTINCT (p.retailer_product_id, p.store_id))
                     FROM prices p
                     JOIN retailer_products rp ON p.retailer_product_id = rp.retailer_product_id
                     WHERE rp.retailer_id = %s) as existing_combinations
            """, (self.RETAILER_ID, self.RETAILER_ID, self.RETAILER_ID))

            result = self.cursor.fetchone()
            products_with_prices = result[0]
            active_stores = result[1]
            existing_combinations = result[2]

            # Calculate how many new combinations we'll create
            total_possible = products_with_prices * active_stores
            count = total_possible - existing_combinations

            logger.info(f"Products with prices: {products_with_prices:,}")
            logger.info(f"Active stores: {active_stores}")
            logger.info(f"Existing combinations: {existing_combinations:,}")
            logger.info(f"Will create {count:,} synthetic price records")

            if not self.dry_run and count > 0:
                # Execute the actual insert using more efficient CTE approach
                logger.info("Inserting synthetic prices (this may take a moment)...")
                self.cursor.execute("""
                    WITH baseline_prices AS (
                        -- Get the latest price for each product
                        SELECT DISTINCT ON (retailer_product_id)
                            retailer_product_id,
                            price as baseline_price
                        FROM prices
                        WHERE retailer_product_id IN (
                            SELECT retailer_product_id
                            FROM retailer_products
                            WHERE retailer_id = %s
                        )
                        ORDER BY retailer_product_id, price_timestamp DESC
                    ),
                    all_combinations AS (
                        -- Generate all product-store combinations that should exist
                        SELECT
                            bp.retailer_product_id,
                            s.storeid as store_id,
                            bp.baseline_price
                        FROM baseline_prices bp
                        CROSS JOIN stores s
                        WHERE s.retailerid = %s
                        AND s.isactive = true
                    ),
                    missing_combinations AS (
                        -- Filter to only missing combinations
                        SELECT ac.*
                        FROM all_combinations ac
                        LEFT JOIN prices p ON (
                            p.retailer_product_id = ac.retailer_product_id
                            AND p.store_id = ac.store_id
                        )
                        WHERE p.price_id IS NULL
                    )
                    INSERT INTO prices (
                        retailer_product_id, store_id, price,
                        price_timestamp, scraped_at, is_synthetic
                    )
                    SELECT
                        retailer_product_id,
                        store_id,
                        baseline_price,
                        NOW(),
                        NOW(),
                        true
                    FROM missing_combinations
                    ON CONFLICT (retailer_product_id, store_id, price_timestamp, scraped_at)
                    DO NOTHING
                """, (self.RETAILER_ID, self.RETAILER_ID))

                rows_inserted = self.cursor.rowcount
                self.conn.commit()
                self.stats['synthetic_prices_created'] = rows_inserted
                logger.info(f"Successfully inserted {rows_inserted:,} synthetic prices")
            elif self.dry_run:
                self.stats['synthetic_prices_created'] = count
                logger.info(f"DRY RUN: Would insert {count:,} synthetic prices")

        except Exception as e:
            logger.error(f"Error in SQL synthesis: {e}")
            self.conn.rollback()
            self.stats['errors'] += 1
            raise

    def create_backup_command(self) -> str:
        """Generate backup command for user"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = f"be_pharm_backup_{timestamp}.sql"

        command = f"""
# Create full database backup
pg_dump -h localhost -p 5432 -U postgres -d price_comparison_app_v2 -f {backup_file}

# Or backup only Be Pharm data:
pg_dump -h localhost -p 5432 -U postgres -d price_comparison_app_v2 \\
    -t "retailer_products" -t "prices" -t "stores" -t "promotions" \\
    --where="retailer_id=150" -f be_pharm_only_{timestamp}.sql
"""
        return command

    def run(self, dry_run: bool = False, use_sql_approach: bool = False):
        """
        Main execution method

        Args:
            dry_run: If True, analyze but don't modify database
            use_sql_approach: If True, use pure SQL approach instead of Python loops
        """
        try:
            self.dry_run = dry_run

            logger.info("="*80)
            logger.info("BE PHARM PRICE SYNTHESIS")
            logger.info(f"Mode: {'DRY RUN' if dry_run else 'PRODUCTION'}")
            logger.info(f"Approach: {'Pure SQL' if use_sql_approach else 'Python Batch'}")
            logger.info("="*80)

            if not dry_run:
                logger.warning("IMPORTANT: Create a database backup before proceeding!")
                logger.warning("Run this command:")
                logger.warning(self.create_backup_command())
                logger.warning("Press Ctrl+C to cancel if backup not created")
                import time
                time.sleep(5)  # Give user time to cancel

            # Step 1: Analyze current coverage
            self.analyze_current_coverage()

            if use_sql_approach:
                # Use pure SQL approach for maximum performance
                self.synthesize_prices_sql()
            else:
                # Use Python batch approach
                # Step 2: Find baseline prices
                baseline_prices = self.find_baseline_prices()

                if not baseline_prices:
                    logger.warning("No baseline prices found. Cannot proceed with synthesis.")
                    return

                # Step 3: Optionally add synthetic flag column
                if not dry_run:
                    self.add_synthetic_flag_column()

                # Step 4: Synthesize missing prices
                self.synthesize_prices(baseline_prices)

            # Step 5: Verify results
            self.verify_synthesis()

            # Print summary
            self.print_summary()

        except Exception as e:
            logger.error(f"Fatal error during synthesis: {e}")
            raise
        finally:
            self.cleanup()

    def print_summary(self):
        """Print execution summary"""
        logger.info("\n" + "="*80)
        logger.info("SYNTHESIS SUMMARY")
        logger.info("="*80)
        logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'PRODUCTION'}")
        logger.info(f"Total products: {self.stats['total_products']:,}")
        logger.info(f"Total stores: {self.stats['total_stores']:,}")
        logger.info(f"Products with baseline price: {self.stats['products_with_baseline']:,}")
        logger.info(f"Products without any price: {self.stats['products_without_baseline']:,}")
        logger.info(f"Initial missing combinations: {self.stats['missing_combinations']:,}")
        logger.info(f"Synthetic prices created: {self.stats['synthetic_prices_created']:,}")
        logger.info(f"Errors encountered: {self.stats['errors']}")
        logger.info("="*80)

    def cleanup(self):
        """Clean up database connection"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Synthesize missing Be Pharm price data"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Analyze but don't insert data (recommended for first run)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of records to insert per batch (default: 1000)"
    )
    parser.add_argument(
        "--sql-approach",
        action="store_true",
        help="Use pure SQL approach for maximum performance (recommended)"
    )

    args = parser.parse_args()

    try:
        synthesizer = BePharmPriceSynthesizer(dry_run=args.dry_run)
        synthesizer.run(dry_run=args.dry_run, use_sql_approach=args.sql_approach)
    except Exception as e:
        logger.error(f"Synthesis failed: {e}")
        exit(1)