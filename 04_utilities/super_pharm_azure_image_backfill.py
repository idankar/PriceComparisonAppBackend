#!/usr/bin/env python3
"""
Super-Pharm Azure Image Backfill Script

This script backfills missing images for Super-Pharm products by:
1. Constructing Azure Blob Storage URLs directly from barcodes
2. Verifying images exist with HTTP HEAD requests (fast, no downloads)
3. Validating image URLs thoroughly before database insertion
4. Running comprehensive tests before any DB writes

WHY THIS APPROACH:
- Super-Pharm stores images on Azure: https://superpharmstorage.blob.core.windows.net/hybris/products/desktop/small/{barcode}.jpg
- No Selenium needed - just HTTP requests
- Fast and reliable
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
from dotenv import load_dotenv
from datetime import datetime
import json
import logging
from collections import defaultdict
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('super_pharm_azure_backfill.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment
load_dotenv()

DB_NAME = os.getenv("DB_NAME", "price_comparison_app_v2")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "025655358")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

SUPER_PHARM_RETAILER_ID = 52
GOOD_PHARM_RETAILER_ID = 97
AZURE_IMAGE_URL_TEMPLATE = "https://superpharmstorage.blob.core.windows.net/hybris/products/desktop/small/{barcode}.jpg"

# Known placeholder patterns to reject
PLACEHOLDER_PATTERNS = [
    'placeholder',
    'no-image',
    'default',
    'missing',
    'notfound',
    '404',
    'blank',
    'generic'
]

class SuperPharmAzureBackfiller:
    def __init__(self, test_mode=True, batch_size=100, auto_confirm=False):
        self.test_mode = test_mode
        self.batch_size = batch_size
        self.auto_confirm = auto_confirm
        self.session = self._create_session()
        self.conn = None
        self.stats = {
            'total_products': 0,
            'images_verified': 0,
            'images_not_found': 0,
            'invalid_images': 0,
            'db_updated': 0,
            'errors': 0
        }
        self.verified_images = []  # Store verified images for batch update

    def _create_session(self):
        """Create requests session with retry logic"""
        session = requests.Session()

        # Retry strategy: 3 retries with exponential backoff
        retry = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=(500, 502, 504)
        )

        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        # Timeout for requests
        session.timeout = 5

        return session

    def _connect_db(self):
        """Connect to database"""
        try:
            self.conn = psycopg2.connect(
                dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
                host=DB_HOST, port=DB_PORT
            )
            logger.info("✅ Database connected")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise

    def _close_db(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("🔌 Database connection closed")

    def get_products_missing_images(self, limit=None):
        """Get ALL inactive Good Pharm products without images (for Azure backfill)"""
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)

        query = """
            SELECT DISTINCT cp.barcode, cp.name, cp.category
            FROM canonical_products cp
            JOIN retailer_products rp ON cp.barcode = rp.barcode
            WHERE rp.retailer_id = %s
            AND cp.is_active = FALSE
            AND cp.image_url IS NULL
            ORDER BY cp.barcode
        """

        if limit:
            query += f" LIMIT {limit}"

        cursor.execute(query, (GOOD_PHARM_RETAILER_ID,))
        products = cursor.fetchall()
        cursor.close()

        logger.info(f"📦 Found {len(products)} Good Pharm inactive products without images")
        return products

    def construct_azure_url(self, barcode):
        """Construct Azure Blob Storage URL from barcode"""
        return AZURE_IMAGE_URL_TEMPLATE.format(barcode=barcode)

    def validate_image_url(self, url, barcode):
        """
        Thoroughly validate image URL before accepting it.
        Returns (is_valid, reason)
        """
        # Check for placeholder patterns (but exclude the barcode part to avoid false positives)
        url_lower = url.lower()
        barcode_lower = barcode.lower()
        url_without_barcode = url_lower.replace(barcode_lower, '')
        for pattern in PLACEHOLDER_PATTERNS:
            if pattern in url_without_barcode:
                return False, f"Contains placeholder pattern: {pattern}"

        # Must be HTTPS
        if not url.startswith('https://'):
            return False, "Not HTTPS"

        # Must be from Super-Pharm's Azure storage
        if 'superpharmstorage.blob.core.windows.net' not in url:
            return False, "Not from Super-Pharm Azure storage"

        # Must end with .jpg
        if not url.endswith('.jpg'):
            return False, "Not a JPG image"

        # Must contain the barcode in the URL (ensures it's product-specific)
        if barcode not in url:
            return False, f"Barcode {barcode} not in URL"

        # URL should be reasonable length (not too short or too long)
        if len(url) < 50 or len(url) > 200:
            return False, f"Unusual URL length: {len(url)}"

        return True, "Valid"

    def verify_image_exists(self, url, barcode):
        """
        Verify image exists on Azure with HTTP HEAD request.
        Returns (exists, status_code, validation_result)
        """
        try:
            # First validate the URL structure
            is_valid, reason = self.validate_image_url(url, barcode)
            if not is_valid:
                return False, None, f"Invalid URL: {reason}"

            # Send HEAD request (doesn't download image, just checks if it exists)
            response = self.session.head(url, timeout=5, allow_redirects=True)

            if response.status_code == 200:
                # Additional check: Content-Type should be image/jpeg
                content_type = response.headers.get('Content-Type', '')
                if 'image' not in content_type.lower():
                    return False, response.status_code, f"Not an image (Content-Type: {content_type})"

                # Check Content-Length (should be reasonable for a product image)
                content_length = response.headers.get('Content-Length')
                if content_length:
                    size_kb = int(content_length) / 1024
                    if size_kb < 1:  # Less than 1KB is suspicious
                        return False, response.status_code, f"Image too small: {size_kb:.1f}KB"
                    if size_kb > 5000:  # More than 5MB is suspicious
                        return False, response.status_code, f"Image too large: {size_kb:.1f}KB"

                return True, response.status_code, "Valid"
            else:
                return False, response.status_code, f"HTTP {response.status_code}"

        except requests.exceptions.Timeout:
            return False, None, "Timeout"
        except requests.exceptions.RequestException as e:
            return False, None, f"Request error: {str(e)[:50]}"
        except Exception as e:
            return False, None, f"Error: {str(e)[:50]}"

    def test_verification(self, sample_size=10):
        """
        Test the verification process on a sample before running full backfill.
        Returns True if tests pass, False otherwise.
        """
        logger.info("="*80)
        logger.info("🧪 RUNNING PRE-FLIGHT TESTS")
        logger.info("="*80)

        # Get a small sample
        products = self.get_products_missing_images(limit=sample_size)

        if not products:
            logger.warning("⚠️  No products to test!")
            return False

        test_results = {
            'verified': 0,
            'not_found': 0,
            'invalid': 0
        }

        logger.info(f"\n📋 Testing {len(products)} sample products...\n")

        for product in products:
            barcode = product['barcode']
            name = product['name']

            # Construct URL
            url = self.construct_azure_url(barcode)

            # Verify
            exists, status, validation = self.verify_image_exists(url, barcode)

            if exists:
                test_results['verified'] += 1
                logger.info(f"✅ {barcode[:15]:15s} | {status} | {name[:40]}")
            elif status == 404:
                test_results['not_found'] += 1
                logger.info(f"❌ {barcode[:15]:15s} | 404 | {name[:40]}")
            else:
                test_results['invalid'] += 1
                logger.warning(f"⚠️  {barcode[:15]:15s} | {validation} | {name[:40]}")

        # Calculate test success rate
        total = len(products)
        verified_pct = (test_results['verified'] / total * 100) if total > 0 else 0
        not_found_pct = (test_results['not_found'] / total * 100) if total > 0 else 0
        invalid_pct = (test_results['invalid'] / total * 100) if total > 0 else 0

        logger.info("\n" + "="*80)
        logger.info("🧪 TEST RESULTS")
        logger.info("="*80)
        logger.info(f"Verified images:   {test_results['verified']:3d} / {total} ({verified_pct:5.1f}%)")
        logger.info(f"Not found (404):   {test_results['not_found']:3d} / {total} ({not_found_pct:5.1f}%)")
        logger.info(f"Invalid/Errors:    {test_results['invalid']:3d} / {total} ({invalid_pct:5.1f}%)")
        logger.info("="*80)

        # Test passes if we have at least 20% success rate and no invalid results
        if verified_pct >= 20 and test_results['invalid'] == 0:
            logger.info(f"✅ Tests PASSED! Expected recovery: ~{verified_pct:.0f}% of {7276} products = ~{int(7276 * verified_pct / 100)} images")
            return True
        else:
            logger.error("❌ Tests FAILED! Aborting backfill.")
            if test_results['invalid'] > 0:
                logger.error(f"   Reason: {test_results['invalid']} invalid results detected")
            else:
                logger.error(f"   Reason: Success rate too low ({verified_pct:.1f}%)")
            return False

    def process_products(self, products):
        """Process products and verify images"""
        logger.info("="*80)
        logger.info("🚀 PROCESSING PRODUCTS")
        logger.info("="*80)

        self.stats['total_products'] = len(products)

        for i, product in enumerate(products, 1):
            barcode = product['barcode']
            name = product['name']

            # Construct Azure URL
            url = self.construct_azure_url(barcode)

            # Verify image exists
            exists, status, validation = self.verify_image_exists(url, barcode)

            if exists:
                self.stats['images_verified'] += 1
                self.verified_images.append({
                    'barcode': barcode,
                    'image_url': url,
                    'name': name
                })

                # Progress logging
                if i % 100 == 0 or i <= 10:
                    logger.info(f"✅ [{i:5d}/{len(products)}] {barcode[:15]:15s} | Verified")

            elif status == 404:
                self.stats['images_not_found'] += 1
                if i <= 10:  # Log first few 404s
                    logger.debug(f"❌ [{i:5d}/{len(products)}] {barcode[:15]:15s} | 404 Not Found")
            else:
                self.stats['invalid_images'] += 1
                logger.warning(f"⚠️  [{i:5d}/{len(products)}] {barcode[:15]:15s} | {validation}")

            # Small delay to avoid overwhelming Azure
            if i % 100 == 0:
                time.sleep(0.5)
                self.print_progress()

        logger.info(f"\n✅ Processing complete: {self.stats['images_verified']} images verified")

    def update_database(self):
        """Update database with verified images"""
        if not self.verified_images:
            logger.warning("⚠️  No images to update in database")
            return

        if self.test_mode:
            logger.info("="*80)
            logger.info("🧪 TEST MODE - Database will NOT be updated")
            logger.info("="*80)
            logger.info(f"Would update {len(self.verified_images)} products with images")
            logger.info("\nSample of verified images:")
            for img in self.verified_images[:5]:
                logger.info(f"  {img['barcode']}: {img['image_url']}")
            logger.info(f"\n... and {len(self.verified_images) - 5} more")
            return

        logger.info("="*80)
        logger.info(f"💾 UPDATING DATABASE - {len(self.verified_images)} products")
        logger.info("="*80)

        cursor = self.conn.cursor()

        try:
            # Update in batches
            for i in range(0, len(self.verified_images), self.batch_size):
                batch = self.verified_images[i:i + self.batch_size]

                for item in batch:
                    cursor.execute("""
                        UPDATE canonical_products
                        SET image_url = %s,
                            last_scraped_at = %s
                        WHERE barcode = %s
                    """, (item['image_url'], datetime.now(), item['barcode']))

                    self.stats['db_updated'] += cursor.rowcount

                self.conn.commit()
                logger.info(f"  💾 Committed batch {i//self.batch_size + 1}/{(len(self.verified_images)-1)//self.batch_size + 1}")

            logger.info(f"✅ Database updated: {self.stats['db_updated']} products")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Database update failed: {e}")
            self.stats['errors'] += 1
        finally:
            cursor.close()

    def print_progress(self):
        """Print progress statistics"""
        total = self.stats['total_products']
        verified = self.stats['images_verified']
        not_found = self.stats['images_not_found']
        invalid = self.stats['invalid_images']

        processed = verified + not_found + invalid

        if processed > 0:
            logger.info(f"\n📊 Progress: {processed}/{total} processed | "
                       f"Verified: {verified} ({verified/processed*100:.1f}%) | "
                       f"404: {not_found} | Invalid: {invalid}")

    def print_final_stats(self):
        """Print final statistics"""
        logger.info("\n" + "="*80)
        logger.info("📊 FINAL STATISTICS")
        logger.info("="*80)
        logger.info(f"Total products processed:  {self.stats['total_products']}")
        logger.info(f"Images verified:           {self.stats['images_verified']} ({self.stats['images_verified']/self.stats['total_products']*100:.1f}%)")
        logger.info(f"Images not found (404):    {self.stats['images_not_found']} ({self.stats['images_not_found']/self.stats['total_products']*100:.1f}%)")
        logger.info(f"Invalid/Errors:            {self.stats['invalid_images']}")

        if not self.test_mode:
            logger.info(f"Database updated:          {self.stats['db_updated']}")
        else:
            logger.info(f"Database updated:          0 (TEST MODE)")

        logger.info("="*80)

    def run(self, limit=None, skip_tests=False):
        """Main execution"""
        logger.info("="*80)
        logger.info("🔍 GOOD PHARM IMAGE BACKFILL (via Super-Pharm Azure)")
        logger.info("="*80)
        logger.info(f"Mode: {'TEST (no DB writes)' if self.test_mode else 'PRODUCTION (will update DB)'}")
        if limit:
            logger.info(f"Limit: {limit} products")
        logger.info("="*80)

        try:
            # Connect to database
            self._connect_db()

            # Run pre-flight tests (unless skipped)
            if not skip_tests:
                if not self.test_verification(sample_size=20):
                    logger.error("❌ Pre-flight tests failed. Aborting.")
                    return False

                # Only prompt in production mode
                if not self.test_mode:
                    logger.info("\n" + "="*80)
                    if not self.auto_confirm:
                        input("⏸️  Tests passed. Press ENTER to continue with full backfill (or Ctrl+C to abort)...")
                    else:
                        logger.info("⏸️  Tests passed. Auto-proceeding with --yes flag...")
                    logger.info("="*80)
                else:
                    logger.info("\n🧪 Test mode: Auto-proceeding with full test run...")
                    logger.info("="*80)

            # Get products
            products = self.get_products_missing_images(limit=limit)

            if not products:
                logger.info("✅ No products to process")
                return True

            # Process products
            self.process_products(products)

            # Update database
            self.update_database()

            # Print final stats
            self.print_final_stats()

            return True

        except KeyboardInterrupt:
            logger.warning("\n⚠️  Process interrupted by user")
            return False
        except Exception as e:
            logger.error(f"❌ Fatal error: {e}")
            return False
        finally:
            self._close_db()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Backfill Super-Pharm images from Azure Blob Storage')
    parser.add_argument('--production', action='store_true',
                       help='Run in PRODUCTION mode (will update database). Default is TEST mode.')
    parser.add_argument('--limit', type=int,
                       help='Limit number of products to process')
    parser.add_argument('--skip-tests', action='store_true',
                       help='Skip pre-flight tests (not recommended)')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Database update batch size')
    parser.add_argument('--yes', action='store_true',
                       help='Skip confirmation prompt in production mode')

    args = parser.parse_args()

    # Warn if production mode
    if args.production:
        logger.warning("="*80)
        logger.warning("⚠️  PRODUCTION MODE - Database WILL be updated!")
        logger.warning("="*80)
        if not args.yes:
            response = input("Are you sure? Type 'YES' to continue: ")
            if response != 'YES':
                logger.info("Aborting.")
                exit(0)
        else:
            logger.info("Auto-confirmed with --yes flag")

    backfiller = SuperPharmAzureBackfiller(
        test_mode=not args.production,
        batch_size=args.batch_size,
        auto_confirm=args.yes
    )

    success = backfiller.run(limit=args.limit, skip_tests=args.skip_tests)

    exit(0 if success else 1)
