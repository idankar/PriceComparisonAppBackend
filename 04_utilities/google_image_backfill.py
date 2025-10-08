#!/usr/bin/env python3
"""
Google Images API Backfill Script

Uses Google Custom Search JSON API to find missing product images for inactive Good Pharm products.
Requires Google API credentials configured in .env file.

Setup:
1. Get Google API Key: https://console.developers.google.com/
2. Create Custom Search Engine: https://cse.google.com/cse/
3. Add to .env file:
   GOOGLE_API_KEY=your_key_here
   GOOGLE_CSE_ID=your_cse_id_here
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
from dotenv import load_dotenv
from datetime import datetime
import logging
import time
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('google_image_backfill.log'),
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

# Google API credentials
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
GOOGLE_CSE_ID = os.getenv("GOOGLE_CSE_ID", "")

# Google Custom Search API endpoint
GOOGLE_SEARCH_URL = "https://www.googleapis.com/customsearch/v1"

# Checkpoint file for resuming
CHECKPOINT_FILE = "google_image_backfill_checkpoint.json"


class GoogleImageBackfiller:
    def __init__(self, test_mode=True, limit=None, batch_size=100):
        self.test_mode = test_mode
        self.limit = limit
        self.batch_size = batch_size
        self.session = self._create_session()
        self.conn = None
        self.stats = {
            'total_products': 0,
            'images_found': 0,
            'images_not_found': 0,
            'api_errors': 0,
            'db_updated': 0,
            'api_calls': 0,
            'skipped': 0
        }
        self.verified_images = []  # Store verified images for batch update
        self.checkpoint = self._load_checkpoint()

    def _load_checkpoint(self):
        """Load checkpoint from file"""
        if os.path.exists(CHECKPOINT_FILE):
            try:
                with open(CHECKPOINT_FILE, 'r') as f:
                    checkpoint = json.load(f)
                    logger.info(f"📍 Loaded checkpoint: {checkpoint['processed']} products already processed")
                    return checkpoint
            except:
                pass
        return {'processed': 0, 'last_barcode': None, 'timestamp': None}

    def _save_checkpoint(self, processed, last_barcode):
        """Save checkpoint to file"""
        checkpoint = {
            'processed': processed,
            'last_barcode': last_barcode,
            'timestamp': datetime.now().isoformat()
        }
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f)

    def _create_session(self):
        """Create requests session with retry logic"""
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=(500, 502, 504, 429)
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
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

    def get_products_missing_images(self):
        """Get Good Pharm inactive products without images"""
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)

        query = """
            SELECT DISTINCT cp.barcode, cp.name, cp.brand, cp.category
            FROM canonical_products cp
            JOIN retailer_products rp ON cp.barcode = rp.barcode
            WHERE rp.retailer_id = 97
            AND cp.is_active = FALSE
            AND cp.image_url IS NULL
            ORDER BY cp.barcode
        """

        if self.limit:
            query += f" LIMIT {self.limit}"

        cursor.execute(query)
        products = cursor.fetchall()
        cursor.close()

        logger.info(f"📦 Found {len(products)} Good Pharm products without images")
        return products

    def construct_search_query(self, product):
        """
        Construct optimal Google search query from product data
        Format: "brand product_name barcode" for best accuracy
        """
        brand = product.get('brand', '')
        name = product.get('name', '')
        barcode = product.get('barcode', '')

        # Clean up brand
        if brand and brand != 'לא ידוע' and brand != '':
            # Remove common suffixes
            brand = brand.replace(' - ', ' ').strip()
        else:
            brand = ''

        # Clean up name - remove brand if it's at the start
        if brand and name.startswith(brand):
            name = name[len(brand):].strip()
            if name.startswith('-'):
                name = name[1:].strip()

        # Construct query with barcode for better accuracy
        if brand:
            query = f"{brand} {name} {barcode}"
        else:
            query = f"{name} {barcode}"

        # Remove extra spaces and limit length
        query = ' '.join(query.split())
        if len(query) > 120:
            query = query[:120]

        return query

    def search_google_images(self, query, barcode):
        """
        Search Google Custom Search API for images
        Returns the best image URL or None
        """
        if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
            logger.error("❌ Google API credentials not configured!")
            return None

        try:
            params = {
                'key': GOOGLE_API_KEY,
                'cx': GOOGLE_CSE_ID,
                'q': query,
                'searchType': 'image',
                'num': 3,  # Get top 3 results
                'imgSize': 'medium',  # Prefer medium-sized images
                'safe': 'off'
            }

            response = self.session.get(GOOGLE_SEARCH_URL, params=params, timeout=10)
            self.stats['api_calls'] += 1

            if response.status_code == 200:
                data = response.json()

                # Check if we got results
                if 'items' in data and len(data['items']) > 0:
                    # IMPROVED: Try to find an image with barcode in URL first (most reliable)
                    for item in data['items']:
                        image_url = item['link']
                        if barcode and barcode in image_url:
                            if self.validate_image_url(image_url, barcode):
                                logger.debug(f"Found barcode match in URL: {image_url}")
                                return image_url

                    # If no barcode match, try first result
                    image_url = data['items'][0]['link']
                    if self.validate_image_url(image_url, barcode):
                        return image_url

                    # Try second result if first fails
                    if len(data['items']) > 1:
                        image_url = data['items'][1]['link']
                        if self.validate_image_url(image_url, barcode):
                            return image_url

                    # Try third result if second fails
                    if len(data['items']) > 2:
                        image_url = data['items'][2]['link']
                        if self.validate_image_url(image_url, barcode):
                            return image_url

                logger.debug(f"No suitable image found for: {query}")
                return None

            elif response.status_code == 429:
                logger.warning("⚠️  API quota exceeded! Pausing for 60 seconds...")
                time.sleep(60)
                return None
            else:
                logger.warning(f"API error {response.status_code}: {response.text[:100]}")
                self.stats['api_errors'] += 1
                return None

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout searching for: {query}")
            return None
        except Exception as e:
            logger.error(f"Error searching for '{query}': {str(e)[:100]}")
            self.stats['api_errors'] += 1
            return None

    def validate_image_url(self, url, barcode=None):
        """
        Validate image URL:
        - Must be HTTP/HTTPS
        - Should end with common image extension or be from known CDN
        - Should not be a placeholder
        - IMPROVED: Prefer URLs that contain the barcode
        """
        if not url:
            return False

        url_lower = url.lower()

        # Must be HTTP/HTTPS
        if not url.startswith('http://') and not url.startswith('https://'):
            return False

        # Check for placeholder patterns
        bad_patterns = ['placeholder', 'no-image', 'default', 'missing', 'blank', 'generic']
        for pattern in bad_patterns:
            if pattern in url_lower:
                return False

        # Must have reasonable length
        if len(url) < 10 or len(url) > 500:
            return False

        # Prefer common image extensions or known CDNs
        image_extensions = ['.jpg', '.jpeg', '.png', '.webp', '.gif']
        known_cdns = ['cloudinary', 'imgix', 'cloudfront', 'amazonaws', 'googleusercontent', 'wikimedia']

        has_extension = any(ext in url_lower for ext in image_extensions)
        is_cdn = any(cdn in url_lower for cdn in known_cdns)

        if not (has_extension or is_cdn):
            return False

        # IMPROVED: Bonus if barcode is in URL (much more likely to be correct product)
        if barcode and barcode in url:
            return True

        return True

    def verify_image_exists(self, url):
        """
        Verify image URL is accessible with HEAD request
        Returns (exists, reason)
        """
        try:
            response = self.session.head(url, timeout=5, allow_redirects=True)
            if response.status_code == 200:
                # Check if it's actually an image
                content_type = response.headers.get('Content-Type', '')
                if 'image' in content_type.lower():
                    return True, "Valid"
                else:
                    return False, f"Not an image (Content-Type: {content_type})"
            else:
                return False, f"HTTP {response.status_code}"
        except:
            # If HEAD fails, image might still be valid (some servers don't support HEAD)
            return True, "HEAD request failed, assuming valid"

    def process_products(self, products):
        """Process products and search for images"""
        logger.info("="*80)
        logger.info("🚀 SEARCHING FOR IMAGES")
        logger.info("="*80)

        self.stats['total_products'] = len(products)
        start_index = self.checkpoint.get('processed', 0)

        for i, product in enumerate(products[start_index:], start=start_index+1):
            barcode = product['barcode']
            name = product['name']
            brand = product.get('brand', '')

            # Construct search query
            query = self.construct_search_query(product)

            # Search Google Images
            image_url = self.search_google_images(query, barcode)

            if image_url:
                # Verify image exists
                exists, reason = self.verify_image_exists(image_url)

                if exists:
                    self.stats['images_found'] += 1
                    self.verified_images.append({
                        'barcode': barcode,
                        'image_url': image_url,
                        'name': name
                    })

                    # Progress logging
                    if i % 10 == 0 or i <= 10:
                        logger.info(f"✅ [{i:5d}/{len(products)}] {barcode[:15]:15s} | Found: {brand[:20]:20s}")
                else:
                    self.stats['images_not_found'] += 1
                    logger.debug(f"❌ [{i:5d}/{len(products)}] {barcode[:15]:15s} | Invalid: {reason}")
            else:
                self.stats['images_not_found'] += 1
                if i <= 5:  # Log first few failures
                    logger.debug(f"❌ [{i:5d}/{len(products)}] {barcode[:15]:15s} | Not found: {query[:40]}")

            # Save checkpoint every 100 products
            if i % 100 == 0:
                self._save_checkpoint(i, barcode)
                self.print_progress()
                # Rate limiting: pause to respect API limits
                time.sleep(1)

        logger.info(f"\n✅ Processing complete: {self.stats['images_found']} images found")

    def update_database(self):
        """Update database with found images and activate products"""
        if not self.verified_images:
            logger.warning("⚠️  No images to update in database")
            return

        if self.test_mode:
            logger.info("="*80)
            logger.info("🧪 TEST MODE - Database will NOT be updated")
            logger.info("="*80)
            logger.info(f"Would update {len(self.verified_images)} products with images")
            logger.info("\nSample of found images:")
            for img in self.verified_images[:10]:
                logger.info(f"  {img['barcode']}: {img['name'][:40]:40s} → {img['image_url'][:60]}")
            if len(self.verified_images) > 10:
                logger.info(f"\n... and {len(self.verified_images) - 10} more")
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
                            is_active = TRUE,
                            last_scraped_at = %s
                        WHERE barcode = %s
                    """, (item['image_url'], datetime.now(), item['barcode']))

                    self.stats['db_updated'] += cursor.rowcount

                self.conn.commit()
                logger.info(f"  💾 Committed batch {i//self.batch_size + 1}/{(len(self.verified_images)-1)//self.batch_size + 1}")

            logger.info(f"✅ Database updated: {self.stats['db_updated']} products activated")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Database update failed: {e}")
        finally:
            cursor.close()

    def print_progress(self):
        """Print progress statistics"""
        total = self.stats['total_products']
        found = self.stats['images_found']
        not_found = self.stats['images_not_found']
        errors = self.stats['api_errors']

        processed = found + not_found

        if processed > 0:
            logger.info(f"\n📊 Progress: {processed}/{total} processed | "
                       f"Found: {found} ({found/processed*100:.1f}%) | "
                       f"Not found: {not_found} | API calls: {self.stats['api_calls']} | Errors: {errors}")

    def print_final_stats(self):
        """Print final statistics"""
        logger.info("\n" + "="*80)
        logger.info("📊 FINAL STATISTICS")
        logger.info("="*80)
        logger.info(f"Total products processed:  {self.stats['total_products']}")
        logger.info(f"Images found:              {self.stats['images_found']} ({self.stats['images_found']/self.stats['total_products']*100:.1f}%)")
        logger.info(f"Images not found:          {self.stats['images_not_found']} ({self.stats['images_not_found']/self.stats['total_products']*100:.1f}%)")
        logger.info(f"API calls made:            {self.stats['api_calls']}")
        logger.info(f"API errors:                {self.stats['api_errors']}")

        if not self.test_mode:
            logger.info(f"Database updated:          {self.stats['db_updated']}")
        else:
            logger.info(f"Database updated:          0 (TEST MODE)")

        logger.info("="*80)

    def run(self):
        """Main execution"""
        logger.info("="*80)
        logger.info("🔍 GOOGLE IMAGES API BACKFILL (IMPROVED WITH BARCODE)")
        logger.info("="*80)
        logger.info(f"Mode: {'TEST (no DB writes)' if self.test_mode else 'PRODUCTION (will update DB)'}")
        if self.limit:
            logger.info(f"Limit: {self.limit} products")
        logger.info("="*80)

        # Check API credentials
        if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
            logger.error("❌ Google API credentials not configured!")
            logger.error("Please add GOOGLE_API_KEY and GOOGLE_CSE_ID to your .env file")
            return False

        try:
            # Connect to database
            self._connect_db()

            # Get products
            products = self.get_products_missing_images()

            if not products:
                logger.info("✅ No products to process")
                return True

            # Process products
            self.process_products(products)

            # Update database
            self.update_database()

            # Print final stats
            self.print_final_stats()

            # Clean up checkpoint file on success
            if os.path.exists(CHECKPOINT_FILE):
                os.remove(CHECKPOINT_FILE)

            return True

        except KeyboardInterrupt:
            logger.warning("\n⚠️  Process interrupted by user")
            logger.info(f"Checkpoint saved. Run again to resume from {self.checkpoint['processed']} products")
            return False
        except Exception as e:
            logger.error(f"❌ Fatal error: {e}")
            return False
        finally:
            self._close_db()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Backfill product images using Google Custom Search API')
    parser.add_argument('--production', action='store_true',
                       help='Run in PRODUCTION mode (will update database). Default is TEST mode.')
    parser.add_argument('--limit', type=int,
                       help='Limit number of products to process')
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

    backfiller = GoogleImageBackfiller(
        test_mode=not args.production,
        limit=args.limit,
        batch_size=args.batch_size
    )

    success = backfiller.run()

    exit(0 if success else 1)
