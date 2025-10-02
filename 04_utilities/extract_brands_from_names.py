#!/usr/bin/env python3
"""
Brand Extraction Script for Good Pharm Products

Extracts brand names from product names for inactive Good Pharm products.
This improves metadata quality and increases Google Images search accuracy.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import re
import os
from dotenv import load_dotenv
import logging
from collections import defaultdict

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('brand_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment
load_dotenv()

DB_NAME = os.getenv("DB_NAME", "price_comparison_app_v2")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "***REMOVED***")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

# Known pharmacy and cosmetics brands (both English and Hebrew)
KNOWN_BRANDS = {
    # International Cosmetics
    'catrice', 'קטריס', 'catr.', 'cat.',
    'the inky list', 'אינקי ליסט', 'inky list',
    'nivea', 'ניוואה',
    'loreal', "l'oreal", 'לוריאל',
    'maybelline', 'מייבלין',
    'garnier', 'גרנייה',
    'essence', 'אסנס',
    'sence', 'סנס',
    'beter', 'בטר',
    'cosnova', 'קוסנובא',

    # Oral Care
    'colgate', 'קולגייט',
    'oral-b', 'oral b', 'אורל בי',
    'listerine', 'ליסטרין',
    'sensodyne', 'סנסודיין',

    # Personal Care
    'dove', 'דאב',
    'palmolive', 'פלמוליב',
    'head & shoulders', 'הד אנד שולדרס',
    'pantene', 'פנטן',
    'tresemme', 'טרזמה',
    'axe', 'אקס',
    'rexona', 'רקסונה',
    'lady speed stick', 'ליידי ספיד',

    # Israeli Brands
    'dr fischer', 'dr. fischer', "ד\"ר פישר", 'דר פישר',
    'careline', 'קרליין',
    'סליידר', 'slider',
    'מורז', 'moraz',
    'baby care', 'בייבי כיף',
    'dsc',
    'pharmex', 'פארמקס',
    'טופ מד', 'top med',
    'פארמה קר', 'pharma care',

    # Food & Beverage
    'osem', 'אוסם',
    'elite', 'עלית',
    'strauss', 'שטראוס',
    'telma', 'תלמה',
    'milka', 'מילקה',
    'fitness', 'פיטנס',
    'nestle', 'נסטלה',
    'coca cola', 'קוקה קולה',
    '7up', '7-up', 'סבן אפ',
    'sprite', 'ספרייט',
    'tempo', 'טמפו',

    # Household
    'fairy', 'פיירי',
    'persil', 'פרסיל',
    'ariel', 'אריאל',
    'lenor', 'לנור',
    'calgon', 'קלגון',
    'airwick', 'אירוויק', 'air wick',
    'sano', 'סנו',
    'alma', 'עלמה',
    'st moritz', 'st. moritz', 'סנט מוריץ',
    'musko', 'מוסקו',

    # Health & Wellness
    'o.b.', 'ob', 'או בה',
    'always', 'אולווייז',
    'tampax', 'טמפקס',
    'durex', 'דורקס',

    # Electronics/Home
    'fujikum', 'פוג\'יקום', 'פוגיקום',
    'sodastream', 'סודה סטרים',

    # Pet Food
    'badu', 'באדו',
    'pedigree', 'פדיגרי',
    'whiskas', 'ויסקאס',

    # Other
    'good pharm', 'גוד פארם',
    'duniz',
    'ross',
    'babe',
    'even',
    'derbo', 'דירבו',
    'laline', 'ללין',
}

# Compile brand patterns for faster matching
BRAND_PATTERNS = [re.compile(r'\b' + re.escape(brand) + r'\b', re.IGNORECASE) for brand in KNOWN_BRANDS]


class BrandExtractor:
    def __init__(self, test_mode=True, limit=None):
        self.test_mode = test_mode
        self.limit = limit
        self.conn = None
        self.stats = {
            'total_products': 0,
            'brands_extracted': 0,
            'brands_updated': 0,
            'no_brand_found': 0,
            'already_had_brand': 0
        }
        self.extracted_brands = defaultdict(int)

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

    def get_products_needing_brands(self):
        """Get ALL products without brands (across all retailers)"""
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)

        query = """
            SELECT barcode, name, brand, source_retailer_id
            FROM canonical_products
            WHERE (brand IS NULL OR brand = '' OR brand = 'לא ידוע')
            ORDER BY barcode
        """

        if self.limit:
            query += f" LIMIT {self.limit}"

        cursor.execute(query)
        products = cursor.fetchall()
        cursor.close()

        logger.info(f"📦 Found {len(products)} products needing brand extraction")
        return products

    def extract_brand_from_name(self, name):
        """
        Extract brand from product name using multiple strategies:
        1. Check against known brands list
        2. Extract from "Brand - Product" pattern
        3. Extract from "Brand Product" pattern (first word/words)
        """
        if not name:
            return None

        name_clean = name.strip()

        # Strategy 1: Match against known brands
        for pattern in BRAND_PATTERNS:
            match = pattern.search(name_clean)
            if match:
                brand = match.group(0)
                # Capitalize properly
                if brand.isascii():
                    brand = brand.title()
                return brand

        # Strategy 2: Extract from "Brand - Product" pattern
        if ' - ' in name_clean:
            potential_brand = name_clean.split(' - ')[0].strip()
            # If it's short enough to be a brand name (not a long description)
            if len(potential_brand.split()) <= 3:
                return potential_brand

        # Strategy 3: Extract from "Brand:" or "Brand :" pattern
        if ':' in name_clean:
            potential_brand = name_clean.split(':')[0].strip()
            if len(potential_brand.split()) <= 3:
                return potential_brand

        # Strategy 4: First word(s) if they look like a brand
        words = name_clean.split()
        if len(words) > 0:
            # Check if first word is all caps (common for brand names)
            first_word = words[0]
            if first_word.isupper() and len(first_word) > 2:
                # Check if next word is also caps (multi-word brand)
                if len(words) > 1 and words[1].isupper():
                    return f"{first_word} {words[1]}"
                return first_word

            # Check first 2-3 words if they're capitalized
            if len(words) >= 2:
                first_two = ' '.join(words[:2])
                # If it contains a period (like "Dr. Fischer"), it's likely a brand
                if '.' in first_two or '"' in first_two:
                    return words[0] if len(words[0]) > 2 else first_two

        return None

    def update_brands(self, updates):
        """Update database with extracted brands"""
        if not updates:
            logger.warning("⚠️  No brands to update")
            return

        if self.test_mode:
            logger.info("="*80)
            logger.info("🧪 TEST MODE - Database will NOT be updated")
            logger.info("="*80)
            logger.info(f"Would update {len(updates)} products with brands")
            logger.info("\nSample of extracted brands:")
            for update in updates[:10]:
                logger.info(f"  {update['barcode']}: {update['name'][:50]:50s} → {update['brand']}")
            if len(updates) > 10:
                logger.info(f"\n... and {len(updates) - 10} more")
            return

        logger.info("="*80)
        logger.info(f"💾 UPDATING DATABASE - {len(updates)} products")
        logger.info("="*80)

        cursor = self.conn.cursor()

        try:
            for update in updates:
                cursor.execute("""
                    UPDATE canonical_products
                    SET brand = %s
                    WHERE barcode = %s
                """, (update['brand'], update['barcode']))

                self.stats['brands_updated'] += cursor.rowcount

            self.conn.commit()
            logger.info(f"✅ Database updated: {self.stats['brands_updated']} products")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Database update failed: {e}")
        finally:
            cursor.close()

    def process_products(self, products):
        """Process products and extract brands"""
        logger.info("="*80)
        logger.info("🚀 EXTRACTING BRANDS")
        logger.info("="*80)

        self.stats['total_products'] = len(products)
        updates = []

        for i, product in enumerate(products, 1):
            barcode = product['barcode']
            name = product['name']
            current_brand = product['brand']

            # Skip if already has a valid brand
            if current_brand and current_brand != 'לא ידוע' and current_brand != '':
                self.stats['already_had_brand'] += 1
                continue

            # Extract brand
            extracted_brand = self.extract_brand_from_name(name)

            if extracted_brand:
                self.stats['brands_extracted'] += 1
                self.extracted_brands[extracted_brand] += 1
                updates.append({
                    'barcode': barcode,
                    'name': name,
                    'brand': extracted_brand
                })

                # Progress logging
                if i % 100 == 0 or i <= 10:
                    logger.info(f"✅ [{i:5d}/{len(products)}] {barcode[:15]:15s} | {extracted_brand}")
            else:
                self.stats['no_brand_found'] += 1
                if i <= 5:  # Log first few failures
                    logger.debug(f"❌ [{i:5d}/{len(products)}] {barcode[:15]:15s} | No brand found: {name[:50]}")

            if i % 500 == 0:
                self.print_progress()

        logger.info(f"\n✅ Processing complete: {self.stats['brands_extracted']} brands extracted")

        return updates

    def print_progress(self):
        """Print progress statistics"""
        total = self.stats['total_products']
        extracted = self.stats['brands_extracted']
        not_found = self.stats['no_brand_found']
        already_had = self.stats['already_had_brand']

        processed = extracted + not_found + already_had

        if processed > 0:
            logger.info(f"\n📊 Progress: {processed}/{total} processed | "
                       f"Extracted: {extracted} ({extracted/processed*100:.1f}%) | "
                       f"Not found: {not_found} | Already had: {already_had}")

    def print_final_stats(self):
        """Print final statistics"""
        logger.info("\n" + "="*80)
        logger.info("📊 FINAL STATISTICS")
        logger.info("="*80)
        logger.info(f"Total products processed:  {self.stats['total_products']}")
        logger.info(f"Already had brands:        {self.stats['already_had_brand']}")
        logger.info(f"Brands extracted:          {self.stats['brands_extracted']} ({self.stats['brands_extracted']/self.stats['total_products']*100:.1f}%)")
        logger.info(f"No brand found:            {self.stats['no_brand_found']} ({self.stats['no_brand_found']/self.stats['total_products']*100:.1f}%)")

        if not self.test_mode:
            logger.info(f"Database updated:          {self.stats['brands_updated']}")
        else:
            logger.info(f"Database updated:          0 (TEST MODE)")

        logger.info("\n📊 TOP 20 EXTRACTED BRANDS:")
        for brand, count in sorted(self.extracted_brands.items(), key=lambda x: x[1], reverse=True)[:20]:
            logger.info(f"  {brand:30s} : {count:4d}")

        logger.info("="*80)

    def run(self):
        """Main execution"""
        logger.info("="*80)
        logger.info("🔍 BRAND EXTRACTION FOR ALL PRODUCTS")
        logger.info("="*80)
        logger.info(f"Mode: {'TEST (no DB writes)' if self.test_mode else 'PRODUCTION (will update DB)'}")
        if self.limit:
            logger.info(f"Limit: {self.limit} products")
        logger.info("="*80)

        try:
            # Connect to database
            self._connect_db()

            # Get products
            products = self.get_products_needing_brands()

            if not products:
                logger.info("✅ No products to process")
                return True

            # Process products
            updates = self.process_products(products)

            # Update database
            self.update_brands(updates)

            # Print final stats
            self.print_final_stats()

            return True

        except Exception as e:
            logger.error(f"❌ Fatal error: {e}")
            return False
        finally:
            self._close_db()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Extract brand names from product names')
    parser.add_argument('--production', action='store_true',
                       help='Run in PRODUCTION mode (will update database). Default is TEST mode.')
    parser.add_argument('--limit', type=int,
                       help='Limit number of products to process')
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

    extractor = BrandExtractor(
        test_mode=not args.production,
        limit=args.limit
    )

    success = extractor.run()

    exit(0 if success else 1)
