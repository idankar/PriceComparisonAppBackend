#!/usr/bin/env python3
"""
LLM-Based Category Backfill Script for Canonical Products

This script uses GPT-4o to intelligently categorize products based on their names and brands.
It's faster, more reliable, and more comprehensive than web scraping approaches.

Features:
- Uses OpenAI GPT-4o for intelligent categorization
- Batch processing for efficiency (up to 50 products per API call)
- Checkpoint/resume functionality for interrupted runs
- Category validation against existing categories
- Progress tracking and detailed logging
- Cost estimation and tracking
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import openai
import json
import os
import logging
from datetime import datetime
from typing import List, Dict, Optional
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('llm_category_backfill.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'price_comparison_app_v2',
    'user': 'postgres',
    'password': '025655358'
}

# OpenAI configuration
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY environment variable not set. Please set it before running.")

openai.api_key = OPENAI_API_KEY

# Configuration
BATCH_SIZE = 50  # Number of products to categorize per API call
CHECKPOINT_FILE = 'llm_category_checkpoint.json'
CATEGORIZED_DATA_FILE = 'categorized_products.json'  # Output file for review
MODEL = "gpt-4o"  # GPT-4o model


class LLMCategoryBackfiller:
    """LLM-based product category backfiller"""

    def __init__(self, batch_size: int = BATCH_SIZE, dry_run: bool = False):
        self.batch_size = batch_size
        self.dry_run = dry_run
        self.checkpoint = self.load_checkpoint()
        self.stats = {
            'processed': 0,
            'categorized': 0,
            'failed': 0,
            'api_calls': 0,
            'total_tokens': 0,
            'estimated_cost': 0.0
        }
        self.existing_categories = self.load_existing_categories()
        self.categorized_products = []

    def load_checkpoint(self) -> Dict:
        """Load checkpoint from file if exists"""
        if os.path.exists(CHECKPOINT_FILE):
            try:
                with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                    checkpoint = json.load(f)
                    logger.info(f"📂 Loaded checkpoint: {checkpoint.get('processed', 0)} products processed")
                    return checkpoint
            except Exception as e:
                logger.warning(f"⚠️  Could not load checkpoint: {e}")
        return {'processed': 0, 'last_barcode': None, 'timestamp': None}

    def save_checkpoint(self, last_barcode: str):
        """Save checkpoint to file"""
        try:
            checkpoint = {
                'processed': self.stats['processed'],
                'last_barcode': last_barcode,
                'timestamp': datetime.now().isoformat(),
                'stats': self.stats
            }
            with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
                json.dump(checkpoint, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"❌ Failed to save checkpoint: {e}")

    def load_existing_categories(self) -> List[str]:
        """Load existing categories from database for reference"""
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()

            cur.execute("""
                SELECT DISTINCT category
                FROM canonical_products
                WHERE category IS NOT NULL
                  AND category <> ''
                  AND is_active = TRUE
                ORDER BY category
            """)

            categories = [row[0] for row in cur.fetchall()]
            cur.close()
            conn.close()

            logger.info(f"📋 Loaded {len(categories)} existing unique categories")
            return categories

        except Exception as e:
            logger.error(f"❌ Failed to load existing categories: {e}")
            return []

    def get_products_without_categories(self, limit: Optional[int] = None) -> List[Dict]:
        """Get active products without categories"""
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor(cursor_factory=RealDictCursor)

            query = """
                SELECT barcode, name, brand
                FROM canonical_products
                WHERE is_active = TRUE
                  AND (category IS NULL OR category = '')
            """

            if self.checkpoint.get('last_barcode'):
                query += f" AND barcode > '{self.checkpoint['last_barcode']}'"
                logger.info(f"📍 Resuming from barcode: {self.checkpoint['last_barcode']}")

            query += " ORDER BY barcode"

            if limit:
                query += f" LIMIT {limit}"

            cur.execute(query)
            products = cur.fetchall()

            cur.close()
            conn.close()

            logger.info(f"📦 Found {len(products)} products without categories")
            return products

        except Exception as e:
            logger.error(f"❌ Failed to get products: {e}")
            return []

    def create_categorization_prompt(self, products: List[Dict]) -> str:
        """Create prompt for GPT-4o to categorize products"""

        # Sample existing categories for reference
        sample_categories = self.existing_categories[:30] if self.existing_categories else []

        prompt = f"""You are a product categorization expert for a pharmaceutical and cosmetics price comparison platform in Israel.

Your task is to categorize products into Hebrew categories following the existing category structure.

EXISTING CATEGORY EXAMPLES (use these as reference for style and hierarchy):
{chr(10).join(f"- {cat}" for cat in sample_categories)}

CATEGORY STRUCTURE RULES:
1. Use Hebrew language for all categories
2. Use "/" to separate hierarchical levels (e.g., "טיפוח/טיפוח שיער/שמפו")
3. Categories should be 1-3 levels deep
4. Be consistent with existing categories when possible
5. Main categories: טיפוח, קוסמטיקה, תינוקות ופעוטות, אורתופדיה, מותג הבית, תוספי תזונה, ויטמינים ומינרלים, מוצרי בריאות, מוצרים למטבח, חד פעמי

PRODUCTS TO CATEGORIZE:
{chr(10).join(f'{i+1}. Barcode: {p["barcode"]}, Name: {p["name"]}, Brand: {p.get("brand", "N/A")}' for i, p in enumerate(products))}

INSTRUCTIONS:
- Analyze each product name and brand
- Assign the most appropriate category
- Return ONLY a valid JSON array with this exact structure:
[
  {{"barcode": "1234567890123", "category": "קטגוריה/תת-קטגוריה"}},
  {{"barcode": "9876543210987", "category": "קטגוריה/תת-קטגוריה/תת-תת-קטגוריה"}}
]

Return ONLY the JSON array, no additional text or explanation."""

        return prompt

    def categorize_batch_with_llm(self, products: List[Dict]) -> List[Dict]:
        """Use GPT-4o to categorize a batch of products"""
        try:
            prompt = self.create_categorization_prompt(products)

            logger.info(f"🤖 Sending {len(products)} products to GPT-4o...")

            response = openai.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": "You are a product categorization expert. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,  # Lower temperature for more consistent categorization
                max_tokens=4000
            )

            # Track API usage
            self.stats['api_calls'] += 1
            tokens_used = response.usage.total_tokens
            self.stats['total_tokens'] += tokens_used

            # Estimate cost (GPT-4o pricing: ~$5/1M input tokens, ~$15/1M output tokens)
            # Rough estimate: $10/1M tokens average
            cost = (tokens_used / 1_000_000) * 10
            self.stats['estimated_cost'] += cost

            logger.info(f"📊 API call complete. Tokens: {tokens_used}, Est. cost: ${cost:.4f}")

            # Parse response
            content = response.choices[0].message.content.strip()

            # Remove markdown code blocks if present
            if content.startswith('```'):
                content = content.split('```')[1]
                if content.startswith('json'):
                    content = content[4:]
                content = content.strip()

            categorized = json.loads(content)

            if not isinstance(categorized, list):
                raise ValueError("Response is not a JSON array")

            logger.info(f"✅ Successfully categorized {len(categorized)} products")
            return categorized

        except json.JSONDecodeError as e:
            logger.error(f"❌ Failed to parse JSON response: {e}")
            logger.error(f"Response content: {content[:500]}")
            return []
        except Exception as e:
            logger.error(f"❌ LLM categorization error: {e}")
            return []

    def update_product_category(self, barcode: str, category: str):
        """Update category for a single product"""
        if self.dry_run:
            logger.info(f"  [DRY RUN] Would update {barcode} with category: {category}")
            return True

        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()

            cur.execute("""
                UPDATE canonical_products
                SET category = %s,
                    last_scraped_at = %s
                WHERE barcode = %s
            """, (category, datetime.now(), barcode))

            conn.commit()
            cur.close()
            conn.close()

            return True

        except Exception as e:
            logger.error(f"❌ Database update error for {barcode}: {e}")
            return False

    def process_batch(self, products: List[Dict]):
        """Process a batch of products"""
        # Get categorizations from LLM
        categorized = self.categorize_batch_with_llm(products)

        if not categorized:
            logger.warning(f"⚠️  No categorizations returned for batch")
            self.stats['failed'] += len(products)
            return

        # Create barcode lookup
        product_lookup = {p['barcode']: p for p in products}

        # Update database
        for item in categorized:
            barcode = item.get('barcode')
            category = item.get('category')

            if not barcode or not category:
                logger.warning(f"⚠️  Invalid item: {item}")
                self.stats['failed'] += 1
                continue

            # Verify barcode exists in our batch
            if barcode not in product_lookup:
                logger.warning(f"⚠️  Barcode {barcode} not in batch")
                continue

            # Update database
            if self.update_product_category(barcode, category):
                self.stats['categorized'] += 1
                self.categorized_products.append({
                    'barcode': barcode,
                    'name': product_lookup[barcode]['name'],
                    'brand': product_lookup[barcode].get('brand'),
                    'category': category
                })
                logger.info(f"  ✅ {barcode}: {category}")
            else:
                self.stats['failed'] += 1

        self.stats['processed'] += len(products)

    def run(self, limit: Optional[int] = None):
        """Run the categorization process"""
        logger.info("="*80)
        logger.info("STARTING LLM-BASED CATEGORY BACKFILL")
        logger.info("="*80)
        logger.info(f"Model: {MODEL}")
        logger.info(f"Batch size: {self.batch_size}")
        if self.dry_run:
            logger.info("🧪 DRY RUN MODE - No database updates will be made")
        logger.info("="*80)

        # Get products to process
        products = self.get_products_without_categories(limit)

        if not products:
            logger.info("✅ No products to process")
            return

        # Process in batches
        total_batches = (len(products) + self.batch_size - 1) // self.batch_size

        for i in range(0, len(products), self.batch_size):
            batch_num = (i // self.batch_size) + 1
            batch = products[i:i + self.batch_size]

            logger.info(f"\n{'='*80}")
            logger.info(f"BATCH {batch_num}/{total_batches} ({len(batch)} products)")
            logger.info(f"{'='*80}")

            try:
                self.process_batch(batch)

                # Save checkpoint after each batch
                last_barcode = batch[-1]['barcode']
                self.save_checkpoint(last_barcode)

                # Progress report
                self.print_progress()

                # Rate limiting - small delay between batches
                if i + self.batch_size < len(products):
                    time.sleep(1)

            except Exception as e:
                logger.error(f"❌ Batch processing error: {e}")
                self.stats['failed'] += len(batch)

        # Save categorized products to file for review
        self.save_categorized_products()

        # Final statistics
        self.print_final_stats()

        # Clear checkpoint on successful completion
        if not self.dry_run and self.stats['processed'] >= len(products):
            self.clear_checkpoint()

    def save_categorized_products(self):
        """Save categorized products to JSON file for review"""
        try:
            with open(CATEGORIZED_DATA_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.categorized_products, f, indent=2, ensure_ascii=False)
            logger.info(f"💾 Saved {len(self.categorized_products)} categorizations to {CATEGORIZED_DATA_FILE}")
        except Exception as e:
            logger.error(f"❌ Failed to save categorized products: {e}")

    def clear_checkpoint(self):
        """Clear checkpoint file"""
        try:
            if os.path.exists(CHECKPOINT_FILE):
                os.remove(CHECKPOINT_FILE)
                logger.info("🗑️  Checkpoint cleared")
        except Exception as e:
            logger.warning(f"⚠️  Could not remove checkpoint: {e}")

    def print_progress(self):
        """Print progress statistics"""
        success_rate = (self.stats['categorized'] / self.stats['processed'] * 100) if self.stats['processed'] > 0 else 0
        logger.info(f"\n📊 PROGRESS:")
        logger.info(f"  Processed: {self.stats['processed']}")
        logger.info(f"  Categorized: {self.stats['categorized']} ({success_rate:.1f}%)")
        logger.info(f"  Failed: {self.stats['failed']}")
        logger.info(f"  API calls: {self.stats['api_calls']}")
        logger.info(f"  Total tokens: {self.stats['total_tokens']:,}")
        logger.info(f"  Estimated cost: ${self.stats['estimated_cost']:.4f}")

    def print_final_stats(self):
        """Print final statistics"""
        logger.info("\n" + "="*80)
        logger.info("CATEGORIZATION COMPLETE - FINAL STATISTICS")
        logger.info("="*80)
        logger.info(f"Total processed: {self.stats['processed']}")
        logger.info(f"Successfully categorized: {self.stats['categorized']}")
        logger.info(f"Failed: {self.stats['failed']}")
        logger.info(f"Total API calls: {self.stats['api_calls']}")
        logger.info(f"Total tokens used: {self.stats['total_tokens']:,}")
        logger.info(f"Estimated total cost: ${self.stats['estimated_cost']:.2f}")

        if self.stats['processed'] > 0:
            success_rate = (self.stats['categorized'] / self.stats['processed']) * 100
            logger.info(f"\n✅ Success rate: {success_rate:.1f}%")

        logger.info("="*80)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='LLM-based category backfill for canonical products')
    parser.add_argument('--limit', type=int, help='Limit number of products to process (for testing)')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE, help=f'Batch size (default: {BATCH_SIZE})')
    parser.add_argument('--dry-run', action='store_true', help='Dry run mode - no database updates')

    args = parser.parse_args()

    backfiller = LLMCategoryBackfiller(
        batch_size=args.batch_size,
        dry_run=args.dry_run
    )

    backfiller.run(limit=args.limit)
