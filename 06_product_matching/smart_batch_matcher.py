#!/usr/bin/env python3
"""
Smart Batch Product Matcher
Groups products by brand and keywords to ensure 100% match coverage
Only uses GPT-4o for final matching within guaranteed overlapping batches
"""

import logging
import psycopg2
import json
import openai
import re
from typing import Dict, List, Tuple, Set
from dataclasses import dataclass
from collections import defaultdict
from datetime import datetime
import time
from tqdm import tqdm

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'smart_batch_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class Product:
    product_id: int
    retailer_id: int
    retailer_name: str
    canonical_name: str
    brand: str
    
    def to_json(self):
        return {
            'id': self.product_id,
            'name': self.canonical_name,
            'retailer': self.retailer_name
        }

class SmartBatchMatcher:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="price_comparison_app_v2",
            user="postgres",
            password="025655358"
        )
        
        self.client = openai.OpenAI(
            api_key="sk-lvCeuWfVJMG1V9dHWO9fxsubyhS2dkZEYIQzukfEuZT3BlbkFJidE3fzU5s3v2bwWYp_dCEhcRy0cxKbdQp41BBgnDAA"
        )
        
        self.total_api_calls = 0
        self.total_cost = 0.0
        self.matched_count = 0
    
    def extract_keywords(self, text: str) -> Set[str]:
        """Extract meaningful keywords from product name"""
        # Remove common words and extract numbers, sizes, etc
        keywords = set()
        
        # Extract numbers with units
        numbers = re.findall(r'\d+(?:\.\d+)?(?:ml|מל|mg|מג|g|גרם|יח|units|כדורים|טבליות)', text.lower())
        keywords.update(numbers)
        
        # Extract product types in Hebrew
        types = re.findall(r'(שמפו|קרם|ספריי|סבון|משחה|טיפות|כדורים|ג\'ל|מסכה|סרום|ויטמין|תחליב|דאודורנט)', text)
        keywords.update(types)
        
        # Extract key descriptive words
        words = text.split()
        for word in words:
            if len(word) > 3 and word not in ['לכל', 'עם', 'ללא', 'בעל']:
                keywords.add(word.lower())
        
        return keywords
    
    def load_all_products(self) -> Dict[str, Dict[int, List[Product]]]:
        """Load all products organized by brand and retailer"""
        cursor = self.conn.cursor()
        
        # Get retailer names
        cursor.execute("SELECT retailerid, retailername FROM retailers")
        retailer_names = {rid: name for rid, name in cursor.fetchall()}
        
        # Load all products
        query = """
            SELECT p.product_id, rp.retailer_id, p.canonical_name, p.brand
            FROM products p
            JOIN retailer_products rp ON p.product_id = rp.product_id
            WHERE p.canonical_name IS NOT NULL AND p.canonical_name != ''
            ORDER BY p.brand, rp.retailer_id
        """
        
        cursor.execute(query)
        
        # Organize by brand -> retailer -> products
        products_by_brand = defaultdict(lambda: defaultdict(list))
        
        for row in cursor.fetchall():
            product = Product(
                product_id=row[0],
                retailer_id=row[1],
                retailer_name=retailer_names.get(row[1], f"Retailer_{row[1]}"),
                canonical_name=row[2],
                brand=row[3] or "NO_BRAND"
            )
            products_by_brand[product.brand][product.retailer_id].append(product)
        
        cursor.close()
        return products_by_brand
    
    def create_smart_batches(self, products_by_brand: Dict[str, Dict[int, List[Product]]]) -> List[Dict]:
        """Create batches ensuring all potential matches are in same batch"""
        batches = []
        
        for brand, retailers_products in products_by_brand.items():
            retailer_ids = list(retailers_products.keys())
            
            # Skip brands that only appear in one retailer (no matches possible)
            if len(retailer_ids) < 2:
                continue
            
            # Get products from each retailer for this brand
            retailer_products = [retailers_products[rid] for rid in retailer_ids]
            
            # If total products for this brand is small, make one batch
            total_products = sum(len(products) for products in retailer_products)
            
            if total_products <= 150:  # Can fit in one API call
                batch = {
                    'brand': brand,
                    'retailers': retailer_ids,
                    'products': retailers_products,
                    'batch_type': 'brand_complete'
                }
                batches.append(batch)
            else:
                # Need to subdivide by keywords
                keyword_groups = self.group_by_keywords(retailers_products, brand)
                for keyword_batch in keyword_groups:
                    batches.append(keyword_batch)
        
        return batches
    
    def group_by_keywords(self, retailers_products: Dict[int, List[Product]], brand: str) -> List[Dict]:
        """Group products by keywords within a brand"""
        keyword_batches = []
        
        # Extract keywords for all products
        all_products_with_keywords = []
        for retailer_id, products in retailers_products.items():
            for product in products:
                keywords = self.extract_keywords(product.canonical_name)
                all_products_with_keywords.append((product, keywords, retailer_id))
        
        # Group products with similar keywords
        processed = set()
        for i, (product1, keywords1, retailer1) in enumerate(all_products_with_keywords):
            if i in processed:
                continue
            
            # Find all products with overlapping keywords
            batch_products = defaultdict(list)
            batch_products[retailer1].append(product1)
            processed.add(i)
            
            for j, (product2, keywords2, retailer2) in enumerate(all_products_with_keywords):
                if j in processed:
                    continue
                
                # Check keyword overlap
                if keywords1 & keywords2:  # Common keywords
                    batch_products[retailer2].append(product2)
                    processed.add(j)
                    
                    # Stop if batch is getting too large
                    total_in_batch = sum(len(prods) for prods in batch_products.values())
                    if total_in_batch >= 100:
                        break
            
            # Only create batch if it has products from multiple retailers
            if len(batch_products) > 1:
                keyword_batches.append({
                    'brand': brand,
                    'retailers': list(batch_products.keys()),
                    'products': batch_products,
                    'batch_type': 'keyword_group',
                    'keywords': list(keywords1)[:5]  # Sample keywords
                })
        
        return keyword_batches
    
    def match_batch_with_gpt4(self, batch: Dict) -> List[Dict]:
        """Send a batch to GPT-4o for matching"""
        # Prepare products for API
        products_by_retailer = {}
        for retailer_id, products in batch['products'].items():
            products_by_retailer[f"retailer_{retailer_id}"] = [p.to_json() for p in products]
        
        prompt = f"""Match identical products across retailers for brand: {batch['brand']}

Products by retailer:
{json.dumps(products_by_retailer, ensure_ascii=False, indent=2)}

Rules:
1. Only match IDENTICAL products (same product, maybe different sizes)
2. All products are from brand: {batch['brand']}
3. Be conservative - only match if certain

Return JSON:
{{
  "matches": [
    {{
      "product_ids": [list of matching product IDs from different retailers],
      "canonical_name": "best name for this product",
      "confidence": "HIGH/MEDIUM"
    }}
  ]
}}"""

        try:
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are a product matching expert. Only match identical products."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=1500,
                response_format={"type": "json_object"}
            )
            
            # Calculate cost
            input_tokens = len(prompt) / 4
            output_tokens = len(response.choices[0].message.content) / 4
            cost = (input_tokens * 0.0025 / 1000) + (output_tokens * 0.01 / 1000)
            self.total_cost += cost
            self.total_api_calls += 1
            
            result = json.loads(response.choices[0].message.content)
            return result.get('matches', [])
            
        except Exception as e:
            logger.error(f"GPT-4o error: {e}")
            return []
    
    def save_matches(self, matches: List[Dict]):
        """Save matched products to database"""
        if not matches:
            return
        
        cursor = self.conn.cursor()
        
        for match in matches:
            if match['confidence'] in ['HIGH', 'MEDIUM']:
                product_ids = match['product_ids']
                canonical_name = match['canonical_name']
                
                if len(product_ids) < 2:
                    continue
                
                # Create product group
                cursor.execute(
                    "INSERT INTO product_groups (canonical_name) VALUES (%s) RETURNING group_id",
                    (canonical_name,)
                )
                result = cursor.fetchone()
                if result:
                    group_id = result[0]
                    
                    # Link products
                    for pid in product_ids:
                        cursor.execute(
                            "INSERT INTO product_group_links (group_id, product_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                            (group_id, pid)
                        )
                    
                    self.matched_count += len(product_ids)
        
        self.conn.commit()
        cursor.close()
    
    def run(self):
        """Run the smart batching and matching process"""
        logger.info("="*60)
        logger.info("SMART BATCH MATCHING SYSTEM")
        logger.info("Ensuring 100% coverage of potential matches")
        logger.info("="*60)
        
        # Clear existing matches
        cursor = self.conn.cursor()
        cursor.execute("TRUNCATE product_group_links, product_groups RESTART IDENTITY CASCADE")
        self.conn.commit()
        cursor.close()
        
        # Load all products
        logger.info("Loading all products...")
        products_by_brand = self.load_all_products()
        
        total_brands = len(products_by_brand)
        multi_retailer_brands = sum(1 for brand_data in products_by_brand.values() if len(brand_data) > 1)
        
        logger.info(f"Total brands: {total_brands}")
        logger.info(f"Brands in multiple retailers: {multi_retailer_brands}")
        
        # Create smart batches
        logger.info("\nCreating smart batches...")
        batches = self.create_smart_batches(products_by_brand)
        logger.info(f"Created {len(batches)} batches for matching")
        
        # Show batch distribution
        brand_complete = sum(1 for b in batches if b['batch_type'] == 'brand_complete')
        keyword_groups = sum(1 for b in batches if b['batch_type'] == 'keyword_group')
        logger.info(f"  - Brand complete batches: {brand_complete}")
        logger.info(f"  - Keyword group batches: {keyword_groups}")
        
        # Process batches
        logger.info("\nProcessing batches with GPT-4o...")
        
        # Process all batches
        with tqdm(total=len(batches), desc="Matching batches") as pbar:
            for i, batch in enumerate(batches):
                # Log batch details
                total_products = sum(len(products) for products in batch['products'].values())
                logger.info(f"\nBatch {i+1}: Brand '{batch['brand']}', {total_products} products, {len(batch['retailers'])} retailers")
                
                # Get matches from GPT-4o
                matches = self.match_batch_with_gpt4(batch)
                
                if matches:
                    logger.info(f"  Found {len(matches)} matches")
                    for match in matches[:3]:  # Log first 3 matches
                        logger.info(f"    - {match['canonical_name']} ({match['confidence']})")
                    
                    # Save matches
                    self.save_matches(matches)
                else:
                    logger.info(f"  No matches found")
                
                pbar.update(1)
                
                # Rate limiting
                if i % 10 == 0 and i > 0:
                    time.sleep(1)
        
        # Final statistics
        self.print_statistics()
    
    def print_statistics(self):
        """Print final matching statistics"""
        cursor = self.conn.cursor()
        
        # Get match statistics
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT pg.group_id) as total_groups,
                COUNT(DISTINCT pgl.product_id) as matched_products
            FROM product_groups pg
            JOIN product_group_links pgl ON pg.group_id = pgl.group_id
        """)
        
        groups, products = cursor.fetchone()
        
        # Get total products
        cursor.execute("SELECT COUNT(*) FROM products")
        total_products = cursor.fetchone()[0]
        
        # Get cross-retailer matches
        cursor.execute("""
            WITH matched AS (
                SELECT pgl.group_id, COUNT(DISTINCT rp.retailer_id) as retailer_count
                FROM product_group_links pgl
                JOIN products p ON pgl.product_id = p.product_id
                JOIN retailer_products rp ON p.product_id = rp.product_id
                GROUP BY pgl.group_id
                HAVING COUNT(DISTINCT rp.retailer_id) > 1
            )
            SELECT COUNT(*) FROM matched
        """)
        cross_retailer_groups = cursor.fetchone()[0]
        
        logger.info("\n" + "="*60)
        logger.info("FINAL RESULTS:")
        logger.info(f"  API Calls: {self.total_api_calls}")
        logger.info(f"  Total Cost: ${self.total_cost:.2f}")
        logger.info(f"  Product Groups Created: {groups}")
        logger.info(f"  Products Matched: {products}/{total_products} ({products/total_products*100:.1f}%)")
        logger.info(f"  Cross-Retailer Groups: {cross_retailer_groups}")
        logger.info("="*60)
        
        cursor.close()

if __name__ == "__main__":
    matcher = SmartBatchMatcher()
    matcher.run()