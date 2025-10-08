#!/usr/bin/env python3
"""
GPT-4o Product Matcher with Strict Matching Rules
Only matches truly identical products
"""

import logging
import psycopg2
import json
import openai
from typing import Dict, List, Tuple
from dataclasses import dataclass
from collections import defaultdict
from datetime import datetime
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'gpt4_matches_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class Product:
    product_id: int
    retailer_id: int
    name: str
    brand: str
    retailer_name: str
    
    def to_json(self):
        return {
            'id': self.product_id,
            'name': self.name,
            'brand': self.brand or "NO_BRAND"
        }
    
    def __str__(self):
        return f"[{self.retailer_name}] {self.name} | Brand: {self.brand or 'none'}"

class GPT4StrictMatcher:
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
        
        self.total_cost = 0.0
    
    def load_test_batch(self) -> Tuple[List[Product], List[Product], List[Product]]:
        """Load 50 products from each retailer for testing"""
        cursor = self.conn.cursor()
        
        # Get retailer names
        cursor.execute("SELECT retailerid, retailername FROM retailers")
        retailer_names = {rid: name for rid, name in cursor.fetchall()}
        
        # Load 50 products from each retailer
        query = """
            WITH sampled AS (
                SELECT p.product_id, rp.retailer_id, p.canonical_name, p.brand,
                       ROW_NUMBER() OVER (PARTITION BY rp.retailer_id ORDER BY p.product_id) as rn
                FROM products p
                JOIN retailer_products rp ON p.product_id = rp.product_id
                WHERE p.canonical_name IS NOT NULL AND p.canonical_name != ''
            )
            SELECT product_id, retailer_id, canonical_name, brand
            FROM sampled
            WHERE rn <= 50
            ORDER BY retailer_id, product_id
        """
        
        cursor.execute(query)
        products_by_retailer = defaultdict(list)
        
        for row in cursor.fetchall():
            product = Product(
                product_id=row[0],
                retailer_id=row[1],
                name=row[2],
                brand=row[3] or "",
                retailer_name=retailer_names.get(row[1], f"Retailer_{row[1]}")
            )
            products_by_retailer[row[1]].append(product)
        
        cursor.close()
        
        # Return products from 3 retailers
        retailers = list(products_by_retailer.keys())[:3]
        if len(retailers) < 3:
            logger.error("Need 3 retailers for testing")
            return [], [], []
        
        return (products_by_retailer[retailers[0]], 
                products_by_retailer[retailers[1]], 
                products_by_retailer[retailers[2]])
    
    def match_with_gpt4(self, retailer1_products: List[Product], 
                        retailer2_products: List[Product],
                        retailer3_products: List[Product]) -> Dict:
        """Match products using GPT-4o with strict rules"""
        
        # Prepare product lists
        r1 = [p.to_json() for p in retailer1_products]
        r2 = [p.to_json() for p in retailer2_products]
        r3 = [p.to_json() for p in retailer3_products]
        
        prompt = f"""You are a pharmacy product matching expert. Match IDENTICAL products across 3 retailers.

STRICT MATCHING RULES:
1. Products match ONLY if they are the EXACT SAME product (same active ingredient, same purpose, same type)
2. Brand MUST match exactly (exception: generic vs brand name of same medicine)
3. Different sizes/quantities of the SAME product are matches (e.g., 50 tablets vs 100 tablets)
4. Different product types are NEVER matches (shampoo ≠ soap, vitamin K ≠ face cream, candy ≠ protein bar)
5. Be VERY conservative - when in doubt, DON'T match

Retailer 1 products:
{json.dumps(r1, ensure_ascii=False, indent=2)}

Retailer 2 products:
{json.dumps(r2, ensure_ascii=False, indent=2)}

Retailer 3 products:
{json.dumps(r3, ensure_ascii=False, indent=2)}

Return JSON with matched groups. Each group should contain product IDs that are the SAME product across retailers:
{{
  "matched_groups": [
    {{
      "product_ids": [id1_from_r1, id2_from_r2, id3_from_r3],
      "canonical_name": "best product name to use",
      "confidence": "HIGH/MEDIUM/LOW",
      "reason": "why these match"
    }}
  ],
  "unmatched": {{
    "retailer1": [unmatched_ids],
    "retailer2": [unmatched_ids],
    "retailer3": [unmatched_ids]
  }}
}}

Remember: Only match truly IDENTICAL products. Different product categories must NEVER match."""

        try:
            # Estimate token count (rough)
            input_tokens = len(prompt) / 4
            
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are a strict product matcher. Only match identical products."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=2000,
                response_format={"type": "json_object"}
            )
            
            # Estimate cost (GPT-4o pricing)
            output_tokens = len(response.choices[0].message.content) / 4
            cost = (input_tokens * 0.0025 / 1000) + (output_tokens * 0.01 / 1000)
            self.total_cost += cost
            
            result = json.loads(response.choices[0].message.content)
            
            # Log the matches
            matched_groups = result.get('matched_groups', [])
            logger.info(f"\n{'='*60}")
            logger.info(f"GPT-4o found {len(matched_groups)} matched groups")
            logger.info(f"Estimated cost for this batch: ${cost:.4f}")
            
            for i, group in enumerate(matched_groups, 1):
                logger.info(f"\nGroup {i} ({group['confidence']} confidence):")
                logger.info(f"  Canonical name: {group['canonical_name']}")
                logger.info(f"  Reason: {group['reason']}")
                
                # Show actual products
                for pid in group['product_ids']:
                    # Find the product
                    product = None
                    for p in retailer1_products + retailer2_products + retailer3_products:
                        if p.product_id == pid:
                            product = p
                            break
                    if product:
                        logger.info(f"    - {product}")
            
            return result
            
        except Exception as e:
            logger.error(f"GPT-4o API error: {e}")
            return {"matched_groups": [], "unmatched": {}}
    
    def save_matches(self, matched_groups: List[Dict]):
        """Save matched groups to database"""
        if not matched_groups:
            return
        
        cursor = self.conn.cursor()
        
        for group in matched_groups:
            if group['confidence'] in ['HIGH', 'MEDIUM']:
                product_ids = group['product_ids']
                canonical_name = group['canonical_name']
                
                if len(product_ids) < 2:
                    continue
                
                # Create product group
                cursor.execute(
                    "INSERT INTO product_groups (canonical_name) VALUES (%s) RETURNING product_group_id",
                    (canonical_name,)
                )
                result = cursor.fetchone()
                if not result:
                    continue
                    
                group_id = result[0]
                
                # Link products
                for pid in product_ids:
                    cursor.execute(
                        "INSERT INTO product_group_links (group_id, product_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (group_id, pid)
                    )
        
        self.conn.commit()
        cursor.close()
    
    def run_test_batch(self):
        """Run a test batch of 50 products from each retailer"""
        logger.info("="*60)
        logger.info("STARTING GPT-4o STRICT MATCHING TEST")
        logger.info("Testing with 50 products from each of 3 retailers")
        logger.info("="*60)
        
        # Clear existing groups
        cursor = self.conn.cursor()
        cursor.execute("TRUNCATE product_group_links, product_groups RESTART IDENTITY CASCADE")
        self.conn.commit()
        cursor.close()
        
        # Load test batch
        r1_products, r2_products, r3_products = self.load_test_batch()
        
        if not r1_products or not r2_products or not r3_products:
            logger.error("Failed to load test data")
            return
        
        logger.info(f"\nLoaded products:")
        logger.info(f"  Retailer 1: {len(r1_products)} products")
        logger.info(f"  Retailer 2: {len(r2_products)} products")
        logger.info(f"  Retailer 3: {len(r3_products)} products")
        
        # Show sample products
        logger.info(f"\nSample products from each retailer:")
        for products, num in [(r1_products, 1), (r2_products, 2), (r3_products, 3)]:
            logger.info(f"  Retailer {num}:")
            for p in products[:3]:
                logger.info(f"    - {p}")
        
        # Run matching
        logger.info(f"\nCalling GPT-4o for matching...")
        result = self.match_with_gpt4(r1_products, r2_products, r3_products)
        
        # Save matches
        matched_groups = result.get('matched_groups', [])
        if matched_groups:
            self.save_matches(matched_groups)
            logger.info(f"\nSaved {len([g for g in matched_groups if g['confidence'] in ['HIGH', 'MEDIUM']])} groups to database")
        
        # Print statistics
        self.print_stats(result)
    
    def print_stats(self, result: Dict):
        """Print matching statistics"""
        matched_groups = result.get('matched_groups', [])
        
        high_conf = [g for g in matched_groups if g['confidence'] == 'HIGH']
        medium_conf = [g for g in matched_groups if g['confidence'] == 'MEDIUM']
        low_conf = [g for g in matched_groups if g['confidence'] == 'LOW']
        
        total_matched_products = sum(len(g['product_ids']) for g in matched_groups)
        
        logger.info("\n" + "="*60)
        logger.info("MATCHING RESULTS:")
        logger.info(f"  Total matched groups: {len(matched_groups)}")
        logger.info(f"    - HIGH confidence: {len(high_conf)}")
        logger.info(f"    - MEDIUM confidence: {len(medium_conf)}")
        logger.info(f"    - LOW confidence: {len(low_conf)}")
        logger.info(f"  Total products in matches: {total_matched_products}")
        logger.info(f"  Estimated API cost: ${self.total_cost:.4f}")
        logger.info("="*60)
        
        # Check database
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT COUNT(DISTINCT group_id) as groups,
                   COUNT(DISTINCT product_id) as products
            FROM product_group_links
        """)
        db_groups, db_products = cursor.fetchone()
        
        logger.info(f"\nDatabase contains:")
        logger.info(f"  Groups: {db_groups}")
        logger.info(f"  Products: {db_products}")
        
        cursor.close()
        
        logger.info(f"\nFull log saved to gpt4_matches_*.log")

if __name__ == "__main__":
    matcher = GPT4StrictMatcher()
    matcher.run_test_batch()