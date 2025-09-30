#!/usr/bin/env python3
"""
Fast LLM-Based Product Matching using GPT-3.5-Turbo
Optimized for speed with intelligent batching
"""

import logging
import psycopg2
import json
import openai
from typing import Dict, List, Set, Tuple
from dataclasses import dataclass
from collections import defaultdict
from tqdm import tqdm
import time
import random

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass 
class Product:
    product_id: int
    retailer_id: int
    name: str
    brand: str
    
    def to_json(self):
        return {
            'id': self.product_id,
            'n': self.name[:80],  # Shorten for API limits
            'b': self.brand[:30] if self.brand else ""
        }

class FastLLMMatcher:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="price_comparison_app_v2",
            user="postgres",
            password="***REMOVED***"
        )
        
        self.client = openai.OpenAI(
            api_key="sk-lvCeuWfVJMG1V9dHWO9fxsubyhS2dkZEYIQzukfEuZT3BlbkFJidE3fzU5s3v2bwWYp_dCEhcRy0cxKbdQp41BBgnDAA"
        )
    
    def load_products(self) -> Dict[int, List[Product]]:
        """Load products grouped by retailer"""
        cursor = self.conn.cursor()
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
            WHERE rn <= 3000  -- Get 3000 products per retailer
            ORDER BY retailer_id, product_id
        """
        
        cursor.execute(query)
        products_by_retailer = defaultdict(list)
        
        for row in cursor.fetchall():
            product = Product(
                product_id=row[0],
                retailer_id=row[1], 
                name=row[2] or "",
                brand=row[3] or ""
            )
            products_by_retailer[row[1]].append(product)
        
        cursor.close()
        return products_by_retailer
    
    def match_batch(self, source: List[Product], targets: List[Product]) -> List[Dict]:
        """Match products using GPT-3.5-Turbo"""
        
        # Prepare compact JSON
        src = [p.to_json() for p in source[:15]]  # 15 source products
        tgt = [p.to_json() for p in targets[:30]]  # 30 target products
        
        prompt = f"""Match pharmacy products from A to B. Same product = same active ingredient/purpose, ignore minor differences.

A:{json.dumps(src, ensure_ascii=False)}
B:{json.dumps(tgt, ensure_ascii=False)}

Return JSON: {{"m":[[src_id,tgt_id,confidence]...]}} where confidence=1-3 (3=best)"""

        try:
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo-1106",
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=500
            )
            
            text = response.choices[0].message.content
            # Extract JSON from response
            if '{' in text:
                json_str = text[text.index('{'):text.rindex('}')+1]
                result = json.loads(json_str)
                return result.get('m', [])
            return []
            
        except Exception as e:
            logger.error(f"API error: {e}")
            return []
    
    def save_matches(self, matches: List[Tuple[int, int, int]]):
        """Save matched products to database"""
        if not matches:
            return
            
        cursor = self.conn.cursor()
        
        # Group matches
        groups = defaultdict(set)
        for p1, p2, conf in matches:
            if conf >= 2:  # Only high/medium confidence
                # Find existing group or create new one
                found = False
                for gid, members in groups.items():
                    if p1 in members or p2 in members:
                        members.add(p1)
                        members.add(p2)
                        found = True
                        break
                if not found:
                    groups[len(groups)] = {p1, p2}
        
        # Save to database
        for group_products in groups.values():
            if len(group_products) < 2:
                continue
                
            # Get canonical name from first product
            cursor.execute(
                "SELECT canonical_name FROM products WHERE product_id = %s",
                (list(group_products)[0],)
            )
            name = cursor.fetchone()[0] if cursor.fetchone() else "Product Group"
            
            # Create group
            cursor.execute(
                "INSERT INTO product_groups (canonical_name) VALUES (%s) RETURNING product_group_id",
                (name,)
            )
            group_id = cursor.fetchone()[0]
            
            # Link products
            for pid in group_products:
                cursor.execute(
                    "INSERT INTO product_group_links (group_id, product_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (group_id, pid)
                )
        
        self.conn.commit()
        cursor.close()
    
    def run(self):
        """Run the matching process"""
        logger.info("Starting fast LLM matching...")
        
        # Clear existing groups
        cursor = self.conn.cursor()
        cursor.execute("TRUNCATE product_group_links, product_groups RESTART IDENTITY CASCADE")
        self.conn.commit()
        cursor.close()
        
        # Load products
        products = self.load_products()
        retailers = list(products.keys())
        
        if len(retailers) < 2:
            logger.error("Need at least 2 retailers")
            return
        
        # Use retailer with most products as base
        base_retailer = max(retailers, key=lambda r: len(products[r]))
        other_retailers = [r for r in retailers if r != base_retailer]
        
        base_products = products[base_retailer]
        logger.info(f"Base retailer {base_retailer}: {len(base_products)} products")
        
        # Process in batches
        batch_size = 15
        all_matches = []
        
        with tqdm(total=len(base_products), desc="Matching") as pbar:
            for i in range(0, len(base_products), batch_size):
                source_batch = base_products[i:i+batch_size]
                
                for other_retailer in other_retailers:
                    # Random sample of target products for efficiency
                    target_sample = random.sample(
                        products[other_retailer], 
                        min(100, len(products[other_retailer]))
                    )
                    
                    # Get matches
                    matches = self.match_batch(source_batch, target_sample)
                    
                    # Convert to product IDs
                    for match in matches:
                        if len(match) >= 3:
                            src_id = match[0]
                            tgt_id = match[1]
                            conf = match[2]
                            
                            # Map back to actual product IDs
                            src_product = next((p for p in source_batch if p.to_json()['id'] == src_id), None)
                            tgt_product = next((p for p in target_sample if p.to_json()['id'] == tgt_id), None)
                            
                            if src_product and tgt_product:
                                all_matches.append((src_product.product_id, tgt_product.product_id, conf))
                
                # Save periodically
                if len(all_matches) > 100:
                    self.save_matches(all_matches)
                    all_matches = []
                
                pbar.update(min(batch_size, len(base_products) - i))
                time.sleep(0.2)  # Rate limiting
        
        # Save remaining
        if all_matches:
            self.save_matches(all_matches)
        
        # Print statistics
        self.print_stats()
    
    def print_stats(self):
        """Print matching statistics"""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(DISTINCT pg.product_group_id) as groups,
                   COUNT(DISTINCT pgl.product_id) as products
            FROM product_groups pg
            JOIN product_group_links pgl ON pg.product_group_id = pgl.group_id
        """)
        
        groups, products = cursor.fetchone()
        
        cursor.execute("SELECT COUNT(*) FROM products")
        total = cursor.fetchone()[0]
        
        logger.info(f"\n{'='*50}")
        logger.info(f"RESULTS:")
        logger.info(f"  Matched groups: {groups}")
        logger.info(f"  Matched products: {products}")
        logger.info(f"  Coverage: {products/total*100:.1f}%")
        logger.info(f"{'='*50}")
        
        cursor.close()

if __name__ == "__main__":
    matcher = FastLLMMatcher()
    matcher.run()