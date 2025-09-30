#!/usr/bin/env python3
"""
Transparent LLM-Based Product Matching with Decision Logging
Shows what matches the LLM is making for review
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
from datetime import datetime

# Set up logging to both file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'llm_matches_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
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
            'n': self.name[:80],
            'b': self.brand[:30] if self.brand else ""
        }
    
    def __str__(self):
        return f"[{self.retailer_name}] {self.name} ({self.brand or 'no brand'})"

class TransparentLLMMatcher:
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
        
        self.match_count = 0
        self.no_match_count = 0
    
    def check_duplicates(self):
        """Check for duplicates in the products table"""
        cursor = self.conn.cursor()
        
        logger.info("\n" + "="*60)
        logger.info("CHECKING FOR DUPLICATES IN PRODUCTS TABLE...")
        
        # Check for exact name duplicates
        cursor.execute("""
            SELECT canonical_name, brand, COUNT(*) as cnt
            FROM products
            WHERE canonical_name IS NOT NULL
            GROUP BY canonical_name, brand
            HAVING COUNT(*) > 1
            ORDER BY cnt DESC
            LIMIT 10
        """)
        
        duplicates = cursor.fetchall()
        if duplicates:
            logger.warning(f"Found {len(duplicates)} duplicate product entries:")
            for name, brand, count in duplicates[:5]:
                logger.warning(f"  - '{name}' ({brand or 'no brand'}): {count} duplicates")
        else:
            logger.info("No exact duplicates found in products table")
        
        cursor.close()
        return len(duplicates) if duplicates else 0
    
    def load_products(self) -> Dict[int, List[Product]]:
        """Load products grouped by retailer with retailer names"""
        cursor = self.conn.cursor()
        
        # First get retailer names
        cursor.execute("SELECT retailerid, retailername FROM retailers")
        retailer_names = {rid: name for rid, name in cursor.fetchall()}
        
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
            WHERE rn <= 500  -- Start with 500 products per retailer for testing
            ORDER BY retailer_id, product_id
        """
        
        cursor.execute(query)
        products_by_retailer = defaultdict(list)
        
        for row in cursor.fetchall():
            product = Product(
                product_id=row[0],
                retailer_id=row[1],
                name=row[2] or "",
                brand=row[3] or "",
                retailer_name=retailer_names.get(row[1], f"Retailer_{row[1]}")
            )
            products_by_retailer[row[1]].append(product)
        
        cursor.close()
        return products_by_retailer
    
    def match_batch(self, source: List[Product], targets: List[Product]) -> List[Dict]:
        """Match products using GPT-3.5-Turbo with logging"""
        
        # Prepare compact JSON
        src = [p.to_json() for p in source[:10]]  # 10 source products
        tgt = [p.to_json() for p in targets[:30]]  # 30 target products
        
        prompt = f"""Match pharmacy products from Source to Target. Same product = same active ingredient/purpose.

Source:{json.dumps(src, ensure_ascii=False)}
Target:{json.dumps(tgt, ensure_ascii=False)}

Rules:
- Match identical or very similar products (ignore minor brand/size differences)
- Confidence: 3=exact match, 2=very likely same, 1=possibly same
- Return JSON: {{"matches":[[src_id,tgt_id,confidence]...], "no_match":[unmatched_src_ids]}}"""

        try:
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo-1106",
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=800
            )
            
            text = response.choices[0].message.content
            
            # Log the LLM's raw response
            logger.debug(f"LLM Response: {text[:200]}...")
            
            # Extract JSON from response
            if '{' in text:
                json_str = text[text.index('{'):text.rindex('}')+1]
                result = json.loads(json_str)
                
                # Log matching decisions
                matches = result.get('matches', [])
                no_matches = result.get('no_match', [])
                
                if matches:
                    logger.info(f"\n--- LLM Found {len(matches)} matches ---")
                    for match in matches[:5]:  # Show first 5
                        if len(match) >= 3:
                            src_id, tgt_id, conf = match[0], match[1], match[2]
                            src_prod = next((p for p in source if p.to_json()['id'] == src_id), None)
                            tgt_prod = next((p for p in targets if p.to_json()['id'] == tgt_id), None)
                            
                            if src_prod and tgt_prod:
                                confidence_str = ['', 'LOW', 'MEDIUM', 'HIGH'][min(conf, 3)]
                                logger.info(f"  MATCH ({confidence_str}): {src_prod} <=> {tgt_prod}")
                
                if no_matches:
                    logger.info(f"  No matches for {len(no_matches)} products")
                
                return matches
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
                for gid, members in list(groups.items()):
                    if p1 in members or p2 in members:
                        members.add(p1)
                        members.add(p2)
                        found = True
                        break
                if not found:
                    groups[len(groups)] = {p1, p2}
        
        # Save to database
        saved_count = 0
        for group_products in groups.values():
            if len(group_products) < 2:
                continue
                
            # Get canonical name from first product
            cursor.execute(
                "SELECT canonical_name FROM products WHERE product_id = %s",
                (list(group_products)[0],)
            )
            result = cursor.fetchone()
            name = result[0] if result else "Product Group"
            
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
            saved_count += 1
        
        self.conn.commit()
        cursor.close()
        
        if saved_count > 0:
            logger.info(f"Saved {saved_count} product groups to database")
    
    def run(self):
        """Run the matching process"""
        logger.info("="*60)
        logger.info("STARTING TRANSPARENT LLM MATCHING")
        logger.info("="*60)
        
        # Check for duplicates first
        dup_count = self.check_duplicates()
        if dup_count > 0:
            logger.warning(f"Consider deduplicating {dup_count} product entries before matching")
        
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
        logger.info(f"\nBase retailer {base_retailer}: {len(base_products)} products")
        for other in other_retailers:
            logger.info(f"Target retailer {other}: {len(products[other])} products")
        
        # Process in batches
        batch_size = 10
        all_matches = []
        
        logger.info(f"\nProcessing {len(base_products)} products in batches of {batch_size}...")
        logger.info("="*60)
        
        with tqdm(total=len(base_products), desc="Matching") as pbar:
            for i in range(0, min(len(base_products), 100), batch_size):  # Limit to 100 for testing
                source_batch = base_products[i:i+batch_size]
                
                logger.info(f"\n--- Batch {i//batch_size + 1} ---")
                logger.info(f"Source products: {[str(p) for p in source_batch[:3]]}...")
                
                for other_retailer in other_retailers:
                    # Random sample of target products
                    target_sample = random.sample(
                        products[other_retailer], 
                        min(50, len(products[other_retailer]))
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
                                self.match_count += 1
                
                # Save periodically
                if len(all_matches) > 50:
                    self.save_matches(all_matches)
                    all_matches = []
                
                pbar.update(min(batch_size, len(base_products) - i))
                time.sleep(0.3)  # Rate limiting
        
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
        
        result = cursor.fetchone()
        groups = result[0] if result[0] else 0
        products = result[1] if result[1] else 0
        
        cursor.execute("SELECT COUNT(*) FROM products")
        total = cursor.fetchone()[0]
        
        logger.info("\n" + "="*60)
        logger.info("FINAL RESULTS:")
        logger.info(f"  Total matches attempted: {self.match_count}")
        logger.info(f"  Matched groups created: {groups}")
        logger.info(f"  Products in groups: {products}")
        logger.info(f"  Coverage: {products/total*100:.1f}% of all products")
        logger.info("="*60)
        
        # Show sample matches
        cursor.execute("""
            SELECT pg.canonical_name, COUNT(pgl.product_id) as product_count
            FROM product_groups pg
            JOIN product_group_links pgl ON pg.product_group_id = pgl.group_id
            GROUP BY pg.product_group_id, pg.canonical_name
            ORDER BY product_count DESC
            LIMIT 5
        """)
        
        samples = cursor.fetchall()
        if samples:
            logger.info("\nTop matched product groups:")
            for name, count in samples:
                logger.info(f"  - {name[:60]}: {count} products")
        
        cursor.close()
        logger.info(f"\nFull log saved to llm_matches_*.log")

if __name__ == "__main__":
    matcher = TransparentLLMMatcher()
    matcher.run()