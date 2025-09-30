#!/usr/bin/env python3
"""
LLM-Based Product Matching System using GPT-3.5-Turbo
Handles cross-retailer matching and smart deduplication
"""

import logging
import psycopg2
import json
import openai
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass, asdict
from collections import defaultdict
from tqdm import tqdm
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class ProductInfo:
    """Product information for matching"""
    product_id: int
    retailer_id: int
    retailer_name: str
    canonical_name: str
    brand: str
    description: str
    attributes: Dict
    
    def to_dict(self):
        return {
            'id': self.product_id,
            'name': self.canonical_name,
            'brand': self.brand,
            'desc': self.description[:100] if self.description else None
        }

class LLMProductMatcher:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            port="5432", 
            database="price_comparison_app_v2",
            user="postgres",
            password="***REMOVED***"
        )
        
        # OpenAI client for GPT-3.5-Turbo
        self.client = openai.OpenAI(
            api_key="sk-lvCeuWfVJMG1V9dHWO9fxsubyhS2dkZEYIQzukfEuZT3BlbkFJidE3fzU5s3v2bwWYp_dCEhcRy0cxKbdQp41BBgnDAA"
        )
        
        self.matched_groups = []
        self.processed_products = set()
        
    def load_products(self) -> Dict[int, List[ProductInfo]]:
        """Load all products grouped by retailer"""
        cursor = self.conn.cursor()
        query = """
            SELECT 
                p.product_id,
                rp.retailer_id,
                r.retailername,
                p.canonical_name,
                p.brand,
                p.description,
                p.attributes
            FROM products p
            JOIN retailer_products rp ON p.product_id = rp.product_id
            JOIN retailers r ON rp.retailer_id = r.retailerid
            ORDER BY rp.retailer_id, p.canonical_name
        """
        
        cursor.execute(query)
        products_by_retailer = defaultdict(list)
        
        for row in cursor.fetchall():
            product = ProductInfo(
                product_id=row[0],
                retailer_id=row[1],
                retailer_name=row[2],
                canonical_name=row[3] or "",
                brand=row[4] or "",
                description=row[5] or "",
                attributes=row[6] or {}
            )
            products_by_retailer[row[1]].append(product)
        
        cursor.close()
        return products_by_retailer
    
    def match_products_batch(self, source_products: List[ProductInfo], 
                            target_products: List[ProductInfo]) -> List[Dict]:
        """Match a batch of products using GPT-3.5-Turbo"""
        
        # Prepare source and target product lists
        source_list = [p.to_dict() for p in source_products[:20]]  # Max 20 per batch
        target_list = [p.to_dict() for p in target_products[:50]]  # Sample of targets
        
        prompt = f"""You are a product matching expert for Israeli pharmacy products. Match products from Source to Target based on name, brand, and description.

Source Products (to match):
{json.dumps(source_list, ensure_ascii=False, indent=2)}

Target Products (to match against):
{json.dumps(target_list, ensure_ascii=False, indent=2)}

Rules:
1. Match same products even with slight name variations (Hebrew/English, abbreviations)
2. Brand must match (if specified)
3. Size/quantity should match (100ml = 100 מ"ל)
4. Return matches with confidence: high/medium/low
5. Group duplicate products in source (same product, different entries)

Return JSON only:
{{
  "matches": [
    {{"source_ids": [id1, id2], "target_ids": [id3], "confidence": "high", "canonical_name": "best name"}},
    ...
  ],
  "no_match": [id4, id5]
}}"""

        try:
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo-1106",
                messages=[
                    {"role": "system", "content": "You are a product matching expert. Return valid JSON only."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=2000,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            return result.get('matches', [])
            
        except Exception as e:
            logger.error(f"LLM matching error: {e}")
            return []
    
    def deduplicate_within_retailer(self, products: List[ProductInfo]) -> List[List[ProductInfo]]:
        """Group duplicate products within a single retailer"""
        groups = []
        processed = set()
        
        for i, product in enumerate(products):
            if i in processed:
                continue
                
            group = [product]
            processed.add(i)
            
            # Find similar products
            for j in range(i + 1, len(products)):
                if j in processed:
                    continue
                    
                # Simple similarity check
                if (self.normalize_string(products[j].canonical_name) == self.normalize_string(product.canonical_name) or
                    (product.brand and products[j].brand and 
                     self.normalize_string(product.brand) == self.normalize_string(products[j].brand) and
                     self.similar_names(product.canonical_name, products[j].canonical_name))):
                    group.append(products[j])
                    processed.add(j)
            
            groups.append(group)
        
        return groups
    
    def normalize_string(self, s: str) -> str:
        """Normalize string for comparison"""
        if not s:
            return ""
        # Remove common variations
        s = s.lower().strip()
        s = s.replace("'", "").replace('"', "").replace("-", " ")
        s = ' '.join(s.split())  # Normalize whitespace
        return s
    
    def similar_names(self, name1: str, name2: str) -> bool:
        """Check if two names are similar enough"""
        n1 = self.normalize_string(name1)
        n2 = self.normalize_string(name2)
        
        # Check if one contains the other
        if n1 in n2 or n2 in n1:
            return True
        
        # Check word overlap
        words1 = set(n1.split())
        words2 = set(n2.split())
        overlap = len(words1 & words2)
        
        if overlap >= min(len(words1), len(words2)) * 0.7:
            return True
        
        return False
    
    def save_matches(self, matches: List[Dict]):
        """Save matched products to the database"""
        cursor = self.conn.cursor()
        
        # Clear existing groups for products we're updating
        all_product_ids = []
        for match in matches:
            all_product_ids.extend(match.get('source_ids', []))
            all_product_ids.extend(match.get('target_ids', []))
        
        if all_product_ids:
            cursor.execute(
                "DELETE FROM product_group_links WHERE product_id = ANY(%s)",
                (all_product_ids,)
            )
        
        # Create new groups
        for match in matches:
            product_ids = match.get('source_ids', []) + match.get('target_ids', [])
            canonical_name = match.get('canonical_name', '')
            
            if len(product_ids) < 2:
                continue
            
            # Create product group
            cursor.execute(
                """INSERT INTO product_groups (canonical_name, created_at)
                   VALUES (%s, NOW())
                   RETURNING product_group_id""",
                (canonical_name,)
            )
            group_id = cursor.fetchone()[0]
            
            # Link products to group
            for pid in product_ids:
                cursor.execute(
                    """INSERT INTO product_group_links (group_id, product_id)
                       VALUES (%s, %s)
                       ON CONFLICT DO NOTHING""",
                    (group_id, pid)
                )
        
        self.conn.commit()
        cursor.close()
    
    def run_matching(self):
        """Run the complete matching process"""
        logger.info("Starting LLM-based product matching...")
        
        # Load products
        products_by_retailer = self.load_products()
        retailer_ids = list(products_by_retailer.keys())
        
        logger.info(f"Loaded products from {len(retailer_ids)} retailers")
        for rid in retailer_ids:
            logger.info(f"  Retailer {rid}: {len(products_by_retailer[rid])} products")
        
        # Get the retailer with most products as base
        base_retailer = max(retailer_ids, key=lambda r: len(products_by_retailer[r]))
        base_products = products_by_retailer[base_retailer]
        other_retailers = [r for r in retailer_ids if r != base_retailer]
        
        logger.info(f"Using retailer {base_retailer} as base with {len(base_products)} products")
        
        # First, deduplicate within each retailer
        logger.info("Deduplicating products within retailers...")
        deduplicated = {}
        for rid in retailer_ids:
            groups = self.deduplicate_within_retailer(products_by_retailer[rid])
            deduplicated[rid] = groups
            logger.info(f"  Retailer {rid}: {len(products_by_retailer[rid])} → {len(groups)} unique groups")
        
        # Process in batches
        all_matches = []
        batch_size = 20
        base_groups = deduplicated[base_retailer]
        
        logger.info(f"Matching {len(base_groups)} product groups across retailers...")
        
        with tqdm(total=len(base_groups), desc="Matching products") as pbar:
            for i in range(0, len(base_groups), batch_size):
                batch = base_groups[i:i+batch_size]
                
                # Flatten groups to individual products for matching
                source_products = []
                for group in batch:
                    source_products.extend(group)
                
                # Match against other retailers
                for other_retailer in other_retailers:
                    target_products = []
                    for group in deduplicated[other_retailer]:
                        target_products.extend(group)
                    
                    # Call LLM for matching
                    matches = self.match_products_batch(source_products, target_products)
                    
                    # Process matches
                    for match in matches:
                        # Convert to actual product IDs
                        processed_match = {
                            'source_ids': [],
                            'target_ids': [],
                            'confidence': match.get('confidence', 'medium'),
                            'canonical_name': match.get('canonical_name', '')
                        }
                        
                        # Map back to actual product IDs
                        for sid in match.get('source_ids', []):
                            for product in source_products:
                                if product.to_dict()['id'] == sid:
                                    processed_match['source_ids'].append(product.product_id)
                        
                        for tid in match.get('target_ids', []):
                            for product in target_products:
                                if product.to_dict()['id'] == tid:
                                    processed_match['target_ids'].append(product.product_id)
                        
                        if processed_match['source_ids'] and processed_match['target_ids']:
                            all_matches.append(processed_match)
                
                # Save batch of matches
                if all_matches:
                    self.save_matches(all_matches)
                    all_matches = []
                
                pbar.update(min(batch_size, len(base_groups) - i))
                
                # Rate limiting
                time.sleep(0.5)  # Avoid hitting rate limits
        
        # Final statistics
        self.print_statistics()
    
    def print_statistics(self):
        """Print matching statistics"""
        cursor = self.conn.cursor()
        
        # Count matched products
        cursor.execute("""
            WITH matched_products AS (
                SELECT pgl.group_id,
                       COUNT(DISTINCT p.product_id) as product_count,
                       COUNT(DISTINCT rp.retailer_id) as retailer_count
                FROM product_group_links pgl
                JOIN products p ON pgl.product_id = p.product_id
                JOIN retailer_products rp ON p.product_id = rp.product_id
                GROUP BY pgl.group_id
                HAVING COUNT(DISTINCT rp.retailer_id) > 1
            )
            SELECT 
                COUNT(*) as matched_groups,
                SUM(product_count) as total_matched_products,
                AVG(retailer_count) as avg_retailers_per_group
            FROM matched_products
        """)
        
        result = cursor.fetchone()
        if result:
            groups, products, avg_retailers = result
            logger.info(f"\n{'='*60}")
            logger.info(f"MATCHING RESULTS:")
            logger.info(f"  Matched product groups: {groups}")
            logger.info(f"  Total matched products: {products}")
            logger.info(f"  Average retailers per group: {avg_retailers:.2f}")
            
            # Calculate coverage
            cursor.execute("SELECT COUNT(*) FROM products")
            total_products = cursor.fetchone()[0]
            coverage = (products / total_products * 100) if total_products > 0 else 0
            logger.info(f"  Coverage: {coverage:.1f}% of all products")
            logger.info(f"{'='*60}\n")
        
        cursor.close()

def main():
    matcher = LLMProductMatcher()
    matcher.run_matching()

if __name__ == "__main__":
    main()