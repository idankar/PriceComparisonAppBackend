#!/usr/bin/env python3
"""
Unified Product Matching System
Runs all matching methods in sequence: Barcode → Fuzzy → Vector → LLM
"""

import psycopg2
import psycopg2.extras
import numpy as np
from typing import Dict, List, Set, Tuple, Optional
import re
import logging
from fuzzywuzzy import fuzz
from dataclasses import dataclass
import json
from datetime import datetime
from tqdm import tqdm
import openai  # For LLM matching
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class MatchResult:
    product_ids: List[int]
    confidence: float
    method: str
    details: Dict

class UnifiedProductMatcher:
    def __init__(self, db_config: Dict[str, str], openai_api_key: Optional[str] = None):
        self.db_config = db_config
        self.conn = None
        self.openai_api_key = openai_api_key or os.getenv('OPENAI_API_KEY')
        
        # Statistics tracking
        self.stats = {
            'total_products': 0,
            'matched_by_barcode': 0,
            'matched_by_fuzzy': 0,
            'matched_by_vector': 0,
            'matched_by_llm': 0,
            'unmatched': 0
        }
        
    def connect(self):
        """Connect to database"""
        self.conn = psycopg2.connect(**self.db_config)
        logger.info("Connected to database")
        
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            
    def initialize_tables(self):
        """Create or update necessary tables"""
        cursor = self.conn.cursor()
        
        # Create product_matches table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_matches (
            match_id SERIAL PRIMARY KEY,
            master_product_id INTEGER NOT NULL,
            retailer_product_ids INTEGER[] NOT NULL,
            match_confidence DECIMAL(3,2) NOT NULL,
            match_method VARCHAR(50) NOT NULL,
            match_details JSONB,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            reviewed BOOLEAN DEFAULT FALSE
        );
        
        CREATE INDEX IF NOT EXISTS idx_matches_method ON product_matches(match_method);
        CREATE INDEX IF NOT EXISTS idx_matches_products ON product_matches USING GIN(retailer_product_ids);
        """)
        
        # Create staging table for unmatched products
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS unmatched_products (
            product_id INTEGER PRIMARY KEY,
            retailer_id INTEGER,
            product_name TEXT,
            brand TEXT,
            attempted_methods TEXT[],
            last_attempt TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        self.conn.commit()
        cursor.close()
        logger.info("Tables initialized")
        
    def get_all_products(self) -> Dict[int, List[Dict]]:
        """Get all products grouped by retailer"""
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        cursor.execute("""
        SELECT 
            p.product_id,
            p.canonical_name,
            p.brand,
            p.description,
            p.attributes,
            rp.retailer_id,
            rp.retailer_item_code,
            r.retailername
        FROM products p
        JOIN retailer_products rp ON p.product_id = rp.product_id
        JOIN retailers r ON r.retailerid = rp.retailer_id
        WHERE rp.retailer_id IN (52, 150, 97)
        ORDER BY rp.retailer_id, p.brand, p.canonical_name
        """)
        
        products_by_retailer = {}
        for row in cursor.fetchall():
            retailer_id = row['retailer_id']
            if retailer_id not in products_by_retailer:
                products_by_retailer[retailer_id] = []
            products_by_retailer[retailer_id].append(dict(row))
            
        cursor.close()
        
        total = sum(len(products) for products in products_by_retailer.values())
        self.stats['total_products'] = total
        logger.info(f"Loaded {total} products from {len(products_by_retailer)} retailers")
        
        return products_by_retailer
        
    def extract_barcode(self, text: str) -> Optional[str]:
        """Extract barcode from text"""
        if not text:
            return None
            
        # Common barcode patterns
        patterns = [
            r'\b(\d{13})\b',  # EAN-13
            r'\b(\d{12})\b',  # UPC-A
            r'\b(\d{8})\b',   # EAN-8
            r'ברקוד[:\s]*(\d+)',
            r'מקט[:\s]*(\d+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, str(text), re.IGNORECASE)
            if match:
                barcode = match.group(1)
                if len(barcode) in [8, 12, 13]:
                    return barcode
        return None
        
    def match_by_barcode(self, products_by_retailer: Dict) -> List[MatchResult]:
        """Step 1: Match products by barcode"""
        logger.info("Starting barcode matching...")
        matches = []
        matched_products = set()
        
        # Extract barcodes from all products
        barcode_map = {}  # barcode -> [(product_id, retailer_id)]
        
        for retailer_id, products in products_by_retailer.items():
            for product in products:
                if product['product_id'] in matched_products:
                    continue
                    
                # Try to find barcode in various fields
                barcode = None
                for field in ['canonical_name', 'description', 'retailer_item_code']:
                    barcode = self.extract_barcode(product.get(field, ''))
                    if barcode:
                        break
                        
                if barcode:
                    if barcode not in barcode_map:
                        barcode_map[barcode] = []
                    barcode_map[barcode].append((product['product_id'], retailer_id))
        
        # Create matches from products sharing barcodes
        for barcode, product_list in barcode_map.items():
            if len(product_list) > 1:
                # Check if products are from different retailers
                retailer_ids = set(item[1] for item in product_list)
                if len(retailer_ids) > 1:
                    product_ids = [item[0] for item in product_list]
                    match = MatchResult(
                        product_ids=product_ids,
                        confidence=1.0,
                        method='barcode',
                        details={'barcode': barcode, 'retailers': len(retailer_ids)}
                    )
                    matches.append(match)
                    matched_products.update(product_ids)
                    self.stats['matched_by_barcode'] += len(product_ids)
        
        logger.info(f"Barcode matching: found {len(matches)} matches covering {len(matched_products)} products")
        return matches, matched_products
        
    def calculate_fuzzy_score(self, prod1: Dict, prod2: Dict) -> Tuple[float, Dict]:
        """Calculate fuzzy matching score between two products"""
        scores = {}
        
        # Brand matching (40% weight)
        brand1 = (prod1.get('brand') or '').lower().strip()
        brand2 = (prod2.get('brand') or '').lower().strip()
        if brand1 and brand2:
            scores['brand'] = fuzz.ratio(brand1, brand2) / 100
        else:
            scores['brand'] = 0.5 if not brand1 and not brand2 else 0
            
        # Name matching (40% weight)
        name1 = prod1.get('canonical_name', '').lower()
        name2 = prod2.get('canonical_name', '').lower()
        scores['name'] = fuzz.token_set_ratio(name1, name2) / 100
        
        # Size matching (20% weight)
        size1 = prod1.get('attributes', {}).get('size_value')
        size2 = prod2.get('attributes', {}).get('size_value')
        if size1 and size2:
            try:
                scores['size'] = 1.0 if float(size1) == float(size2) else 0.5
            except:
                scores['size'] = 0.5
        else:
            scores['size'] = 0.5
            
        # Calculate weighted score
        weights = {'brand': 0.4, 'name': 0.4, 'size': 0.2}
        total_score = sum(scores[k] * weights[k] for k in weights)
        
        return total_score, scores
        
    def match_by_fuzzy(self, products_by_retailer: Dict, already_matched: Set[int], 
                      threshold: float = 0.85) -> List[MatchResult]:
        """Step 2: Match products by fuzzy string matching"""
        logger.info("Starting fuzzy matching...")
        matches = []
        matched_products = set()
        
        retailer_ids = list(products_by_retailer.keys())
        
        # Compare products between different retailers
        for i in range(len(retailer_ids)):
            for j in range(i + 1, len(retailer_ids)):
                retailer1, retailer2 = retailer_ids[i], retailer_ids[j]
                products1 = [p for p in products_by_retailer[retailer1] 
                           if p['product_id'] not in already_matched and p['product_id'] not in matched_products]
                products2 = [p for p in products_by_retailer[retailer2] 
                           if p['product_id'] not in already_matched and p['product_id'] not in matched_products]
                
                for prod1 in tqdm(products1, desc=f"Fuzzy matching {retailer1} vs {retailer2}"):
                    best_match = None
                    best_score = 0
                    best_details = {}
                    
                    for prod2 in products2:
                        score, details = self.calculate_fuzzy_score(prod1, prod2)
                        if score > best_score and score >= threshold:
                            best_score = score
                            best_match = prod2
                            best_details = details
                            
                    if best_match:
                        match = MatchResult(
                            product_ids=[prod1['product_id'], best_match['product_id']],
                            confidence=best_score,
                            method='fuzzy',
                            details=best_details
                        )
                        matches.append(match)
                        matched_products.update([prod1['product_id'], best_match['product_id']])
                        self.stats['matched_by_fuzzy'] += 2
        
        logger.info(f"Fuzzy matching: found {len(matches)} matches covering {len(matched_products)} products")
        return matches, matched_products
        
    def match_by_llm(self, products_by_retailer: Dict, unmatched: Set[int], 
                     sample_size: int = 100) -> List[MatchResult]:
        """Step 4: Use LLM for difficult matches"""
        if not self.openai_api_key:
            logger.warning("OpenAI API key not provided, skipping LLM matching")
            return [], set()
            
        logger.info("Starting LLM matching for remaining products...")
        matches = []
        matched_products = set()
        
        # Get sample of unmatched products
        unmatched_products = []
        for retailer_id, products in products_by_retailer.items():
            for product in products:
                if product['product_id'] in unmatched:
                    unmatched_products.append(product)
                    if len(unmatched_products) >= sample_size:
                        break
                        
        # Group by potential matches using LLM
        openai.api_key = self.openai_api_key
        
        # Create batches for LLM processing
        batch_size = 20
        for i in range(0, len(unmatched_products), batch_size):
            batch = unmatched_products[i:i+batch_size]
            
            # Prepare products for LLM
            products_text = []
            for j, product in enumerate(batch):
                products_text.append(
                    f"{j}. {product['canonical_name']} | Brand: {product.get('brand', 'N/A')} | "
                    f"Retailer: {product['retailername']} | Size: {product.get('attributes', {}).get('size_value', 'N/A')}"
                )
                
            prompt = f"""
            Below are {len(batch)} pharmacy products from different retailers in Israel.
            Identify which products are the same (just sold by different retailers).
            
            Products:
            {chr(10).join(products_text)}
            
            Return a JSON array of matches. Each match should have:
            - "indices": array of product indices that match
            - "confidence": 0.0 to 1.0
            - "reason": brief explanation
            
            Only return matches where confidence > 0.8.
            """
            
            try:
                response = openai.ChatCompletion.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {"role": "system", "content": "You are an expert at matching pharmacy products across retailers."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.1
                )
                
                # Parse LLM response
                llm_matches = json.loads(response.choices[0].message.content)
                
                for llm_match in llm_matches:
                    indices = llm_match['indices']
                    if len(indices) > 1:
                        product_ids = [batch[idx]['product_id'] for idx in indices]
                        match = MatchResult(
                            product_ids=product_ids,
                            confidence=llm_match['confidence'],
                            method='llm',
                            details={'reason': llm_match['reason']}
                        )
                        matches.append(match)
                        matched_products.update(product_ids)
                        self.stats['matched_by_llm'] += len(product_ids)
                        
            except Exception as e:
                logger.error(f"LLM matching error: {e}")
                
        logger.info(f"LLM matching: found {len(matches)} matches covering {len(matched_products)} products")
        return matches, matched_products
        
    def save_matches(self, matches: List[MatchResult]):
        """Save all matches to database"""
        cursor = self.conn.cursor()
        
        for match in matches:
            cursor.execute("""
            INSERT INTO product_matches 
            (master_product_id, retailer_product_ids, match_confidence, match_method, match_details)
            VALUES (%s, %s, %s, %s, %s)
            """, (
                min(match.product_ids),  # Use lowest ID as master
                match.product_ids,
                match.confidence,
                match.method,
                psycopg2.extras.Json(match.details)
            ))
            
        self.conn.commit()
        cursor.close()
        logger.info(f"Saved {len(matches)} matches to database")
        
    def run_complete_matching(self):
        """Run the complete matching pipeline"""
        logger.info("="*60)
        logger.info("Starting Unified Product Matching")
        logger.info("="*60)
        
        # Initialize
        self.connect()
        self.initialize_tables()
        
        # Get all products
        products_by_retailer = self.get_all_products()
        all_matches = []
        all_matched = set()
        
        # Step 1: Barcode matching
        barcode_matches, barcode_matched = self.match_by_barcode(products_by_retailer)
        all_matches.extend(barcode_matches)
        all_matched.update(barcode_matched)
        
        # Step 2: Fuzzy matching on remaining products
        fuzzy_matches, fuzzy_matched = self.match_by_fuzzy(
            products_by_retailer, all_matched, threshold=0.85
        )
        all_matches.extend(fuzzy_matches)
        all_matched.update(fuzzy_matched)
        
        # Step 3: Vector matching (if embeddings available)
        # TODO: Implement if you have embeddings
        
        # Step 4: LLM matching for remaining difficult cases
        remaining = set()
        for products in products_by_retailer.values():
            for product in products:
                if product['product_id'] not in all_matched:
                    remaining.add(product['product_id'])
                    
        if self.openai_api_key and remaining:
            llm_matches, llm_matched = self.match_by_llm(
                products_by_retailer, remaining, sample_size=100
            )
            all_matches.extend(llm_matches)
            all_matched.update(llm_matched)
            
        # Calculate unmatched
        self.stats['unmatched'] = self.stats['total_products'] - len(all_matched)
        
        # Save all matches
        self.save_matches(all_matches)
        
        # Print summary
        self.print_summary()
        
        # Save unmatched products for review
        self.save_unmatched(products_by_retailer, all_matched)
        
        self.close()
        
    def print_summary(self):
        """Print matching summary"""
        print("\n" + "="*60)
        print("MATCHING SUMMARY")
        print("="*60)
        print(f"Total products: {self.stats['total_products']:,}")
        print(f"Matched by barcode: {self.stats['matched_by_barcode']:,} ({self.stats['matched_by_barcode']/self.stats['total_products']*100:.1f}%)")
        print(f"Matched by fuzzy: {self.stats['matched_by_fuzzy']:,} ({self.stats['matched_by_fuzzy']/self.stats['total_products']*100:.1f}%)")
        print(f"Matched by LLM: {self.stats['matched_by_llm']:,} ({self.stats['matched_by_llm']/self.stats['total_products']*100:.1f}%)")
        print(f"Unmatched: {self.stats['unmatched']:,} ({self.stats['unmatched']/self.stats['total_products']*100:.1f}%)")
        print(f"\nTotal matched: {self.stats['total_products'] - self.stats['unmatched']:,} ({(self.stats['total_products'] - self.stats['unmatched'])/self.stats['total_products']*100:.1f}%)")
        print("="*60)
        
    def save_unmatched(self, products_by_retailer: Dict, matched: Set[int]):
        """Save unmatched products for manual review"""
        cursor = self.conn.cursor()
        
        # Clear old unmatched
        cursor.execute("TRUNCATE TABLE unmatched_products")
        
        # Insert new unmatched
        for retailer_id, products in products_by_retailer.items():
            for product in products:
                if product['product_id'] not in matched:
                    cursor.execute("""
                    INSERT INTO unmatched_products 
                    (product_id, retailer_id, product_name, brand, attempted_methods)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (product_id) DO NOTHING
                    """, (
                        product['product_id'],
                        retailer_id,
                        product['canonical_name'],
                        product.get('brand'),
                        ['barcode', 'fuzzy', 'llm'] if self.openai_api_key else ['barcode', 'fuzzy']
                    ))
                    
        self.conn.commit()
        cursor.close()
        
        # Also export to CSV
        cursor = self.conn.cursor()
        cursor.execute("""
        SELECT 
            up.product_id,
            up.product_name,
            up.brand,
            r.retailername
        FROM unmatched_products up
        JOIN retailers r ON up.retailer_id = r.retailerid
        ORDER BY up.brand, up.product_name
        """)
        
        with open(f'unmatched_products_{datetime.now().strftime("%Y%m%d")}.csv', 'w', encoding='utf-8-sig') as f:
            f.write("product_id,product_name,brand,retailer\n")
            for row in cursor.fetchall():
                f.write(f"{row[0]},\"{row[1]}\",\"{row[2]}\",{row[3]}\n")
                
        cursor.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Unified product matching system')
    parser.add_argument('--openai-key', help='OpenAI API key for LLM matching')
    args = parser.parse_args()
    
    db_config = {
        'host': 'localhost',
        'database': 'price_comparison_app_v2',
        'user': 'postgres',
        'password': '***REMOVED***',
        'port': 5432
    }
    
    matcher = UnifiedProductMatcher(db_config, openai_api_key=args.openai_key)
    matcher.run_complete_matching()