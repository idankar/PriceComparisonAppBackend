#!/usr/bin/env python3
"""
Fast Vector-Based Product Matching System
Uses sentence transformers and cosine similarity for efficient matching
"""

import logging
import psycopg2
import numpy as np
from typing import Dict, List, Set, Tuple, Optional
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import json
from dataclasses import dataclass, asdict
from collections import defaultdict
import openai
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class MatchResult:
    """Result of a product match"""
    product_ids: List[int]
    confidence: float
    method: str
    details: Dict = None
    
    def to_dict(self):
        return asdict(self)

class VectorProductMatcher:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            port="5432", 
            database="price_comparison_app_v2",
            user="postgres",
            password="***REMOVED***"
        )
        
        # Load sentence transformer for embeddings - using better model for Hebrew
        logger.info("Loading sentence transformer model...")
        self.encoder = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2')
        
        # OpenAI for LLM matching
        self.llm_client = openai.OpenAI(
            api_key="sk-lvCeuWfVJMG1V9dHWO9fxsubyhS2dkZEYIQzukfEuZT3BlbkFJidE3fzU5s3v2bwWYp_dCEhcRy0cxKbdQp41BBgnDAA"
        )
        
        self.stats = {
            'matched_by_barcode': 0,
            'matched_by_vector': 0,
            'matched_by_llm': 0,
            'total_products': 0
        }
        
        self.init_database()
        
    def init_database(self):
        """Initialize database with embeddings column"""
        cursor = self.conn.cursor()
        
        # Drop and recreate embedding column with correct dimensions
        cursor.execute("ALTER TABLE products DROP COLUMN IF EXISTS embedding")
        cursor.execute("ALTER TABLE products ADD COLUMN embedding TEXT")
        
        # Ensure product_matches table exists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_matches (
            match_id SERIAL PRIMARY KEY,
            master_product_id INTEGER,
            retailer_product_ids INTEGER[],
            match_confidence DECIMAL(3,2),
            match_method VARCHAR(20),
            match_details JSONB,
            created_at TIMESTAMP DEFAULT NOW()
        )
        """)
        
        cursor.close()
        self.conn.commit()
        logger.info("Database initialized with vector support")
        
    def generate_embeddings(self):
        """Generate embeddings for all products"""
        cursor = self.conn.cursor()
        
        # Get products without embeddings
        cursor.execute("""
            SELECT p.product_id, p.canonical_name, p.brand, p.description, p.attributes
            FROM products p
            JOIN retailer_products rp ON p.product_id = rp.product_id
            WHERE rp.retailer_id IN (52, 150, 97) AND p.embedding IS NULL
            LIMIT 5000
        """)
        
        products = cursor.fetchall()
        logger.info(f"Generating embeddings for {len(products)} products...")
        
        batch_size = 100
        for i in tqdm(range(0, len(products), batch_size), desc="Generating embeddings"):
            batch = products[i:i+batch_size]
            
            # Create text representations
            texts = []
            for product in batch:
                product_id, name, brand, desc, attrs = product
                
                # Combine all text fields
                text_parts = [name or ""]
                if brand:
                    text_parts.append(brand)
                if desc:
                    text_parts.append(desc)
                if attrs and isinstance(attrs, dict):
                    if attrs.get('size_value'):
                        text_parts.append(f"Size: {attrs['size_value']}")
                    if attrs.get('product_type'):
                        text_parts.append(attrs['product_type'])
                        
                texts.append(" ".join(text_parts))
            
            # Generate embeddings
            embeddings = self.encoder.encode(texts)
            
            # Store in database
            for j, (product_id, _, _, _, _) in enumerate(batch):
                embedding_json = json.dumps(embeddings[j].tolist())
                cursor.execute("""
                    UPDATE products SET embedding = %s WHERE product_id = %s
                """, (embedding_json, product_id))
        
        cursor.close()
        self.conn.commit()
        logger.info(f"Generated embeddings for {len(products)} products")
        
    def match_by_barcode(self, products_by_retailer: Dict) -> Tuple[List[MatchResult], Set[int]]:
        """Step 1: Match products by barcode"""
        logger.info("Starting barcode matching...")
        matches = []
        matched_products = set()
        
        # Group by retailer item code (as barcode substitute)
        barcode_groups = defaultdict(list)
        for retailer_id, products in products_by_retailer.items():
            for product in products:
                item_code = product.get('retailer_item_code')
                # Look for barcode-like patterns in item codes or names
                if item_code and len(str(item_code).strip()) >= 8:
                    # Extract numeric sequences that look like barcodes
                    import re
                    numeric_parts = re.findall(r'\d{8,}', str(item_code))
                    if numeric_parts:
                        barcode_groups[numeric_parts[0]].append(product)
                
                # Also check product name/description for barcode patterns
                name = product.get('canonical_name', '')
                desc = product.get('description', '')
                combined_text = f"{name} {desc}"
                numeric_parts = re.findall(r'\b\d{12,13}\b', combined_text)  # EAN-13/UPC patterns
                for barcode in numeric_parts:
                    barcode_groups[barcode].append(product)
        
        # Create matches for products with same barcode
        for barcode, products in barcode_groups.items():
            if len(products) > 1:
                product_ids = [p['product_id'] for p in products]
                match = MatchResult(
                    product_ids=product_ids,
                    confidence=1.0,
                    method='barcode',
                    details={'barcode': barcode}
                )
                matches.append(match)
                matched_products.update(product_ids)
                self.stats['matched_by_barcode'] += len(product_ids)
        
        logger.info(f"Barcode matching: found {len(matches)} matches covering {len(matched_products)} products")
        return matches, matched_products
        
    def match_by_vectors(self, products_by_retailer: Dict, already_matched: Set[int],
                        threshold: float = 0.85) -> Tuple[List[MatchResult], Set[int]]:
        """Step 2: Match products using vector similarity"""
        logger.info("Starting vector similarity matching...")
        matches = []
        matched_products = set()
        
        # Get all unmatched products with embeddings
        cursor = self.conn.cursor()
        unmatched_products = []
        for retailer_id, products in products_by_retailer.items():
            for product in products:
                if product['product_id'] not in already_matched:
                    cursor.execute("""
                        SELECT embedding FROM products WHERE product_id = %s
                    """, (product['product_id'],))
                    result = cursor.fetchone()
                    if result and result[0]:
                        product['embedding'] = np.array(json.loads(result[0]))
                        unmatched_products.append(product)
        
        if not unmatched_products:
            return matches, matched_products
            
        logger.info(f"Computing similarities for {len(unmatched_products)} products...")
        
        # Compute pairwise similarities
        embeddings = np.array([p['embedding'] for p in unmatched_products])
        similarities = cosine_similarity(embeddings)
        
        # Find matches above threshold
        used = set()
        for i in range(len(unmatched_products)):
            if i in used:
                continue
                
            product_group = [i]
            for j in range(i + 1, len(unmatched_products)):
                if j in used:
                    continue
                    
                # Check if products are from different retailers
                if (unmatched_products[i]['retailer_id'] != unmatched_products[j]['retailer_id'] and
                    similarities[i][j] >= threshold):
                    
                    # Additional validation: similar size if available
                    attrs_i = unmatched_products[i].get('attributes', {}) or {}
                    attrs_j = unmatched_products[j].get('attributes', {}) or {}
                    size_i = attrs_i.get('size_value')
                    size_j = attrs_j.get('size_value')
                    
                    size_match = True
                    if size_i and size_j:
                        try:
                            size_match = abs(float(size_i) - float(size_j)) / max(float(size_i), float(size_j)) < 0.2
                        except:
                            size_match = True  # If can't parse, assume match
                    
                    if size_match:
                        product_group.append(j)
            
            if len(product_group) > 1:
                product_ids = [unmatched_products[idx]['product_id'] for idx in product_group]
                avg_similarity = np.mean([similarities[product_group[0]][idx] for idx in product_group[1:]])
                
                match = MatchResult(
                    product_ids=product_ids,
                    confidence=float(avg_similarity),
                    method='vector',
                    details={'similarity': float(avg_similarity)}
                )
                matches.append(match)
                matched_products.update(product_ids)
                used.update(product_group)
                self.stats['matched_by_vector'] += len(product_ids)
        
        cursor.close()
        logger.info(f"Vector matching: found {len(matches)} matches covering {len(matched_products)} products")
        return matches, matched_products
        
    def match_by_llm(self, products_by_retailer: Dict, unmatched: Set[int],
                     sample_size: int = 200) -> Tuple[List[MatchResult], Set[int]]:
        """Step 3: Use LLM for remaining difficult matches"""
        if not self.llm_client:
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
        
        # Process in batches
        batch_size = 15
        for i in range(0, len(unmatched_products), batch_size):
            batch = unmatched_products[i:i+batch_size]
            
            # Prepare products for LLM
            products_text = []
            for j, product in enumerate(batch):
                attrs = product.get('attributes', {}) or {}
                size = attrs.get('size_value', 'N/A')
                category = attrs.get('product_type', 'N/A')
                
                products_text.append(
                    f"{j}. {product['canonical_name']} | Brand: {product.get('brand', 'N/A')} | "
                    f"Retailer: {product['retailername']} | Size: {size} | Category: {category}"
                )
                
            prompt = f"""
            Below are {len(batch)} pharmacy products from different retailers in Israel.
            Identify which products are the SAME product (just sold by different retailers).
            
            Products:
            {chr(10).join(products_text)}
            
            Return a JSON object with a "matches" key containing an array of matches.
            Each match should have:
            - "indices": array of product indices that match (e.g., [0, 5, 12])
            - "confidence": 0.0 to 1.0
            - "reason": brief explanation
            
            Example format:
            {{"matches": [{{"indices": [0, 3], "confidence": 0.95, "reason": "Same brand and product name"}}]}}
            
            Only return matches where confidence > 0.8.
            If no matches found, return: {{"matches": []}}
            """
            
            try:
                response = self.llm_client.chat.completions.create(
                    model="gpt-4o",
                    messages=[
                        {"role": "system", "content": "You are an expert at matching pharmacy products across retailers. Always respond with valid JSON."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.1,
                    response_format={"type": "json_object"}
                )
                
                response_content = response.choices[0].message.content
                parsed = json.loads(response_content)
                llm_matches = parsed.get('matches', [])
                
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
        
        # Clear existing matches
        cursor.execute("DELETE FROM product_matches")
        
        for match in matches:
            cursor.execute("""
            INSERT INTO product_matches 
            (master_product_id, retailer_product_ids, match_confidence, match_method, match_details)
            VALUES (%s, %s, %s, %s, %s)
            """, (
                match.product_ids[0],
                match.product_ids,
                match.confidence,
                match.method,
                json.dumps(match.details) if match.details else None
            ))
        
        cursor.close()
        self.conn.commit()
        logger.info(f"Saved {len(matches)} matches to database")
        
    def run_matching_pipeline(self):
        """Run the complete matching pipeline"""
        logger.info("=" * 60)
        logger.info("Starting Vector-Based Product Matching Pipeline")
        logger.info("=" * 60)
        
        # Load products
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT p.product_id, p.canonical_name, p.brand, p.description,
                   p.attributes, rp.retailer_id, rt.retailername, rp.retailer_item_code
            FROM products p
            JOIN retailer_products rp ON p.product_id = rp.product_id
            JOIN retailers rt ON rp.retailer_id = rt.retailerid
            WHERE rp.retailer_id IN (52, 150, 97)
        """)
        
        results = cursor.fetchall()
        cursor.close()
        
        # Organize by retailer
        products_by_retailer = defaultdict(list)
        for row in results:
            product_id, name, brand, desc, attrs, retailer_id, retailer_name, retailer_item_code = row
            product = {
                'product_id': product_id,
                'canonical_name': name,
                'brand': brand,
                'description': desc,
                'retailer_item_code': retailer_item_code,
                'attributes': attrs,
                'retailer_id': retailer_id,
                'retailername': retailer_name
            }
            products_by_retailer[retailer_id].append(product)
        
        total_products = len(results)
        self.stats['total_products'] = total_products
        logger.info(f"Loaded {total_products} products from {len(products_by_retailer)} retailers")
        
        # Generate embeddings if needed
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM products WHERE embedding IS NULL AND product_id IN (SELECT product_id FROM retailer_products WHERE retailer_id IN (52, 150, 97))")
        missing_embeddings = cursor.fetchone()[0]
        cursor.close()
        
        if missing_embeddings > 0:
            logger.info(f"Generating embeddings for {missing_embeddings} products...")
            self.generate_embeddings()
        
        all_matches = []
        all_matched = set()
        
        # Step 1: Barcode matching
        barcode_matches, barcode_matched = self.match_by_barcode(products_by_retailer)
        all_matches.extend(barcode_matches)
        all_matched.update(barcode_matched)
        
        # Step 2: Vector similarity matching
        vector_matches, vector_matched = self.match_by_vectors(
            products_by_retailer, all_matched, threshold=0.7
        )
        all_matches.extend(vector_matches)
        all_matched.update(vector_matched)
        
        # Step 3: LLM matching for remaining products
        remaining = set()
        for products in products_by_retailer.values():
            for product in products:
                if product['product_id'] not in all_matched:
                    remaining.add(product['product_id'])
        
        if remaining:
            llm_matches, llm_matched = self.match_by_llm(
                products_by_retailer, remaining, sample_size=2000
            )
            all_matches.extend(llm_matches)
            all_matched.update(llm_matched)
        
        # Save matches
        self.save_matches(all_matches)
        
        # Calculate final stats
        total_matched = len(all_matched)
        coverage = (total_matched / total_products * 100) if total_products > 0 else 0
        
        logger.info("=" * 60)
        logger.info("MATCHING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total products: {total_products:,}")
        logger.info(f"Matched by barcode: {self.stats['matched_by_barcode']:,} ({self.stats['matched_by_barcode']/total_products*100:.1f}%)")
        logger.info(f"Matched by vector: {self.stats['matched_by_vector']:,} ({self.stats['matched_by_vector']/total_products*100:.1f}%)")
        logger.info(f"Matched by LLM: {self.stats['matched_by_llm']:,} ({self.stats['matched_by_llm']/total_products*100:.1f}%)")
        logger.info(f"Unmatched: {total_products - total_matched:,} ({100-coverage:.1f}%)")
        logger.info("")
        logger.info(f"Total matched: {total_matched:,} ({coverage:.1f}%)")
        logger.info("=" * 60)
        
        return coverage

if __name__ == "__main__":
    matcher = VectorProductMatcher()
    coverage = matcher.run_matching_pipeline()
    
    if coverage > 50:
        logger.info("🎯 SUCCESS: Achieved >50% match coverage!")
    else:
        logger.info(f"📊 Coverage: {coverage:.1f}% - Additional methods may be needed")