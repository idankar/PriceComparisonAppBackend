#!/usr/bin/env python3
"""
Multi-Tier Product Matching System
Matches commercial website products to government transparency data
Uses tiered approach: Barcode → Brand+Name → LLM Verification
"""

import psycopg2
import json
import logging
import re
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple, Set
from collections import defaultdict
from fuzzywuzzy import fuzz
import openai
from tqdm import tqdm
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class CommercialProduct:
    product_id: str
    ean: str
    name: str
    brand: str
    price: float
    image_url: str
    product_url: str
    category: str = ""

@dataclass
class GovernmentProduct:
    product_id: int
    canonical_name: str
    brand: str
    price: float
    attributes: dict

@dataclass
class MatchResult:
    commercial_id: str
    government_id: int
    match_method: str  # 'barcode', 'brand_fuzzy', 'llm_verified'
    confidence: float
    commercial_data: dict
    government_data: dict

class MultiTierMatcher:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="price_comparison_app_v2",
            user="postgres",
            password="***REMOVED***"
        )
        
        # Initialize OpenAI client if needed for LLM matching
        self.client = openai.OpenAI(
            api_key="sk-lvCeuWfVJMG1V9dHWO9fxsubyhS2dkZEYIQzukfEuZT3BlbkFJidE3fzU5s3v2bwWYp_dCEhcRy0cxKbdQp41BBgnDAA"
        )
        
        self.matches = []
        self.unmatched_commercial = []
        self.unmatched_government = []
    
    def extract_category(self, url: str) -> str:
        """Extract category from scraped URL"""
        categories = {
            '15170000': 'Hair Care',
            '15160000': 'Oral Hygiene', 
            '15150000': 'Deodorants',
            '15140000': 'Shaving',
            '15120000': 'Bath & Hygiene',
            '15210000': 'Feminine Hygiene',
            '15100000': 'Sun Protection',
            '15130000': 'Kids Care',
            '15230000': 'Facial Care',
            '15220000': 'Body Care',
            '20110000': 'Perfumes',
            '20180000': 'Makeup',
            '25130000': 'Baby Care',
            '30140000': 'Medicines',
            '30300000': 'Supplements'
        }
        
        for code, cat in categories.items():
            if code in url:
                return cat
        return 'Other'
    
    def load_commercial_products(self, jsonl_path: str) -> List[CommercialProduct]:
        """Load commercial products from JSONL file"""
        products = []
        
        with open(jsonl_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line.strip())
                    product = CommercialProduct(
                        product_id=data.get('productId', ''),
                        ean=data.get('ean', ''),
                        name=data.get('name', ''),
                        brand=data.get('brand', ''),
                        price=float(data.get('price', 0) or 0),
                        image_url=data.get('imageUrl', ''),
                        product_url=data.get('productUrl', ''),
                        category=self.extract_category(data.get('scrapedFrom', ''))
                    )
                    products.append(product)
                except Exception as e:
                    logger.warning(f"Error parsing product: {e}")
        
        logger.info(f"Loaded {len(products)} commercial products")
        return products
    
    def load_government_products(self) -> List[GovernmentProduct]:
        """Load government transparency products from database"""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT DISTINCT
                p.product_id,
                p.canonical_name,
                p.brand,
                pr.price,
                p.attributes
            FROM products p
            JOIN retailer_products rp ON p.product_id = rp.product_id
            JOIN prices pr ON rp.retailer_product_id = pr.retailer_product_id
            JOIN retailers r ON rp.retailer_id = r.retailerid
            WHERE r.retailername = 'Super-Pharm'
            AND p.canonical_name IS NOT NULL
            ORDER BY p.canonical_name
        """)
        
        products = []
        seen = set()
        for row in cursor.fetchall():
            # Deduplicate by name+brand
            key = (row[1], row[2])
            if key not in seen:
                product = GovernmentProduct(
                    product_id=row[0],
                    canonical_name=row[1],
                    brand=row[2] or '',
                    price=float(row[3]),
                    attributes=row[4] or {}
                )
                products.append(product)
                seen.add(key)
        
        cursor.close()
        logger.info(f"Loaded {len(products)} unique government products")
        return products
    
    def normalize_text(self, text: str) -> str:
        """Normalize Hebrew text for better matching"""
        if not text:
            return ""
        
        # Remove extra spaces
        text = ' '.join(text.split())
        
        # Remove common units that might differ
        text = re.sub(r'\d+\s*(מ"ל|מל|ml|יח\'|יחידות|גרם|g)', '', text)
        
        # Remove quotes and special chars
        text = re.sub(r'["\'\-\(\)]', ' ', text)
        
        return text.strip()
    
    def tier1_barcode_matching(self, commercial: List[CommercialProduct], 
                              government: List[GovernmentProduct]) -> List[MatchResult]:
        """Tier 1: Match by EAN/Barcode (highest confidence)"""
        matches = []
        matched_gov_ids = set()
        
        # Build EAN index from government products if they have EAN in attributes
        gov_by_ean = {}
        for gov in government:
            if gov.attributes and 'ean' in gov.attributes:
                ean = gov.attributes['ean']
                if ean:
                    gov_by_ean[ean] = gov
        
        # Match commercial products by EAN
        for comm in commercial:
            if comm.ean and comm.ean in gov_by_ean:
                gov = gov_by_ean[comm.ean]
                if gov.product_id not in matched_gov_ids:
                    match = MatchResult(
                        commercial_id=comm.product_id,
                        government_id=gov.product_id,
                        match_method='barcode',
                        confidence=1.0,
                        commercial_data=asdict(comm),
                        government_data=asdict(gov)
                    )
                    matches.append(match)
                    matched_gov_ids.add(gov.product_id)
        
        logger.info(f"Tier 1 (Barcode): Found {len(matches)} matches")
        return matches
    
    def tier2_brand_fuzzy_matching(self, commercial: List[CommercialProduct],
                                   government: List[GovernmentProduct],
                                   already_matched: Set[int]) -> List[MatchResult]:
        """Tier 2: Match by brand + fuzzy name matching"""
        matches = []
        matched_gov_ids = set()
        
        # Group by brand for faster matching
        gov_by_brand = defaultdict(list)
        for gov in government:
            if gov.product_id not in already_matched:
                brand = self.normalize_text(gov.brand)
                if brand:
                    gov_by_brand[brand].append(gov)
        
        # Match commercial products
        for comm in tqdm(commercial, desc="Tier 2 matching"):
            comm_brand = self.normalize_text(comm.brand)
            if not comm_brand:
                continue
            
            # Find government products with similar brand
            best_match = None
            best_score = 0
            
            for brand_key in gov_by_brand.keys():
                brand_similarity = fuzz.ratio(comm_brand, brand_key)
                
                if brand_similarity > 85:  # Brand must be very similar
                    for gov in gov_by_brand[brand_key]:
                        if gov.product_id in matched_gov_ids:
                            continue
                        
                        # Compare product names
                        comm_name = self.normalize_text(comm.name)
                        gov_name = self.normalize_text(gov.canonical_name)
                        
                        # Try multiple fuzzy matching strategies
                        name_score = max(
                            fuzz.ratio(comm_name, gov_name),
                            fuzz.partial_ratio(comm_name, gov_name),
                            fuzz.token_sort_ratio(comm_name, gov_name)
                        )
                        
                        # Combined score
                        combined_score = (brand_similarity * 0.3) + (name_score * 0.7)
                        
                        if combined_score > best_score and combined_score > 80:
                            best_score = combined_score
                            best_match = gov
            
            if best_match:
                match = MatchResult(
                    commercial_id=comm.product_id,
                    government_id=best_match.product_id,
                    match_method='brand_fuzzy',
                    confidence=best_score / 100.0,
                    commercial_data=asdict(comm),
                    government_data=asdict(best_match)
                )
                matches.append(match)
                matched_gov_ids.add(best_match.product_id)
        
        logger.info(f"Tier 2 (Brand+Fuzzy): Found {len(matches)} matches")
        return matches
    
    def tier3_llm_verification(self, commercial: List[CommercialProduct],
                               government: List[GovernmentProduct],
                               already_matched: Set[int],
                               sample_size: int = 50) -> List[MatchResult]:
        """Tier 3: Use LLM for uncertain matches (limited sample for cost control)"""
        matches = []
        
        # Get unmatched products
        unmatched_comm = [c for c in commercial[:sample_size]]
        unmatched_gov = [g for g in government if g.product_id not in already_matched][:sample_size]
        
        if not unmatched_comm or not unmatched_gov:
            return matches
        
        # Prepare batch for LLM
        batch_prompt = self.prepare_llm_batch(unmatched_comm[:10], unmatched_gov[:30])
        
        try:
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a product matching expert. Match identical products."},
                    {"role": "user", "content": batch_prompt}
                ],
                temperature=0,
                max_tokens=1000
            )
            
            # Parse LLM response
            response_text = response.choices[0].message.content
            if "MATCHES:" in response_text:
                matches_section = response_text.split("MATCHES:")[1]
                
                # Simple parsing of matches
                for line in matches_section.split('\n'):
                    if '->' in line:
                        try:
                            parts = line.split('->')
                            comm_id = parts[0].strip().split(':')[0]
                            gov_id = parts[1].strip().split(':')[0]
                            
                            # Find the actual products
                            comm_prod = next((c for c in unmatched_comm if c.product_id == comm_id), None)
                            gov_prod = next((g for g in unmatched_gov if str(g.product_id) == gov_id), None)
                            
                            if comm_prod and gov_prod:
                                match = MatchResult(
                                    commercial_id=comm_prod.product_id,
                                    government_id=gov_prod.product_id,
                                    match_method='llm_verified',
                                    confidence=0.85,
                                    commercial_data=asdict(comm_prod),
                                    government_data=asdict(gov_prod)
                                )
                                matches.append(match)
                        except:
                            continue
            
        except Exception as e:
            logger.error(f"LLM verification error: {e}")
        
        logger.info(f"Tier 3 (LLM): Found {len(matches)} matches")
        return matches
    
    def prepare_llm_batch(self, commercial: List[CommercialProduct], 
                         government: List[GovernmentProduct]) -> str:
        """Prepare batch prompt for LLM"""
        prompt = """Match identical pharmacy products between commercial and government data.

COMMERCIAL PRODUCTS:
"""
        for c in commercial:
            prompt += f"{c.product_id}: {c.name} | Brand: {c.brand} | ₪{c.price}\n"
        
        prompt += "\nGOVERNMENT PRODUCTS:\n"
        for g in government:
            prompt += f"{g.product_id}: {g.canonical_name} | Brand: {g.brand} | ₪{g.price}\n"
        
        prompt += """
Match identical products only. Consider same active ingredients, purpose, and brand.
Format: MATCHES:
comm_id -> gov_id
"""
        return prompt
    
    def save_matches_to_database(self, matches: List[MatchResult]):
        """Save all matches to database"""
        cursor = self.conn.cursor()
        
        # Create matching results table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS commercial_government_matches (
                match_id SERIAL PRIMARY KEY,
                commercial_product_id VARCHAR(255),
                government_product_id INTEGER,
                match_method VARCHAR(50),
                confidence FLOAT,
                commercial_name TEXT,
                commercial_brand VARCHAR(255),
                commercial_price DECIMAL(10,2),
                commercial_image_url TEXT,
                government_name TEXT,
                government_brand VARCHAR(255),
                government_price DECIMAL(10,2),
                price_difference DECIMAL(10,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Clear existing matches
        cursor.execute("TRUNCATE commercial_government_matches")
        
        # Insert new matches
        for match in matches:
            price_diff = abs(match.commercial_data['price'] - match.government_data['price'])
            
            cursor.execute("""
                INSERT INTO commercial_government_matches
                (commercial_product_id, government_product_id, match_method, confidence,
                 commercial_name, commercial_brand, commercial_price, commercial_image_url,
                 government_name, government_brand, government_price, price_difference)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                match.commercial_id,
                match.government_id,
                match.match_method,
                match.confidence,
                match.commercial_data['name'],
                match.commercial_data['brand'],
                match.commercial_data['price'],
                match.commercial_data['image_url'],
                match.government_data['canonical_name'],
                match.government_data['brand'],
                match.government_data['price'],
                price_diff
            ))
        
        self.conn.commit()
        cursor.close()
        
        logger.info(f"Saved {len(matches)} matches to database")
    
    def run_matching(self, commercial_jsonl: str):
        """Run the complete multi-tier matching process"""
        logger.info("="*60)
        logger.info("STARTING MULTI-TIER MATCHING SYSTEM")
        logger.info("="*60)
        
        # Load data
        commercial_products = self.load_commercial_products(commercial_jsonl)
        government_products = self.load_government_products()
        
        all_matches = []
        matched_government_ids = set()
        
        # Tier 1: Barcode matching
        tier1_matches = self.tier1_barcode_matching(commercial_products, government_products)
        all_matches.extend(tier1_matches)
        matched_government_ids.update(m.government_id for m in tier1_matches)
        
        # Tier 2: Brand + Fuzzy matching
        tier2_matches = self.tier2_brand_fuzzy_matching(
            commercial_products, government_products, matched_government_ids
        )
        all_matches.extend(tier2_matches)
        matched_government_ids.update(m.government_id for m in tier2_matches)
        
        # Tier 3: LLM verification (limited sample)
        tier3_matches = self.tier3_llm_verification(
            commercial_products, government_products, matched_government_ids
        )
        all_matches.extend(tier3_matches)
        
        # Save all matches
        self.save_matches_to_database(all_matches)
        
        # Statistics
        self.print_statistics(all_matches, commercial_products, government_products)
        
        return all_matches
    
    def print_statistics(self, matches: List[MatchResult], 
                        commercial: List[CommercialProduct],
                        government: List[GovernmentProduct]):
        """Print matching statistics"""
        
        # Group by method
        by_method = defaultdict(list)
        for match in matches:
            by_method[match.match_method].append(match)
        
        # Calculate statistics
        total_commercial = len(commercial)
        total_government = len(government)
        total_matches = len(matches)
        match_rate = (total_matches / min(total_commercial, total_government)) * 100
        
        logger.info("\n" + "="*60)
        logger.info("MATCHING RESULTS:")
        logger.info(f"Commercial products: {total_commercial}")
        logger.info(f"Government products: {total_government}")
        logger.info(f"Total matches: {total_matches} ({match_rate:.1f}% coverage)")
        logger.info("\nMatches by method:")
        logger.info(f"  Barcode matching: {len(by_method['barcode'])}")
        logger.info(f"  Brand+Fuzzy matching: {len(by_method['brand_fuzzy'])}")
        logger.info(f"  LLM verified: {len(by_method['llm_verified'])}")
        
        # Average confidence by method
        for method, method_matches in by_method.items():
            avg_conf = np.mean([m.confidence for m in method_matches])
            logger.info(f"  {method} avg confidence: {avg_conf:.2f}")
        
        logger.info("="*60)
        
        # Show sample matches
        logger.info("\nSample high-confidence matches:")
        high_conf = sorted(matches, key=lambda m: m.confidence, reverse=True)[:5]
        for match in high_conf:
            logger.info(f"  [{match.confidence:.2f}] {match.commercial_data['name']} -> {match.government_data['canonical_name']}")

if __name__ == "__main__":
    matcher = MultiTierMatcher()
    matcher.run_matching("/Users/noa/Desktop/PriceComparisonApp/04_utilities/superpharm_products_final.jsonl")