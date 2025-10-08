#!/usr/bin/env python3
"""
Hybrid Data Matching System
Combines commercial website data with government transparency pricing
Uses EAN codes + fuzzy matching + LLM verification for best data quality
"""

import psycopg2
import json
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from fuzzywuzzy import fuzz
import openai

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class CommercialProduct:
    """Rich product data from commercial websites"""
    product_id: str
    ean: str
    name: str
    brand: str
    price: float
    image_url: str
    category: str
    product_url: str
    retailer: str

@dataclass
class GovernmentProduct:
    """Basic product data from government transparency"""
    canonical_name: str
    brand: str
    price: float
    retailer: str
    attributes: dict

class HybridDataMatcher:
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
    
    def load_commercial_data(self, jsonl_file: str) -> List[CommercialProduct]:
        """Load rich commercial data from JSONL scraper output"""
        commercial_products = []
        
        try:
            with open(jsonl_file, 'r', encoding='utf-8') as f:
                for line in f:
                    data = json.loads(line.strip())
                    product = CommercialProduct(
                        product_id=data.get('productId', ''),
                        ean=data.get('ean', ''),
                        name=data.get('name', ''),
                        brand=data.get('brand', ''),
                        price=float(data.get('price', 0) or 0),
                        image_url=data.get('imageUrl', ''),
                        category=self.extract_category(data.get('scrapedFrom', '')),
                        product_url=data.get('productUrl', ''),
                        retailer='Super-Pharm'
                    )
                    commercial_products.append(product)
        except Exception as e:
            logger.error(f"Error loading commercial data: {e}")
        
        return commercial_products
    
    def extract_category(self, scraped_url: str) -> str:
        """Extract category from scraped URL"""
        category_mapping = {
            '15170000': 'Hair Care',
            '15160000': 'Oral Hygiene', 
            '15150000': 'Deodorants',
            '15140000': 'Shaving & Hair Removal',
            '15120000': 'Bath & Hygiene',
            '20110000': 'Perfumes',
            '25130000': 'Baby Care',
            '30140000': 'Medicines',
            '30300000': 'Supplements'
        }
        
        for code, category in category_mapping.items():
            if code in scraped_url:
                return category
        return 'Other'
    
    def load_government_data(self) -> List[GovernmentProduct]:
        """Load government transparency data"""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT DISTINCT
                p.canonical_name,
                p.brand,
                pr.price,
                r.retailername,
                p.attributes
            FROM products p
            JOIN retailer_products rp ON p.product_id = rp.product_id
            JOIN prices pr ON rp.retailer_product_id = pr.retailer_product_id
            JOIN retailers r ON rp.retailer_id = r.retailerid
            WHERE r.retailername = 'Super-Pharm'
        """)
        
        government_products = []
        for row in cursor.fetchall():
            product = GovernmentProduct(
                canonical_name=row[0],
                brand=row[1] or '',
                price=float(row[2]),
                retailer=row[3],
                attributes=row[4] or {}
            )
            government_products.append(product)
        
        cursor.close()
        return government_products
    
    def match_by_ean(self, commercial: List[CommercialProduct], 
                     government: List[GovernmentProduct]) -> Dict[str, Tuple]:
        """Match products using EAN codes (most reliable)"""
        matches = {}
        
        # Create EAN lookup for government products (if any have EANs)
        gov_by_attributes = {}
        for gov_prod in government:
            if gov_prod.attributes and 'ean' in gov_prod.attributes:
                ean = gov_prod.attributes['ean']
                if ean:
                    gov_by_attributes[ean] = gov_prod
        
        # Match commercial products by EAN
        for comm_prod in commercial:
            if comm_prod.ean and comm_prod.ean in gov_by_attributes:
                matches[comm_prod.product_id] = (comm_prod, gov_by_attributes[comm_prod.ean])
        
        logger.info(f"EAN matching found {len(matches)} matches")
        return matches
    
    def match_by_fuzzy(self, commercial: List[CommercialProduct],
                       government: List[GovernmentProduct],
                       existing_matches: set) -> Dict[str, Tuple]:
        """Match remaining products using fuzzy string matching"""
        matches = {}
        used_government = set()
        
        for comm_prod in commercial:
            if comm_prod.product_id in existing_matches:
                continue
            
            best_match = None
            best_score = 0
            
            for gov_prod in government:
                if gov_prod in used_government:
                    continue
                
                # Brand matching (must be similar)
                brand_score = fuzz.ratio(comm_prod.brand.lower(), gov_prod.brand.lower())
                if brand_score < 80:  # Brand must be quite similar
                    continue
                
                # Name matching
                name_score = fuzz.partial_ratio(comm_prod.name.lower(), gov_prod.canonical_name.lower())
                
                # Combined score
                combined_score = (brand_score * 0.4) + (name_score * 0.6)
                
                if combined_score > best_score and combined_score > 85:  # High threshold
                    best_score = combined_score
                    best_match = gov_prod
            
            if best_match:
                matches[comm_prod.product_id] = (comm_prod, best_match)
                used_government.add(best_match)
        
        logger.info(f"Fuzzy matching found {len(matches)} additional matches")
        return matches
    
    def verify_matches_with_llm(self, uncertain_matches: Dict[str, Tuple]) -> Dict[str, Tuple]:
        """Use LLM to verify uncertain matches"""
        verified_matches = {}
        
        for product_id, (commercial, government) in uncertain_matches.items():
            try:
                prompt = f"""
                Are these the same product?
                
                Commercial Site: "{commercial.name}" by "{commercial.brand}" - {commercial.price}₪
                Government Data: "{government.canonical_name}" by "{government.brand}" - {government.price}₪
                
                Consider:
                - Same active ingredients/purpose
                - Brand consistency  
                - Price reasonableness
                
                Respond: YES/NO
                """
                
                response = self.client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0,
                    max_tokens=10
                )
                
                if "YES" in response.choices[0].message.content.upper():
                    verified_matches[product_id] = (commercial, government)
                    
            except Exception as e:
                logger.warning(f"LLM verification error: {e}")
        
        logger.info(f"LLM verification confirmed {len(verified_matches)} matches")
        return verified_matches
    
    def create_master_product_registry(self, all_matches: Dict[str, Tuple]):
        """Create master product registry with best data from both sources"""
        cursor = self.conn.cursor()
        
        # Create enhanced products table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS master_products (
                master_product_id SERIAL PRIMARY KEY,
                commercial_id VARCHAR(255),
                ean VARCHAR(255),
                name TEXT NOT NULL,
                brand VARCHAR(255),
                category VARCHAR(255),
                image_url TEXT,
                product_url TEXT,
                government_price DECIMAL(10,2),
                commercial_price DECIMAL(10,2),
                price_difference DECIMAL(10,2),
                data_quality_score INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        for product_id, (commercial, government) in all_matches.items():
            # Calculate data quality score
            quality_score = 0
            quality_score += 30 if commercial.ean else 0           # EAN available
            quality_score += 25 if commercial.image_url else 0    # Image available  
            quality_score += 20 if commercial.product_url else 0  # Product page available
            quality_score += 15 if commercial.category != 'Other' else 0  # Category known
            quality_score += 10 if abs(commercial.price - government.price) < 5 else 0  # Prices similar
            
            cursor.execute("""
                INSERT INTO master_products 
                (commercial_id, ean, name, brand, category, image_url, product_url,
                 government_price, commercial_price, price_difference, data_quality_score)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                commercial.product_id,
                commercial.ean,
                commercial.name,  # Use commercial name (better quality)
                commercial.brand,  # Use commercial brand (better quality)
                commercial.category,
                commercial.image_url,
                commercial.product_url,
                government.price,  # Government price (official)
                commercial.price,  # Commercial price (may differ)
                abs(commercial.price - government.price),
                quality_score
            ))
        
        self.conn.commit()
        cursor.close()
        
        logger.info(f"Created master registry with {len(all_matches)} high-quality products")
    
    def run_hybrid_matching(self, commercial_jsonl: str):
        """Run the complete hybrid matching process"""
        logger.info("Starting hybrid data matching...")
        
        # Load data
        commercial_products = self.load_commercial_data(commercial_jsonl)
        government_products = self.load_government_data()
        
        logger.info(f"Loaded {len(commercial_products)} commercial products")
        logger.info(f"Loaded {len(government_products)} government products")
        
        # Match using multiple methods
        ean_matches = self.match_by_ean(commercial_products, government_products)
        
        fuzzy_matches = self.match_by_fuzzy(
            commercial_products, 
            government_products, 
            set(ean_matches.keys())
        )
        
        # Combine all matches
        all_matches = {**ean_matches, **fuzzy_matches}
        
        # Create master registry
        self.create_master_product_registry(all_matches)
        
        # Print statistics
        total_commercial = len(commercial_products)
        total_matched = len(all_matches)
        match_rate = (total_matched / total_commercial) * 100 if total_commercial > 0 else 0
        
        logger.info("="*60)
        logger.info("HYBRID MATCHING COMPLETE")
        logger.info(f"Commercial products: {total_commercial}")
        logger.info(f"Government products: {len(government_products)}")
        logger.info(f"Successfully matched: {total_matched} ({match_rate:.1f}%)")
        logger.info(f"EAN matches: {len(ean_matches)}")
        logger.info(f"Fuzzy matches: {len(fuzzy_matches)}")
        logger.info("="*60)

if __name__ == "__main__":
    matcher = HybridDataMatcher()
    # Will run once commercial scraper completes
    # matcher.run_hybrid_matching("superpharm_products_final.jsonl")