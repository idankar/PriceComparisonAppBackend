#!/usr/bin/env python3
"""
Clean Canonical Product System
Step-by-step approach:
1. Load commercial products as canonical (they have images and good descriptions)
2. Link all transparency products by barcode
3. Assess cross-retailer coverage
4. Use LLM only for unmatched products
"""

import psycopg2
import json
import logging
from dataclasses import dataclass
from typing import Dict, List, Set
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CleanCanonicalSystem:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="price_comparison_app_v2",
            user="postgres",
            password="025655358"
        )
        
    def step1_create_canonical_from_commercial(self):
        """Step 1: Use commercial products as canonical (they have images)"""
        logger.info("STEP 1: Creating canonical products from commercial data")
        
        cursor = self.conn.cursor()
        
        # Create canonical products table
        cursor.execute("""
            DROP TABLE IF EXISTS canonical_products_clean CASCADE;
            CREATE TABLE canonical_products_clean (
                canonical_id SERIAL PRIMARY KEY,
                barcode VARCHAR(255) UNIQUE,
                name TEXT NOT NULL,
                brand VARCHAR(255),
                category VARCHAR(255),
                image_url TEXT,
                product_url TEXT,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX idx_canonical_barcode_clean ON canonical_products_clean(barcode);
        """)
        
        # Load commercial products from Super-Pharm scraper
        canonical_count = 0
        try:
            with open('/Users/noa/Desktop/PriceComparisonApp/04_utilities/superpharm_products_final.jsonl', 'r') as f:
                for line in f:
                    data = json.loads(line.strip())
                    
                    # Only use products with valid barcodes
                    barcode = data.get('ean', '')
                    if barcode and len(barcode) >= 8:
                        cursor.execute("""
                            INSERT INTO canonical_products_clean 
                            (barcode, name, brand, category, image_url, product_url, description)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (barcode) DO NOTHING
                        """, (
                            barcode,
                            data.get('name', ''),
                            data.get('brand', ''),
                            self.extract_category(data.get('scrapedFrom', '')),
                            data.get('imageUrl', ''),
                            data.get('productUrl', ''),
                            data.get('name', '')  # Use name as description for now
                        ))
                        canonical_count += cursor.rowcount
                        
        except Exception as e:
            logger.error(f"Error loading commercial data: {e}")
        
        self.conn.commit()
        logger.info(f"Created {canonical_count} canonical products with barcodes and images")
        
        return canonical_count
    
    def extract_category(self, url: str) -> str:
        """Extract category from URL"""
        category_map = {
            '15170000': 'Hair Care',
            '15160000': 'Oral Hygiene',
            '15150000': 'Deodorants',
            '15140000': 'Shaving',
            '15120000': 'Bath & Hygiene',
            '25130000': 'Baby Care',
            '30140000': 'Medicines',
            '30300000': 'Supplements'
        }
        
        for code, cat in category_map.items():
            if code in url:
                return cat
        return 'Other'
    
    def step2_link_transparency_products(self):
        """Step 2: Link all transparency products to canonical by barcode"""
        logger.info("STEP 2: Linking transparency products to canonical products by barcode")
        
        cursor = self.conn.cursor()
        
        # Create linking table
        cursor.execute("""
            DROP TABLE IF EXISTS product_to_canonical CASCADE;
            CREATE TABLE product_to_canonical (
                product_id INTEGER REFERENCES products(product_id),
                canonical_id INTEGER REFERENCES canonical_products_clean(canonical_id),
                retailer_id INTEGER,
                match_method VARCHAR(50),
                PRIMARY KEY (product_id)
            );
            CREATE INDEX idx_prod_canonical ON product_to_canonical(canonical_id);
        """)
        
        # Link products that have matching barcodes
        cursor.execute("""
            INSERT INTO product_to_canonical (product_id, canonical_id, retailer_id, match_method)
            SELECT DISTINCT
                p.product_id,
                c.canonical_id,
                rp.retailer_id,
                'barcode'
            FROM products p
            JOIN retailer_products rp ON p.product_id = rp.product_id
            JOIN canonical_products_clean c ON (
                p.attributes->>'barcode' = c.barcode OR
                p.attributes->>'ean' = c.barcode
            )
            WHERE rp.retailer_id IN (52, 97, 150)  -- Pharmacy retailers only
            ON CONFLICT DO NOTHING
        """)
        
        linked_count = cursor.rowcount
        self.conn.commit()
        
        logger.info(f"Linked {linked_count} transparency products to canonical products")
        
        return linked_count
    
    def step3_assess_coverage(self):
        """Step 3: Assess how many products are available across all 3 retailers"""
        logger.info("STEP 3: Assessing cross-retailer coverage")
        
        cursor = self.conn.cursor()
        
        # Count products by retailer coverage
        cursor.execute("""
            WITH retailer_coverage AS (
                SELECT 
                    canonical_id,
                    COUNT(DISTINCT retailer_id) as retailer_count,
                    array_agg(DISTINCT retailer_id ORDER BY retailer_id) as retailer_ids
                FROM product_to_canonical
                GROUP BY canonical_id
            )
            SELECT 
                retailer_count,
                COUNT(*) as product_count
            FROM retailer_coverage
            GROUP BY retailer_count
            ORDER BY retailer_count DESC
        """)
        
        coverage_stats = cursor.fetchall()
        
        print("\n" + "="*60)
        print("CROSS-RETAILER COVERAGE ANALYSIS")
        print("="*60)
        
        for retailer_count, product_count in coverage_stats:
            print(f"Products in {retailer_count} retailer(s): {product_count}")
        
        # Get products available in all 3 retailers
        cursor.execute("""
            WITH full_coverage AS (
                SELECT 
                    c.canonical_id,
                    c.name,
                    c.brand,
                    c.barcode,
                    COUNT(DISTINCT pc.retailer_id) as retailer_count
                FROM canonical_products_clean c
                JOIN product_to_canonical pc ON c.canonical_id = pc.canonical_id
                GROUP BY c.canonical_id, c.name, c.brand, c.barcode
                HAVING COUNT(DISTINCT pc.retailer_id) = 3
            )
            SELECT * FROM full_coverage LIMIT 10
        """)
        
        full_coverage_products = cursor.fetchall()
        
        if full_coverage_products:
            print(f"\nSample products available in ALL 3 retailers:")
            for row in full_coverage_products:
                print(f"  • {row[1]} ({row[2]}) - Barcode: {row[3]}")
        
        # Count unmatched products
        cursor.execute("""
            SELECT COUNT(DISTINCT p.product_id)
            FROM products p
            JOIN retailer_products rp ON p.product_id = rp.product_id
            LEFT JOIN product_to_canonical pc ON p.product_id = pc.product_id
            WHERE rp.retailer_id IN (52, 97, 150)
            AND pc.product_id IS NULL
        """)
        
        unmatched_count = cursor.fetchone()[0]
        
        print(f"\nUnmatched transparency products (no barcode match): {unmatched_count}")
        print("These will need smart matching with LLM")
        
        return coverage_stats, unmatched_count
    
    def step4_get_unmatched_products(self):
        """Step 4: Get products that couldn't be matched by barcode"""
        logger.info("STEP 4: Identifying products needing smart matching")
        
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT 
                p.product_id,
                p.canonical_name,
                p.brand,
                r.retailername,
                p.attributes
            FROM products p
            JOIN retailer_products rp ON p.product_id = rp.product_id
            JOIN retailers r ON rp.retailer_id = r.retailerid
            LEFT JOIN product_to_canonical pc ON p.product_id = pc.product_id
            WHERE rp.retailer_id IN (52, 97, 150)
            AND pc.product_id IS NULL
            LIMIT 100
        """)
        
        unmatched = cursor.fetchall()
        
        print(f"\nSample unmatched products needing LLM matching:")
        for i, (pid, name, brand, retailer, attrs) in enumerate(unmatched[:5], 1):
            print(f"{i}. {name} ({brand}) from {retailer}")
            if attrs and 'barcode' in attrs:
                print(f"   Has barcode but no match: {attrs['barcode']}")
        
        return len(unmatched)
    
    def run_clean_pipeline(self):
        """Run the complete clean pipeline"""
        print("\n" + "="*80)
        print("STARTING CLEAN CANONICAL PRODUCT SYSTEM")
        print("="*80)
        
        # Step 1: Create canonical from commercial
        canonical_count = self.step1_create_canonical_from_commercial()
        
        # Step 2: Link transparency products
        linked_count = self.step2_link_transparency_products()
        
        # Step 3: Assess coverage
        coverage_stats, unmatched_count = self.step3_assess_coverage()
        
        # Step 4: Identify unmatched
        self.step4_get_unmatched_products()
        
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        print(f"Canonical products created: {canonical_count}")
        print(f"Transparency products linked: {linked_count}")
        print(f"Products needing smart matching: {unmatched_count}")
        
        match_rate = (linked_count / (linked_count + unmatched_count) * 100) if (linked_count + unmatched_count) > 0 else 0
        print(f"Barcode match rate: {match_rate:.1f}%")

if __name__ == "__main__":
    system = CleanCanonicalSystem()
    system.run_clean_pipeline()