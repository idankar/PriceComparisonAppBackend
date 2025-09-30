#!/usr/bin/env python3
"""
Barcode-based Canonical Product System
Since commercial data lacks barcodes, we'll:
1. Create canonical products from unique barcodes in transparency data
2. Group all products by barcode
3. Assess retailer coverage
"""

import psycopg2
import json
import logging
from collections import defaultdict
from typing import Dict, List, Set

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BarcodeBasedCanonical:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="price_comparison_app_v2",
            user="postgres",
            password="***REMOVED***"
        )
    
    def analyze_current_state(self):
        """Analyze current database state"""
        cursor = self.conn.cursor()
        
        print("\n" + "="*80)
        print("CURRENT DATABASE STATE ANALYSIS")
        print("="*80)
        
        # Count total products by retailer
        cursor.execute("""
            SELECT 
                r.retailername,
                r.retailerid,
                COUNT(DISTINCT p.product_id) as product_count,
                COUNT(DISTINCT CASE 
                    WHEN p.attributes->>'barcode' IS NOT NULL 
                    OR p.attributes->>'ean' IS NOT NULL 
                    THEN p.product_id 
                END) as with_barcode
            FROM products p
            JOIN retailer_products rp ON p.product_id = rp.product_id
            JOIN retailers r ON rp.retailer_id = r.retailerid
            WHERE r.retailerid IN (52, 97, 150)
            GROUP BY r.retailername, r.retailerid
            ORDER BY r.retailerid
        """)
        
        retailer_stats = cursor.fetchall()
        print("\nProducts by retailer:")
        for name, rid, total, with_bc in retailer_stats:
            pct = (with_bc/total*100) if total > 0 else 0
            print(f"  {name} (ID: {rid}): {total:,} products, {with_bc:,} with barcodes ({pct:.1f}%)")
        
        return retailer_stats
    
    def create_barcode_canonical(self):
        """Create canonical products based on unique barcodes"""
        cursor = self.conn.cursor()
        
        print("\n" + "="*80)
        print("CREATING BARCODE-BASED CANONICAL PRODUCTS")
        print("="*80)
        
        # Drop and recreate tables
        cursor.execute("""
            DROP TABLE IF EXISTS barcode_canonical CASCADE;
            CREATE TABLE barcode_canonical (
                barcode VARCHAR(255) PRIMARY KEY,
                canonical_name TEXT,
                canonical_brand VARCHAR(255),
                product_count INTEGER DEFAULT 0,
                retailer_count INTEGER DEFAULT 0,
                retailer_ids INTEGER[],
                min_price DECIMAL(10,2),
                max_price DECIMAL(10,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            DROP TABLE IF EXISTS barcode_product_mapping CASCADE;
            CREATE TABLE barcode_product_mapping (
                product_id INTEGER REFERENCES products(product_id),
                barcode VARCHAR(255) REFERENCES barcode_canonical(barcode),
                retailer_id INTEGER,
                PRIMARY KEY (product_id)
            );
        """)
        
        # Get all unique barcodes with their products
        cursor.execute("""
            WITH barcode_products AS (
                SELECT DISTINCT
                    COALESCE(p.attributes->>'barcode', p.attributes->>'ean') as barcode,
                    p.product_id,
                    p.canonical_name,
                    p.brand,
                    rp.retailer_id,
                    MIN(pr.price) as min_price
                FROM products p
                JOIN retailer_products rp ON p.product_id = rp.product_id
                LEFT JOIN prices pr ON rp.retailer_product_id = pr.retailer_product_id
                WHERE (p.attributes->>'barcode' IS NOT NULL OR p.attributes->>'ean' IS NOT NULL)
                AND rp.retailer_id IN (52, 97, 150)
                GROUP BY 
                    COALESCE(p.attributes->>'barcode', p.attributes->>'ean'),
                    p.product_id, p.canonical_name, p.brand, rp.retailer_id
            )
            SELECT 
                barcode,
                COUNT(DISTINCT product_id) as product_count,
                COUNT(DISTINCT retailer_id) as retailer_count,
                array_agg(DISTINCT retailer_id ORDER BY retailer_id) as retailer_ids,
                MIN(min_price) as min_price,
                MAX(min_price) as max_price,
                array_agg(DISTINCT canonical_name) as names,
                array_agg(DISTINCT brand) as brands
            FROM barcode_products
            WHERE barcode IS NOT NULL AND LENGTH(barcode) >= 8
            GROUP BY barcode
        """)
        
        barcode_groups = cursor.fetchall()
        
        # Insert canonical products
        for row in barcode_groups:
            barcode = row[0]
            product_count = row[1]
            retailer_count = row[2]
            retailer_ids = row[3]
            min_price = row[4]
            max_price = row[5]
            names = [n for n in row[6] if n]
            brands = [b for b in row[7] if b]
            
            # Use most common name and brand
            canonical_name = names[0] if names else 'Unknown'
            canonical_brand = brands[0] if brands else 'Unknown'
            
            cursor.execute("""
                INSERT INTO barcode_canonical 
                (barcode, canonical_name, canonical_brand, product_count, 
                 retailer_count, retailer_ids, min_price, max_price)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (barcode, canonical_name, canonical_brand, product_count,
                  retailer_count, retailer_ids, min_price, max_price))
        
        # Create mappings
        cursor.execute("""
            INSERT INTO barcode_product_mapping (product_id, barcode, retailer_id)
            SELECT 
                p.product_id,
                COALESCE(p.attributes->>'barcode', p.attributes->>'ean'),
                rp.retailer_id
            FROM products p
            JOIN retailer_products rp ON p.product_id = rp.product_id
            WHERE (p.attributes->>'barcode' IS NOT NULL OR p.attributes->>'ean' IS NOT NULL)
            AND rp.retailer_id IN (52, 97, 150)
            AND LENGTH(COALESCE(p.attributes->>'barcode', p.attributes->>'ean')) >= 8
        """)
        
        self.conn.commit()
        
        print(f"Created {len(barcode_groups)} canonical products from unique barcodes")
        
        return len(barcode_groups)
    
    def analyze_coverage(self):
        """Analyze retailer coverage for barcode products"""
        cursor = self.conn.cursor()
        
        print("\n" + "="*80)
        print("RETAILER COVERAGE ANALYSIS")
        print("="*80)
        
        # Coverage statistics
        cursor.execute("""
            SELECT 
                retailer_count,
                COUNT(*) as product_count,
                AVG(product_count) as avg_listings_per_product
            FROM barcode_canonical
            GROUP BY retailer_count
            ORDER BY retailer_count DESC
        """)
        
        coverage = cursor.fetchall()
        
        total_canonical = sum(row[1] for row in coverage)
        
        print(f"\nTotal canonical products (unique barcodes): {total_canonical}")
        print("\nCoverage breakdown:")
        for retailer_count, product_count, avg_listings in coverage:
            pct = (product_count/total_canonical*100) if total_canonical > 0 else 0
            print(f"  {retailer_count} retailer(s): {product_count:,} products ({pct:.1f}%)")
            print(f"    Average listings per product: {avg_listings:.1f}")
        
        # Products in all 3 retailers
        cursor.execute("""
            SELECT barcode, canonical_name, canonical_brand, min_price, max_price
            FROM barcode_canonical
            WHERE retailer_count = 3
            ORDER BY canonical_name
            LIMIT 20
        """)
        
        full_coverage = cursor.fetchall()
        
        if full_coverage:
            print(f"\nProducts available in ALL 3 retailers ({len(full_coverage)} shown):")
            for barcode, name, brand, min_p, max_p in full_coverage:
                price_diff = ((max_p - min_p) / min_p * 100) if min_p > 0 else 0
                print(f"  • {name} ({brand})")
                print(f"    Barcode: {barcode}")
                print(f"    Price range: ₪{min_p:.2f} - ₪{max_p:.2f} ({price_diff:.1f}% difference)")
        
        return coverage
    
    def identify_unmatched(self):
        """Identify products without barcodes"""
        cursor = self.conn.cursor()
        
        print("\n" + "="*80)
        print("UNMATCHED PRODUCTS (NO BARCODES)")
        print("="*80)
        
        cursor.execute("""
            SELECT 
                r.retailername,
                COUNT(DISTINCT p.product_id) as unmatched_count
            FROM products p
            JOIN retailer_products rp ON p.product_id = rp.product_id
            JOIN retailers r ON rp.retailer_id = r.retailerid
            WHERE rp.retailer_id IN (52, 97, 150)
            AND (p.attributes->>'barcode' IS NULL AND p.attributes->>'ean' IS NULL)
            GROUP BY r.retailername
        """)
        
        unmatched_by_retailer = cursor.fetchall()
        
        total_unmatched = sum(row[1] for row in unmatched_by_retailer)
        
        print(f"Total products without barcodes: {total_unmatched:,}")
        print("\nBy retailer:")
        for retailer, count in unmatched_by_retailer:
            print(f"  {retailer}: {count:,}")
        
        print("\nThese products will require smart matching using LLM")
        
        return total_unmatched
    
    def run_analysis(self):
        """Run complete analysis"""
        print("\n" + "="*80)
        print("BARCODE-BASED CANONICAL PRODUCT ANALYSIS")
        print("="*80)
        
        # Analyze current state
        self.analyze_current_state()
        
        # Create canonical products
        canonical_count = self.create_barcode_canonical()
        
        # Analyze coverage
        coverage = self.analyze_coverage()
        
        # Identify unmatched
        unmatched = self.identify_unmatched()
        
        print("\n" + "="*80)
        print("FINAL SUMMARY")
        print("="*80)
        print(f"Canonical products created (unique barcodes): {canonical_count}")
        print(f"Products needing smart matching (no barcode): {unmatched:,}")
        
        # Calculate comparison potential
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM barcode_canonical WHERE retailer_count >= 2")
        comparable = cursor.fetchone()[0]
        
        print(f"Products comparable across retailers: {comparable} ({comparable/canonical_count*100:.1f}%)")

if __name__ == "__main__":
    system = BarcodeBasedCanonical()
    system.run_analysis()