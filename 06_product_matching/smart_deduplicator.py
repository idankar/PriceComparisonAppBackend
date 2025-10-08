#!/usr/bin/env python3
"""
Smart Product Deduplicator
Cleans up product data quality issues and consolidates duplicates
"""

import psycopg2
import logging
from typing import Dict, List, Set
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SmartDeduplicator:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            port="5432", 
            database="price_comparison_app_v2",
            user="postgres",
            password="025655358"
        )
    
    def identify_problematic_products(self):
        """Find products with poor quality data"""
        cursor = self.conn.cursor()
        
        # Products with color names only
        logger.info("=== PROBLEMATIC PRODUCTS ANALYSIS ===")
        
        cursor.execute("""
            SELECT 'Color Names Only' as issue, COUNT(*) as count
            FROM products 
            WHERE canonical_name IN ('ורוד', 'לבן', 'שחור', 'תכלת', 'כחול', 'ירוק', 'אדום', 'צהוב')
            UNION ALL
            SELECT 'Very Short Names (<= 3 chars)', COUNT(*)
            FROM products 
            WHERE LENGTH(canonical_name) <= 3
            UNION ALL  
            SELECT 'Missing Brand', COUNT(*)
            FROM products
            WHERE brand IS NULL OR brand = ''
            UNION ALL
            SELECT 'Brand = Product Name', COUNT(*)
            FROM products
            WHERE canonical_name = brand
        """)
        
        results = cursor.fetchall()
        for issue, count in results:
            logger.info(f"{issue}: {count}")
        
        cursor.close()
    
    def consolidate_color_variants(self):
        """Consolidate products that are just color variants"""
        cursor = self.conn.cursor()
        
        logger.info("\n=== CONSOLIDATING COLOR VARIANTS ===")
        
        # Find products where canonical_name is just a color
        cursor.execute("""
            SELECT canonical_name, brand, COUNT(*) as count,
                   array_agg(product_id) as product_ids
            FROM products 
            WHERE canonical_name IN ('ורוד', 'לבן', 'שחור', 'תכלת', 'כחול', 'ירוק', 'אדום', 'צהוב')
            GROUP BY canonical_name, brand
            ORDER BY count DESC
        """)
        
        color_groups = cursor.fetchall()
        consolidated = 0
        
        for color, brand, count, product_ids in color_groups:
            if count > 1:
                # Keep first product, merge others into it
                primary_id = product_ids[0]
                duplicate_ids = product_ids[1:]
                
                logger.info(f"Consolidating {count} '{color}' products from brand '{brand}'")
                
                # Update retailer_products to point to primary product
                for dup_id in duplicate_ids:
                    cursor.execute("""
                        UPDATE retailer_products 
                        SET product_id = %s 
                        WHERE product_id = %s
                    """, (primary_id, dup_id))
                
                # Update product_group_links
                cursor.execute("""
                    UPDATE product_group_links 
                    SET product_id = %s 
                    WHERE product_id = ANY(%s)
                """, (primary_id, duplicate_ids))
                
                # Delete duplicate products
                cursor.execute("""
                    DELETE FROM products 
                    WHERE product_id = ANY(%s)
                """, (duplicate_ids,))
                
                consolidated += len(duplicate_ids)
        
        self.conn.commit()
        logger.info(f"Consolidated {consolidated} color-variant duplicates")
        cursor.close()
    
    def merge_brand_name_duplicates(self):
        """Merge products where brand became the product name"""
        cursor = self.conn.cursor()
        
        logger.info("\n=== MERGING BRAND NAME DUPLICATES ===") 
        
        # Find cases where canonical_name = brand
        cursor.execute("""
            SELECT brand, COUNT(*) as count, array_agg(product_id) as product_ids
            FROM products
            WHERE canonical_name = brand AND brand IS NOT NULL
            GROUP BY brand
            HAVING COUNT(*) > 1
            ORDER BY count DESC
            LIMIT 20
        """)
        
        brand_duplicates = cursor.fetchall()
        merged = 0
        
        for brand, count, product_ids in brand_duplicates:
            if count > 1:
                # Keep first product, merge others
                primary_id = product_ids[0]  
                duplicate_ids = product_ids[1:]
                
                logger.info(f"Merging {count} duplicate products for brand '{brand}'")
                
                # Update retailer_products
                for dup_id in duplicate_ids:
                    cursor.execute("""
                        UPDATE retailer_products 
                        SET product_id = %s 
                        WHERE product_id = %s
                        ON CONFLICT (retailer_id, product_id) DO NOTHING
                    """, (primary_id, dup_id))
                
                # Update product_group_links  
                cursor.execute("""
                    UPDATE product_group_links 
                    SET product_id = %s 
                    WHERE product_id = ANY(%s)
                    ON CONFLICT (group_id, product_id) DO NOTHING
                """, (primary_id, duplicate_ids))
                
                # Delete duplicates
                cursor.execute("""
                    DELETE FROM products 
                    WHERE product_id = ANY(%s)
                """, (duplicate_ids,))
                
                merged += len(duplicate_ids)
        
        self.conn.commit()
        logger.info(f"Merged {merged} brand name duplicates")
        cursor.close()
    
    def improve_product_names(self):
        """Improve product names by combining with brand info"""
        cursor = self.conn.cursor()
        
        logger.info("\n=== IMPROVING PRODUCT NAMES ===")
        
        # Update color-only names to include brand
        cursor.execute("""
            UPDATE products 
            SET canonical_name = brand || ' - ' || canonical_name
            WHERE canonical_name IN ('ורוד', 'לבן', 'שחור', 'תכלת', 'כחול', 'ירוק', 'אדום', 'צהוב')
              AND brand IS NOT NULL 
              AND brand != canonical_name
        """)
        
        improved = cursor.rowcount
        logger.info(f"Improved {improved} color-only product names")
        
        self.conn.commit()
        cursor.close()
    
    def analyze_results(self):
        """Show results after deduplication"""
        cursor = self.conn.cursor()
        
        logger.info("\n=== DEDUPLICATION RESULTS ===")
        
        cursor.execute("""
            SELECT 
                COUNT(*) as total_products,
                COUNT(DISTINCT (canonical_name, brand)) as unique_name_brand_combos,
                COUNT(*) - COUNT(DISTINCT (canonical_name, brand)) as remaining_duplicates
            FROM products
        """)
        
        total, unique, dups = cursor.fetchone()
        logger.info(f"Total products: {total}")
        logger.info(f"Unique name+brand combinations: {unique}")
        logger.info(f"Remaining duplicates: {dups}")
        
        # Check cross-retailer potential after cleanup
        cursor.execute("""
            WITH cross_retailer AS (
                SELECT p.canonical_name, p.brand, COUNT(DISTINCT rp.retailer_id) as retailers
                FROM products p
                JOIN retailer_products rp ON p.product_id = rp.product_id
                GROUP BY p.canonical_name, p.brand
                HAVING COUNT(DISTINCT rp.retailer_id) > 1
            )
            SELECT COUNT(*) as cross_retailer_groups
            FROM cross_retailer
        """)
        
        cross_retailer = cursor.fetchone()[0]
        logger.info(f"Products groups in multiple retailers: {cross_retailer}")
        
        cursor.close()
    
    def run_deduplication(self):
        """Run the full deduplication process"""
        logger.info("STARTING SMART DEDUPLICATION")
        
        self.identify_problematic_products()
        self.consolidate_color_variants()
        self.merge_brand_name_duplicates()  
        self.improve_product_names()
        self.analyze_results()
        
        logger.info("DEDUPLICATION COMPLETE")

if __name__ == "__main__":
    deduplicator = SmartDeduplicator()
    deduplicator.run_deduplication()