#!/usr/bin/env python3
"""
Data Forensics Analysis Script for PharmMate Price Comparison App

This script performs a deep forensic analysis of the database to identify
why products with price data are not being linked to canonical products with images.

The analysis focuses on three key areas:
1. Products with prices but missing images
2. Source of canonical product data (scrapers vs ETL fallbacks)
3. Barcodes present in ETLs but missing from commercial scrapers
"""

import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime
import json
from typing import List, Dict, Any
from collections import Counter
import sys


def get_database_connection():
    """Establish connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="price_comparison_app_v2",
            user="postgres",
            password="***REMOVED***",
            cursor_factory=DictCursor
        )
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        sys.exit(1)


def find_products_with_prices_but_no_images(cursor, limit=50):
    """
    Identifies products that have price data but are missing a canonical image.

    This helps identify the scale of the problem and specific examples.
    """
    query = """
        SELECT DISTINCT
            rp.barcode,
            rp.original_retailer_name,
            r.retailername,
            cp.name AS canonical_name,
            cp.image_url,
            COUNT(DISTINCT p.price_id) AS price_entries
        FROM retailer_products rp
        JOIN retailers r ON rp.retailer_id = r.retailerid
        LEFT JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
        LEFT JOIN canonical_products cp ON rp.barcode = cp.barcode
        WHERE
            rp.barcode IS NOT NULL
            AND (cp.image_url IS NULL OR cp.image_url = '')
            AND p.price_id IS NOT NULL  -- Has price data
        GROUP BY
            rp.barcode,
            rp.original_retailer_name,
            r.retailername,
            cp.name,
            cp.image_url
        ORDER BY price_entries DESC
        LIMIT %s;
    """
    cursor.execute(query, (limit,))
    results = cursor.fetchall()

    print("\n" + "="*80)
    print("1. PRODUCTS WITH PRICES BUT MISSING IMAGES")
    print("="*80)

    if not results:
        print("✅ No products found with prices but missing images.")
        return []

    print(f"Found {len(results)} products (showing top {limit}):\n")
    for i, row in enumerate(results, 1):
        print(f"  {i}. Barcode: {row['barcode']}")
        print(f"     Product: {row['original_retailer_name']}")
        print(f"     Retailer: {row['retailername']}")
        print(f"     Price Entries: {row['price_entries']}")
        print(f"     Canonical Name: {row['canonical_name'] or 'NOT IN CANONICAL TABLE'}")
        print()

    return results


def analyze_canonical_data_sources(cursor):
    """
    Analyzes the source of the data in the canonical_products table.

    This tells us if canonical products are coming from high-quality scrapers
    or low-quality ETL fallbacks.
    """

    # First, check overall stats
    query_overall = """
        SELECT
            COUNT(*) AS total_products,
            COUNT(CASE WHEN image_url IS NOT NULL AND image_url != '' THEN 1 END) AS with_images,
            COUNT(CASE WHEN source_retailer_id IS NOT NULL THEN 1 END) AS from_scrapers,
            COUNT(CASE WHEN source_retailer_id IS NULL THEN 1 END) AS from_etl_fallback
        FROM canonical_products;
    """
    cursor.execute(query_overall)
    overall_stats = cursor.fetchone()

    # Then break down by source
    query_by_source = """
        SELECT
            COALESCE(r.retailername, 'ETL Fallback (No Scraper)') AS data_source,
            COUNT(cp.barcode) AS product_count,
            COUNT(CASE WHEN cp.image_url IS NOT NULL AND cp.image_url != '' THEN 1 END) AS products_with_images,
            COUNT(DISTINCT cp.barcode) AS unique_barcodes
        FROM canonical_products cp
        LEFT JOIN retailers r ON cp.source_retailer_id = r.retailerid
        GROUP BY data_source
        ORDER BY product_count DESC;
    """
    cursor.execute(query_by_source)
    source_breakdown = cursor.fetchall()

    print("\n" + "="*80)
    print("2. CANONICAL DATA SOURCE ANALYSIS")
    print("="*80)

    print("\nOVERALL STATISTICS:")
    print(f"  Total Products: {overall_stats['total_products']:,}")
    print(f"  With Images: {overall_stats['with_images']:,} ({overall_stats['with_images'] * 100 / overall_stats['total_products']:.1f}%)")
    print(f"  From Scrapers: {overall_stats['from_scrapers']:,} ({overall_stats['from_scrapers'] * 100 / overall_stats['total_products']:.1f}%)")
    print(f"  From ETL Fallback: {overall_stats['from_etl_fallback']:,} ({overall_stats['from_etl_fallback'] * 100 / overall_stats['total_products']:.1f}%)")

    print("\nBREAKDOWN BY SOURCE:")
    for row in source_breakdown:
        pct_with_images = (row['products_with_images'] * 100 / row['product_count']) if row['product_count'] > 0 else 0
        print(f"\n  {row['data_source']}:")
        print(f"    - Total Products: {row['product_count']:,}")
        print(f"    - Unique Barcodes: {row['unique_barcodes']:,}")
        print(f"    - With Images: {row['products_with_images']:,} ({pct_with_images:.1f}%)")

    return source_breakdown


def find_unscraped_barcodes(cursor, limit=50):
    """
    Finds barcodes that exist in the government data but not in our scraped commercial data.

    This is critical for understanding if our scrapers are missing products.
    """

    # First get count of total unscraped barcodes
    count_query = """
        SELECT COUNT(DISTINCT rp.barcode) as total_unscraped
        FROM retailer_products rp
        JOIN retailers r ON rp.retailer_id = r.retailerid
        WHERE
            rp.barcode IS NOT NULL
            AND rp.barcode != ''
            AND NOT EXISTS (
                SELECT 1
                FROM canonical_products cp
                WHERE cp.barcode = rp.barcode
                AND cp.source_retailer_id IS NOT NULL
            );
    """
    cursor.execute(count_query)
    total_count = cursor.fetchone()['total_unscraped']

    # Then get sample
    query = """
        SELECT DISTINCT
            rp.barcode,
            rp.original_retailer_name,
            r.retailername,
            COUNT(DISTINCT p.price_id) AS price_entries
        FROM retailer_products rp
        JOIN retailers r ON rp.retailer_id = r.retailerid
        LEFT JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
        WHERE
            rp.barcode IS NOT NULL
            AND rp.barcode != ''
            AND NOT EXISTS (
                SELECT 1
                FROM canonical_products cp
                WHERE cp.barcode = rp.barcode
                AND cp.source_retailer_id IS NOT NULL
            )
        GROUP BY rp.barcode, rp.original_retailer_name, r.retailername
        ORDER BY price_entries DESC NULLS LAST
        LIMIT %s;
    """
    cursor.execute(query, (limit,))
    results = cursor.fetchall()

    print("\n" + "="*80)
    print("3. BARCODES IN PRICE DATA BUT MISSING FROM COMMERCIAL SCRAPERS")
    print("="*80)

    print(f"\nTotal unscraped barcodes: {total_count:,}")

    if not results:
        print("✅ No barcodes found that are missing from commercial scrapers.")
        return []

    print(f"Showing top {limit} by price entry count:\n")
    for i, row in enumerate(results, 1):
        print(f"  {i}. Barcode: {row['barcode']}")
        print(f"     Product: {row['original_retailer_name']}")
        print(f"     Source: {row['retailername']}")
        print(f"     Price Entries: {row['price_entries'] or 0}")
        print()

    return results


def analyze_barcode_formats(cursor):
    """
    Additional analysis: Check for barcode format discrepancies
    """
    query = """
        WITH barcode_analysis AS (
            SELECT
                'retailer_products' as table_name,
                barcode,
                LENGTH(barcode) as barcode_length,
                CASE
                    WHEN barcode ~ '^[0-9]+$' THEN 'numeric'
                    WHEN barcode ~ '^[0-9]+-[0-9]+$' THEN 'hyphenated'
                    ELSE 'other'
                END as barcode_format
            FROM retailer_products
            WHERE barcode IS NOT NULL AND barcode != ''

            UNION ALL

            SELECT
                'canonical_products' as table_name,
                barcode,
                LENGTH(barcode) as barcode_length,
                CASE
                    WHEN barcode ~ '^[0-9]+$' THEN 'numeric'
                    WHEN barcode ~ '^[0-9]+-[0-9]+$' THEN 'hyphenated'
                    ELSE 'other'
                END as barcode_format
            FROM canonical_products
            WHERE barcode IS NOT NULL AND barcode != ''
        )
        SELECT
            table_name,
            barcode_format,
            barcode_length,
            COUNT(*) as count
        FROM barcode_analysis
        GROUP BY table_name, barcode_format, barcode_length
        ORDER BY table_name, count DESC;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    print("\n" + "="*80)
    print("4. BARCODE FORMAT ANALYSIS")
    print("="*80)

    current_table = None
    for row in results:
        if current_table != row['table_name']:
            current_table = row['table_name']
            print(f"\n{current_table}:")
        print(f"  Format: {row['barcode_format']}, Length: {row['barcode_length']}, Count: {row['count']:,}")

    return results


def check_retailer_coverage(cursor):
    """
    Check which retailers have the most unmatched products
    """
    query = """
        WITH retailer_stats AS (
            SELECT
                r.retailername,
                CASE
                    WHEN r.retailername IN ('Super-Pharm', 'Be Pharm', 'Good Pharm') THEN true
                    ELSE false
                END as iscommercial,
                COUNT(DISTINCT rp.barcode) as total_products,
                COUNT(DISTINCT CASE
                    WHEN cp.barcode IS NOT NULL AND cp.image_url IS NOT NULL
                    THEN rp.barcode
                END) as products_with_images,
                COUNT(DISTINCT CASE
                    WHEN cp.barcode IS NULL
                    THEN rp.barcode
                END) as products_not_in_canonical
            FROM retailer_products rp
            JOIN retailers r ON rp.retailer_id = r.retailerid
            LEFT JOIN canonical_products cp ON rp.barcode = cp.barcode
            WHERE rp.barcode IS NOT NULL AND rp.barcode != ''
            GROUP BY r.retailername
        )
        SELECT
            retailername,
            CASE WHEN iscommercial THEN 'Commercial' ELSE 'Government ETL' END as type,
            total_products,
            products_with_images,
            products_not_in_canonical,
            ROUND(products_with_images * 100.0 / total_products, 2) as pct_with_images
        FROM retailer_stats
        ORDER BY total_products DESC;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    print("\n" + "="*80)
    print("5. RETAILER COVERAGE ANALYSIS")
    print("="*80)

    print("\n{:<30} {:<15} {:>15} {:>15} {:>15} {:>10}".format(
        "Retailer", "Type", "Total Products", "With Images", "Not in Canon", "% Images"
    ))
    print("-" * 100)

    for row in results:
        print("{:<30} {:<15} {:>15,} {:>15,} {:>15,} {:>9.1f}%".format(
            row['retailername'][:30],
            row['type'],
            row['total_products'],
            row['products_with_images'],
            row['products_not_in_canonical'],
            row['pct_with_images']
        ))

    return results


def generate_summary_report(cursor):
    """
    Generate a comprehensive summary report
    """
    print("\n" + "="*80)
    print("FORENSIC ANALYSIS SUMMARY REPORT")
    print("="*80)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n" + "="*80)

    # Key finding queries
    queries = {
        "Total Products in System": """
            SELECT COUNT(DISTINCT barcode) FROM retailer_products
            WHERE barcode IS NOT NULL AND barcode != ''
        """,
        "Products with Price Data": """
            SELECT COUNT(DISTINCT rp.barcode)
            FROM retailer_products rp
            JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
            WHERE rp.barcode IS NOT NULL AND rp.barcode != ''
        """,
        "Products in Canonical Table": """
            SELECT COUNT(DISTINCT barcode) FROM canonical_products
            WHERE barcode IS NOT NULL AND barcode != ''
        """,
        "Canonical Products with Images": """
            SELECT COUNT(DISTINCT barcode) FROM canonical_products
            WHERE image_url IS NOT NULL AND image_url != ''
        """,
        "Products Linked (Price + Image)": """
            SELECT COUNT(DISTINCT rp.barcode)
            FROM retailer_products rp
            JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
            JOIN canonical_products cp ON rp.barcode = cp.barcode
            WHERE cp.image_url IS NOT NULL AND cp.image_url != ''
        """
    }

    print("\nKEY METRICS:")
    stats = {}
    for label, query in queries.items():
        cursor.execute(query)
        result = cursor.fetchone()
        value = result[0] if result else 0
        stats[label] = value
        print(f"  {label}: {value:,}")

    # Calculate coverage percentages
    if stats["Products with Price Data"] > 0:
        coverage_pct = stats["Products Linked (Price + Image)"] * 100 / stats["Products with Price Data"]
        print(f"\n  📊 IMAGE COVERAGE RATE: {coverage_pct:.1f}%")
        print(f"     ({stats['Products Linked (Price + Image)']:,} out of {stats['Products with Price Data']:,} products with prices)")

    # Identify the main issue
    print("\n" + "="*80)
    print("DIAGNOSIS:")
    print("="*80)

    missing_from_canonical = stats["Products with Price Data"] - stats["Products in Canonical Table"]
    missing_images = stats["Products in Canonical Table"] - stats["Canonical Products with Images"]

    if missing_from_canonical > missing_images:
        print(f"\n🔴 PRIMARY ISSUE: {missing_from_canonical:,} products with price data are NOT in the canonical table")
        print("   → This suggests our scrapers are not finding these products")
    else:
        print(f"\n🔴 PRIMARY ISSUE: {missing_images:,} products in canonical table are missing images")
        print("   → This suggests the canonical table is being populated with ETL fallback data")

    return stats


def main():
    """Main execution function."""
    print("\n" + "="*80)
    print("PHARMMATE DATA FORENSICS ANALYSIS")
    print("="*80)
    print("Starting deep forensic analysis of database linkage issues...")

    # Connect to database
    conn = get_database_connection()
    cursor = conn.cursor()

    try:
        # Run all diagnostic functions
        find_products_with_prices_but_no_images(cursor, limit=20)
        analyze_canonical_data_sources(cursor)
        find_unscraped_barcodes(cursor, limit=20)
        analyze_barcode_formats(cursor)
        check_retailer_coverage(cursor)

        # Generate summary report
        generate_summary_report(cursor)

        print("\n" + "="*80)
        print("ANALYSIS COMPLETE")
        print("="*80)
        print("\n✅ Forensic analysis completed successfully.")
        print("   Review the findings above to identify the root cause of image coverage issues.")

    except Exception as e:
        print(f"\n❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()