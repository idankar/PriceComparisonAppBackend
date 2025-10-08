#!/usr/bin/env python3
"""
Bidirectional Analysis: Commercial Products vs Price Listings

This script analyzes the relationship between commercial products and price listings
to determine if we're missing entire products or just images.
"""

import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime
import json


def get_database_connection():
    """Establish connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="price_comparison_app_v2",
            user="postgres",
            password="025655358",
            cursor_factory=DictCursor
        )
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        return None


def analyze_commercial_products_without_prices(cursor):
    """
    Find commercial products (from scrapers) that don't have any price listings
    """
    print("\n" + "="*80)
    print("1. COMMERCIAL PRODUCTS WITHOUT PRICE LISTINGS")
    print("="*80)

    # Count commercial products without prices
    query = """
        WITH commercial_products AS (
            SELECT
                cp.barcode,
                cp.name,
                cp.image_url,
                r.retailername as source_retailer
            FROM canonical_products cp
            JOIN retailers r ON cp.source_retailer_id = r.retailerid
            WHERE cp.source_retailer_id IS NOT NULL  -- From commercial scrapers
        )
        SELECT
            cp.source_retailer,
            COUNT(DISTINCT cp.barcode) as total_commercial_products,
            COUNT(DISTINCT CASE
                WHEN rp.barcode IS NULL THEN cp.barcode
            END) as products_without_any_price,
            COUNT(DISTINCT CASE
                WHEN rp.barcode IS NOT NULL AND p.price_id IS NULL THEN cp.barcode
            END) as products_without_current_price,
            COUNT(DISTINCT CASE
                WHEN cp.image_url IS NOT NULL AND cp.image_url != '' THEN cp.barcode
            END) as products_with_images
        FROM commercial_products cp
        LEFT JOIN retailer_products rp ON cp.barcode = rp.barcode
        LEFT JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
        GROUP BY cp.source_retailer
        ORDER BY total_commercial_products DESC;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    print("\nCommercial Products Status by Scraper:")
    print("-" * 70)
    total_commercial = 0
    total_without_price = 0

    for row in results:
        total_commercial += row['total_commercial_products']
        total_without_price += row['products_without_any_price']

        print(f"\n{row['source_retailer']}:")
        print(f"  Total scraped products: {row['total_commercial_products']:,}")
        print(f"  Without ANY price listing: {row['products_without_any_price']:,} ({row['products_without_any_price']*100/row['total_commercial_products']:.1f}%)")
        print(f"  Without current price: {row['products_without_current_price']:,}")
        print(f"  With images: {row['products_with_images']:,} ({row['products_with_images']*100/row['total_commercial_products']:.1f}%)")

    print(f"\n📊 TOTAL: {total_without_price:,} out of {total_commercial:,} commercial products have NO price listings ({total_without_price*100/total_commercial:.1f}%)")

    # Get sample of orphaned commercial products
    sample_query = """
        SELECT
            cp.barcode,
            cp.name,
            cp.image_url,
            r.retailername as source
        FROM canonical_products cp
        JOIN retailers r ON cp.source_retailer_id = r.retailerid
        LEFT JOIN retailer_products rp ON cp.barcode = rp.barcode
        WHERE cp.source_retailer_id IS NOT NULL
        AND rp.barcode IS NULL
        LIMIT 10;
    """

    cursor.execute(sample_query)
    samples = cursor.fetchall()

    if samples:
        print("\nSample of commercial products with NO price listings:")
        for i, row in enumerate(samples, 1):
            print(f"  {i}. {row['barcode']} - {row['name'][:50]} ({row['source']})")
            print(f"     Image: {'Yes' if row['image_url'] else 'No'}")


def analyze_price_listings_without_commercial(cursor):
    """
    Find price listings that don't link to any commercial product
    """
    print("\n" + "="*80)
    print("2. PRICE LISTINGS WITHOUT COMMERCIAL PRODUCTS")
    print("="*80)

    query = """
        WITH price_products AS (
            SELECT DISTINCT
                rp.barcode,
                rp.retailer_id,
                r.retailername,
                COUNT(DISTINCT p.price_id) as price_count
            FROM retailer_products rp
            JOIN retailers r ON rp.retailer_id = r.retailerid
            JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
            WHERE rp.barcode IS NOT NULL AND rp.barcode != ''
            GROUP BY rp.barcode, rp.retailer_id, r.retailername
        )
        SELECT
            pp.retailername,
            COUNT(DISTINCT pp.barcode) as total_with_prices,
            COUNT(DISTINCT CASE
                WHEN cp.barcode IS NULL THEN pp.barcode
            END) as not_in_canonical,
            COUNT(DISTINCT CASE
                WHEN cp.barcode IS NOT NULL AND cp.source_retailer_id IS NULL THEN pp.barcode
            END) as in_canonical_but_etl_only,
            COUNT(DISTINCT CASE
                WHEN cp.barcode IS NOT NULL AND cp.source_retailer_id IS NOT NULL THEN pp.barcode
            END) as linked_to_commercial,
            SUM(pp.price_count) as total_price_points
        FROM price_products pp
        LEFT JOIN canonical_products cp ON pp.barcode = cp.barcode
        GROUP BY pp.retailername
        ORDER BY total_with_prices DESC;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    print("\nPrice Listings Status by Retailer:")
    print("-" * 70)

    total_with_prices = 0
    total_not_linked = 0
    total_etl_only = 0

    for row in results:
        total_with_prices += row['total_with_prices']
        total_not_linked += row['not_in_canonical']
        total_etl_only += row['in_canonical_but_etl_only']

        print(f"\n{row['retailername']}:")
        print(f"  Products with prices: {row['total_with_prices']:,}")
        print(f"  Not in canonical at all: {row['not_in_canonical']:,} ({row['not_in_canonical']*100/row['total_with_prices']:.1f}%)")
        print(f"  In canonical (ETL only): {row['in_canonical_but_etl_only']:,} ({row['in_canonical_but_etl_only']*100/row['total_with_prices']:.1f}%)")
        print(f"  Linked to commercial: {row['linked_to_commercial']:,} ({row['linked_to_commercial']*100/row['total_with_prices']:.1f}%)")
        print(f"  Total price points: {row['total_price_points']:,}")

    print(f"\n📊 SUMMARY:")
    print(f"  - {total_not_linked:,} products with prices are NOT in canonical table at all")
    print(f"  - {total_etl_only:,} products with prices are in canonical but from ETL fallback only")
    print(f"  - {total_not_linked + total_etl_only:,} total products with prices but NO commercial scraper data")


def analyze_image_vs_product_completeness(cursor):
    """
    Determine if we're missing just images or entire product records
    """
    print("\n" + "="*80)
    print("3. IMAGE COMPLETENESS VS PRODUCT COMPLETENESS")
    print("="*80)

    query = """
        WITH analysis AS (
            SELECT
                cp.barcode,
                cp.name,
                cp.image_url,
                cp.source_retailer_id,
                CASE
                    WHEN cp.source_retailer_id IS NOT NULL THEN 'Commercial'
                    ELSE 'ETL Fallback'
                END as source_type,
                CASE
                    WHEN rp.barcode IS NOT NULL THEN true
                    ELSE false
                END as has_price_listing,
                CASE
                    WHEN cp.image_url IS NOT NULL AND cp.image_url != '' THEN true
                    ELSE false
                END as has_image,
                CASE
                    WHEN cp.name IS NOT NULL AND cp.name != '' THEN true
                    ELSE false
                END as has_name,
                CASE
                    WHEN cp.brand IS NOT NULL AND cp.brand != '' THEN true
                    ELSE false
                END as has_brand
            FROM canonical_products cp
            LEFT JOIN retailer_products rp ON cp.barcode = rp.barcode
        )
        SELECT
            source_type,
            COUNT(*) as total_products,
            COUNT(CASE WHEN has_price_listing THEN 1 END) as with_prices,
            COUNT(CASE WHEN has_image THEN 1 END) as with_images,
            COUNT(CASE WHEN has_name THEN 1 END) as with_names,
            COUNT(CASE WHEN has_brand THEN 1 END) as with_brands,
            COUNT(CASE WHEN has_name AND NOT has_image THEN 1 END) as name_but_no_image,
            COUNT(CASE WHEN has_price_listing AND has_image THEN 1 END) as complete_products
        FROM analysis
        GROUP BY source_type;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    print("\nProduct Completeness Analysis:")
    print("-" * 70)

    for row in results:
        print(f"\n{row['source_type']} Products:")
        print(f"  Total: {row['total_products']:,}")
        print(f"  With price listings: {row['with_prices']:,} ({row['with_prices']*100/row['total_products']:.1f}%)")
        print(f"  With images: {row['with_images']:,} ({row['with_images']*100/row['total_products']:.1f}%)")
        print(f"  With names: {row['with_names']:,} ({row['with_names']*100/row['total_products']:.1f}%)")
        print(f"  With brands: {row['with_brands']:,} ({row['with_brands']*100/row['total_products']:.1f}%)")
        print(f"  Have name but NO image: {row['name_but_no_image']:,}")
        print(f"  Complete (price + image): {row['complete_products']:,}")

    # Check for products with partial data
    partial_query = """
        SELECT
            r.retailername,
            COUNT(DISTINCT cp.barcode) as products_with_name_no_image
        FROM canonical_products cp
        LEFT JOIN retailers r ON cp.source_retailer_id = r.retailerid
        WHERE cp.name IS NOT NULL
        AND cp.name != ''
        AND (cp.image_url IS NULL OR cp.image_url = '')
        AND cp.source_retailer_id IS NOT NULL
        GROUP BY r.retailername
        ORDER BY products_with_name_no_image DESC;
    """

    cursor.execute(partial_query)
    partial_results = cursor.fetchall()

    if partial_results:
        print("\n⚠️  Commercial products with names but NO images (indicates scraper issue):")
        for row in partial_results:
            print(f"  {row['retailername']}: {row['products_with_name_no_image']:,} products")


def analyze_barcode_matching_issues(cursor):
    """
    Check if there are barcode formatting issues preventing matches
    """
    print("\n" + "="*80)
    print("4. BARCODE MATCHING ANALYSIS")
    print("="*80)

    # Check for potential barcode mismatches
    query = """
        WITH barcode_formats AS (
            SELECT
                'retailer_products' as source,
                LENGTH(barcode) as barcode_length,
                COUNT(*) as count
            FROM retailer_products
            WHERE barcode IS NOT NULL AND barcode != ''
            GROUP BY LENGTH(barcode)

            UNION ALL

            SELECT
                'canonical_products' as source,
                LENGTH(barcode) as barcode_length,
                COUNT(*) as count
            FROM canonical_products
            WHERE barcode IS NOT NULL AND barcode != ''
            GROUP BY LENGTH(barcode)
        )
        SELECT
            barcode_length,
            MAX(CASE WHEN source = 'retailer_products' THEN count ELSE 0 END) as in_retailer_products,
            MAX(CASE WHEN source = 'canonical_products' THEN count ELSE 0 END) as in_canonical_products
        FROM barcode_formats
        GROUP BY barcode_length
        ORDER BY barcode_length;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    print("\nBarcode Length Distribution Comparison:")
    print("Length | Retailer Products | Canonical Products | Difference")
    print("-" * 60)

    for row in results:
        diff = abs(row['in_retailer_products'] - row['in_canonical_products'])
        flag = "⚠️" if diff > 100 else ""
        print(f"{row['barcode_length']:6} | {row['in_retailer_products']:17,} | {row['in_canonical_products']:18,} | {diff:10,} {flag}")

    # Check for leading zero issues
    zero_query = """
        SELECT
            COUNT(DISTINCT rp.barcode) as potential_zero_mismatches
        FROM retailer_products rp
        WHERE rp.barcode IS NOT NULL
        AND rp.barcode != ''
        AND NOT EXISTS (
            SELECT 1 FROM canonical_products cp
            WHERE cp.barcode = rp.barcode
        )
        AND EXISTS (
            SELECT 1 FROM canonical_products cp
            WHERE cp.barcode = LPAD(rp.barcode, 13, '0')
            OR cp.barcode = LTRIM(rp.barcode, '0')
        );
    """

    cursor.execute(zero_query)
    result = cursor.fetchone()

    if result['potential_zero_mismatches'] > 0:
        print(f"\n⚠️  Found {result['potential_zero_mismatches']:,} potential barcode mismatches due to leading zeros")


def generate_final_summary(cursor):
    """
    Generate comprehensive summary with actionable insights
    """
    print("\n" + "="*80)
    print("FINAL SUMMARY AND RECOMMENDATIONS")
    print("="*80)

    # Get key metrics
    metrics_query = """
        WITH metrics AS (
            SELECT
                (SELECT COUNT(DISTINCT barcode) FROM canonical_products WHERE source_retailer_id IS NOT NULL) as commercial_products,
                (SELECT COUNT(DISTINCT barcode) FROM canonical_products WHERE source_retailer_id IS NULL) as etl_fallback_products,
                (SELECT COUNT(DISTINCT rp.barcode) FROM retailer_products rp JOIN prices p ON rp.retailer_product_id = p.retailer_product_id) as products_with_prices,
                (SELECT COUNT(DISTINCT cp.barcode) FROM canonical_products cp WHERE cp.image_url IS NOT NULL AND cp.image_url != '') as products_with_images,
                (SELECT COUNT(DISTINCT cp.barcode)
                 FROM canonical_products cp
                 JOIN retailer_products rp ON cp.barcode = rp.barcode
                 WHERE cp.source_retailer_id IS NOT NULL) as commercial_with_prices,
                (SELECT COUNT(DISTINCT cp.barcode)
                 FROM canonical_products cp
                 WHERE cp.source_retailer_id IS NOT NULL
                 AND NOT EXISTS (SELECT 1 FROM retailer_products rp WHERE rp.barcode = cp.barcode)) as commercial_without_prices
        )
        SELECT * FROM metrics;
    """

    cursor.execute(metrics_query)
    metrics = cursor.fetchone()

    print("\n📊 KEY METRICS:")
    print(f"  Commercial products scraped: {metrics['commercial_products']:,}")
    print(f"  Commercial products WITH price listings: {metrics['commercial_with_prices']:,}")
    print(f"  Commercial products WITHOUT price listings: {metrics['commercial_without_prices']:,}")
    print(f"  ETL fallback products: {metrics['etl_fallback_products']:,}")
    print(f"  Total products with prices: {metrics['products_with_prices']:,}")
    print(f"  Total products with images: {metrics['products_with_images']:,}")

    print("\n🎯 KEY FINDINGS:")

    if metrics['commercial_without_prices'] > 0:
        pct = metrics['commercial_without_prices'] * 100 / metrics['commercial_products']
        print(f"  1. {metrics['commercial_without_prices']:,} ({pct:.1f}%) commercial products have NO price listings")
        print("     → These are orphaned scraper results not matching government data")

    if metrics['etl_fallback_products'] > 0:
        pct = metrics['etl_fallback_products'] * 100 / (metrics['commercial_products'] + metrics['etl_fallback_products'])
        print(f"  2. {metrics['etl_fallback_products']:,} ({pct:.1f}%) products are ETL fallbacks with no scraper data")
        print("     → These products exist in government data but scrapers aren't finding them")

    coverage = metrics['products_with_images'] * 100 / metrics['products_with_prices']
    print(f"  3. Image coverage is only {coverage:.1f}%")
    print("     → Major gap in visual product data")


def main():
    """Main execution function."""
    print("\n" + "="*80)
    print("BIDIRECTIONAL LINKAGE ANALYSIS")
    print("="*80)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Connect to database
    conn = get_database_connection()
    if not conn:
        return

    cursor = conn.cursor()

    try:
        # Run all analyses
        analyze_commercial_products_without_prices(cursor)
        analyze_price_listings_without_commercial(cursor)
        analyze_image_vs_product_completeness(cursor)
        analyze_barcode_matching_issues(cursor)
        generate_final_summary(cursor)

        print("\n" + "="*80)
        print("ANALYSIS COMPLETE")
        print("="*80)

    except Exception as e:
        print(f"\n❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()