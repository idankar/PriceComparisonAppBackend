#!/usr/bin/env python3
"""
Retailer Data Quality Diagnosis and Fuzzy Matching Analysis

This script performs two critical tests:
1. Identifies if there's a problematic retailer with incomplete/bad data
2. Tests fuzzy matching to see if we can improve product overlap
"""

import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime
from collections import defaultdict
import difflib
import re


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


def diagnose_retailer_data_quality(cursor):
    """
    Test 1: Identify if there's a problematic retailer with bad/incomplete data
    """
    print("\n" + "="*80)
    print("TEST 1: RETAILER DATA QUALITY DIAGNOSIS")
    print("="*80)

    # Get comprehensive metrics for each retailer
    query = """
        WITH retailer_metrics AS (
            SELECT
                r.retailername,
                COUNT(DISTINCT rp.barcode) as unique_products,
                COUNT(DISTINCT p.price_id) as total_price_points,
                COUNT(DISTINCT p.store_id) as stores_with_prices,
                COUNT(DISTINCT DATE(p.price_timestamp)) as days_with_data,
                MIN(DATE(p.price_timestamp)) as earliest_data,
                MAX(DATE(p.price_timestamp)) as latest_data,
                AVG(LENGTH(rp.original_retailer_name)) as avg_name_length,
                COUNT(DISTINCT CASE WHEN LENGTH(rp.barcode) = 13 THEN rp.barcode END) as standard_barcodes,
                COUNT(DISTINCT CASE WHEN LENGTH(rp.barcode) != 13 THEN rp.barcode END) as non_standard_barcodes,
                COUNT(DISTINCT cp.barcode) as products_in_canonical,
                COUNT(DISTINCT CASE WHEN cp.image_url IS NOT NULL THEN cp.barcode END) as products_with_images
            FROM retailer_products rp
            JOIN retailers r ON rp.retailer_id = r.retailerid
            LEFT JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
            LEFT JOIN canonical_products cp ON rp.barcode = cp.barcode
            WHERE r.retailername IN ('Super-Pharm', 'Be Pharm', 'Good Pharm')
            GROUP BY r.retailername
        )
        SELECT * FROM retailer_metrics
        ORDER BY unique_products DESC;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    print("\n📊 RETAILER DATA QUALITY METRICS:")
    print("-" * 80)

    for row in results:
        print(f"\n{row['retailername']}:")
        print(f"  Unique Products: {row['unique_products']:,}")
        print(f"  Total Price Points: {row['total_price_points']:,}")
        print(f"  Stores with Prices: {row['stores_with_prices']}")
        print(f"  Days with Data: {row['days_with_data']}")
        print(f"  Data Range: {row['earliest_data']} to {row['latest_data']}")
        print(f"  Avg Product Name Length: {row['avg_name_length']:.1f} chars")
        print(f"  Standard Barcodes (13-digit): {row['standard_barcodes']:,}")
        print(f"  Non-standard Barcodes: {row['non_standard_barcodes']:,}")
        print(f"  Products in Canonical: {row['products_in_canonical']:,}")
        print(f"  Products with Images: {row['products_with_images']:,}")

    # Analyze pairwise overlaps to identify the problematic retailer
    print("\n" + "-"*80)
    print("PAIRWISE RETAILER OVERLAPS:")
    print("-"*80)

    pairwise_query = """
        WITH retailer_pairs AS (
            SELECT
                r1.retailername as retailer1,
                r2.retailername as retailer2,
                COUNT(DISTINCT rp1.barcode) as shared_barcodes
            FROM retailer_products rp1
            JOIN retailers r1 ON rp1.retailer_id = r1.retailerid
            JOIN retailer_products rp2 ON rp1.barcode = rp2.barcode
            JOIN retailers r2 ON rp2.retailer_id = r2.retailerid
            WHERE r1.retailername IN ('Super-Pharm', 'Be Pharm', 'Good Pharm')
            AND r2.retailername IN ('Super-Pharm', 'Be Pharm', 'Good Pharm')
            AND r1.retailername < r2.retailername
            GROUP BY r1.retailername, r2.retailername
        )
        SELECT * FROM retailer_pairs
        ORDER BY shared_barcodes DESC;
    """

    cursor.execute(pairwise_query)
    pairwise_results = cursor.fetchall()

    for row in pairwise_results:
        print(f"{row['retailer1']} ↔ {row['retailer2']}: {row['shared_barcodes']:,} shared products")

    # Check for data freshness issues
    print("\n" + "-"*80)
    print("DATA FRESHNESS CHECK (Last 30 days):")
    print("-"*80)

    freshness_query = """
        SELECT
            r.retailername,
            COUNT(DISTINCT rp.barcode) as products_with_recent_prices
        FROM retailer_products rp
        JOIN retailers r ON rp.retailer_id = r.retailerid
        JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
        WHERE r.retailername IN ('Super-Pharm', 'Be Pharm', 'Good Pharm')
        AND p.price_timestamp > NOW() - INTERVAL '30 days'
        GROUP BY r.retailername
        ORDER BY products_with_recent_prices DESC;
    """

    cursor.execute(freshness_query)
    freshness_results = cursor.fetchall()

    for row in freshness_results:
        print(f"{row['retailername']}: {row['products_with_recent_prices']:,} products with recent prices")

    return results


def normalize_product_name(name):
    """Normalize product name for fuzzy matching."""
    if not name:
        return ""

    # Convert to lowercase
    name = name.lower()

    # Remove common size indicators and units
    name = re.sub(r'\d+\s*(מ"ל|מל|ml|ג|g|גרם|gram|יח|יחידות|units)', '', name)

    # Remove special characters
    name = re.sub(r'[^\w\s\u0590-\u05FF]', ' ', name)  # Keep Hebrew chars

    # Remove extra spaces
    name = ' '.join(name.split())

    return name


def fuzzy_match_test(cursor):
    """
    Test 2: Use fuzzy matching to see if we can improve product overlap
    """
    print("\n" + "="*80)
    print("TEST 2: FUZZY MATCHING ANALYSIS")
    print("="*80)

    # Get products that are unique to each retailer (no barcode match)
    query = """
        WITH retailer_counts AS (
            SELECT
                rp.barcode,
                COUNT(DISTINCT r.retailername) as retailer_count
            FROM retailer_products rp
            JOIN retailers r ON rp.retailer_id = r.retailerid
            WHERE r.retailername IN ('Super-Pharm', 'Be Pharm', 'Good Pharm')
            AND rp.barcode IS NOT NULL
            GROUP BY rp.barcode
        ),
        single_retailer_products AS (
            SELECT
                rp.barcode,
                rp.original_retailer_name,
                r.retailername
            FROM retailer_products rp
            JOIN retailers r ON rp.retailer_id = r.retailerid
            JOIN retailer_counts rc ON rp.barcode = rc.barcode
            WHERE r.retailername IN ('Super-Pharm', 'Be Pharm', 'Good Pharm')
            AND rp.barcode IS NOT NULL
            AND rp.original_retailer_name IS NOT NULL
            AND rc.retailer_count = 1
        )
        SELECT
            barcode,
            original_retailer_name,
            retailername
        FROM single_retailer_products
        LIMIT 500;
    """

    cursor.execute(query)
    single_retailer_products = cursor.fetchall()

    # Group products by retailer
    products_by_retailer = defaultdict(list)
    for row in single_retailer_products:
        products_by_retailer[row['retailername']].append({
            'barcode': row['barcode'],
            'name': row['original_retailer_name'],
            'normalized': normalize_product_name(row['original_retailer_name'])
        })

    # Perform fuzzy matching between retailers
    fuzzy_matches = []
    threshold = 0.85  # Similarity threshold

    retailers = list(products_by_retailer.keys())
    for i in range(len(retailers)):
        for j in range(i + 1, len(retailers)):
            retailer1 = retailers[i]
            retailer2 = retailers[j]

            products1 = products_by_retailer[retailer1]
            products2 = products_by_retailer[retailer2]

            for p1 in products1[:100]:  # Limit to first 100 for performance
                for p2 in products2[:100]:
                    similarity = difflib.SequenceMatcher(None, p1['normalized'], p2['normalized']).ratio()

                    if similarity >= threshold:
                        fuzzy_matches.append({
                            'retailer1': retailer1,
                            'product1': p1['name'],
                            'barcode1': p1['barcode'],
                            'retailer2': retailer2,
                            'product2': p2['name'],
                            'barcode2': p2['barcode'],
                            'similarity': similarity
                        })

    # Sort by similarity
    fuzzy_matches.sort(key=lambda x: x['similarity'], reverse=True)

    print(f"\n🔍 FUZZY MATCHING RESULTS (threshold: {threshold*100}% similarity)")
    print("-" * 80)
    print(f"Found {len(fuzzy_matches)} potential matches between single-retailer products")

    if fuzzy_matches:
        print("\nTop 10 Potential Matches:")
        print("-" * 60)
        for i, match in enumerate(fuzzy_matches[:10], 1):
            print(f"\n{i}. Similarity: {match['similarity']*100:.1f}%")
            print(f"   {match['retailer1']}: {match['product1'][:50]}")
            print(f"   Barcode: {match['barcode1']}")
            print(f"   {match['retailer2']}: {match['product2'][:50]}")
            print(f"   Barcode: {match['barcode2']}")

    # Estimate potential improvement
    estimate_improvement(cursor, len(fuzzy_matches))

    return fuzzy_matches


def estimate_improvement(cursor, fuzzy_match_count):
    """
    Estimate how much overlap could improve with fuzzy matching
    """
    print("\n" + "="*80)
    print("POTENTIAL OVERLAP IMPROVEMENT")
    print("="*80)

    # Get current overlap stats
    query = """
        WITH barcode_coverage AS (
            SELECT
                rp.barcode,
                COUNT(DISTINCT r.retailername) as retailer_count
            FROM retailer_products rp
            JOIN retailers r ON rp.retailer_id = r.retailerid
            WHERE r.retailername IN ('Super-Pharm', 'Be Pharm', 'Good Pharm')
            GROUP BY rp.barcode
        )
        SELECT
            COUNT(*) as total_products,
            COUNT(CASE WHEN retailer_count >= 2 THEN 1 END) as currently_comparable,
            COUNT(CASE WHEN retailer_count = 1 THEN 1 END) as single_retailer_only
        FROM barcode_coverage;
    """

    cursor.execute(query)
    result = cursor.fetchone()

    current_comparable = result['currently_comparable']
    single_retailer = result['single_retailer_only']
    total = result['total_products']

    # Conservative estimate: assume 50% of fuzzy matches are correct
    potential_new_matches = fuzzy_match_count * 0.5
    new_comparable = current_comparable + potential_new_matches
    new_percentage = (new_comparable / total) * 100

    print(f"Current Status:")
    print(f"  Total unique products: {total:,}")
    print(f"  Currently comparable (2+ retailers): {current_comparable:,} ({current_comparable*100/total:.1f}%)")
    print(f"  Single retailer only: {single_retailer:,} ({single_retailer*100/total:.1f}%)")

    print(f"\nWith Fuzzy Matching (conservative estimate):")
    print(f"  Potential new matches: ~{int(potential_new_matches):,}")
    print(f"  New comparable total: ~{int(new_comparable):,} ({new_percentage:.1f}%)")
    print(f"  Improvement: +{new_percentage - (current_comparable*100/total):.1f} percentage points")


def check_barcode_inconsistencies(cursor):
    """
    Check if barcodes are formatted differently across retailers
    """
    print("\n" + "="*80)
    print("BARCODE FORMAT CONSISTENCY CHECK")
    print("="*80)

    query = """
        WITH barcode_formats AS (
            SELECT
                r.retailername,
                LENGTH(rp.barcode) as barcode_length,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY r.retailername), 1) as percentage
            FROM retailer_products rp
            JOIN retailers r ON rp.retailer_id = r.retailerid
            WHERE r.retailername IN ('Super-Pharm', 'Be Pharm', 'Good Pharm')
            AND rp.barcode IS NOT NULL
            GROUP BY r.retailername, LENGTH(rp.barcode)
        )
        SELECT * FROM barcode_formats
        ORDER BY retailername, barcode_length;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    current_retailer = None
    for row in results:
        if current_retailer != row['retailername']:
            current_retailer = row['retailername']
            print(f"\n{current_retailer} Barcode Lengths:")
        print(f"  {row['barcode_length']}-digit: {row['count']:,} ({row['percentage']}%)")


def identify_problematic_retailer(cursor):
    """
    Final diagnosis: identify which retailer is problematic
    """
    print("\n" + "="*80)
    print("FINAL DIAGNOSIS: PROBLEMATIC RETAILER IDENTIFICATION")
    print("="*80)

    # Score each retailer based on multiple factors
    query = """
        WITH retailer_scores AS (
            SELECT
                r.retailername,
                COUNT(DISTINCT rp.barcode) as product_count,
                COUNT(DISTINCT p.price_id) as price_points,
                COUNT(DISTINCT CASE WHEN LENGTH(rp.barcode) = 13 THEN rp.barcode END) * 100.0 /
                    NULLIF(COUNT(DISTINCT rp.barcode), 0) as standard_barcode_pct,
                AVG(CASE WHEN rp.original_retailer_name IS NOT NULL THEN 1 ELSE 0 END) * 100 as name_completeness,
                COUNT(DISTINCT cp.barcode) * 100.0 / NULLIF(COUNT(DISTINCT rp.barcode), 0) as canonical_match_rate
            FROM retailer_products rp
            JOIN retailers r ON rp.retailer_id = r.retailerid
            LEFT JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
            LEFT JOIN canonical_products cp ON rp.barcode = cp.barcode AND cp.source_retailer_id IS NOT NULL
            WHERE r.retailername IN ('Super-Pharm', 'Be Pharm', 'Good Pharm')
            GROUP BY r.retailername
        )
        SELECT * FROM retailer_scores
        ORDER BY product_count DESC;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    print("\nRETAILER QUALITY SCORES:")
    print("-" * 80)
    print(f"{'Retailer':<15} {'Products':<12} {'Prices':<12} {'Std Barcode%':<15} {'Name Complete%':<15} {'Canon Match%':<12}")
    print("-" * 80)

    problematic_retailers = []
    for row in results:
        print(f"{row['retailername']:<15} {row['product_count']:<12,} {row['price_points']:<12,} "
              f"{row['standard_barcode_pct']:<15.1f} {row['name_completeness']:<15.1f} "
              f"{row['canonical_match_rate']:<12.1f}")

        # Flag potential issues
        issues = []
        if row['standard_barcode_pct'] < 80:
            issues.append("non-standard barcodes")
        if row['name_completeness'] < 95:
            issues.append("missing names")
        if row['canonical_match_rate'] < 40:
            issues.append("low canonical matches")
        if row['price_points'] < 100000:
            issues.append("low price data")

        if issues:
            problematic_retailers.append((row['retailername'], issues))

    if problematic_retailers:
        print("\n⚠️ POTENTIAL ISSUES DETECTED:")
        for retailer, issues in problematic_retailers:
            print(f"  {retailer}: {', '.join(issues)}")
        print("\n💡 RECOMMENDATION: Consider re-scraping these retailers")
    else:
        print("\n✅ All retailers appear to have good data quality")


def main():
    """Main execution function."""
    print("\n" + "="*80)
    print("RETAILER DIAGNOSIS & FUZZY MATCHING ANALYSIS")
    print("="*80)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Connect to database
    conn = get_database_connection()
    if not conn:
        return

    cursor = conn.cursor()

    try:
        # Test 1: Diagnose retailer data quality
        retailer_metrics = diagnose_retailer_data_quality(cursor)

        # Check barcode consistency
        check_barcode_inconsistencies(cursor)

        # Test 2: Fuzzy matching analysis
        fuzzy_matches = fuzzy_match_test(cursor)

        # Final diagnosis
        identify_problematic_retailer(cursor)

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