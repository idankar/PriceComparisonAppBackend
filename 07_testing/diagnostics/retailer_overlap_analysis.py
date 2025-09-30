#!/usr/bin/env python3
"""
Retailer Product Overlap Analysis

Analyzes how many products are available across multiple retailers
to assess the reliability of price comparison functionality.
"""

import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime
from collections import defaultdict
import json


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
        return None


def analyze_retailer_overlap(cursor):
    """
    Analyze product overlap between the three retailers.
    """
    print("\n" + "="*80)
    print("RETAILER PRODUCT OVERLAP ANALYSIS")
    print("="*80)

    # Get products by retailer with price data
    query = """
        WITH retailer_products_with_prices AS (
            SELECT DISTINCT
                rp.barcode,
                r.retailername,
                rp.original_retailer_name as product_name,
                COUNT(DISTINCT p.price_id) as price_points
            FROM retailer_products rp
            JOIN retailers r ON rp.retailer_id = r.retailerid
            JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
            WHERE rp.barcode IS NOT NULL
            AND rp.barcode != ''
            AND r.retailername IN ('Super-Pharm', 'Be Pharm', 'Good Pharm')
            GROUP BY rp.barcode, r.retailername, rp.original_retailer_name
        ),
        barcode_coverage AS (
            SELECT
                barcode,
                STRING_AGG(DISTINCT retailername, ', ' ORDER BY retailername) as retailers,
                COUNT(DISTINCT retailername) as retailer_count,
                MAX(product_name) as sample_name,
                SUM(price_points) as total_price_points
            FROM retailer_products_with_prices
            GROUP BY barcode
        )
        SELECT
            retailer_count,
            retailers,
            COUNT(*) as product_count,
            SUM(total_price_points) as total_prices
        FROM barcode_coverage
        GROUP BY retailer_count, retailers
        ORDER BY retailer_count DESC, product_count DESC;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    # Calculate totals
    total_products = sum(row['product_count'] for row in results)
    products_in_1 = sum(row['product_count'] for row in results if row['retailer_count'] == 1)
    products_in_2 = sum(row['product_count'] for row in results if row['retailer_count'] == 2)
    products_in_3 = sum(row['product_count'] for row in results if row['retailer_count'] == 3)

    print("\n📊 OVERALL PRODUCT AVAILABILITY:")
    print("-" * 60)
    print(f"Total unique products with prices: {total_products:,}")
    print(f"Available in ALL 3 retailers: {products_in_3:,} ({products_in_3*100/total_products:.1f}%)")
    print(f"Available in EXACTLY 2 retailers: {products_in_2:,} ({products_in_2*100/total_products:.1f}%)")
    print(f"Available in ONLY 1 retailer: {products_in_1:,} ({products_in_1*100/total_products:.1f}%)")

    print("\n" + "-"*60)
    print("DETAILED BREAKDOWN:")
    print("-"*60)

    for row in results:
        pct = row['product_count'] * 100 / total_products
        print(f"\n{row['retailers']}:")
        print(f"  Products: {row['product_count']:,} ({pct:.1f}%)")
        print(f"  Price points: {row['total_prices']:,}")

    return results, total_products, products_in_1, products_in_2, products_in_3


def analyze_price_comparison_reliability(cursor, products_in_1, products_in_2, products_in_3, total_products):
    """
    Assess how reliable price comparison will be.
    """
    print("\n" + "="*80)
    print("PRICE COMPARISON RELIABILITY ASSESSMENT")
    print("="*80)

    # Calculate comparison opportunities
    comparable_products = products_in_2 + products_in_3
    comparable_pct = comparable_products * 100 / total_products

    print(f"\n🎯 COMPARISON METRICS:")
    print("-" * 60)
    print(f"Products comparable (2+ retailers): {comparable_products:,} ({comparable_pct:.1f}%)")
    print(f"Products NOT comparable (1 retailer): {products_in_1:,} ({products_in_1*100/total_products:.1f}%)")

    # Get detailed metrics for products in all 3 retailers
    query_all_3 = """
        WITH products_in_all AS (
            SELECT
                rp.barcode,
                MAX(rp.original_retailer_name) as product_name,
                COUNT(DISTINCT r.retailername) as retailer_count,
                COUNT(DISTINCT p.price_id) as price_points,
                MIN(p.price) as min_price,
                MAX(p.price) as max_price,
                AVG(p.price) as avg_price,
                CASE
                    WHEN MAX(p.price) > 0
                    THEN ((MAX(p.price) - MIN(p.price)) / MIN(p.price) * 100)
                    ELSE 0
                END as price_variance_pct
            FROM retailer_products rp
            JOIN retailers r ON rp.retailer_id = r.retailerid
            JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
            WHERE rp.barcode IS NOT NULL
            AND rp.barcode != ''
            AND r.retailername IN ('Super-Pharm', 'Be Pharm', 'Good Pharm')
            GROUP BY rp.barcode
            HAVING COUNT(DISTINCT r.retailername) = 3
                AND MIN(p.price) > 0
        )
        SELECT
            COUNT(*) as total_products,
            AVG(price_variance_pct) as avg_price_variance,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_variance_pct) as median_price_variance,
            MAX(price_variance_pct) as max_price_variance,
            COUNT(CASE WHEN price_variance_pct > 10 THEN 1 END) as high_variance_products,
            COUNT(CASE WHEN price_variance_pct > 20 THEN 1 END) as very_high_variance_products
        FROM products_in_all;
    """

    cursor.execute(query_all_3)
    variance_stats = cursor.fetchone()

    if variance_stats and variance_stats['total_products'] > 0:
        print("\n📈 PRICE VARIANCE ANALYSIS (Products in all 3 retailers):")
        print("-" * 60)
        print(f"Average price difference: {variance_stats['avg_price_variance']:.1f}%")
        print(f"Median price difference: {variance_stats['median_price_variance']:.1f}%")
        print(f"Maximum price difference: {variance_stats['max_price_variance']:.1f}%")
        print(f"Products with >10% price difference: {variance_stats['high_variance_products']:,} ({variance_stats['high_variance_products']*100/variance_stats['total_products']:.1f}%)")
        print(f"Products with >20% price difference: {variance_stats['very_high_variance_products']:,} ({variance_stats['very_high_variance_products']*100/variance_stats['total_products']:.1f}%)")

    # Reliability score calculation
    reliability_score = calculate_reliability_score(comparable_pct, products_in_3, total_products, variance_stats)
    print("\n" + "="*80)
    print("RELIABILITY SCORE")
    print("="*80)
    print(f"\n⭐ Overall Reliability Score: {reliability_score:.1f}/100")

    if reliability_score >= 80:
        rating = "EXCELLENT - Very reliable for price comparison"
        emoji = "🟢"
    elif reliability_score >= 60:
        rating = "GOOD - Reliable for most products"
        emoji = "🟡"
    elif reliability_score >= 40:
        rating = "FAIR - Limited comparison capability"
        emoji = "🟠"
    else:
        rating = "POOR - Insufficient overlap for reliable comparison"
        emoji = "🔴"

    print(f"{emoji} Rating: {rating}")

    return variance_stats


def get_sample_comparable_products(cursor):
    """
    Get samples of products available across multiple retailers.
    """
    print("\n" + "="*80)
    print("SAMPLE COMPARABLE PRODUCTS")
    print("="*80)

    # Get top products available in all 3 retailers with price differences
    query = """
        WITH retailer_prices AS (
            SELECT
                rp.barcode,
                rp.original_retailer_name as product_name,
                r.retailername,
                AVG(p.price) as avg_price
            FROM retailer_products rp
            JOIN retailers r ON rp.retailer_id = r.retailerid
            JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
            WHERE rp.barcode IS NOT NULL
            AND rp.barcode != ''
            AND r.retailername IN ('Super-Pharm', 'Be Pharm', 'Good Pharm')
            AND p.price > 0
            GROUP BY rp.barcode, rp.original_retailer_name, r.retailername
        ),
        products_all_retailers AS (
            SELECT
                barcode,
                MAX(product_name) as product_name,
                COUNT(DISTINCT retailername) as retailer_count,
                MIN(avg_price) as min_price,
                MAX(avg_price) as max_price,
                STRING_AGG(retailername || ': ₪' || ROUND(avg_price, 2)::text, ', ' ORDER BY retailername) as price_by_retailer,
                ((MAX(avg_price) - MIN(avg_price)) / NULLIF(MIN(avg_price), 0) * 100) as savings_pct
            FROM retailer_prices
            GROUP BY barcode
            HAVING COUNT(DISTINCT retailername) = 3
                AND MAX(avg_price) > MIN(avg_price)
        )
        SELECT *
        FROM products_all_retailers
        ORDER BY savings_pct DESC
        LIMIT 10;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    print("\nTop 10 products with highest price variance (available in all 3 retailers):")
    print("-" * 80)

    for i, row in enumerate(results, 1):
        savings = row['max_price'] - row['min_price']
        print(f"\n{i}. [{row['barcode']}] {row['product_name'][:50]}")
        print(f"   Price range: ₪{row['min_price']:.2f} - ₪{row['max_price']:.2f}")
        print(f"   Potential savings: ₪{savings:.2f} ({row['savings_pct']:.1f}%)")
        print(f"   Prices: {row['price_by_retailer']}")


def analyze_category_overlap(cursor):
    """
    Analyze which product categories have the best overlap.
    """
    print("\n" + "="*80)
    print("CATEGORY OVERLAP ANALYSIS")
    print("="*80)

    query = """
        WITH categorized_products AS (
            SELECT
                rp.barcode,
                cp.category,
                COUNT(DISTINCT r.retailername) as retailer_count,
                MAX(rp.original_retailer_name) as product_name
            FROM retailer_products rp
            JOIN retailers r ON rp.retailer_id = r.retailerid
            JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
            LEFT JOIN canonical_products cp ON rp.barcode = cp.barcode
            WHERE rp.barcode IS NOT NULL
            AND rp.barcode != ''
            AND r.retailername IN ('Super-Pharm', 'Be Pharm', 'Good Pharm')
            GROUP BY rp.barcode, cp.category
        )
        SELECT
            COALESCE(category, 'Uncategorized') as category,
            COUNT(*) as total_products,
            COUNT(CASE WHEN retailer_count = 3 THEN 1 END) as in_all_3,
            COUNT(CASE WHEN retailer_count = 2 THEN 1 END) as in_2_only,
            COUNT(CASE WHEN retailer_count = 1 THEN 1 END) as in_1_only,
            ROUND(COUNT(CASE WHEN retailer_count >= 2 THEN 1 END) * 100.0 / COUNT(*), 1) as comparable_pct
        FROM categorized_products
        GROUP BY category
        HAVING COUNT(*) > 50
        ORDER BY comparable_pct DESC, total_products DESC
        LIMIT 15;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    if results:
        print("\nCategory comparison reliability (top categories):")
        print("-" * 80)
        print(f"{'Category':<30} {'Total':<10} {'All 3':<10} {'2 Only':<10} {'1 Only':<10} {'Comparable %':<12}")
        print("-" * 80)

        for row in results:
            print(f"{row['category'][:30]:<30} {row['total_products']:<10,} {row['in_all_3']:<10,} {row['in_2_only']:<10,} {row['in_1_only']:<10,} {row['comparable_pct']:<12.1f}%")


def calculate_reliability_score(comparable_pct, products_in_3, total_products, variance_stats):
    """
    Calculate an overall reliability score for price comparison.
    """
    # Weight factors
    coverage_weight = 0.4  # How many products are comparable
    overlap_weight = 0.3   # How many are in all 3 retailers
    variance_weight = 0.3  # Price variance consistency

    # Coverage score (0-100)
    coverage_score = min(comparable_pct * 1.25, 100)  # Scale up since 80% coverage is excellent

    # Overlap score (0-100)
    overlap_pct = products_in_3 * 100 / total_products
    overlap_score = min(overlap_pct * 3, 100)  # Scale up since 33% in all 3 is good

    # Variance score (0-100)
    if variance_stats and variance_stats['avg_price_variance']:
        # Lower variance is better
        avg_variance = variance_stats['avg_price_variance']
        if avg_variance <= 5:
            variance_score = 100
        elif avg_variance <= 10:
            variance_score = 80
        elif avg_variance <= 20:
            variance_score = 60
        elif avg_variance <= 30:
            variance_score = 40
        else:
            variance_score = 20
    else:
        variance_score = 50  # Default if no data

    # Calculate weighted score
    reliability_score = (
        coverage_score * coverage_weight +
        overlap_score * overlap_weight +
        variance_score * variance_weight
    )

    print("\nScore Components:")
    print(f"  Coverage Score: {coverage_score:.1f} (weight: {coverage_weight*100:.0f}%)")
    print(f"  Overlap Score: {overlap_score:.1f} (weight: {overlap_weight*100:.0f}%)")
    print(f"  Variance Score: {variance_score:.1f} (weight: {variance_weight*100:.0f}%)")

    return reliability_score


def main():
    """Main execution function."""
    print("\n" + "="*80)
    print("PHARMMATE RETAILER OVERLAP & PRICE COMPARISON RELIABILITY")
    print("="*80)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Connect to database
    conn = get_database_connection()
    if not conn:
        return

    cursor = conn.cursor()

    try:
        # Analyze retailer overlap
        results, total_products, products_in_1, products_in_2, products_in_3 = analyze_retailer_overlap(cursor)

        # Assess price comparison reliability
        variance_stats = analyze_price_comparison_reliability(
            cursor, products_in_1, products_in_2, products_in_3, total_products
        )

        # Get sample comparable products
        get_sample_comparable_products(cursor)

        # Analyze category overlap
        analyze_category_overlap(cursor)

        print("\n" + "="*80)
        print("KEY TAKEAWAYS")
        print("="*80)

        comparable = products_in_2 + products_in_3
        print(f"\n✅ {comparable:,} products ({comparable*100/total_products:.1f}%) can be compared across retailers")
        print(f"✅ {products_in_3:,} products ({products_in_3*100/total_products:.1f}%) are available in all 3 retailers")

        if variance_stats and variance_stats['avg_price_variance']:
            print(f"✅ Average price difference of {variance_stats['avg_price_variance']:.1f}% suggests real savings opportunities")

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