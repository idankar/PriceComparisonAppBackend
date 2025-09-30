#!/usr/bin/env python3
"""
Analyze Be Pharm Data Sources - Compare Commercial vs ETL/Transparency Data

This script analyzes the existing Be Pharm data in the database to compare:
1. Commercial scraper results (from canonical_products with source_retailer_id = 150)
2. ETL/Transparency portal data (from prices and retailer_products)
"""

import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime, timedelta
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


def analyze_commercial_data(cursor):
    """Analyze Be Pharm commercial data quality"""
    print("\n" + "="*80)
    print("BE PHARM COMMERCIAL DATA ANALYSIS (from canonical_products)")
    print("="*80)

    # Get commercial products scraped by Be Pharm scraper
    query = """
        SELECT
            COUNT(DISTINCT barcode) as total_products,
            COUNT(DISTINCT CASE WHEN image_url IS NOT NULL AND image_url != '' THEN barcode END) as with_images,
            COUNT(DISTINCT CASE WHEN brand IS NOT NULL AND brand != '' THEN barcode END) as with_brands,
            COUNT(DISTINCT CASE WHEN description IS NOT NULL AND description != '' THEN barcode END) as with_descriptions,
            COUNT(DISTINCT CASE WHEN name IS NOT NULL AND name != '' THEN barcode END) as with_names,
            AVG(LENGTH(name)) as avg_name_length,
            MIN(last_scraped_at) as earliest_scrape,
            MAX(last_scraped_at) as latest_scrape
        FROM canonical_products
        WHERE source_retailer_id = 150;  -- Be Pharm retailer ID
    """

    cursor.execute(query)
    result = cursor.fetchone()

    if result and result['total_products'] > 0:
        print(f"\n📊 COMMERCIAL DATA STATISTICS:")
        print(f"Total products scraped: {result['total_products']:,}")
        print(f"Products with images: {result['with_images']:,} ({result['with_images']*100/result['total_products']:.1f}%)")
        print(f"Products with brands: {result['with_brands']:,} ({result['with_brands']*100/result['total_products']:.1f}%)")
        print(f"Products with descriptions: {result['with_descriptions']:,} ({result['with_descriptions']*100/result['total_products']:.1f}%)")
        print(f"Products with names: {result['with_names']:,} ({result['with_names']*100/result['total_products']:.1f}%)")
        print(f"Average name length: {result['avg_name_length']:.1f} characters")
        print(f"Date range: {result['earliest_scrape']} to {result['latest_scrape']}")

        # Sample products
        sample_query = """
            SELECT barcode, name, brand, description, image_url
            FROM canonical_products
            WHERE source_retailer_id = 150
            AND image_url IS NOT NULL AND image_url != ''
            LIMIT 5;
        """
        cursor.execute(sample_query)
        samples = cursor.fetchall()

        print("\n📦 SAMPLE COMMERCIAL PRODUCTS (with images):")
        for i, product in enumerate(samples, 1):
            print(f"\n{i}. Barcode: {product['barcode']}")
            print(f"   Name: {product['name'][:50]}...")
            print(f"   Brand: {product['brand'] or 'N/A'}")
            print(f"   Has Image: {'Yes' if product['image_url'] else 'No'}")
            print(f"   Has Description: {'Yes' if product['description'] else 'No'}")

    else:
        print("❌ No commercial data found for Be Pharm (retailer_id=150)")

    return result


def analyze_etl_transparency_data(cursor):
    """Analyze Be Pharm ETL/transparency data quality"""
    print("\n" + "="*80)
    print("BE PHARM ETL/TRANSPARENCY DATA ANALYSIS (from prices)")
    print("="*80)

    # Get ETL data for Be Pharm
    query = """
        WITH be_pharm_data AS (
            SELECT
                rp.barcode,
                rp.original_retailer_name,
                p.price,
                p.price_timestamp,
                p.store_id,
                s.storename
            FROM retailer_products rp
            JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
            JOIN stores s ON p.store_id = s.storeid
            WHERE rp.retailer_id = 150  -- Be Pharm
        )
        SELECT
            COUNT(DISTINCT barcode) as total_products,
            COUNT(*) as total_price_points,
            COUNT(DISTINCT store_id) as total_stores,
            COUNT(DISTINCT DATE(price_timestamp)) as days_with_data,
            AVG(price) as avg_price,
            MIN(price) as min_price,
            MAX(price) as max_price,
            AVG(LENGTH(original_retailer_name)) as avg_name_length,
            MIN(price_timestamp) as earliest_price,
            MAX(price_timestamp) as latest_price
        FROM be_pharm_data;
    """

    cursor.execute(query)
    result = cursor.fetchone()

    if result and result['total_products']:
        print(f"\n📊 ETL/TRANSPARENCY DATA STATISTICS:")
        print(f"Total unique products: {result['total_products']:,}")
        print(f"Total price points: {result['total_price_points']:,}")
        print(f"Total stores: {result['total_stores']:,}")
        print(f"Days with data: {result['days_with_data']:,}")
        print(f"Average price: ₪{result['avg_price']:.2f}")
        print(f"Price range: ₪{result['min_price']:.2f} - ₪{result['max_price']:.2f}")
        print(f"Average product name length: {result['avg_name_length']:.1f} characters")
        print(f"Date range: {result['earliest_price']} to {result['latest_price']}")

        # Check data freshness
        if result['latest_price']:
            days_old = (datetime.now() - result['latest_price'].replace(tzinfo=None)).days
            if days_old > 7:
                print(f"⚠️ WARNING: Latest data is {days_old} days old!")
            else:
                print(f"✅ Data freshness: Latest data is {days_old} days old")

        # Sample products
        sample_query = """
            SELECT DISTINCT
                rp.barcode,
                rp.original_retailer_name,
                COUNT(DISTINCT p.store_id) as store_count,
                AVG(p.price) as avg_price,
                MIN(p.price) as min_price,
                MAX(p.price) as max_price
            FROM retailer_products rp
            JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
            WHERE rp.retailer_id = 150
            GROUP BY rp.barcode, rp.original_retailer_name
            ORDER BY store_count DESC
            LIMIT 5;
        """
        cursor.execute(sample_query)
        samples = cursor.fetchall()

        print("\n📦 SAMPLE ETL PRODUCTS (most stores):")
        for i, product in enumerate(samples, 1):
            print(f"\n{i}. Barcode: {product['barcode']}")
            print(f"   Name: {product['original_retailer_name'][:50]}...")
            print(f"   Available in {product['store_count']} stores")
            print(f"   Price range: ₪{product['min_price']:.2f} - ₪{product['max_price']:.2f}")
            print(f"   Avg price: ₪{product['avg_price']:.2f}")

    else:
        print("❌ No ETL/transparency data found for Be Pharm")

    return result


def compare_data_sources(cursor):
    """Compare commercial vs ETL data"""
    print("\n" + "="*80)
    print("COMMERCIAL VS ETL DATA COMPARISON")
    print("="*80)

    # Get products in both sources
    query = """
        WITH commercial_products AS (
            SELECT DISTINCT barcode
            FROM canonical_products
            WHERE source_retailer_id = 150
        ),
        etl_products AS (
            SELECT DISTINCT barcode
            FROM retailer_products
            WHERE retailer_id = 150
        )
        SELECT
            (SELECT COUNT(*) FROM commercial_products) as commercial_count,
            (SELECT COUNT(*) FROM etl_products) as etl_count,
            COUNT(DISTINCT cp.barcode) as in_both,
            (SELECT COUNT(*) FROM commercial_products WHERE barcode NOT IN (SELECT barcode FROM etl_products)) as commercial_only,
            (SELECT COUNT(*) FROM etl_products WHERE barcode NOT IN (SELECT barcode FROM commercial_products)) as etl_only
        FROM commercial_products cp
        JOIN etl_products ep ON cp.barcode = ep.barcode;
    """

    cursor.execute(query)
    result = cursor.fetchone()

    print("\n📊 PRODUCT OVERLAP ANALYSIS:")
    print(f"Commercial products only: {result['commercial_count']:,}")
    print(f"ETL products only: {result['etl_count']:,}")
    print(f"Products in both: {result['in_both']:,}")
    print(f"Commercial exclusive: {result['commercial_only']:,}")
    print(f"ETL exclusive: {result['etl_only']:,}")

    if result['commercial_count'] > 0:
        overlap_pct = result['in_both'] * 100 / result['commercial_count']
        print(f"\nOverlap rate: {overlap_pct:.1f}% of commercial products also in ETL")

    # Analyze price data availability
    price_query = """
        WITH commercial_with_prices AS (
            SELECT DISTINCT cp.barcode
            FROM canonical_products cp
            JOIN retailer_products rp ON cp.barcode = rp.barcode
            JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
            WHERE cp.source_retailer_id = 150
            AND rp.retailer_id = 150
        )
        SELECT
            COUNT(DISTINCT cp.barcode) as commercial_total,
            COUNT(DISTINCT cwp.barcode) as commercial_with_prices
        FROM canonical_products cp
        LEFT JOIN commercial_with_prices cwp ON cp.barcode = cwp.barcode
        WHERE cp.source_retailer_id = 150;
    """

    cursor.execute(price_query)
    price_result = cursor.fetchone()

    if price_result and price_result['commercial_total'] > 0:
        print("\n💰 PRICE DATA COVERAGE:")
        print(f"Commercial products total: {price_result['commercial_total']:,}")
        print(f"Commercial products with ETL prices: {price_result['commercial_with_prices']:,}")
        print(f"Coverage: {price_result['commercial_with_prices']*100/price_result['commercial_total']:.1f}%")


def generate_recommendation(cursor):
    """Generate recommendation based on analysis"""
    print("\n" + "="*80)
    print("📝 RECOMMENDATION")
    print("="*80)

    # Get key metrics
    metrics_query = """
        SELECT
            (SELECT COUNT(DISTINCT barcode) FROM canonical_products WHERE source_retailer_id = 150) as commercial_products,
            (SELECT COUNT(DISTINCT barcode) FROM canonical_products WHERE source_retailer_id = 150 AND image_url IS NOT NULL) as commercial_with_images,
            (SELECT COUNT(DISTINCT barcode) FROM retailer_products WHERE retailer_id = 150) as etl_products,
            (SELECT COUNT(*) FROM prices p JOIN retailer_products rp ON p.retailer_product_id = rp.retailer_product_id WHERE rp.retailer_id = 150) as etl_price_points,
            (SELECT MAX(price_timestamp) FROM prices p JOIN retailer_products rp ON p.retailer_product_id = rp.retailer_product_id WHERE rp.retailer_id = 150) as latest_etl_price;
    """

    cursor.execute(metrics_query)
    metrics = cursor.fetchone()

    print("\n🔍 KEY FINDINGS:")

    issues = []
    recommendations = []

    # Check commercial data
    if metrics['commercial_products'] > 0:
        print(f"✅ Commercial scraper has {metrics['commercial_products']:,} products")
        if metrics['commercial_with_images'] > 0:
            img_pct = metrics['commercial_with_images'] * 100 / metrics['commercial_products']
            print(f"✅ {img_pct:.1f}% of commercial products have images")
    else:
        print("❌ No commercial data found")
        issues.append("No commercial scraper data")
        recommendations.append("Run commercial scraper to get product images and descriptions")

    # Check ETL data
    if metrics['etl_products'] > 0:
        print(f"📊 ETL has {metrics['etl_products']:,} products with {metrics['etl_price_points']:,} price points")

        # Check freshness
        if metrics['latest_etl_price']:
            days_old = (datetime.now() - metrics['latest_etl_price'].replace(tzinfo=None)).days
            if days_old > 30:
                print(f"❌ ETL data is {days_old} days old (STALE)")
                issues.append(f"ETL data is {days_old} days old")
                recommendations.append("Re-run ETL scraper to get fresh price data")
            else:
                print(f"✅ ETL data is {days_old} days old")

        # Check price points per product
        if metrics['etl_price_points'] and metrics['etl_products']:
            price_per_product = metrics['etl_price_points'] / metrics['etl_products']
            if price_per_product < 10:
                print(f"⚠️ Only {price_per_product:.1f} price points per product (LOW)")
                issues.append("Low price coverage from ETL")
    else:
        print("❌ No ETL data found")
        issues.append("No ETL/transparency data")

    print("\n" + "="*80)
    print("💡 FINAL RECOMMENDATION:")
    print("="*80)

    if len(issues) > 2 or "Low price coverage" in str(issues):
        print("\n🎯 SWITCH TO COMMERCIAL WEBSITE FOR PRICE DATA")
        print("\nReasons:")
        print("  1. ETL/Transparency portal has insufficient price coverage")
        print("  2. Commercial website likely has real-time prices for all products")
        print("  3. Commercial scraper already provides better product data (images, descriptions)")
        print("\nAction Plan:")
        print("  1. Modify commercial scraper to also capture prices")
        print("  2. Use commercial prices as primary source")
        print("  3. Keep ETL as fallback/validation only")
    else:
        print("\n🎯 HYBRID APPROACH RECOMMENDED")
        print("\nStrategy:")
        print("  1. Use commercial scraper for product data (names, images, descriptions)")
        print("  2. Use ETL for price data (if fresh and comprehensive)")
        print("  3. Link both via barcode matching")


def main():
    """Main execution function."""
    print("\n" + "="*80)
    print("BE PHARM DATA SOURCE ANALYSIS")
    print("="*80)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Connect to database
    conn = get_database_connection()
    if not conn:
        return

    cursor = conn.cursor()

    try:
        # Analyze both data sources
        commercial_stats = analyze_commercial_data(cursor)
        etl_stats = analyze_etl_transparency_data(cursor)

        # Compare sources
        compare_data_sources(cursor)

        # Generate recommendation
        generate_recommendation(cursor)

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