#!/usr/bin/env python3
import os
import psycopg2
from datetime import datetime, timedelta

# Database Connection Details
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DATABASE = os.environ.get("PG_DATABASE", "price_comparison_app_v2")
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "025655358")

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD
    )

def diagnose_pharmacy_data():
    """Diagnose potential issues with pharmacy data."""
    conn = get_db_connection()
    cur = conn.cursor()

    print("="*80)
    print("PHARMACY DATA DIAGNOSTIC REPORT")
    print("="*80)
    print(f"Report Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # 1. Check retailer and store setup
    print("1. RETAILER AND STORE CONFIGURATION")
    print("-"*40)

    cur.execute("""
        SELECT
            r.retailerid,
            r.retailername,
            COUNT(DISTINCT s.storeid) as store_count,
            COUNT(DISTINCT rp.retailer_product_id) as product_count,
            MIN(s.lastupdatedfromstoresfile) as oldest_store_update,
            MAX(s.lastupdatedfromstoresfile) as newest_store_update
        FROM retailers r
        LEFT JOIN stores s ON r.retailerid = s.retailerid
        LEFT JOIN retailer_products rp ON r.retailerid = rp.retailer_id
        WHERE LOWER(r.retailername) LIKE '%pharm%'
        GROUP BY r.retailerid, r.retailername
        ORDER BY r.retailername
    """)

    retailers = cur.fetchall()
    for rid, name, stores, products, oldest, newest in retailers:
        print(f"\n  {name} (ID: {rid}):")
        print(f"    • Stores: {stores}")
        print(f"    • Products in retailer_products: {products}")
        print(f"    • Store updates: {oldest} to {newest}")

    # 2. Check data freshness
    print("\n2. DATA FRESHNESS CHECK")
    print("-"*40)

    for rid, name, _, _, _, _ in retailers:
        cur.execute("""
            SELECT
                COUNT(*) as total_prices,
                COUNT(CASE WHEN p.scraped_at > %s THEN 1 END) as recent_prices,
                MIN(p.scraped_at) as oldest_price,
                MAX(p.scraped_at) as newest_price
            FROM prices p
            JOIN retailer_products rp ON p.retailer_product_id = rp.retailer_product_id
            WHERE rp.retailer_id = %s
        """, (datetime.now() - timedelta(days=7), rid))

        total, recent, oldest, newest = cur.fetchone()
        print(f"\n  {name}:")
        print(f"    • Total price records: {total:,}")
        print(f"    • Prices updated in last 7 days: {recent:,}")
        print(f"    • Date range: {oldest} to {newest}")

    # 3. Check barcode quality
    print("\n3. BARCODE QUALITY CHECK")
    print("-"*40)

    for rid, name, _, _, _, _ in retailers:
        cur.execute("""
            SELECT
                COUNT(*) as total_products,
                COUNT(CASE WHEN barcode IS NULL OR barcode = '' THEN 1 END) as no_barcode,
                COUNT(CASE WHEN barcode ~ '^[0-9]+$' THEN 1 END) as valid_numeric,
                COUNT(CASE WHEN LENGTH(barcode) IN (8, 12, 13, 14) THEN 1 END) as standard_length,
                COUNT(DISTINCT barcode) as unique_barcodes
            FROM retailer_products
            WHERE retailer_id = %s
        """, (rid,))

        total, no_barcode, valid_numeric, standard_len, unique = cur.fetchone()
        print(f"\n  {name}:")
        print(f"    • Total products: {total:,}")
        print(f"    • Missing barcode: {no_barcode:,}")
        print(f"    • Valid numeric barcodes: {valid_numeric:,}")
        print(f"    • Standard length (8,12,13,14): {standard_len:,}")
        print(f"    • Unique barcodes: {unique:,}")

        # Sample invalid barcodes
        cur.execute("""
            SELECT barcode, original_retailer_name
            FROM retailer_products
            WHERE retailer_id = %s
              AND (barcode !~ '^[0-9]+$' OR LENGTH(barcode) NOT IN (8, 12, 13, 14))
              AND barcode IS NOT NULL
              AND barcode != ''
            LIMIT 5
        """, (rid,))

        invalid = cur.fetchall()
        if invalid:
            print(f"    • Sample problematic barcodes:")
            for barcode, name in invalid:
                print(f"      - '{barcode}' ({len(barcode)} chars) - {name[:50]}")

    # 4. Check price-store relationships
    print("\n4. PRICE-STORE RELATIONSHIP CHECK")
    print("-"*40)

    for rid, name, _, _, _, _ in retailers:
        cur.execute("""
            SELECT
                COUNT(DISTINCT p.store_id) as stores_with_prices,
                COUNT(DISTINCT s.storeid) as total_stores
            FROM stores s
            LEFT JOIN prices p ON s.storeid = p.store_id
            WHERE s.retailerid = %s
        """, (rid,))

        stores_with_prices, total_stores = cur.fetchone()
        print(f"\n  {name}:")
        print(f"    • Total stores: {total_stores}")
        print(f"    • Stores with prices: {stores_with_prices}")
        print(f"    • Coverage: {stores_with_prices/total_stores*100:.1f}%" if total_stores > 0 else "N/A")

    # 5. Check canonical product matching
    print("\n5. CANONICAL PRODUCT MATCHING")
    print("-"*40)

    for rid, name, _, _, _, _ in retailers:
        cur.execute("""
            SELECT
                COUNT(DISTINCT rp.barcode) as retailer_barcodes,
                COUNT(DISTINCT cp.barcode) as matched_canonical
            FROM retailer_products rp
            LEFT JOIN canonical_products cp ON rp.barcode = cp.barcode
            WHERE rp.retailer_id = %s
              AND rp.barcode IS NOT NULL
              AND rp.barcode != ''
        """, (rid,))

        retailer_barcodes, matched = cur.fetchone()
        print(f"\n  {name}:")
        print(f"    • Retailer barcodes: {retailer_barcodes:,}")
        print(f"    • Matched to canonical: {matched:,}")
        print(f"    • Match rate: {matched/retailer_barcodes*100:.1f}%" if retailer_barcodes > 0 else "N/A")

    # 6. Check for duplicate barcodes across retailers
    print("\n6. BARCODE OVERLAP ANALYSIS")
    print("-"*40)

    cur.execute("""
        WITH BarcodeCounts AS (
            SELECT
                rp.barcode,
                COUNT(DISTINCT rp.retailer_id) as retailer_count,
                STRING_AGG(DISTINCT r.retailername, ', ' ORDER BY r.retailername) as retailers
            FROM retailer_products rp
            JOIN retailers r ON rp.retailer_id = r.retailerid
            WHERE LOWER(r.retailername) LIKE '%pharm%'
              AND rp.barcode IS NOT NULL
              AND rp.barcode != ''
            GROUP BY rp.barcode
        )
        SELECT
            retailer_count,
            COUNT(*) as barcode_count
        FROM BarcodeCounts
        GROUP BY retailer_count
        ORDER BY retailer_count
    """)

    overlap_stats = cur.fetchall()
    print("\n  Barcodes by retailer count:")
    for count, barcodes in overlap_stats:
        print(f"    • In {count} retailer(s): {barcodes:,} barcodes")

    # Sample shared barcodes
    cur.execute("""
        WITH SharedBarcodes AS (
            SELECT
                rp.barcode,
                COUNT(DISTINCT rp.retailer_id) as retailer_count,
                STRING_AGG(DISTINCT r.retailername, ' | ' ORDER BY r.retailername) as retailers,
                STRING_AGG(DISTINCT rp.original_retailer_name, ' | ' ORDER BY rp.original_retailer_name) as product_names
            FROM retailer_products rp
            JOIN retailers r ON rp.retailer_id = r.retailerid
            WHERE LOWER(r.retailername) LIKE '%pharm%'
              AND rp.barcode IS NOT NULL
              AND rp.barcode != ''
            GROUP BY rp.barcode
            HAVING COUNT(DISTINCT rp.retailer_id) = 3
        )
        SELECT barcode, product_names
        FROM SharedBarcodes
        LIMIT 5
    """)

    shared_samples = cur.fetchall()
    if shared_samples:
        print("\n  Sample barcodes in all 3 retailers:")
        for barcode, names in shared_samples:
            print(f"    • {barcode}: {names[:100]}")

    # 7. Check for data anomalies
    print("\n7. DATA ANOMALY CHECK")
    print("-"*40)

    # Check for products with prices but no canonical match
    cur.execute("""
        SELECT
            r.retailername,
            COUNT(DISTINCT rp.retailer_product_id) as orphan_products
        FROM retailer_products rp
        JOIN retailers r ON rp.retailer_id = r.retailerid
        LEFT JOIN canonical_products cp ON rp.barcode = cp.barcode
        WHERE LOWER(r.retailername) LIKE '%pharm%'
          AND cp.barcode IS NULL
          AND EXISTS (
              SELECT 1 FROM prices p
              WHERE p.retailer_product_id = rp.retailer_product_id
          )
        GROUP BY r.retailername
    """)

    anomalies = cur.fetchall()
    print("\n  Products with prices but no canonical match:")
    for name, count in anomalies:
        print(f"    • {name}: {count:,} products")

    conn.close()

    print("\n" + "="*80)
    print("DIAGNOSTIC COMPLETE")
    print("="*80)

if __name__ == "__main__":
    diagnose_pharmacy_data()