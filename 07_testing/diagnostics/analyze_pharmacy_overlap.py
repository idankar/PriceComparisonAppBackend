#!/usr/bin/env python3
import os
import psycopg2
from datetime import datetime
from collections import defaultdict

# Database Connection Details
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DATABASE = os.environ.get("PG_DATABASE", "price_comparison_app_v2")
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "***REMOVED***")

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD
    )

def analyze_retailer_overlap():
    """Analyze product overlap between pharmacy retailers."""
    conn = get_db_connection()
    cur = conn.cursor()

    print("=" * 80)
    print("PHARMACY RETAILER PRODUCT OVERLAP ANALYSIS")
    print("=" * 80)
    print(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # First, identify the pharmacy retailers and their stores
    print("1. IDENTIFYING PHARMACY RETAILERS")
    print("-" * 40)

    cur.execute("""
        SELECT r.retailerid, r.retailername,
               COUNT(DISTINCT s.storeid) as store_count
        FROM retailers r
        LEFT JOIN stores s ON r.retailerid = s.retailerid
        WHERE LOWER(r.retailername) LIKE '%pharm%'
           OR r.retailerid IN (
               SELECT DISTINCT retailerid
               FROM stores
               WHERE LOWER(storename) LIKE '%pharm%'
           )
        GROUP BY r.retailerid, r.retailername
        ORDER BY r.retailername
    """)

    retailers = cur.fetchall()
    retailer_map = {}

    for rid, name, store_count in retailers:
        print(f"  • {name} (ID: {rid}): {store_count} stores")
        retailer_map[rid] = name

    if len(retailers) < 3:
        print(f"\n⚠️ Warning: Only found {len(retailers)} pharmacy retailers")

    # Get products by retailer with active products only
    print("\n2. ANALYZING ACTIVE PRODUCTS BY RETAILER")
    print("-" * 40)

    retailer_products = {}
    for rid, name in retailer_map.items():
        cur.execute("""
            SELECT DISTINCT cp.barcode, cp.name, cp.brand
            FROM canonical_products cp
            JOIN retailer_products rp ON cp.barcode = rp.barcode
            JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
            JOIN stores s ON p.store_id = s.storeid
            WHERE s.retailerid = %s
              AND cp.is_active = TRUE
              AND p.price > 0
            ORDER BY cp.name
        """, (rid,))

        products = cur.fetchall()
        retailer_products[name] = {(barcode, prod_name, brand) for barcode, prod_name, brand in products}
        print(f"  • {name}: {len(retailer_products[name]):,} active products with prices")

    # If we don't have exactly 3 retailers, let's work with what we have
    if len(retailer_products) == 0:
        print("\n❌ No pharmacy retailers found with active products.")
        conn.close()
        return

    # Analyze 3-way overlap (products in all retailers) if we have 3+ retailers
    all_three = set()
    if len(retailer_products) >= 3:
        print("\n3. PRODUCTS SHARED ACROSS ALL 3 RETAILERS")
        print("-" * 40)

        retailer_names = list(retailer_products.keys())[:3]  # Take first 3 if more than 3

        # Get intersection of all three
        all_three = retailer_products[retailer_names[0]].copy()
        for name in retailer_names[1:3]:
            all_three = all_three.intersection(retailer_products[name])

        print(f"  Total products in all 3 retailers: {len(all_three):,}")

        if all_three:
            print("\n  Sample products (first 10):")
            for i, (barcode, name, brand) in enumerate(sorted(all_three)[:10], 1):
                brand_str = f" ({brand})" if brand else ""
                print(f"    {i}. {name}{brand_str} - {barcode}")

    # Analyze 2-way overlaps
    print("\n4. PRODUCTS SHARED BETWEEN EXACTLY 2 RETAILERS")
    print("-" * 40)

    if len(retailer_products) >= 2:
        retailer_names = list(retailer_products.keys())
        two_way_overlaps = {}

        for i in range(len(retailer_names)):
            for j in range(i+1, len(retailer_names)):
                name1, name2 = retailer_names[i], retailer_names[j]

                # Products in both these retailers
                both = retailer_products[name1].intersection(retailer_products[name2])

                # Exclude products that are in all three (if applicable)
                if len(retailer_products) >= 3:
                    only_these_two = both - all_three
                else:
                    only_these_two = both

                pair_key = f"{name1} & {name2}"
                two_way_overlaps[pair_key] = only_these_two

                print(f"\n  {pair_key}:")
                print(f"    • Shared products (total): {len(both):,}")
                if len(retailer_products) >= 3:
                    print(f"    • Shared ONLY between these two: {len(only_these_two):,}")

                if only_these_two and len(only_these_two) > 0:
                    print(f"    • Sample (first 5):")
                    for idx, (barcode, prod_name, brand) in enumerate(sorted(only_these_two)[:5], 1):
                        brand_str = f" ({brand})" if brand else ""
                        print(f"      {idx}. {prod_name}{brand_str}")

    # Unique products per retailer
    print("\n5. UNIQUE PRODUCTS PER RETAILER")
    print("-" * 40)

    for name, products in retailer_products.items():
        if not products:  # Skip if no products
            continue

        # Products unique to this retailer
        unique_products = products.copy()
        for other_name, other_products in retailer_products.items():
            if other_name != name:
                unique_products = unique_products - other_products

        print(f"\n  {name}:")
        print(f"    • Unique products: {len(unique_products):,}")
        if len(products) > 0:
            print(f"    • Percentage of their catalog: {len(unique_products)/len(products)*100:.1f}%")

        if unique_products:
            print(f"    • Sample unique products (first 5):")
            for idx, (barcode, prod_name, brand) in enumerate(sorted(unique_products)[:5], 1):
                brand_str = f" ({brand})" if brand else ""
                print(f"      {idx}. {prod_name}{brand_str}")

    # Summary statistics
    print("\n6. SUMMARY STATISTICS")
    print("-" * 40)

    total_unique_products = set()
    for products in retailer_products.values():
        total_unique_products.update(products)

    print(f"  • Total unique active products across all retailers: {len(total_unique_products):,}")

    if len(retailer_products) >= 3 and all_three:
        print(f"  • Products in all 3 retailers: {len(all_three):,} ({len(all_three)/len(total_unique_products)*100:.1f}%)")

        total_in_exactly_two = sum(len(products) for products in two_way_overlaps.values())
        print(f"  • Products in exactly 2 retailers: {total_in_exactly_two:,}")

        # Category analysis for shared products
        print("\n7. CATEGORY ANALYSIS FOR 3-WAY SHARED PRODUCTS")
        print("-" * 40)

        barcodes = [barcode for barcode, _, _ in all_three]
        if barcodes:
            placeholders = ','.join(['%s'] * len(barcodes))

            cur.execute(f"""
                SELECT category, COUNT(DISTINCT barcode) as product_count
                FROM canonical_products
                WHERE barcode IN ({placeholders})
                  AND category IS NOT NULL
                GROUP BY category
                ORDER BY product_count DESC
                LIMIT 15
            """, barcodes)

            categories = cur.fetchall()
            if categories:
                print("\n  Top categories for products in all 3 retailers:")
                for cat, count in categories:
                    print(f"    • {cat}: {count} products")

    # Price comparison for shared products
    if len(retailer_products) >= 2:
        print("\n8. PRICE VARIANCE FOR SHARED PRODUCTS")
        print("-" * 40)

        # Find products that exist in at least 2 retailers
        products_in_multiple = set()
        for name1, prods1 in retailer_products.items():
            for name2, prods2 in retailer_products.items():
                if name1 != name2:
                    products_in_multiple.update(prods1.intersection(prods2))

        if products_in_multiple:
            sample_products = list(products_in_multiple)[:5]
            print("\n  Price comparison for sample shared products:")

            for barcode, prod_name, brand in sample_products:
                cur.execute("""
                    SELECT
                        r.retailername as retailer,
                        MIN(p.price) as min_price,
                        MAX(p.price) as max_price,
                        AVG(p.price) as avg_price
                    FROM prices p
                    JOIN retailer_products rp ON p.retailer_product_id = rp.retailer_product_id
                    JOIN stores s ON p.store_id = s.storeid
                    JOIN retailers r ON s.retailerid = r.retailerid
                    WHERE rp.barcode = %s
                      AND p.price > 0
                      AND r.retailerid IN %s
                    GROUP BY r.retailername
                    ORDER BY avg_price
                """, (barcode, tuple(retailer_map.keys())))

                prices = cur.fetchall()
                if prices and len(prices) > 1:  # Only show if product has prices from multiple retailers
                    brand_str = f" ({brand})" if brand else ""
                    print(f"\n    {prod_name}{brand_str}:")
                    min_avg = min(p[3] for p in prices)
                    max_avg = max(p[3] for p in prices)
                    price_diff = ((max_avg - min_avg) / min_avg * 100) if min_avg > 0 else 0

                    for retailer, min_p, max_p, avg_p in prices:
                        diff_from_min = ((avg_p - min_avg) / min_avg * 100) if min_avg > 0 else 0
                        print(f"      • {retailer}: ₪{avg_p:.2f} (range: ₪{min_p:.2f}-₪{max_p:.2f})")
                        if diff_from_min > 0:
                            print(f"        +{diff_from_min:.1f}% from cheapest")

                    if price_diff > 0:
                        print(f"      → Price difference: {price_diff:.1f}%")

    conn.close()
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    analyze_retailer_overlap()