#!/usr/bin/env python3
"""
Final comprehensive check for missing product images
"""
import psycopg2
from psycopg2.extras import RealDictCursor

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'price_comparison_app_v2',
    'user': 'postgres',
    'password': '***REMOVED***'
}

SEARCH_TERMS = ['אוניון', 'אלמיג', 'דטול']

def final_check():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        print("=" * 80)
        print("FINAL IMAGE VERIFICATION REPORT")
        print("=" * 80)

        for term in SEARCH_TERMS:
            print(f"\n\n{'='*80}")
            print(f"Searching for '{term}'")
            print(f"{'='*80}")

            # Check in retailer_products
            cur.execute("""
                SELECT DISTINCT
                    rp.original_retailer_name,
                    rp.barcode,
                    COUNT(DISTINCT p.store_id) as store_count
                FROM retailer_products rp
                LEFT JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
                WHERE rp.original_retailer_name ILIKE %s
                GROUP BY rp.original_retailer_name, rp.barcode
                ORDER BY rp.original_retailer_name
                LIMIT 10;
            """, (f'%{term}%',))

            retailer_results = cur.fetchall()

            if retailer_results:
                print(f"\n✓ Found {len(retailer_results)} product(s) in RETAILER_PRODUCTS:")
                for i, row in enumerate(retailer_results, 1):
                    print(f"\n  {i}. {row['original_retailer_name']}")
                    print(f"     Barcode: {row['barcode']}")
                    print(f"     Stores: {row['store_count']}")

                    # Check if in canonical
                    if row['barcode']:
                        cur.execute("""
                            SELECT name, image_url
                            FROM canonical_products
                            WHERE barcode = %s;
                        """, (row['barcode'],))
                        canonical = cur.fetchone()

                        if canonical:
                            print(f"     ✓ IN CANONICAL_PRODUCTS as: {canonical['name']}")
                            if canonical['image_url']:
                                print(f"     ✓ Image URL EXISTS: {canonical['image_url'][:50]}...")
                            else:
                                print(f"     ❌ Image URL MISSING")
                        else:
                            print(f"     ❌ NOT in canonical_products (orphaned)")
            else:
                print(f"\n❌ No products found in retailer_products containing '{term}'")

        # Summary statistics
        print(f"\n\n{'='*80}")
        print("SUMMARY STATISTICS")
        print(f"{'='*80}")

        cur.execute("""
            SELECT
                COUNT(*) as total_canonical_products,
                COUNT(image_url) as products_with_images,
                COUNT(*) - COUNT(image_url) as products_without_images,
                ROUND(100.0 * COUNT(image_url) / NULLIF(COUNT(*), 0), 2) as image_coverage_percent
            FROM canonical_products;
        """)

        stats = cur.fetchone()
        print(f"\nTotal canonical products: {stats['total_canonical_products']}")
        print(f"Products with images: {stats['products_with_images']}")
        print(f"Products WITHOUT images: {stats['products_without_images']}")
        print(f"Image coverage: {stats['image_coverage_percent']}%")

        cur.close()
        conn.close()

        print("\n" + "=" * 80)

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    final_check()
