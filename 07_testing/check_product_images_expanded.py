#!/usr/bin/env python3
"""
Expanded search for products with missing images
"""
import psycopg2
from psycopg2.extras import RealDictCursor

# Database connection details
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'price_comparison_app_v2',
    'user': 'postgres',
    'password': '***REMOVED***'
}

# Broader search terms
SEARCH_TERMS = ['אוניון', 'אלמיג', 'דטול']

def expanded_search():
    """Search for products more broadly"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        print("=" * 80)
        print("EXPANDED PRODUCT SEARCH")
        print("=" * 80)

        # Search in canonical_products
        for term in SEARCH_TERMS:
            print(f"\n\n{'='*80}")
            print(f"Searching for '{term}' in CANONICAL_PRODUCTS")
            print(f"{'='*80}")

            query = """
            SELECT
                name,
                brand,
                barcode,
                image_url,
                (SELECT COUNT(DISTINCT store_id)
                 FROM prices p
                 JOIN retailer_products rp ON p.retailer_product_id = rp.retailer_product_id
                 WHERE rp.barcode = cp.barcode) AS store_count
            FROM canonical_products cp
            WHERE name ILIKE %s
            ORDER BY name
            LIMIT 10;
            """

            cur.execute(query, (f'%{term}%',))
            results = cur.fetchall()

            if not results:
                print(f"❌ No products found in canonical_products containing '{term}'")
            else:
                for i, row in enumerate(results, 1):
                    print(f"\n{i}. {row['name']}")
                    print(f"   Brand: {row['brand']}")
                    print(f"   Barcode: {row['barcode']}")
                    print(f"   Image URL: {'✓ EXISTS' if row['image_url'] else '❌ MISSING'}")
                    print(f"   Stores: {row['store_count']}")

        # Also check retailer_products for products that might not be in canonical yet
        print(f"\n\n{'='*80}")
        print("Checking RETAILER_PRODUCTS for non-canonical products")
        print(f"{'='*80}")

        for term in SEARCH_TERMS:
            print(f"\n--- Products containing '{term}' in retailer_products ---")

            query = """
            SELECT
                rp.name,
                rp.barcode,
                rp.image_url,
                s.name as store_name,
                COUNT(*) OVER (PARTITION BY rp.barcode) as product_count
            FROM retailer_products rp
            JOIN stores s ON rp.storeid = s.storeid
            WHERE rp.name ILIKE %s
                AND rp.barcode NOT IN (SELECT barcode FROM canonical_products WHERE barcode IS NOT NULL)
            ORDER BY rp.barcode, s.name
            LIMIT 10;
            """

            cur.execute(query, (f'%{term}%',))
            results = cur.fetchall()

            if not results:
                print(f"   No orphaned products found (all are in canonical_products)")
            else:
                for row in results:
                    print(f"\n   • {row['name']}")
                    print(f"     Store: {row['store_name']}")
                    print(f"     Barcode: {row['barcode']}")
                    print(f"     Image URL: {'✓ EXISTS' if row['image_url'] else '❌ MISSING'}")
                    print(f"     (Found in {row['product_count']} store(s))")

        cur.close()
        conn.close()

        print("\n" + "=" * 80)
        print("END OF EXPANDED SEARCH")
        print("=" * 80)

    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    expanded_search()
