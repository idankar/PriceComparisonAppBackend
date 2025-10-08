#!/usr/bin/env python3
"""
Script to check if products have image URLs in the database
"""
import psycopg2
from psycopg2.extras import RealDictCursor

# Database connection details
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'price_comparison_app_v2',
    'user': 'postgres',
    'password': '025655358'
}

# Products to check (from screenshots)
PRODUCTS_TO_CHECK = [
    'אוניון סבון לידיים ויטמין C',
    'דטול - סבון ידיים בניחוח אורן',
    'אלמיג סבון ניקוי חימר'
]

def check_product_images():
    """Query database for product image URLs and related info"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        query = """
        SELECT
            name,
            brand,
            barcode,
            image_url,
            (SELECT COUNT(DISTINCT store_id)
             FROM prices p
             JOIN retailer_products rp ON p.retailer_product_id = rp.retailer_product_id
             WHERE rp.barcode = cp.barcode) AS store_count,
            (SELECT MIN(price)
             FROM prices p
             JOIN retailer_products rp ON p.retailer_product_id = rp.retailer_product_id
             WHERE rp.barcode = cp.barcode) AS lowest_price
        FROM
            canonical_products cp
        WHERE
            name ILIKE %s;
        """

        print("=" * 80)
        print("PRODUCT IMAGE VERIFICATION REPORT")
        print("=" * 80)
        print()

        for product_search in PRODUCTS_TO_CHECK:
            print(f"\n--- Checking: {product_search} ---")
            cur.execute(query, (f'%{product_search}%',))
            results = cur.fetchall()

            if not results:
                print(f"❌ NOT FOUND in canonical_products table")
            else:
                for row in results:
                    print(f"\n✓ Found in database:")
                    print(f"  Name: {row['name']}")
                    print(f"  Brand: {row['brand']}")
                    print(f"  Barcode: {row['barcode']}")
                    print(f"  Image URL: {row['image_url'] or '❌ NULL/MISSING'}")
                    print(f"  Store Count: {row['store_count']}")
                    print(f"  Lowest Price: ₪{row['lowest_price']:.2f}" if row['lowest_price'] else "  Lowest Price: N/A")

                    # Diagnosis
                    if row['image_url'] is None or row['image_url'] == '':
                        print("\n  🔍 DIAGNOSIS: BACKEND DATA PROBLEM")
                        print("     → Image URL is missing from database")
                        print("     → Scrapers did not capture image for this product")
                    else:
                        print("\n  🔍 DIAGNOSIS: POSSIBLE FRONTEND PROBLEM")
                        print("     → Image URL exists in database")
                        print("     → Frontend may be failing to render the image")

            print("\n" + "-" * 80)

        cur.close()
        conn.close()

        print("\n" + "=" * 80)
        print("END OF REPORT")
        print("=" * 80)

    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_product_images()
