"""
Fix Outlier Prices Script
==========================
This script corrects the pricing errors identified in the data quality audit:
- Sets outlier creams/serums to their median prices
- Sets Vitamin D-400 to ₪38.90
- Leaves the syringe unchanged

Changes will be made to the prices table by inserting new price records
with the corrected values and current timestamp.
"""

import psycopg2
from datetime import datetime

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'price_comparison_app_v2',
    'user': 'postgres',
    'password': '***REMOVED***'
}

# Price corrections to make
# Format: (barcode, new_price, median_price, product_name)
PRICE_CORRECTIONS = [
    # Creams and Serums (set to median)
    ('887167327665', 135.00, 135.00, 'DAY WEAR קרם עיניים'),
    ('192333102749', 240.00, 240.00, 'קרם עיניים סמארט קליניק'),
    ('4005808827336', 128.00, 128.00, 'CELLULAR קרם עיניים'),
    ('3600524025540', 96.00, 96.00, 'סרום אנטי-אייג\'ינג לעיניים- רויטליפט פילר'),
    ('3614274170207', 368.10, 368.10, 'טריפל סרום עיניים רנרג\'י 20 מ"ל'),
    ('192333101674', 303.75, 303.75, 'סרום טיפולי לתיקון קמטים'),
    ('3600542541602', 67.00, 67.00, 'SKIN ACTIVE סרום לפנים ויטמין C'),
    ('7290006788269', 100.00, 100.00, 'PERFECT CARE סרום למיצוק העור 30 מ"ל'),
    ('9000100760591', 10.00, 10.00, 'רול און לגבר אינוויזיבל פאוואר'),

    # Vitamin (set to user-specified price)
    ('7290014775510', 38.90, 41.90, 'ויטמין D-400 כמוסות רכות'),
]

def get_retailer_product_info(conn, barcode, retailer_name='Super-Pharm'):
    """Get retailer_product_id and current price for a barcode at specific retailer."""
    query = """
    WITH latest_price AS (
        SELECT DISTINCT ON (rp.retailer_product_id)
            rp.retailer_product_id,
            p.price,
            p.price_timestamp,
            p.store_id
        FROM retailer_products rp
        LEFT JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
        JOIN retailers r ON rp.retailer_id = r.retailerid
        WHERE rp.barcode = %s
            AND r.retailername = %s
        ORDER BY rp.retailer_product_id, p.price_timestamp DESC NULLS LAST
    )
    SELECT
        retailer_product_id,
        price as current_price,
        store_id
    FROM latest_price;
    """

    cursor = conn.cursor()
    cursor.execute(query, (barcode, retailer_name))
    result = cursor.fetchone()
    cursor.close()

    if result:
        return {
            'retailer_product_id': result[0],
            'current_price': float(result[1]) if result[1] else None,
            'store_id': result[2] if result[2] else None
        }
    return None

def insert_new_price(conn, retailer_product_id, new_price, store_id):
    """Insert a new price record with current timestamp."""
    query = """
    INSERT INTO prices (retailer_product_id, store_id, price, price_timestamp, scraped_at)
    VALUES (%s, %s, %s, %s, %s)
    RETURNING price_id;
    """

    now = datetime.now()
    cursor = conn.cursor()

    try:
        cursor.execute(query, (retailer_product_id, store_id, new_price, now, now))
        price_id = cursor.fetchone()[0]
        cursor.close()
        return price_id
    except Exception as e:
        cursor.close()
        raise e

def main():
    """Main execution function."""
    print("=" * 80)
    print("FIXING OUTLIER PRICES")
    print("=" * 80)

    conn = psycopg2.connect(**DB_CONFIG)

    updated_count = 0
    skipped_count = 0

    print(f"\nProcessing {len(PRICE_CORRECTIONS)} price corrections...\n")

    for barcode, new_price, median_price, product_name in PRICE_CORRECTIONS:
        print(f"Processing: {product_name[:60]}")
        print(f"  Barcode: {barcode}")

        # Get retailer product info
        info = get_retailer_product_info(conn, barcode)

        if not info:
            print(f"  ⚠️  WARNING: Product not found at Super-Pharm")
            skipped_count += 1
            print()
            continue

        retailer_product_id = info['retailer_product_id']
        current_price = info['current_price']
        store_id = info['store_id']

        if current_price is None:
            print(f"  ⚠️  WARNING: No current price found")
            skipped_count += 1
            print()
            continue

        print(f"  Current Price: ₪{current_price:.2f}")
        print(f"  New Price: ₪{new_price:.2f}")

        # Insert new price record
        try:
            price_id = insert_new_price(conn, retailer_product_id, new_price, store_id)
            conn.commit()
            print(f"  ✓ Updated (new price_id: {price_id})")
            updated_count += 1
        except Exception as e:
            conn.rollback()
            print(f"  ✗ ERROR: {str(e)}")
            skipped_count += 1

        print()

    conn.close()

    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"\nSuccessfully updated: {updated_count} prices")
    print(f"Skipped: {skipped_count} prices")
    print("\n" + "=" * 80)

if __name__ == "__main__":
    main()
