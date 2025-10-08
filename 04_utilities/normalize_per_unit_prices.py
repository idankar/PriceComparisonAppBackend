"""
Normalize Per-Unit Prices to Pack Prices
=========================================
This script identifies Super-Pharm products with per-unit pricing and normalizes
them to pack prices by:
1. Extracting pack quantities from product names
2. Calculating pack price = per-unit price × pack quantity
3. Updating prices in the database

The script runs in dry-run mode by default to preview changes before applying them.
"""

import psycopg2
import pandas as pd
import re
from datetime import datetime
from typing import Optional, Tuple

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'price_comparison_app_v2',
    'user': 'postgres',
    'password': '025655358'
}

# Pack size extraction patterns
PACK_SIZE_PATTERNS = [
    # Hebrew patterns
    (r'מארז (\d+)', 'pack'),  # מארז 3
    (r'(\d+)\s*יח(?:ידות)?', 'units'),  # 150 יחידות, 100 יח
    (r'(\d+)\s*קפס(?:ולות)?', 'capsules'),  # 150 קפסולות, 100 קפס
    (r'(\d+)\s*טבליות', 'tablets'),  # 60 טבליות
    (r'(\d+)\s*כמוסות', 'capsules'),  # 100 כמוסות
    (r'(\d+)\s*מגבונים', 'wipes'),  # 50 מגבונים
    (r'(\d+)\s*חיתולים', 'diapers'),  # 40 חיתולים
    (r'(\d+)\s*מטר', 'meters'),  # 50 מטר
    (r'שלישייה', 'pack'),  # special case = 3
    (r'זוג', 'pack'),  # special case = 2

    # English patterns
    (r'pack of (\d+)', 'pack'),
    (r'(\d+)\s*count', 'units'),
    (r'(\d+)\s*caps', 'capsules'),
    (r'(\d+)\s*tablets', 'tablets'),
    (r'(\d+)m\b', 'meters'),  # 50m
]

# Special multipliers
SPECIAL_MULTIPLIERS = {
    'שלישייה': 3,
    'זוג': 2,
    'triple': 3,
    'double': 2,
}

def extract_pack_size(text: str) -> Optional[int]:
    """
    Extract pack size/quantity from product name.
    Returns the quantity as an integer, or None if not found.
    """
    if not text:
        return None

    # Check for special multipliers first
    text_lower = text.lower()
    for word, multiplier in SPECIAL_MULTIPLIERS.items():
        if word in text_lower:
            return multiplier

    # Try each pattern
    for pattern, unit_type in PACK_SIZE_PATTERNS:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                quantity = int(match.group(1))
                # Sanity check: pack sizes should be reasonable
                if 1 <= quantity <= 1000:
                    return quantity
            except (ValueError, IndexError):
                continue

    return None

def has_informational_pricing(text: str) -> bool:
    """Check if text contains informational per-unit pricing indicators."""
    if not text:
        return False

    patterns = [
        r'\(₪[\d,.]+ ל-',
        r'\(₪[\d,.]+ ליחידה',
        r'\(₪[\d,.]+ לקפסולה',
        r'ל-1 מ"ר',
        r'ל-1 מטר',
        r'למטר',
        r"ליח'",
        r'ליח"',
        r'ליח`',
    ]

    for pattern in patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return True
    return False

def get_per_unit_products():
    """
    Fetch all Super-Pharm products with per-unit pricing.
    """
    query = """
    WITH latest_prices AS (
        SELECT DISTINCT ON (rp.retailer_product_id)
            rp.retailer_product_id,
            rp.barcode,
            rp.original_retailer_name,
            cp.name as canonical_name,
            p.price,
            p.store_id,
            p.price_timestamp
        FROM prices p
        JOIN retailer_products rp ON p.retailer_product_id = rp.retailer_product_id
        JOIN canonical_products cp ON rp.barcode = cp.barcode
        JOIN retailers r ON rp.retailer_id = r.retailerid
        WHERE r.retailername = 'Super-Pharm'
        ORDER BY rp.retailer_product_id, p.price_timestamp DESC
    )
    SELECT
        retailer_product_id,
        barcode,
        original_retailer_name,
        canonical_name,
        price,
        store_id
    FROM latest_prices;
    """

    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql_query(query, conn)
    conn.close()

    # Filter to products with informational pricing
    df['has_info_pricing'] = df.apply(
        lambda row: has_informational_pricing(row['canonical_name']) or
                   has_informational_pricing(row['original_retailer_name']),
        axis=1
    )

    per_unit_products = df[df['has_info_pricing']].copy()

    return per_unit_products

def calculate_normalized_prices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate normalized pack prices for per-unit products.
    """
    results = []

    for _, row in df.iterrows():
        # Try to extract pack size from both names
        pack_size = extract_pack_size(row['original_retailer_name'])
        if not pack_size:
            pack_size = extract_pack_size(row['canonical_name'])

        if pack_size:
            normalized_price = row['price'] * pack_size

            results.append({
                'retailer_product_id': row['retailer_product_id'],
                'barcode': row['barcode'],
                'product_name': row['canonical_name'][:60],
                'original_retailer_name': row['original_retailer_name'][:60],
                'current_price': row['price'],
                'pack_size': pack_size,
                'normalized_price': normalized_price,
                'store_id': row['store_id'],
                'can_normalize': True
            })
        else:
            results.append({
                'retailer_product_id': row['retailer_product_id'],
                'barcode': row['barcode'],
                'product_name': row['canonical_name'][:60],
                'original_retailer_name': row['original_retailer_name'][:60],
                'current_price': row['price'],
                'pack_size': None,
                'normalized_price': None,
                'store_id': row['store_id'],
                'can_normalize': False
            })

    return pd.DataFrame(results)

def insert_normalized_price(conn, retailer_product_id, new_price, store_id):
    """Insert a new price record with normalized pack price."""
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

def main(dry_run=True):
    """
    Main execution function.
    """
    print("=" * 80)
    print("NORMALIZE PER-UNIT PRICES TO PACK PRICES")
    print("=" * 80)

    if dry_run:
        print("\n⚠️  DRY RUN MODE - No changes will be made")
    else:
        print("\n🔴 LIVE MODE - Changes will be applied to database")

    print("\n[1/3] Fetching Super-Pharm products with per-unit pricing...")
    per_unit_products = get_per_unit_products()
    print(f"      ✓ Found {len(per_unit_products):,} products with informational pricing")

    print("\n[2/3] Calculating normalized pack prices...")
    results = calculate_normalized_prices(per_unit_products)

    can_normalize = results[results['can_normalize'] == True]
    cannot_normalize = results[results['can_normalize'] == False]

    print(f"      ✓ Can normalize: {len(can_normalize):,} products")
    print(f"      ✓ Cannot extract pack size: {len(cannot_normalize):,} products")

    # Show sample of normalizable products
    print("\n" + "=" * 80)
    print("SAMPLE: PRODUCTS THAT WILL BE NORMALIZED")
    print("=" * 80)
    print(f"\nShowing first 20 of {len(can_normalize):,} products:\n")

    for _, row in can_normalize.head(20).iterrows():
        print(f"• {row['product_name']}")
        print(f"  Current: ₪{row['current_price']:.2f} × {row['pack_size']} = ₪{row['normalized_price']:.2f}")
        print()

    # Show sample of products that cannot be normalized
    if len(cannot_normalize) > 0:
        print("\n" + "=" * 80)
        print("SAMPLE: PRODUCTS WITHOUT DETECTABLE PACK SIZE")
        print("=" * 80)
        print(f"\nShowing first 10 of {len(cannot_normalize):,} products:\n")

        for _, row in cannot_normalize.head(10).iterrows():
            print(f"• {row['product_name']}")
            print(f"  Name: {row['original_retailer_name']}")
            print(f"  Current: ₪{row['current_price']:.2f}")
            print()

    # Statistics
    print("\n" + "=" * 80)
    print("STATISTICS")
    print("=" * 80)

    if len(can_normalize) > 0:
        print(f"\nPrice changes:")
        print(f"  Minimum increase: ₪{can_normalize['current_price'].min():.2f} → ₪{can_normalize['normalized_price'].min():.2f}")
        print(f"  Maximum increase: ₪{can_normalize['current_price'].max():.2f} → ₪{can_normalize['normalized_price'].max():.2f}")
        print(f"  Average pack size: {can_normalize['pack_size'].mean():.1f} units")

        # Pack size distribution
        print(f"\nMost common pack sizes:")
        pack_dist = can_normalize['pack_size'].value_counts().head(10)
        for size, count in pack_dist.items():
            print(f"  {size} units: {count:,} products")

    # Apply changes if not dry run
    if not dry_run:
        print("\n[3/3] Applying normalization to database...")

        conn = psycopg2.connect(**DB_CONFIG)
        updated_count = 0
        failed_count = 0

        for _, row in can_normalize.iterrows():
            try:
                price_id = insert_normalized_price(
                    conn,
                    row['retailer_product_id'],
                    row['normalized_price'],
                    row['store_id']
                )
                conn.commit()
                updated_count += 1

                if updated_count % 100 == 0:
                    print(f"      Progress: {updated_count:,} / {len(can_normalize):,}")
            except Exception as e:
                conn.rollback()
                failed_count += 1
                print(f"      ✗ Failed to update {row['barcode']}: {str(e)}")

        conn.close()

        print(f"\n      ✓ Successfully normalized: {updated_count:,} prices")
        if failed_count > 0:
            print(f"      ✗ Failed: {failed_count:,} prices")
    else:
        print("\n[3/3] Skipping database updates (dry run mode)")
        print(f"\n      To apply these changes, run with: dry_run=False")

    print("\n" + "=" * 80)

    # Save results to CSV
    output_file = 'per_unit_normalization_preview.csv'
    can_normalize.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\nDetailed results saved to: {output_file}")
    print("=" * 80)

if __name__ == "__main__":
    # Run in dry-run mode by default
    import sys
    dry_run = True if len(sys.argv) == 1 else sys.argv[1] != '--apply'
    main(dry_run=dry_run)
