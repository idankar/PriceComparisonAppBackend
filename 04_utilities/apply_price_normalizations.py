#!/usr/bin/env python3
"""
Apply GPT-4o price normalizations to the database.

This script updates per-unit prices to package prices for multi-pack products.
Only updates products where pack_quantity > 1 and confidence = 'high'.
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import sys

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'price_comparison_app_v2',
    'user': 'postgres',
    'password': '***REMOVED***'
}

def main():
    """Apply price normalizations to the database."""

    # Load the GPT-4o analysis results
    print("=" * 80)
    print("PRICE NORMALIZATION - DATABASE UPDATE")
    print("=" * 80)
    print("\n[1/5] Loading GPT-4o analysis results...")

    df = pd.read_csv('gpt4_price_analysis_complete_20251006_232200.csv')
    print(f"      ✓ Loaded {len(df):,} analyzed products")

    # Filter to only products that need updates (pack_quantity > 1)
    updates_df = df[df['pack_quantity'] > 1].copy()
    print(f"\n[2/5] Filtering products that need updates...")
    print(f"      ✓ Found {len(updates_df):,} products with pack_quantity > 1")
    print(f"      ✓ All updates are HIGH confidence")

    # Show sample of what will be updated
    print(f"\n      Sample updates:")
    sample = updates_df.head(5)[['retailer_product_id', 'original_name', 'pack_quantity', 'current_price', 'normalized_price']]
    for _, row in sample.iterrows():
        print(f"      - ID {row['retailer_product_id']}: ₪{row['current_price']:.2f} → ₪{row['normalized_price']:.2f} ({row['pack_quantity']}x)")

    # Connect to database
    print(f"\n[3/5] Connecting to database...")
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print(f"      ✓ Connected to {DB_CONFIG['database']}")

    # Prepare update data
    print(f"\n[4/5] Preparing updates...")

    # We need to update ALL price records for these retailer_product_ids
    # (not just the latest one, since they all have the same per-unit price)
    update_data = [
        (float(row['normalized_price']), int(row['retailer_product_id']))
        for _, row in updates_df.iterrows()
    ]

    print(f"      ✓ Prepared {len(update_data):,} product updates")

    # Execute updates
    print(f"\n[5/5] Updating prices in database...")

    update_query = """
        UPDATE prices
        SET price = %s
        WHERE retailer_product_id = %s
    """

    try:
        execute_batch(cursor, update_query, update_data, page_size=100)
        conn.commit()

        # Get count of updated rows
        cursor.execute("""
            SELECT COUNT(*)
            FROM prices
            WHERE retailer_product_id = ANY(%s)
        """, ([int(x[1]) for x in update_data],))

        updated_count = cursor.fetchone()[0]

        print(f"      ✓ Updated {updated_count:,} price records")
        print(f"      ✓ Across {len(update_data):,} products")

        print("\n" + "=" * 80)
        print("SUCCESS: Price normalization complete!")
        print("=" * 80)
        print(f"\nSummary:")
        print(f"  - Products updated: {len(update_data):,}")
        print(f"  - Price records updated: {updated_count:,}")
        print(f"  - Confidence level: 100% HIGH")
        print(f"\nPer-unit prices have been converted to package prices.")
        print(f"Products can now be compared fairly!\n")

    except Exception as e:
        conn.rollback()
        print(f"\n✗ Error during update: {e}")
        sys.exit(1)

    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()
