#!/usr/bin/env python3
"""
Test script to debug batch insert issues
"""

import psycopg2
from psycopg2.extras import execute_values

# Database connection
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="price_comparison_app_v2",
    user="postgres",
    password="025655358"
)
cursor = conn.cursor()

try:
    # Test data
    canonical_data = [
        ('111111111111', 'Product A', 'Brand A', 97),
        ('222222222222', 'Product B', 'Brand B', 97)
    ]

    retailer_data = [
        ('111111111111', 97, 'ITEM_A', 'Product A Name'),
        ('222222222222', 97, 'ITEM_B', 'Product B Name')
    ]

    print("Testing canonical_products batch insert...")
    execute_values(
        cursor,
        """
        INSERT INTO canonical_products (barcode, name, brand, source_retailer_id, last_scraped_at)
        VALUES %s
        ON CONFLICT (barcode) DO UPDATE SET
            name = COALESCE(canonical_products.name, EXCLUDED.name),
            brand = COALESCE(canonical_products.brand, EXCLUDED.brand),
            last_scraped_at = NOW()
        """,
        canonical_data,
        template="(%s, %s, %s, %s, NOW())"
    )
    print("✓ Canonical products batch insert successful")

    print("Testing retailer_products batch insert...")
    execute_values(
        cursor,
        """
        INSERT INTO retailer_products (barcode, retailer_id, retailer_item_code, original_retailer_name)
        VALUES %s
        ON CONFLICT (retailer_id, retailer_item_code)
        DO UPDATE SET
            barcode = EXCLUDED.barcode,
            original_retailer_name = EXCLUDED.original_retailer_name
        """,
        retailer_data,
        template="(%s, %s, %s, %s)"
    )
    print("✓ Retailer products batch insert successful")

    conn.commit()
    print("✓ All operations committed successfully")

except Exception as e:
    print(f"✗ Error: {e}")
    conn.rollback()
finally:
    conn.close()