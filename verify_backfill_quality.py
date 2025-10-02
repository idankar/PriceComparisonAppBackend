#!/usr/bin/env python3
"""
Quick script to verify the quality of images found by the backfill script.
Checks a sample of products that were processed.
"""

import psycopg2
import re

DB_CONFIG = {
    'dbname': 'price_comparison_app_v2',
    'user': 'postgres',
    'password': '***REMOVED***',
    'host': 'localhost',
    'port': 5432
}

def check_sample_products():
    """Check a sample of products from the log to verify image quality"""

    # Sample barcodes from the log that reportedly found images
    sample_barcodes = [
        '10181025396',  # פלמרס
        '10181025938',  # פלמרס
        '10900034067',  # ריינולדס
        '10900145015',  # דאיימונד
        '3423473020493', # דולצ'ה גבאנה
        '3574661561226', # ליסטרין
        '3574669909099', # ג'ונסון
        '3616301623359', # הוגו בוס
    ]

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    print("=" * 80)
    print("Checking sample products from the backfill process...")
    print("=" * 80)

    for barcode in sample_barcodes:
        cursor.execute("""
            SELECT barcode, name, brand, image_url, is_active
            FROM canonical_products
            WHERE barcode = %s
        """, (barcode,))

        result = cursor.fetchone()
        if result:
            barcode_val, name, brand, image_url, is_active = result
            print(f"\nBarcode: {barcode_val}")
            print(f"Name: {name}")
            print(f"Brand: {brand}")
            print(f"Active: {is_active}")
            print(f"Image URL: {image_url}")

            # Check if URL contains barcode (good signal)
            if image_url and barcode in image_url:
                print("✅ URL contains barcode (good signal)")
            elif image_url:
                print("⚠️  URL doesn't contain barcode")
            else:
                print("❌ No image URL found")
        else:
            print(f"\n❌ Barcode {barcode} not found in database")

    cursor.close()
    conn.close()

if __name__ == '__main__':
    check_sample_products()
