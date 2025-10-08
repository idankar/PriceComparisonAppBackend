#!/usr/bin/env python3
"""
Export all Good Pharm products missing images to JSON
"""

import psycopg2
import json
from datetime import datetime

DB_CONFIG = {
    'dbname': 'price_comparison_app_v2',
    'user': 'postgres',
    'password': '025655358',
    'host': 'localhost',
    'port': 5432
}

def export_missing_images():
    """Export all Good Pharm products without images to JSON"""

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Query for all Good Pharm products (retailer_id=97) without images
    query = """
        SELECT DISTINCT
            cp.barcode,
            cp.name,
            cp.brand,
            cp.category,
            cp.source_retailer_id,
            cp.is_active
        FROM canonical_products cp
        WHERE cp.source_retailer_id = 97
            AND (cp.image_url IS NULL OR cp.image_url = '')
        ORDER BY cp.brand, cp.name
    """

    cursor.execute(query)
    results = cursor.fetchall()

    products = []
    for row in results:
        barcode, name, brand, category, source_retailer_id, is_active = row
        products.append({
            'barcode': barcode,
            'name': name,
            'brand': brand or '',
            'category': category or '',
            'source_retailer_id': source_retailer_id,
            'is_active': is_active
        })

    cursor.close()
    conn.close()

    # Create output
    output = {
        'export_date': datetime.now().isoformat(),
        'total_products': len(products),
        'retailer': 'Good Pharm',
        'retailer_id': 97,
        'products': products
    }

    # Write to JSON file
    output_file = 'good_pharm_missing_images.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"✅ Exported {len(products)} products to {output_file}")

    # Print summary by category
    categories = {}
    for p in products:
        cat = p['category'] or 'Uncategorized'
        categories[cat] = categories.get(cat, 0) + 1

    print("\n📊 Breakdown by category:")
    for cat, count in sorted(categories.items(), key=lambda x: -x[1]):
        print(f"  {cat}: {count}")

if __name__ == '__main__':
    export_missing_images()
