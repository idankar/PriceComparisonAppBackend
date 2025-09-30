#!/usr/bin/env python3
"""
Monitor the progress of the image backfill process
"""

import psycopg2
from datetime import datetime

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'price_comparison_app_v2',
    'user': 'postgres',
    'password': '***REMOVED***'
}

def check_progress():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    print("\n" + "="*80)
    print("IMAGE BACKFILL PROGRESS REPORT")
    print("="*80)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Total NULL source products
    cur.execute("""
        SELECT COUNT(DISTINCT barcode)
        FROM canonical_products
        WHERE source_retailer_id IS NULL
    """)
    total_null_source = cur.fetchone()[0]

    # NULL source products now WITH images
    cur.execute("""
        SELECT COUNT(DISTINCT barcode)
        FROM canonical_products
        WHERE source_retailer_id IS NOT NULL
            AND (image_url IS NOT NULL AND image_url != '')
            AND last_scraped_at >= '2025-09-30 12:00:00'
    """)
    backfilled = cur.fetchone()[0]

    # NULL source products STILL without images
    cur.execute("""
        SELECT COUNT(DISTINCT barcode)
        FROM canonical_products
        WHERE source_retailer_id IS NULL
            AND (image_url IS NULL OR image_url = '')
    """)
    remaining = cur.fetchone()[0]

    # Overall image coverage
    cur.execute("""
        SELECT
            COUNT(DISTINCT rp.retailer_product_id) as total,
            COUNT(DISTINCT CASE WHEN cp.image_url IS NOT NULL AND cp.image_url != ''
                                THEN rp.retailer_product_id END) as with_images
        FROM retailer_products rp
        JOIN canonical_products cp ON rp.barcode = cp.barcode
        WHERE rp.retailer_id IN (52, 97, 150)
    """)
    result = cur.fetchone()
    total_portal = result[0]
    portal_with_images = result[1]

    progress_pct = (backfilled / total_null_source * 100) if total_null_source > 0 else 0
    image_coverage = (portal_with_images / total_portal * 100) if total_portal > 0 else 0

    print(f"\n📊 NULL SOURCE PRODUCTS:")
    print(f"   Total NULL source: {total_null_source:,}")
    print(f"   Backfilled: {backfilled:,} ({progress_pct:.1f}%)")
    print(f"   Remaining: {remaining:,}")

    print(f"\n🖼️  OVERALL IMAGE COVERAGE:")
    print(f"   Total portal products: {total_portal:,}")
    print(f"   With images: {portal_with_images:,} ({image_coverage:.1f}%)")
    print(f"   Without images: {total_portal - portal_with_images:,}")

    # Recently updated products
    cur.execute("""
        SELECT COUNT(*)
        FROM canonical_products
        WHERE last_scraped_at >= '2025-09-30 12:00:00'
            AND source_retailer_id IS NOT NULL
    """)
    recent_updates = cur.fetchone()[0]

    print(f"\n⏱️  RECENT ACTIVITY:")
    print(f"   Products updated since 12:00 PM today: {recent_updates:,}")

    if backfilled > 0 and remaining > 0:
        estimated_time = (remaining / backfilled) * ((datetime.now() - datetime(2025, 9, 30, 12, 0, 0)).total_seconds() / 3600)
        print(f"   Estimated time to completion: ~{estimated_time:.1f} hours")

    print("="*80)

    cur.close()
    conn.close()

if __name__ == "__main__":
    check_progress()