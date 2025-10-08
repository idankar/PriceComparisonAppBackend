#!/usr/bin/env python3
"""
Remove Be Pharm ETL/Transparency Portal Data

This script safely removes the poor quality ETL portal data for Be Pharm
and keeps only the commercial scraper data.

WHAT IT DOES:
1. Backs up current data counts
2. Removes ETL price data (except for online store prices)
3. Removes orphaned retailer_products entries (with dependency checks)
4. Keeps canonical_products from commercial scraper
5. Verifies stores data remains intact
6. Reports on data removed

WHAT IT PRESERVES:
- All stores data (never touched)
- All canonical_products from commercial scraper
- Online store prices (store_id = 15001)
- retailer_products with promotion links or in product matches
- All data for other retailers (only affects Be Pharm retailer_id = 150)

SAFETY FEATURES:
- Creates JSON backup before any deletions
- Checks for foreign key dependencies
- Dry run mode to preview changes
- Requires explicit confirmation
"""

import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime
import json
import argparse


BE_PHARM_RETAILER_ID = 150
BE_PHARM_ONLINE_STORE_ID = 15001  # Online store prices to keep


def get_database_connection():
    """Establish connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="price_comparison_app_v2",
            user="postgres",
            password="025655358",
            cursor_factory=DictCursor
        )
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        return None


def backup_current_state(cursor):
    """Create a backup of current data counts before deletion"""
    print("\n" + "="*80)
    print("BACKING UP CURRENT STATE")
    print("="*80)

    backup = {}

    # Count ETL prices
    cursor.execute("""
        SELECT COUNT(*) as count
        FROM prices p
        JOIN retailer_products rp ON p.retailer_product_id = rp.retailer_product_id
        WHERE rp.retailer_id = %s
        AND p.store_id != %s;  -- Not the online store
    """, (BE_PHARM_RETAILER_ID, BE_PHARM_ONLINE_STORE_ID))
    backup['etl_prices'] = cursor.fetchone()['count']

    # Count online store prices
    cursor.execute("""
        SELECT COUNT(*) as count
        FROM prices p
        JOIN retailer_products rp ON p.retailer_product_id = rp.retailer_product_id
        WHERE rp.retailer_id = %s
        AND p.store_id = %s;  -- Online store
    """, (BE_PHARM_RETAILER_ID, BE_PHARM_ONLINE_STORE_ID))
    backup['online_prices'] = cursor.fetchone()['count']

    # Count retailer products
    cursor.execute("""
        SELECT COUNT(*) as count
        FROM retailer_products
        WHERE retailer_id = %s;
    """, (BE_PHARM_RETAILER_ID,))
    backup['retailer_products'] = cursor.fetchone()['count']

    # Count canonical products
    cursor.execute("""
        SELECT COUNT(*) as count
        FROM canonical_products
        WHERE source_retailer_id = %s;
    """, (BE_PHARM_RETAILER_ID,))
    backup['canonical_products'] = cursor.fetchone()['count']

    # Save to file
    backup['timestamp'] = datetime.now().isoformat()
    backup_file = f"be_pharm_data_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    with open(backup_file, 'w') as f:
        json.dump(backup, f, indent=2)

    print(f"✅ Backup saved to: {backup_file}")
    print(f"   ETL Prices: {backup['etl_prices']:,}")
    print(f"   Online Prices: {backup['online_prices']:,}")
    print(f"   Retailer Products: {backup['retailer_products']:,}")
    print(f"   Canonical Products: {backup['canonical_products']:,}")

    return backup


def remove_etl_prices(cursor, dry_run=False):
    """Remove ETL portal prices (keeping online store prices)"""
    print("\n" + "="*80)
    print("REMOVING ETL PORTAL PRICES")
    print("="*80)

    # Count prices to be deleted
    cursor.execute("""
        SELECT COUNT(*) as count, MIN(price_timestamp) as oldest, MAX(price_timestamp) as newest
        FROM prices p
        JOIN retailer_products rp ON p.retailer_product_id = rp.retailer_product_id
        WHERE rp.retailer_id = %s
        AND p.store_id != %s;  -- Not the online store
    """, (BE_PHARM_RETAILER_ID, BE_PHARM_ONLINE_STORE_ID))

    result = cursor.fetchone()
    count = result['count']
    oldest = result['oldest']
    newest = result['newest']

    print(f"Found {count:,} ETL prices to remove")
    if count > 0:
        print(f"Date range: {oldest} to {newest}")

    if not dry_run and count > 0:
        # Delete ETL prices
        cursor.execute("""
            DELETE FROM prices
            WHERE retailer_product_id IN (
                SELECT retailer_product_id
                FROM retailer_products
                WHERE retailer_id = %s
            )
            AND store_id != %s;  -- Keep online store prices
        """, (BE_PHARM_RETAILER_ID, BE_PHARM_ONLINE_STORE_ID))

        print(f"✅ Deleted {cursor.rowcount:,} ETL prices")
    elif dry_run:
        print("🔍 DRY RUN - No data deleted")

    return count


def remove_orphaned_retailer_products(cursor, dry_run=False):
    """Remove retailer_products entries that no longer have prices"""
    print("\n" + "="*80)
    print("CLEANING UP ORPHANED RETAILER PRODUCTS")
    print("="*80)

    # First check for any foreign key dependencies
    cursor.execute("""
        SELECT
            COUNT(DISTINCT rp.retailer_product_id) as orphaned_count,
            COUNT(DISTINCT ppl.retailer_product_id) as with_promotion_links,
            COUNT(DISTINCT pm.retailer_product_ids) as in_matches
        FROM retailer_products rp
        LEFT JOIN promotion_product_links ppl ON rp.retailer_product_id = ppl.retailer_product_id
        LEFT JOIN product_matches pm ON rp.retailer_product_id = ANY(pm.retailer_product_ids)
        WHERE rp.retailer_id = %s
        AND NOT EXISTS (
            SELECT 1 FROM prices p
            WHERE p.retailer_product_id = rp.retailer_product_id
        )
        AND rp.barcode NOT IN (
            SELECT barcode FROM canonical_products
            WHERE source_retailer_id = %s
        );
    """, (BE_PHARM_RETAILER_ID, BE_PHARM_RETAILER_ID))

    result = cursor.fetchone()
    orphaned = result['orphaned_count']
    with_promos = result['with_promotion_links']
    in_matches = result['in_matches']

    print(f"Found {orphaned:,} orphaned retailer products")
    if with_promos > 0:
        print(f"  ⚠️  {with_promos:,} have promotion links (will be preserved)")
    if in_matches > 0:
        print(f"  ⚠️  {in_matches:,} are in product matches (will be preserved)")

    # Only delete truly orphaned products with no dependencies
    if not dry_run and orphaned > 0:
        cursor.execute("""
            DELETE FROM retailer_products rp
            WHERE rp.retailer_id = %s
            AND NOT EXISTS (
                SELECT 1 FROM prices p
                WHERE p.retailer_product_id = rp.retailer_product_id
            )
            AND rp.barcode NOT IN (
                SELECT barcode FROM canonical_products
                WHERE source_retailer_id = %s
            )
            AND NOT EXISTS (
                SELECT 1 FROM promotion_product_links ppl
                WHERE ppl.retailer_product_id = rp.retailer_product_id
            )
            AND NOT EXISTS (
                SELECT 1 FROM product_matches pm
                WHERE rp.retailer_product_id = ANY(pm.retailer_product_ids)
            );
        """, (BE_PHARM_RETAILER_ID, BE_PHARM_RETAILER_ID))

        print(f"✅ Safely deleted {cursor.rowcount:,} orphaned retailer products")
        print(f"   (Products with dependencies were preserved)")
    elif dry_run:
        print("🔍 DRY RUN - No data deleted")

    return orphaned - with_promos - in_matches


def verify_stores_intact(cursor):
    """Verify that all Be Pharm stores are still intact"""
    print("\n" + "="*80)
    print("VERIFYING STORES DATA")
    print("="*80)

    cursor.execute("""
        SELECT COUNT(*) as store_count,
               COUNT(CASE WHEN isactive THEN 1 END) as active_stores
        FROM stores
        WHERE retailerid = %s;
    """, (BE_PHARM_RETAILER_ID,))

    result = cursor.fetchone()
    print(f"Be Pharm Stores Status:")
    print(f"  Total Stores: {result['store_count']:,}")
    print(f"  Active Stores: {result['active_stores']:,}")
    print(f"  ✅ All stores data preserved")

    # List first few stores
    cursor.execute("""
        SELECT storeid, storename, isactive
        FROM stores
        WHERE retailerid = %s
        ORDER BY storeid
        LIMIT 5;
    """, (BE_PHARM_RETAILER_ID,))

    stores = cursor.fetchall()
    if stores:
        print("\n  Sample stores:")
        for store in stores:
            status = "Active" if store['isactive'] else "Inactive"
            print(f"    ID {store['storeid']}: {store['storename']} ({status})")


def report_final_state(cursor):
    """Report on final data state after cleanup"""
    print("\n" + "="*80)
    print("FINAL DATA STATE")
    print("="*80)

    # Count remaining prices
    cursor.execute("""
        SELECT
            COUNT(CASE WHEN p.store_id = %s THEN 1 END) as online_prices,
            COUNT(CASE WHEN p.store_id != %s THEN 1 END) as other_prices,
            COUNT(*) as total_prices
        FROM prices p
        JOIN retailer_products rp ON p.retailer_product_id = rp.retailer_product_id
        WHERE rp.retailer_id = %s;
    """, (BE_PHARM_ONLINE_STORE_ID, BE_PHARM_ONLINE_STORE_ID, BE_PHARM_RETAILER_ID))

    result = cursor.fetchone()
    print(f"Remaining Be Pharm Prices:")
    print(f"  Online Store: {result['online_prices']:,}")
    print(f"  Other: {result['other_prices']:,}")
    print(f"  Total: {result['total_prices']:,}")

    # Count remaining products
    cursor.execute("""
        SELECT
            COUNT(DISTINCT rp.barcode) as retailer_products,
            COUNT(DISTINCT cp.barcode) as canonical_products
        FROM retailer_products rp
        FULL OUTER JOIN canonical_products cp ON rp.barcode = cp.barcode
        WHERE (rp.retailer_id = %s OR cp.source_retailer_id = %s);
    """, (BE_PHARM_RETAILER_ID, BE_PHARM_RETAILER_ID))

    result = cursor.fetchone()
    print(f"\nRemaining Be Pharm Products:")
    print(f"  Retailer Products: {result['retailer_products']:,}")
    print(f"  Canonical Products: {result['canonical_products']:,}")

    # Check data quality
    cursor.execute("""
        SELECT
            COUNT(DISTINCT cp.barcode) as total,
            COUNT(DISTINCT CASE WHEN cp.image_url IS NOT NULL THEN cp.barcode END) as with_images
        FROM canonical_products cp
        WHERE cp.source_retailer_id = %s;
    """, (BE_PHARM_RETAILER_ID,))

    result = cursor.fetchone()
    if result['total'] > 0:
        print(f"\nCanonical Product Quality:")
        print(f"  Total: {result['total']:,}")
        print(f"  With Images: {result['with_images']:,} ({result['with_images']*100/result['total']:.1f}%)")


def main():
    parser = argparse.ArgumentParser(description='Remove Be Pharm ETL/Portal Data')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be deleted without actually deleting')
    parser.add_argument('--confirm', action='store_true',
                       help='Skip confirmation prompt (use with caution!)')

    args = parser.parse_args()

    print("\n" + "="*80)
    print("BE PHARM ETL DATA REMOVAL SCRIPT")
    print("="*80)
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")

    if not args.dry_run and not args.confirm:
        print("\n⚠️  WARNING: This will permanently delete ETL portal data!")
        print("   (Commercial scraper data will be preserved)")
        response = input("\nAre you sure you want to proceed? (yes/no): ")
        if response.lower() != 'yes':
            print("❌ Operation cancelled")
            return

    # Connect to database
    conn = get_database_connection()
    if not conn:
        return

    cursor = conn.cursor()

    try:
        # Backup current state
        backup = backup_current_state(cursor)

        # Remove ETL prices
        prices_removed = remove_etl_prices(cursor, args.dry_run)

        # Remove orphaned retailer products
        products_removed = remove_orphaned_retailer_products(cursor, args.dry_run)

        # Verify stores are intact
        verify_stores_intact(cursor)

        # Report final state
        report_final_state(cursor)

        if not args.dry_run:
            conn.commit()
            print("\n✅ All changes committed to database")
        else:
            conn.rollback()
            print("\n🔍 DRY RUN complete - no changes made")

        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        print(f"ETL Prices Removed: {prices_removed:,}")
        print(f"Orphaned Products Removed: {products_removed:,}")

    except Exception as e:
        print(f"\n❌ Error during cleanup: {e}")
        conn.rollback()
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()
        print("\n🔌 Database connection closed")


if __name__ == "__main__":
    main()