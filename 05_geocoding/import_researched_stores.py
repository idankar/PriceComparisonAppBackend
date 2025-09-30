#!/usr/bin/env python3
"""
Import researched store addresses and update database
Then run geocoding for the newly updated stores
"""

import csv
import psycopg2
import sys

# Database connection
PG_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'price_comparison_app_v2',
    'user': 'postgres',
    'password': '***REMOVED***'
}

def import_store_data(csv_file='completed_store_data.csv'):
    """Import researched store data and update database"""

    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()

    updated_count = 0
    skipped_count = 0
    error_count = 0

    print("Reading researched store data...")

    with open(csv_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)

        for row in reader:
            store_id = row['store_id']
            address = row['address'].strip() if row['address'] else None
            city = row['city'].strip() if row['city'] else None
            notes = row.get('notes', '')

            # Skip if no data found
            if not address or not city:
                skipped_count += 1
                print(f"⊘ Skipped store {store_id}: No address/city data")
                continue

            try:
                # Update the store
                cur.execute("""
                    UPDATE stores
                    SET address = %s,
                        city = %s,
                        updatedat = CURRENT_TIMESTAMP
                    WHERE storeid = %s
                """, (address, city, store_id))

                updated_count += 1
                print(f"✓ Updated store {store_id}: {address}, {city}")

                if notes:
                    print(f"  Note: {notes}")

            except Exception as e:
                error_count += 1
                print(f"✗ Error updating store {store_id}: {e}")
                conn.rollback()
                continue

    # Commit all changes
    conn.commit()
    cur.close()
    conn.close()

    print("\n" + "="*60)
    print("IMPORT SUMMARY")
    print("="*60)
    print(f"Stores updated: {updated_count}")
    print(f"Stores skipped: {skipped_count}")
    print(f"Errors: {error_count}")
    print("="*60)

    if updated_count > 0:
        print(f"\n✓ Successfully updated {updated_count} stores!")
        print("\nNext step: Run geocoding for newly updated stores:")
        print("  python3 pharmacy_only_geocoding.py --google-key '***REMOVED***'")

    return updated_count

if __name__ == "__main__":
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    else:
        csv_file = 'completed_store_data.csv'

    import_store_data(csv_file)
