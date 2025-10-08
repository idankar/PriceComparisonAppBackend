#!/usr/bin/env python3
import os
import sys
import subprocess
import psycopg2
from datetime import datetime
from psycopg2 import sql

# Database Connection Details
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DATABASE = os.environ.get("PG_DATABASE", "price_comparison_app_v2")
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "025655358")

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD
    )

def create_backup():
    """Create a full database backup before making schema changes."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_filename = f"backup_before_active_flag_{timestamp}.sql"

    print("--- Step 1: Creating full database backup ---")
    print(f"Backup file: {backup_filename}")

    # Build the pg_dump command
    env = os.environ.copy()
    env['PGPASSWORD'] = PG_PASSWORD

    cmd = [
        'pg_dump',
        '-h', PG_HOST,
        '-p', PG_PORT,
        '-U', PG_USER,
        '-d', PG_DATABASE,
        '-f', backup_filename
    ]

    try:
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error creating backup: {result.stderr}")
            return None

        # Check file size
        file_size = os.path.getsize(backup_filename)
        print(f"✅ Backup complete: {backup_filename} ({file_size:,} bytes)")
        return backup_filename
    except FileNotFoundError:
        print("❌ pg_dump not found. Creating SQL-based backup instead...")
        # Fall back to Python-based export
        return create_python_backup()

def create_python_backup():
    """Create a Python-based backup of critical data."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_filename = f"backup_before_active_flag_{timestamp}_data.sql"

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        with open(backup_filename, 'w') as f:
            # Export canonical_products data
            f.write("-- Backup of canonical_products table before is_active migration\n")
            f.write(f"-- Generated on {datetime.now()}\n\n")

            # Get table structure
            cur.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = 'canonical_products'
                ORDER BY ordinal_position
            """)
            columns = cur.fetchall()

            f.write("-- Table structure for canonical_products\n")
            f.write("-- Columns: ")
            f.write(", ".join([col[0] for col in columns]) + "\n\n")

            # Export data
            cur.execute("SELECT * FROM canonical_products")
            rows = cur.fetchall()

            f.write(f"-- Data: {len(rows)} rows\n")
            f.write("-- To restore: Use these INSERT statements\n\n")

            for row in rows[:100]:  # Save first 100 rows as sample
                values = []
                for val in row:
                    if val is None:
                        values.append("NULL")
                    elif isinstance(val, str):
                        escaped_val = val.replace("'", "''")
                        values.append(f"'{escaped_val}'")
                    else:
                        values.append(str(val))
                f.write(f"-- INSERT INTO canonical_products VALUES ({', '.join(values)});\n")

            if len(rows) > 100:
                f.write(f"\n-- ... and {len(rows) - 100} more rows\n")

        conn.close()
        file_size = os.path.getsize(backup_filename)
        print(f"✅ Python backup complete: {backup_filename} ({file_size:,} bytes)")
        return backup_filename
    except Exception as e:
        print(f"❌ Error creating backup: {e}")
        return None

def add_active_column():
    """Add is_active column to canonical_products table."""
    print("\n--- Step 2: Adding is_active column to canonical_products ---")

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Check if column already exists
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'canonical_products'
            AND column_name = 'is_active'
        """)

        if cur.fetchone():
            print("⚠️  is_active column already exists. Skipping...")
            conn.close()
            return True

        # Add the column
        cur.execute("""
            ALTER TABLE canonical_products
            ADD COLUMN is_active BOOLEAN DEFAULT TRUE
        """)

        conn.commit()
        print("✅ is_active column added successfully (defaulted to TRUE)")
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Error adding column: {e}")
        if conn:
            conn.rollback()
            conn.close()
        return False

def deactivate_incomplete_products():
    """Set is_active to false for products missing images or prices."""
    print("\n--- Step 3: Deactivating products without images or prices ---")

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # First, count how many products will be affected
        cur.execute("""
            WITH ProductsWithPrices AS (
                SELECT DISTINCT cp.barcode
                FROM canonical_products cp
                JOIN retailer_products rp ON cp.barcode = rp.barcode
                JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
                WHERE p.price > 0
            )
            SELECT COUNT(*)
            FROM canonical_products
            WHERE
                image_url IS NULL
                OR image_url = ''
                OR barcode NOT IN (SELECT barcode FROM ProductsWithPrices)
        """)

        count_to_deactivate = cur.fetchone()[0]
        print(f"Found {count_to_deactivate} products to deactivate")

        # Perform the update
        cur.execute("""
            WITH ProductsWithPrices AS (
                SELECT DISTINCT cp.barcode
                FROM canonical_products cp
                JOIN retailer_products rp ON cp.barcode = rp.barcode
                JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
                WHERE p.price > 0
            )
            UPDATE canonical_products
            SET is_active = FALSE
            WHERE
                image_url IS NULL
                OR image_url = ''
                OR barcode NOT IN (SELECT barcode FROM ProductsWithPrices)
        """)

        rows_updated = cur.rowcount
        conn.commit()
        print(f"✅ Deactivated {rows_updated} products")
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Error deactivating products: {e}")
        if conn:
            conn.rollback()
            conn.close()
        return False

def verify_results():
    """Verify the migration results."""
    print("\n--- Step 4: Verifying results ---")

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Get counts
        cur.execute("""
            SELECT is_active, COUNT(*)
            FROM canonical_products
            GROUP BY is_active
            ORDER BY is_active DESC
        """)

        results = cur.fetchall()
        print("\n📊 Product counts by status:")
        for is_active, count in results:
            status = "Active" if is_active else "Inactive"
            print(f"  {status}: {count:,} products")

        # Show sample of deactivated products
        cur.execute("""
            SELECT name, brand, barcode,
                   CASE WHEN image_url IS NULL OR image_url = '' THEN 'No image' ELSE 'Has image' END as image_status
            FROM canonical_products
            WHERE is_active = FALSE
            LIMIT 10
        """)

        deactivated_sample = cur.fetchall()
        if deactivated_sample:
            print("\n📋 Sample of deactivated products:")
            for row in deactivated_sample:
                print(f"  - {row[0]} | Brand: {row[1]} | Barcode: {row[2]} | {row[3]}")

        # Check products with prices but no image
        cur.execute("""
            WITH ProductsWithPrices AS (
                SELECT DISTINCT cp.barcode, cp.name, cp.image_url
                FROM canonical_products cp
                JOIN retailer_products rp ON cp.barcode = rp.barcode
                JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
                WHERE p.price > 0
            )
            SELECT COUNT(*)
            FROM ProductsWithPrices
            WHERE image_url IS NULL OR image_url = ''
        """)

        no_image_with_price = cur.fetchone()[0]
        print(f"\n⚠️  Products with prices but no image: {no_image_with_price}")

        conn.close()
        return True

    except Exception as e:
        print(f"❌ Error verifying results: {e}")
        if conn:
            conn.close()
        return False

def main():
    """Main migration function."""
    print("=" * 60)
    print("ACTIVE PRODUCT FILTERING MIGRATION")
    print("=" * 60)

    # Step 1: Create backup
    backup_file = create_backup()
    if not backup_file:
        print("\n⛔ Backup failed. Aborting migration for safety.")
        sys.exit(1)

    # Step 2: Add is_active column
    if not add_active_column():
        print("\n⛔ Failed to add column. Aborting migration.")
        sys.exit(1)

    # Step 3: Deactivate incomplete products
    if not deactivate_incomplete_products():
        print("\n⛔ Failed to deactivate products. Migration partially complete.")
        print(f"   To restore: psql -f {backup_file}")
        sys.exit(1)

    # Step 4: Verify results
    verify_results()

    print("\n" + "=" * 60)
    print("✅ MIGRATION COMPLETE!")
    print(f"📁 Backup saved to: {backup_file}")
    print("=" * 60)

if __name__ == "__main__":
    main()