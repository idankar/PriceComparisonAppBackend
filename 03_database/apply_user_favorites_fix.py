#!/usr/bin/env python3
"""
Apply the user_favorites table fix to add updated_at column and trigger.
This resolves the 500 Internal Server Error when adding items to cart.
"""

import psycopg2
import os

# Database configuration
DB_NAME = "price_comparison_app_v2"
DB_USER = "postgres"
DB_PASSWORD = "***REMOVED***"
DB_HOST = "localhost"
DB_PORT = "5432"

def apply_migration():
    """Apply the migration to fix user_favorites table"""

    print("🔧 Applying user_favorites table fix...")

    try:
        # Connect to database
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = False
        cursor = conn.cursor()

        print("✓ Connected to database")

        # Read the SQL migration file
        sql_file = os.path.join(os.path.dirname(__file__), 'fix_user_favorites_trigger.sql')
        with open(sql_file, 'r') as f:
            sql = f.read()

        # Execute the migration
        cursor.execute(sql)
        conn.commit()

        print("✅ Migration applied successfully!")
        print("   - Added updated_at column to user_favorites")
        print("   - Created trigger for automatic timestamp updates")
        print("   - Database is now ready for cart operations")

        # Verify the changes
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'user_favorites'
            ORDER BY ordinal_position
        """)

        columns = cursor.fetchall()
        print("\n📋 user_favorites table structure:")
        for col_name, col_type in columns:
            print(f"   - {col_name}: {col_type}")

        cursor.close()
        conn.close()

        print("\n✨ Migration complete! Restart your backend server to apply changes.")

    except Exception as e:
        print(f"❌ Error applying migration: {e}")
        if conn:
            conn.rollback()
        raise

if __name__ == "__main__":
    apply_migration()
