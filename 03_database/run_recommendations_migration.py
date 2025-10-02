#!/usr/bin/env python3
"""
Migration Runner: Creates recommendations system tables
Run this script to set up the database schema for personalized recommendations
"""

import os
import sys
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Database configuration
DB_NAME = "price_comparison_app_v2"
DB_USER = "postgres"
DB_PASSWORD = "***REMOVED***"
DB_HOST = "localhost"
DB_PORT = "5432"

def run_migration():
    """Execute the migration SQL file"""
    try:
        # Connect to database
        print(f"Connecting to database: {DB_NAME}...")
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        # Read migration file
        migration_file = os.path.join(os.path.dirname(__file__), 'create_recommendations_system.sql')
        print(f"Reading migration file: {migration_file}")

        with open(migration_file, 'r') as f:
            sql = f.read()

        # Execute migration
        print("Executing migration...")
        cursor.execute(sql)

        # Verify tables were created
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name IN ('users', 'user_favorites', 'user_cart', 'user_preferences')
            ORDER BY table_name
        """)
        tables = cursor.fetchall()

        print("\n✅ Migration completed successfully!")
        print(f"Tables created: {[t[0] for t in tables]}")

        # Show sample schema
        print("\n📊 Table Schemas:")
        for table_name in ['users', 'user_favorites', 'user_cart', 'user_preferences']:
            cursor.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
            """)
            columns = cursor.fetchall()
            print(f"\n{table_name}:")
            for col in columns:
                print(f"  - {col[0]}: {col[1]}")

        cursor.close()
        conn.close()

        return True

    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
