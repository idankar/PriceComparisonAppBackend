#!/usr/bin/env python3
"""
Migration Runner: Creates user_interactions table and preference profiles
Run this script to set up the database schema for personalized recommendations
"""

import os
import sys
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
        migration_file = os.path.join(os.path.dirname(__file__), 'create_user_interactions_table.sql')
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
            AND table_name IN ('user_interactions')
        """)
        tables = cursor.fetchall()

        cursor.execute("""
            SELECT matviewname
            FROM pg_matviews
            WHERE schemaname = 'public'
            AND matviewname IN ('user_preference_profiles')
        """)
        views = cursor.fetchall()

        print("\n✅ Migration completed successfully!")
        print(f"Tables created: {[t[0] for t in tables]}")
        print(f"Materialized views created: {[v[0] for v in views]}")

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
