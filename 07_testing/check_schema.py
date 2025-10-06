#!/usr/bin/env python3
"""
Check database schema
"""
import psycopg2

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'price_comparison_app_v2',
    'user': 'postgres',
    'password': '***REMOVED***'
}

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    tables = ['retailer_products', 'canonical_products', 'stores', 'prices']

    for table in tables:
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position;
        """, (table,))

        print(f"\n{table.upper()} columns:")
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]}")

    cur.close()
    conn.close()

except Exception as e:
    print(f"Error: {e}")
