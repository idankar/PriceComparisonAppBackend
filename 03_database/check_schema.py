#!/usr/bin/env python3
import psycopg2
from psycopg2.extras import DictCursor

def check_schema():
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="price_comparison_app_v2",
        user="postgres",
        password="***REMOVED***",
        cursor_factory=DictCursor
    )
    cursor = conn.cursor()
    
    # Check retailer_products columns
    cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'retailer_products'
        ORDER BY ordinal_position;
    """)
    print("retailer_products columns:")
    for row in cursor.fetchall():
        print(f"  {row['column_name']}: {row['data_type']}")
    
    # Check prices columns
    cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'prices'
        ORDER BY ordinal_position;
    """)
    print("\nprices columns:")
    for row in cursor.fetchall():
        print(f"  {row['column_name']}: {row['data_type']}")
        
    # Check canonical_products columns
    cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'canonical_products'
        ORDER BY ordinal_position;
    """)
    print("\ncanonical_products columns:")
    for row in cursor.fetchall():
        print(f"  {row['column_name']}: {row['data_type']}")
    
    conn.close()

if __name__ == "__main__":
    check_schema()