import os
import csv
import psycopg2
from collections import defaultdict

# --- CONFIGURATION ---
CSV_FILE = "duplicate_groups_final.csv" # The final, verified output from the analysis script

# Database Connection Details
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DATABASE = os.environ.get("PG_DATABASE", "price_comparison_app")
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "025655358")

def apply_consolidation():
    """
    Reads the validated CSV and updates the database to link duplicates
    to their canonical master product.
    """
    # 1. Read the CSV and group products by their group_id
    try:
        groups = defaultdict(list)
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                groups[row['group_id']].append({
                    'id': int(row['masterproductid']),
                    'name': row['productname']
                })
    except FileNotFoundError:
        print(f"FATAL: The input file '{CSV_FILE}' was not found. Please ensure it is in the same directory.")
        return

    print(f"Found {len(groups)} groups to process from '{CSV_FILE}'.")

    conn = None
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD
        )
        # Use a transaction to ensure all updates succeed or none do
        with conn.cursor() as cur:
            processed_count = 0
            for group_id, products in groups.items():
                if not products:
                    continue

                # 2. Choose a "canonical" representative for the group.
                # A simple but effective strategy: choose the one with the shortest, most concise name.
                canonical_product = min(products, key=lambda p: len(p['name']))
                canonical_id = canonical_product['id']
                
                duplicate_ids = [p['id'] for p in products] # Get ALL IDs in the group

                print(f"\nProcessing Group {group_id}: Canonical is '{canonical_product['name']}' ({canonical_id})")

                # 3. Update the database
                # This single command updates all products in the group (including the canonical one)
                # to point to the canonical ID.
                if duplicate_ids:
                    # The psycopg2 library requires a tuple for the IN clause
                    cur.execute(
                        "UPDATE products SET canonical_masterproductid = %s WHERE masterproductid IN %s;",
                        (canonical_id, tuple(duplicate_ids))
                    )
                print(f"  > Linked {len(duplicate_ids)} total products in the group.")
                processed_count += 1
        
        # 4. If all updates were successful, commit the changes to the database
        conn.commit()
        print(f"\nSuccessfully processed {processed_count} groups.")
        print("All changes have been committed to the database!")

    except Exception as e:
        print(f"\nAn error occurred: {e}")
        if conn:
            conn.rollback() # If anything fails, undo all changes from this run
            print("All changes have been rolled back.")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # --- SAFETY WARNING ---
    print("!!! WARNING: This script will make permanent changes to your database. !!!")
    print("It will read the `duplicate_groups_final.csv` and update the `canonical_masterproductid` for all products found.")
    print("It is highly recommended to back up your database before proceeding.")
    
    user_input = input("Are you sure you want to continue? (yes/no): ")
    
    if user_input.lower() == 'yes':
        apply_consolidation()
    else:
        print("Operation cancelled by user.")