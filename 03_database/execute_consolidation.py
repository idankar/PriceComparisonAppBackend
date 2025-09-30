# cleanup_non_pharm_products.py
import os
import csv
import psycopg2
from collections import defaultdict

# --- CONFIGURATION ---
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DATABASE = os.environ.get("PG_DATABASE", "price_comparison_app")
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "***REMOVED***")

# 1. The (stale) consolidation plan that was already applied
CONSOLIDATION_PLAN_CSV = "duplicate_groups_pharmacy_canonical.csv"

# 2. The (stale) list of products classified as pharmacy items
PHARMACY_CLASSIFICATION_CSV = "/Users/noa/Desktop/PriceComparisonApp/pharmacy_only_masterproductids.csv"

# 3. The retailers you want to clean up
TARGET_RETAILER_IDS = [150, 97, 52]


# --- HELPER FUNCTIONS ---

def load_pharmacy_ids(filename: str) -> set:
    """Loads the masterproductids from the pharmacy classification CSV into a set."""
    print(f"Loading pharmacy product IDs from '{filename}'...")
    pharmacy_ids = set()
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                pharmacy_ids.add(int(row['masterproductid']))
        print(f"  > Found {len(pharmacy_ids)} IDs in the original pharmacy classification.")
        return pharmacy_ids
    except FileNotFoundError:
        print(f"FATAL: Pharmacy classification file not found at '{filename}'")
        return set()

def load_consolidation_map(filename: str) -> dict:
    """
    Loads the (already executed) consolidation plan.
    Returns a dictionary mapping {old_id: canonical_id}.
    """
    print(f"Loading the executed consolidation map from '{filename}'...")
    product_map = {}
    groups = defaultdict(list)
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                groups[row['group_id']].append(row)

        for group_id, items in groups.items():
            canonical_item = next((item for item in items if item['is_canonical'].upper() == 'TRUE'), None)
            if not canonical_item:
                continue
            canonical_pid = int(canonical_item['masterproductid'])
            for item in items:
                pid = int(item['masterproductid'])
                product_map[pid] = canonical_pid
        print(f"  > Loaded a map of {len(product_map)} historical consolidations.")
        return product_map
    except FileNotFoundError:
        print(f"FATAL: Consolidation plan file not found at '{filename}'")
        return {}


# --- MAIN EXECUTION BLOCK ---
def main():
    print("--- Starting Post-Consolidation Cleanup Script ---")

    # Step 1: Load historical data from CSVs
    all_pharmacy_ids = load_pharmacy_ids(PHARMACY_CLASSIFICATION_CSV)
    consolidation_map = load_consolidation_map(CONSOLIDATION_PLAN_CSV)

    if not all_pharmacy_ids or not consolidation_map:
        print("Could not load necessary CSV files. Exiting.")
        return

    # Step 2: Resolve stale pharmacy IDs to their current, canonical form
    print("Resolving stale pharmacy IDs to their current canonical forms...")
    canonical_pharmacy_ids = set()
    for pid in all_pharmacy_ids:
        # If the ID was part of the consolidation, find its canonical parent.
        # Otherwise, the ID itself is its own canonical form.
        canonical_id = consolidation_map.get(pid, pid)
        canonical_pharmacy_ids.add(canonical_id)
    print(f"  > Determined there are {len(canonical_pharmacy_ids)} unique canonical pharmacy products.")


    # Step 3: Connect to DB and get the current state
    conn = None
    try:
        conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD)
        cur = conn.cursor()

        print(f"Fetching all current product IDs for retailers: {TARGET_RETAILER_IDS}...")
        cur.execute("""
            SELECT DISTINCT masterproductid FROM retailerproductlistings
            WHERE retailerid = ANY(%s);
        """, (TARGET_RETAILER_IDS,))
        
        current_product_ids = {row[0] for row in cur.fetchall()}
        print(f"  > Found {len(current_product_ids)} unique products currently listed for these retailers.")

        # Step 4: Identify non-pharmacy products to be deleted
        ids_to_delete = list(current_product_ids - canonical_pharmacy_ids)
        
        if not ids_to_delete:
            print("\nSuccess! All products currently listed for the target retailers are valid pharmacy products.")
            return

        print(f"\nIdentified {len(ids_to_delete)} non-pharmacy products to be deleted.")
        # print(f"  > Example IDs to delete: {ids_to_delete[:10]}") # Uncomment for debugging

        # Step 5: Execute deletion in a safe transaction
        print("\n=== STARTING DELETION TRANSACTION ===")

        # Note: We assume ON DELETE CASCADE is set for the foreign key from `prices` to `retailerproductlistings`.
        # If not, you would need a separate DELETE statement for the `prices` table first.
        
        print(f"Step A: Deleting {len(ids_to_delete)} products from 'retailerproductlistings'...")
        delete_listings_query = "DELETE FROM retailerproductlistings WHERE masterproductid = ANY(%s);"
        cur.execute(delete_listings_query, (ids_to_delete,))
        print(f"  > {cur.rowcount} listings deleted.")
        
        print(f"Step B: Deleting {len(ids_to_delete)} products from 'products' table...")
        delete_products_query = "DELETE FROM products WHERE masterproductid = ANY(%s);"
        cur.execute(delete_products_query, (ids_to_delete,))
        print(f"  > {cur.rowcount} master products deleted.")

        print("\nCleanup complete. Committing transaction...")
        conn.commit()
        print("=== TRANSACTION COMMITTED SUCCESSFULLY ===")

    except Exception as e:
        print(f"\nFATAL ERROR: An error occurred: {e}")
        if conn:
            conn.rollback()
        print("=== TRANSACTION ROLLED BACK. DATABASE IS UNCHANGED. ===")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()