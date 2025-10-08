import os
import psycopg2
from psycopg2.extras import execute_batch

# --- Configuration ---
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "price_comparison_app_v2"
PG_USER = "postgres"
PG_PASSWORD = "025655358"

# --- Database Connection ---
def get_db_connection():
    """Establishes and returns a database connection."""
    try:
        return psycopg2.connect(
            host=PG_HOST, port=PG_PORT, database=PG_DATABASE,
            user=PG_USER, password=PG_PASSWORD
        )
    except psycopg2.Error as e:
        raise

# --- Main Clustering Logic ---
def main():
    """
    Groups products based on shared attributes and populates the grouping tables.
    This script is safe to re-run.
    """
    print("--- 🔗 Starting Product Clustering Process ---")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Step 1: Clear out any old grouping data to start fresh.
            # The CASCADE ensures that product_group_links is also cleared.
            print("-> Clearing old grouping data...")
            cursor.execute("TRUNCATE TABLE product_groups RESTART IDENTITY CASCADE;")

            # Step 2: Define the equivalence rule and find all groups of products.
            # We group by brand, type, size, and unit. We only consider groups
            # with more than one product, as single-product groups are not comparisons.
            print("-> Finding product groups based on attributes...")
            query = """
                SELECT
                    LOWER(brand) as brand_key,
                    attributes->>'product_type' as type_key,
                    attributes->>'size_value' as size_key,
                    attributes->>'size_unit' as unit_key,
                    array_agg(product_id) as product_ids
                FROM products
                WHERE
                    brand IS NOT NULL AND
                    attributes->>'product_type' IS NOT NULL AND
                    attributes->>'size_value' IS NOT NULL AND
                    attributes->>'size_unit' IS NOT NULL
                GROUP BY brand_key, type_key, size_key, unit_key
                HAVING count(product_id) > 1;
            """
            cursor.execute(query)
            groups = cursor.fetchall()

            if not groups:
                print("-> No product groups found to cluster. Ensure enrichment script has run.")
                return

            print(f"-> Found {len(groups)} groups of equivalent products to create.")

            # Step 3: Create the group entries and the links.
            links_to_insert = []
            for group in groups:
                product_ids = group[4]
                
                # Create a new entry in the product_groups table and get its new group_id
                cursor.execute(
                    "INSERT INTO product_groups DEFAULT VALUES RETURNING group_id;"
                )
                group_id = cursor.fetchone()[0]
                
                # Prepare the links for this group for batch insertion
                for product_id in product_ids:
                    links_to_insert.append((group_id, product_id))

            print(f"-> Inserting {len(links_to_insert)} product-to-group links...")
            
            # Use execute_batch for efficient insertion of all links
            execute_batch(
                cursor,
                "INSERT INTO product_group_links (group_id, product_id) VALUES (%s, %s);",
                links_to_insert
            )

            conn.commit()
            print(f"\n--- ✅ Clustering Complete. Created {len(groups)} product groups. ---")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"\n[FATAL ERROR] An error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
