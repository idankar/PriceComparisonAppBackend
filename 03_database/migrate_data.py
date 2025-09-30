import psycopg2
from psycopg2.extras import execute_batch
import os

# --- IMPORTANT: Please use your actual connection details ---
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "price_comparison_app_v2"
PG_USER = "postgres"
PG_PASSWORD = "***REMOVED***" # Your actual password

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        print("✅ Database connection established.")
        return conn
    except psycopg2.Error as e:
        print(f"❌ CRITICAL: Unable to connect to PostgreSQL: {e}")
        raise

def clear_new_tables(cursor):
    """Clears out the new tables to ensure a fresh start for the script."""
    print("  -> Clearing target tables ('_new') for a fresh migration...")
    cursor.execute("TRUNCATE TABLE public.prices_new, public.retailer_products_new, public.products_new RESTART IDENTITY;")
    print("  -> Target tables cleared.")

def migrate_products(cursor):
    """
    Migrates products from the old 'products' table to 'products_new'.
    This version correctly handles duplicates and builds the ID map.
    Returns a map of {old_masterproductid: new_product_id}.
    """
    print("  [1/3] Migrating products...")
    cursor.execute("SELECT masterproductid, productname, brand, description, imageurl, embedding, createdat, updatedat FROM public.products WHERE productname IS NOT NULL AND brand IS NOT NULL;")
    old_products = cursor.fetchall()

    # Create a dictionary of unique products based on a composite key of lowercased name and brand
    unique_products = {}
    for prod in old_products:
        prod_name = prod[1]
        prod_brand = prod[2]
        key = (prod_name.lower().strip(), prod_brand.lower().strip())
        if key not in unique_products:
            # Store the full record for insertion
            unique_products[key] = prod[1:]
    
    products_to_insert = list(unique_products.values())
    print(f"    -> Found {len(products_to_insert)} unique products to insert.")

    # Bulk insert only the unique products
    execute_batch(cursor,
        """
        INSERT INTO public.products_new (canonical_name, brand, description, image_url, embedding, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;
        """,
        products_to_insert, page_size=5000
    )
    
    # Now, create a map of (name, brand) -> new_product_id from the data we just inserted
    print("    -> Fetching new product IDs to build mapping...")
    cursor.execute("SELECT product_id, LOWER(canonical_name), LOWER(brand) FROM public.products_new;")
    new_product_map = {(name, brand): pid for pid, name, brand in cursor.fetchall()}

    # Finally, create the mapping from old masterproductid to new product_id
    old_to_new_id_map = {}
    for old_prod in old_products:
        old_master_id = old_prod[0]
        prod_name = old_prod[1]
        prod_brand = old_prod[2]
        key = (prod_name.lower().strip(), prod_brand.lower().strip())
        new_id = new_product_map.get(key)
        if new_id:
            old_to_new_id_map[old_master_id] = new_id

    print(f"    -> Successfully created {len(old_to_new_id_map)} product mappings.")
    return old_to_new_id_map

def migrate_retailer_products(cursor, old_to_new_product_map):
    """
    Migrates data from 'retailerproductlistings' to 'retailer_products_new',
    using the product ID map. Returns a map of {old_listingid: new_retailer_product_id}.
    """
    print("  [2/3] Migrating retailer product listings...")
    cursor.execute("SELECT listingid, masterproductid, retailerid, storeid, retaileritemcode, retailerproductname FROM public.retailerproductlistings;")
    old_listings = cursor.fetchall()
    
    new_retailer_products_data = []
    # This map will hold {(old_listingid): new_product_id} to build the final map
    temp_listing_map = {}
    
    for listing in old_listings:
        old_listingid, old_masterproductid, retailerid, storeid, itemcode, original_name = listing
        new_product_id = old_to_new_product_map.get(old_masterproductid)
        
        if new_product_id and retailerid and itemcode:
            # (product_id, retailer_id, retailer_item_code, original_retailer_name)
            record = (new_product_id, retailerid, itemcode, original_name)
            new_retailer_products_data.append(record)
            temp_listing_map[old_listingid] = (retailerid, itemcode)

    print(f"    -> Inserting {len(new_retailer_products_data)} unique retailer product mappings...")
    execute_batch(cursor, 
        """
        INSERT INTO public.retailer_products_new (product_id, retailer_id, retailer_item_code, original_retailer_name)
        VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;
        """,
        new_retailer_products_data, page_size=10000
    )

    print("    -> Fetching new retailer_product_id values to create mapping...")
    cursor.execute("SELECT retailer_product_id, retailer_id, retailer_item_code FROM public.retailer_products_new;")
    new_rp_map = {(r_id, r_code): rp_id for rp_id, r_id, r_code in cursor.fetchall()}
    
    old_listingid_to_new_rpid_map = {}
    for old_listing_id, (retailerid, itemcode) in temp_listing_map.items():
        new_rp_id = new_rp_map.get((retailerid, itemcode))
        if new_rp_id:
            old_listingid_to_new_rpid_map[old_listing_id] = new_rp_id
            
    print(f"    -> Successfully created {len(old_listingid_to_new_rpid_map)} listing-to-retailer-product mappings.")
    return old_listingid_to_new_rpid_map

def migrate_prices(cursor, old_listingid_to_new_rpid_map):
    """
    Migrates prices from the old 'prices' table to 'prices_new'.
    """
    print("  [3/3] Migrating prices...")
    cursor.execute("""
        SELECT
            prices.listingid,
            rpl.storeid,
            prices.price,
            prices.priceupdatetimestamp
        FROM
            public.prices
        INNER JOIN
            public.retailerproductlistings rpl ON prices.listingid = rpl.listingid;
    """)
    old_prices = cursor.fetchall()
    
    new_prices_data = []
    for price_entry in old_prices:
        old_listingid, storeid, price, price_ts = price_entry
        new_retailer_product_id = old_listingid_to_new_rpid_map.get(old_listingid)
        
        if new_retailer_product_id and storeid:
            new_prices_data.append((new_retailer_product_id, storeid, price, price_ts))
            
    print(f"    -> Bulk inserting {len(new_prices_data)} price records...")
    execute_batch(cursor,
        """
        INSERT INTO public.prices_new (retailer_product_id, store_id, price, price_timestamp)
        VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;
        """,
        new_prices_data, page_size=50000
    )
    print("    -> Price migration complete.")


def main():
    """Main function to run the entire migration process."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            clear_new_tables(cursor)
            
            old_to_new_product_map = migrate_products(cursor)
            
            old_listingid_to_new_rpid_map = migrate_retailer_products(cursor, old_to_new_product_map)
            
            migrate_prices(cursor, old_listingid_to_new_rpid_map)
            
            conn.commit()
            print("\n✅ All data migrated successfully!")
        
    except (Exception, psycopg2.Error) as error:
        print(f"❌ Migration failed. Rolling back changes. Error: {error}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("🛑 Database connection closed.")

if __name__ == '__main__':
    main()