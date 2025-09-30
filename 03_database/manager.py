import os
import subprocess
import json
import psycopg2
from database_logic import find_or_create_product, insert_listing_and_price

# --- CONFIGURATION ---

# 1. List of all your scraper scripts
#    The manager will run these one by one.
SCRAPERS_TO_RUN = [
    'official_scrapers/Carrefour_scraper.py',
    'official_scrapers/Good_pharm_scraper.py',
    'official_scrapers/Hahishook_scraper.py',
    'official_scrapers/Hazi_Hinam_scraper.py',
    'official_scrapers/Rami_Levi_scraper.py',
    'official_scrapers/shufersal_scraper.py',
    'official_scrapers/Shuk_Hair_Scraper.py',
    'official_scrapers/super_pharm_scraper.py',
    'official_scrapers/super_sapir_scraper.py',
    'official_scrapers/TivTaam_scraper.py',
    'official_scrapers/victory_scraper.py',
    'official_scrapers/Ybitan_scraper.py',
    'official_scrapers/yohananof_scraper.py',
    'official_scrapers/Zol_Vebegadol_scraper.py'
]

# 2. Directory where scrapers will save their raw output
RAW_OUTPUT_DIR = 'raw_scraper_output'

# 3. Database Connection Details
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DATABASE = os.environ.get("PG_DATABASE", "price_comparison_app")
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "***REMOVED***")


def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD
    )

def run_manager():
    """
    Main orchestration script.
    1. Runs all scrapers.
    2. Processes the raw data from each scraper's output file.
    3. Loads the cleaned data into the main database.
    """
    print("--- Starting Daily Scrape & Ingestion Manager ---")

    # --- Stage 1: EXTRACT (Run all scrapers) ---
    os.makedirs(RAW_OUTPUT_DIR, exist_ok=True) # Ensure the output directory exists
    
    for scraper_path in SCRAPERS_TO_RUN:
        print(f"\nRunning scraper: {scraper_path}...")
        try:
            # This runs the scraper script as a separate process
            # It assumes the scraper will save its output to a file in RAW_OUTPUT_DIR
            subprocess.run(['python', scraper_path], check=True, capture_output=True, text=True)
            print(f" > {scraper_path} finished successfully.")
        except subprocess.CalledProcessError as e:
            print(f" ! ERROR: {scraper_path} failed!")
            print(f"   - Exit Code: {e.returncode}")
            print(f"   - Stderr: {e.stderr}")
            # Optional: Add logic here to send an email or Slack notification on failure
            continue # Continue to the next scraper even if one fails

    # --- Stage 2: TRANSFORM & LOAD ---
    print("\n--- Starting Data Processing and Database Load ---")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Process files in a consistent order
            for filename in sorted(os.listdir(RAW_OUTPUT_DIR)):
                if filename.endswith('.json'):
                    filepath = os.path.join(RAW_OUTPUT_DIR, filename)
                    print(f"\nProcessing file: {filepath}...")
                    
                    with open(filepath, 'r', encoding='utf-8') as f:
                        try:
                            scraped_data = json.load(f)
                        except json.JSONDecodeError:
                            print(f" ! ERROR: Could not decode JSON from {filename}. Skipping.")
                            continue
                    
                    for item in scraped_data:
                        product_name = item.get('product_name')
                        price = item.get('price')
                        store_id = item.get('store_id')
                        retailer_item_code = item.get('retailer_item_code')

                        if not all([product_name, price, store_id, retailer_item_code]):
                            print(f"  - Skipping item due to missing data: {item}")
                            continue

                        # Use our centralized logic to get the correct master product ID
                        canonical_id = find_or_create_product(product_name, cur)
                        
                        if canonical_id:
                            # Now, load the price and listing into the database
                            insert_listing_and_price(canonical_id, store_id, retailer_item_code, price, cur)
            
            # Commit all changes for this run
            conn.commit()
            print("\n--- Daily Ingestion Complete ---")
            
    except Exception as e:
        print(f"\nFATAL ERROR during database processing: {e}")
        if conn:
            conn.rollback() # Roll back any partial changes if an error occurs
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    run_manager()