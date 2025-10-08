# image_acquisition_v6_prioritized.py
import os
import csv
import time
import requests
import psycopg2
import logging
import re
from urllib.parse import urlparse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- CONFIGURATION ---
logging.basicConfig(
    filename='image_acquisition.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DATABASE = os.environ.get("PG_DATABASE", "price_comparison_app")
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD")

GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GOOGLE_CSE_ID = os.environ.get("GOOGLE_CSE_ID")

MIN_IMAGE_WIDTH = 400
MIN_IMAGE_HEIGHT = 400

# Whitelist of preferred domains for our priority check
ACCEPTABLE_DOMAINS = {
    'm.media-amazon.com', 'i.ebayimg.com', 'res.cloudinary.com', 
    'd226b0iufwcjmj.cloudfront.net', 'img.gs1.co', 'pricez.co.il',
    'www.pricez.co.il', 'www.dynstore.co.il', 'www.foodis.co.il',
    '365mashbir.co.il', 'i.makeupstore.co.il', 'commons.wikimedia.org'
}

OUTPUT_CSV_FILE = "products_without_images.csv"
TEST_MODE = True
TEST_MODE_LIMIT = 20

# --- HELPER FUNCTIONS ---

def get_products_to_process(conn):
    print("Fetching products that need images from target retailers...")
    target_retailer_ids = [150, 97, 52]
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT p.masterproductid, p.brand, p.productname
            FROM products p
            JOIN retailerproductlistings rpl ON p.masterproductid = rpl.masterproductid
            LEFT JOIN product_images pi ON p.masterproductid = pi.masterproductid
            WHERE rpl.retailerid = ANY(%s) AND pi.image_id IS NULL
            ORDER BY p.masterproductid;
        """, (target_retailer_ids,))
        products = cur.fetchall()
        print(f"  > Found {len(products)} products to process.")
        return products

def clean_search_query(name: str, brand: str) -> str:
    heb_stop_words = ['מארז', 'מבצע', 'יחידות', 'של', 'עם', 'בטעם', 'אריזת']
    name = re.sub(r'|'.join(map(re.escape, heb_stop_words)), '', name)
    # --- FIXED: Corrected the regex to avoid the FutureWarning ---
    name = re.sub(r'[,"\'./-]+', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return f"{brand or ''} {name}".strip()

def search_api(query: str):
    logging.info(f"Searching API for query: '{query}'")
    try:
        url = "https://www.googleapis.com/customsearch/v1"
        params = {'key': GOOGLE_API_KEY, 'cx': GOOGLE_CSE_ID, 'q': query,
                  'searchType': 'image', 'num': 10}
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json().get('items', [])
    except requests.exceptions.RequestException as e:
        logging.error(f"API Request Failed for query '{query}': {e}")
        return None

def find_best_image_with_priority(results: list):
    """
    --- NEW LOGIC: Analyzes results based on a priority system. ---
    Priority 1: Creative Commons License
    Priority 2: From an ACCEPTABLE_DOMAIN
    Priority 3: Any other valid image
    """
    if not results: return None
    
    cc_image = None
    trusted_domain_image = None
    other_valid_image = None

    for item in results:
        img_info = item.get('image', {})
        width, height = img_info.get('width', 0), img_info.get('height', 0)
        url = item.get('link')

        if not url or not url.startswith(('http://', 'https://')):
            continue

        if width >= MIN_IMAGE_WIDTH and height >= MIN_IMAGE_HEIGHT:
            image_details = {'image_url': url,
                             'source_domain': urlparse(item.get('image', {}).get('contextLink', '')).netloc,
                             'source_page_url': item.get('image', {}).get('contextLink'),
                             'license_type': item.get('rights'),
                             'width': width, 'height': height}
            
            # Check priorities
            if image_details['license_type']:
                cc_image = image_details
                break # A CC image is always the best, so we can stop looking
            
            if image_details['source_domain'] in ACCEPTABLE_DOMAINS and not trusted_domain_image:
                trusted_domain_image = image_details
            
            if not other_valid_image:
                other_valid_image = image_details
    
    # Return the best image found based on our priority system
    return cc_image or trusted_domain_image or other_valid_image

def add_image_to_db(conn, product_id: int, image_data: dict):
    # (Unchanged)
    with conn.cursor() as cur:
        cur.execute("SELECT masterproductid FROM product_images WHERE image_url = %s", (image_data['image_url'],))
        existing_row = cur.fetchone()
        if existing_row:
            log_msg = f"URL {image_data['image_url']} is already in use for PID {existing_row[0]}. Skipping insert for PID {product_id}."
            print(f"  > WARNING: {log_msg}")
            logging.warning(log_msg)
            return

        cur.execute("SELECT 1 FROM product_images WHERE masterproductid = %s", (product_id,))
        is_primary = not cur.fetchone()

        cur.execute("""
            INSERT INTO product_images (masterproductid, image_url, source_domain, license_type, width, height, is_primary_image, source_page_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, (product_id, image_data['image_url'], image_data['source_domain'], image_data['license_type'],
              image_data['width'], image_data['height'], is_primary, image_data['source_page_url']))
        
        log_msg = f"Successfully inserted image for PID {product_id} from {image_data['source_page_url']}"
        logging.info(log_msg)

def write_not_found_csv(not_found_list: list, filename: str):
    # (Unchanged)
    if not not_found_list:
        if not TEST_MODE: print("All products processed have found an image. No CSV generated.")
        return
    print(f"Writing {len(not_found_list)} products without images to '{filename}'...")
    logging.info(f"Writing {len(not_found_list)} products without images to '{filename}'.")
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['masterproductid', 'brand', 'productname'])
        writer.writerows(not_found_list)

# --- MAIN EXECUTION ---
def main():
    logging.info("--- Starting Image Acquisition Script (V6 - Prioritized) ---")
    print("--- Starting Image Acquisition Script (V6 - Prioritized) ---")
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        print("FATAL: Please set GOOGLE_API_KEY and GOOGLE_CSE_ID in your .env file.")
        logging.critical("API Keys not set. Exiting.")
        return

    conn = None
    not_found_list = []
    try:
        conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD)
        products_to_process = get_products_to_process(conn)
        
        if TEST_MODE:
            print(f"--- RUNNING IN TEST MODE: PROCESSING FIRST {TEST_MODE_LIMIT} PRODUCTS ---")
            products_to_process = products_to_process[:TEST_MODE_LIMIT]

        if not products_to_process:
            print("No products to process. Exiting.")
            return

        for i, (pid, brand, name) in enumerate(products_to_process):
            query = clean_search_query(name, brand)
            print(f"[{i+1}/{len(products_to_process)}] Processing PID {pid}: Searching for '{query}'...")
            
            # --- MODIFIED: Use the single, broad search API call ---
            results = search_api(query)
            
            if results:
                # --- MODIFIED: Use the new prioritized selection logic ---
                best_image = find_best_image_with_priority(results)
                if best_image:
                    print(f"  > SUCCESS: Found image from {best_image['source_domain']} ({best_image['width']}x{best_image['height']})")
                    add_image_to_db(conn, pid, best_image)
                    conn.commit()
                else:
                    msg = "No image in results met quality/format criteria."
                    print(f"  > FAILED: {msg}")
                    logging.warning(f"PID {pid} ({query}): {msg}")
                    not_found_list.append((pid, brand, name))
            else:
                msg = "No results returned from API."
                print(f"  > FAILED: {msg}")
                logging.warning(f"PID {pid} ({query}): {msg}")
                not_found_list.append((pid, brand, name))
            
            time.sleep(0.5) # Be respectful to the API

    except KeyboardInterrupt:
        print("\n! KeyboardInterrupt detected. Saving progress before exiting.")
    except Exception as e:
        print(f"\nFATAL ERROR: An error occurred: {e}")
        logging.critical(f"A fatal error occurred: {e}", exc_info=True)
        if conn: conn.rollback()
    finally:
        write_not_found_csv(not_found_list, OUTPUT_CSV_FILE)
        if conn:
            conn.close()
            logging.info("Database connection closed.")
            print("\nDatabase connection closed.")
        logging.info("--- Script Finished ---")
        print("--- Script Finished ---")

if __name__ == "__main__":
    main()