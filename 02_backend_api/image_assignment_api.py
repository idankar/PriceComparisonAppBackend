import psycopg2
import requests
import os

# ==============================================================================
# --- CONFIGURATION ---
# ==============================================================================
# --- Database Credentials ---
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "price_comparison_app_v2"
PG_USER = "postgres"
PG_PASSWORD = "***REMOVED***"

# --- API Keys (IMPORTANT: Fill these in) ---
BARCODE_LOOKUP_API_KEY = "YOUR_BARCODE_LOOKUP_API_KEY"
GOOGLE_API_KEY = "YOUR_GOOGLE_CLOUD_API_KEY"
GOOGLE_SEARCH_ENGINE_ID = "YOUR_Google Search_ENGINE_ID"

# How many products to process in this test run
TEST_LIMIT = 10

# ==============================================================================
# --- API CLIENT FUNCTIONS ---
# ==============================================================================
def get_image_from_barcode_api(barcode):
    """Queries the Barcode Lookup API and returns an image URL if found."""
    if not BARCODE_LOOKUP_API_KEY or BARCODE_LOOKUP_API_KEY == "YOUR_BARCODE_LOOKUP_API_KEY":
        return None
        
    url = f"https://api.barcodelookup.com/v3/products?barcode={barcode}&key={BARCODE_LOOKUP_API_KEY}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data.get('products') and data['products'][0].get('images'):
            return data['products'][0]['images'][0]
    except requests.RequestException as e:
        print(f"    [WARN] Barcode API request failed: {e}")
    except (KeyError, IndexError):
        pass
    return None

def get_image_from_Google_Search(product_name, brand):
    """Queries the Google Custom Search API and returns the first image URL."""
    if not GOOGLE_API_KEY or GOOGLE_API_KEY == "YOUR_GOOGLE_CLOUD_API_KEY":
        return None

    query = f"{brand} {product_name}"
    url = f"https://www.googleapis.com/customsearch/v1?key={GOOGLE_API_KEY}&cx={GOOGLE_SEARCH_ENGINE_ID}&q={query}&searchType=image"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if "items" in data and len(data["items"]) > 0:
            return data["items"][0]["link"]
    except requests.RequestException as e:
        print(f"    [WARN] Google Search API request failed: {e}")
    except (KeyError, IndexError):
        pass
    return None

# ==============================================================================
# --- DATABASE FUNCTIONS ---
# ==============================================================================
def get_db_connection():
    try:
        return psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD)
    except psycopg2.Error as e:
        print(f"[DB CRITICAL] Unable to connect: {e}")
        raise

def fetch_products_without_images(conn, limit):
    """Fetches products that need an image, along with one of their barcodes."""
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT DISTINCT ON (p.product_id)
                p.product_id,
                p.canonical_name,
                p.brand,
                rp.retailer_item_code
            FROM products p
            JOIN retailer_products rp ON p.product_id = rp.product_id
            WHERE p.image_url IS NULL
              AND rp.retailer_item_code ~ '^\d{12,13}$'
            LIMIT %s;
        """, (limit,))
        return cursor.fetchall()

def update_product_image(conn, product_id, image_url):
    """Updates the image_url for a given product_id."""
    with conn.cursor() as cursor:
        cursor.execute("UPDATE products SET image_url = %s WHERE product_id = %s", (image_url, product_id))
    conn.commit()
    print(f"    -> ✅ Updated product {product_id} with image: {image_url}")

# ==============================================================================
# --- MAIN EXECUTION BLOCK ---
# ==============================================================================
if __name__ == "__main__":
    conn = get_db_connection()
    try:
        products_to_process = fetch_products_without_images(conn, TEST_LIMIT)
        if not products_to_process:
            print("No products found that need images. Exiting.")
        else:
            print(f"--- Starting image assignment test for {len(products_to_process)} products ---")

        for product_id, name, brand, barcode in products_to_process:
            print(f"\nProcessing: '{brand} {name}' (ID: {product_id}, Barcode: {barcode})")
            image_url = None

            # Step 1: Try the high-accuracy Barcode API first
            print("  -> Trying Barcode API...")
            image_url = get_image_from_barcode_api(barcode)
            if image_url:
                print("    -> Found image via Barcode API.")
                update_product_image(conn, product_id, image_url)
                continue

            # Step 2: If barcode fails, fall back to Google Image Search
            print("    -> Barcode API failed. Trying Google Image Search...")
            image_url = get_image_from_Google Search(name, brand)
            if image_url:
                print("    -> Found image via Google Search.")
                update_product_image(conn, product_id, image_url)
            else:
                print("    -> ❌ No image found for this product.")
    
    finally:
        if conn:
            conn.close()
            print("\n--- ✅ Test complete. Database connection closed. ---")