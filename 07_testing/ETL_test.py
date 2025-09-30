import requests
import re
import time
import gzip
import xml.etree.ElementTree as ET
import json
import psycopg2
import psycopg2.extras
from datetime import datetime
import io
import zipfile
import pandas as pd
import os
import textdistance # You might need to run: pip install textdistance

# --- SCRIPT MODE ---
# Set to True to run a test on one file, saving output locally without DB changes.
# Set to False to run the full scrape and load to the database.
TEST_MODE = True

# --- PostgreSQL Configuration ---
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "price_comparison_app_v2" # Updated to use the v2 DB
PG_USER = "postgres"
PG_PASSWORD = "your_password" # !!! IMPORTANT: Use your actual PostgreSQL password !!!

# --- Good Pharm Configuration ---
RETAILER_NAME = "Good Pharm"
BASE_URL = "https://goodpharm.binaprojects.com"
FILE_LIST_URL = f"{BASE_URL}/MainIO_Hok.aspx"
DOWNLOAD_URL_ENDPOINT = f"{BASE_URL}/Download.aspx"
GOOD_PHARM_CHAIN_ID = "7290058197699"

# --- ETL Configuration ---
GOLDEN_FILE_PATH = '/Users/noa/Desktop/PriceComparisonApp/superpharm_products_enriched.jsonl'
# NEW: Path to the JSON file containing the list of known brands.
BRANDS_FILE_PATH = './discovered_brands.json'
JACCARD_THRESHOLD = 0.8 # Threshold for considering names a potential match

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Referer': f"{BASE_URL}/Main.aspx",
}

# --- ETL Helper Functions ---

def load_known_brands(file_path):
    """Loads the list of known brands from a JSON file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            brands = json.load(f)
        print(f"✅ Successfully loaded {len(brands)} brands from '{file_path}'.")
        return brands
    except FileNotFoundError:
        print(f"⚠️ WARNING: Brands file not found at '{file_path}'. Using a small default list.")
        return ['מייבלין', 'אססי', 'לוריאל', 'פפסי']
    except Exception as e:
        print(f"❌ Error loading brands file: {e}")
        return []

def clean_text(text):
    """Converts text to lowercase and removes punctuation for better matching."""
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)
    return text

def get_jaccard_similarity(text1, text2):
    """Calculates Jaccard similarity between two strings."""
    set1 = set(text1.split())
    set2 = set(text2.split())
    return textdistance.jaccard(set1, set2)

def extract_brand_from_name(item_name, known_brands):
    """Intelligently extracts a brand from a product name string."""
    item_name = item_name or '' # Ensure item_name is not None
    for brand in known_brands:
        if item_name.startswith(brand):
            clean_name = item_name.replace(brand, '', 1).strip(' -')
            return brand, clean_name
    parts = item_name.split(' - ')
    if len(parts) > 1:
        return parts[0].strip(), ' - '.join(parts[1:]).strip()
    return "Unknown", item_name

def process_and_transform_items(items_from_xml, df_golden, known_brands):
    """
    Takes a list of raw item dictionaries from the XML, cleans them,
    and matches them against the golden standard DataFrame, choosing the best name.
    """
    processed_items = []
    skipped_count = 0
    matched_count = 0
    
    print("  [ETL] Starting product transformation and matching process...")

    # Pre-clean golden data for faster matching
    df_golden['clean_name'] = df_golden['name'].apply(clean_text)
    df_golden['clean_brand'] = df_golden['brand'].apply(clean_text)
    golden_groups = df_golden.groupby('clean_brand')

    for raw_item in items_from_xml:
        raw_item_name = raw_item.get('ItemName', '')
        
        brand, clean_name = extract_brand_from_name(raw_item_name, known_brands)
        
        if brand == "Unknown" or "שקית" in raw_item_name:
            skipped_count += 1
            continue

        best_match_golden_product = None
        highest_score = 0
        clean_candidate_name = clean_text(clean_name)
        clean_candidate_brand = clean_text(brand)

        if clean_candidate_brand in golden_groups.groups:
            potential_matches = golden_groups.get_group(clean_candidate_brand)
            for _, golden_product in potential_matches.iterrows():
                score = get_jaccard_similarity(clean_candidate_name, golden_product['clean_name'])
                if score > highest_score:
                    highest_score = score
                    best_match_golden_product = golden_product
        
        canonical_product_id = None
        status = "New (Needs Consolidation)"
        final_canonical_name = clean_name

        if highest_score >= JACCARD_THRESHOLD:
            matched_count += 1
            status = "Matched"
            canonical_product_id = best_match_golden_product['productId']
            if len(best_match_golden_product['name']) > len(clean_name):
                final_canonical_name = best_match_golden_product['name']
        
        processed_items.append({
            'retailer_item_code': raw_item.get('ItemCode'),
            'raw_name': raw_item_name,
            'extracted_brand': brand,
            'final_canonical_name': final_canonical_name,
            'price': float(raw_item.get('ItemPrice', 0)),
            'price_update_date': raw_item.get('PriceUpdateDate'),
            'match_status': status,
            'match_score': highest_score,
            'canonical_product_id': canonical_product_id,
        })
    
    print(f"  [ETL] Products processed: {len(items_from_xml)}. Skipped: {skipped_count}. Matched: {matched_count}.")
    return pd.DataFrame(processed_items)

# --- Database & Scraper Functions ---

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD)
        return conn
    except psycopg2.Error as e:
        print(f"[CRITICAL DB ERROR] Unable to connect to PostgreSQL: {e}")
        raise

def get_xml_element_text(parent_element, child_tag_name, default=None):
    """Safely gets text from an XML element."""
    child = parent_element.find(child_tag_name)
    if child is not None and child.text is not None:
        return child.text.strip()
    return default

def parse_prices_xml_content(xml_string):
    """Parses PriceFull XML content and returns a list of item dictionaries."""
    products_list = []
    try:
        root = ET.fromstring(xml_string)
        item_elements = root.findall('.//Items/Item')
        for item_element in item_elements:
            products_list.append({
                'PriceUpdateDate': get_xml_element_text(item_element, 'PriceUpdateDate'),
                'ItemCode': get_xml_element_text(item_element, 'ItemCode'),
                'ItemName': get_xml_element_text(item_element, 'ItemNm'),
                'ItemPrice': get_xml_element_text(item_element, 'ItemPrice', '0'),
            })
        print(f"  [XML PARSE] Found {len(products_list)} product price entries.")
        return products_list
    except ET.ParseError as e:
        print(f"  [ERROR] XML Price Parsing Error: {e}")
        return []

def parse_promotions_xml_content(xml_string):
    """Parses PromoFull XML content and returns a structured list of promotions."""
    promotions_list = []
    try:
        root = ET.fromstring(xml_string)
        promo_elements = root.findall('.//Promotions/Promotion')
        for promo_element in promo_elements:
            promotions_list.append({
                'promotion_id': get_xml_element_text(promo_element, 'PromotionId'),
                'description': get_xml_element_text(promo_element, 'PromotionDescription'),
                'start_date': get_xml_element_text(promo_element, 'PromotionStartDate'),
                'end_date': get_xml_element_text(promo_element, 'PromotionEndDate'),
                'items': [
                    item.find('ItemCode').text for item in promo_element.findall('.//PromotionItems/Item')
                    if item.find('ItemCode') is not None and item.find('ItemCode').text is not None
                ]
            })
        print(f"  [XML PARSE] Found {len(promotions_list)} promotions.")
        return promotions_list
    except ET.ParseError as e:
        print(f"  [ERROR] XML Promotion Parsing Error: {e}")
        return []

def fetch_file_metadata(session):
    """Gets the list of all file metadata."""
    print("--- Fetching file metadata from Good Pharm portal ---")
    try:
        response = session.post(FILE_LIST_URL, headers=HEADERS, timeout=60)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"  [CRITICAL] Failed to fetch file list: {e}")
        return []

def download_and_decompress(session, file_name):
    """Downloads and decompresses a file, returning its content."""
    params = {'FileNm': file_name}
    try:
        response = session.post(DOWNLOAD_URL_ENDPOINT, params=params, headers=HEADERS, timeout=60)
        response.raise_for_status()
        final_url = response.json()[0]['SPath']
        
        response = session.get(final_url, headers=HEADERS, timeout=120)
        response.raise_for_status()
        
        file_content_bytes = response.content
        
        if file_content_bytes.startswith(b'PK'):
            print("    [INFO] File is a ZIP archive. Extracting...")
            with io.BytesIO(file_content_bytes) as zip_buffer:
                with zipfile.ZipFile(zip_buffer) as zip_file:
                    xml_files = [f for f in zip_file.namelist() if f.lower().endswith('.xml')]
                    if not xml_files: return None
                    return zip_file.read(xml_files[0]).decode('utf-8-sig')
        else:
            print("    [INFO] File appears to be GZIP. Decompressing...")
            return gzip.decompress(file_content_bytes).decode('utf-8-sig')

    except Exception as e:
        print(f"    [ERROR] Failed during download/decompression for {file_name}: {e}")
    return None

# --- Main Execution Block ---
if __name__ == "__main__":
    session = requests.Session()
    
    # --- NEW: Load brands from JSON file ---
    KNOWN_BRANDS = load_known_brands(BRANDS_FILE_PATH)
    if not KNOWN_BRANDS:
        print("Could not load brand list. Aborting.")
        exit()
    
    try:
        df_golden = pd.read_json(GOLDEN_FILE_PATH, lines=True)
        print(f"✅ Loaded {len(df_golden)} products from the Super-Pharm golden file.")
    except Exception as e:
        print(f"❌ Error loading golden file: {e}")
        exit()

    file_metadata_list = fetch_file_metadata(session)
    if not file_metadata_list:
        exit()
        
    prices_files = [f for f in file_metadata_list if f['FileNm'].startswith('PriceFull')]
    promos_files = [f for f in file_metadata_list if f['FileNm'].startswith('PromoFull')]
    
    print(f"\n--- Starting ETL Process in {'TEST' if TEST_MODE else 'LIVE'} Mode ---")

    if TEST_MODE:
        prices_files = prices_files[:1] if prices_files else []
        promos_files = promos_files[:1] if promos_files else []
        print(f"Running in TEST_MODE. Will process {len(prices_files)} price file and {len(promos_files)} promo file.")
    
    # --- Process Prices ---
    if prices_files:
        for file_meta in prices_files:
            file_name = file_meta['FileNm']
            print(f"\nProcessing Price file: {file_name}")
            xml_content = download_and_decompress(session, file_name)
            if not xml_content: continue
            raw_items = parse_prices_xml_content(xml_string=xml_content)
            if not raw_items: continue
            df_clean_data = process_and_transform_items(raw_items, df_golden, KNOWN_BRANDS)

            if TEST_MODE:
                processed_output_path = f"./{file_name}_prices_processed.json"
                df_clean_data.to_json(processed_output_path, orient='records', indent=2, force_ascii=False)
                print(f"  --> Saved PROCESSED PRICES JSON to: {processed_output_path}")
            else:
                print("  --> LIVE MODE: Price data processed. Ready for DB load.")
    
    # --- Process Promotions ---
    if promos_files:
        for file_meta in promos_files:
            file_name = file_meta['FileNm']
            print(f"\nProcessing Promotion file: {file_name}")
            xml_content = download_and_decompress(session, file_name)
            if not xml_content: continue
            promotions_data = parse_promotions_xml_content(xml_string=xml_content)
            
            if TEST_MODE:
                processed_output_path = f"./{file_name}_promotions_processed.json"
                with open(processed_output_path, 'w', encoding='utf-8') as f:
                    json.dump(promotions_data, f, indent=2, ensure_ascii=False)
                print(f"  --> Saved PROCESSED PROMOTIONS JSON to: {processed_output_path}")
            else:
                print("  --> LIVE MODE: Promotion data processed. Ready for DB load.")

    print("\n--- Script finished ---")
