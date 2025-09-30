import requests
import re
import gzip
from xml.etree import ElementTree as ET
import json
import psycopg2
from psycopg2.extras import execute_batch, Json
import io
import zipfile
import pandas as pd
import os
from datetime import datetime
import urllib3
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import numpy
from psycopg2.extensions import register_adapter, AsIs
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from openai import OpenAI
import itertools
import collections
import argparse
# ==============================================================================
# --- CONFIGURATION ---
# ==============================================================================

# Database Configuration
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "price_comparison_app_v2"
PG_USER = "postgres"
PG_PASSWORD = "***REMOVED***"

# AI & File Configuration
# It's recommended to set your OpenAI key as an environment variable
# Example: export OPENAI_API_KEY='your_key_here'
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "***REMOVED***-TSybPJY6Yt9JIJ4k066J06XvV_Vz1E0QasT8jEEx6tZw70bg9RRMZQ-3oBBSjT3BlbkFJ2sUCNgLmgep2y2wrGb39IeJsJiVeEyLqiI_ufaK30DByYW6hkcyDdCx-Gsa0W63EmLZmy-bI4A")
LLM_MODEL = "gpt-4o"
CLASSIFICATION_VERDICTS_FILE = "classification_verdicts.json"
JACCARD_THRESHOLD_FOR_GROUPING = 0.90

# General Settings
HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'}
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Register numpy adapters for psycopg2
def addapt_numpy_types(numpy_number):
    return AsIs(numpy_number)
register_adapter(numpy.int64, addapt_numpy_types)
register_adapter(numpy.float64, addapt_numpy_types)

# ==============================================================================
# --- RETAILER CONFIGURATIONS ---
# ==============================================================================
RETAILERS = {
    'good_pharm': {
        'name': 'Good Pharm', 'retailer_id': 97, 'config': {
            'file_list_url': 'https://goodpharm.binaprojects.com/MainIO_Hok.aspx',
            'download_endpoint': 'https://goodpharm.binaprojects.com/Download.aspx'
        }
    },
    'super_pharm': {
        'name': 'Super-Pharm', 'retailer_id': 52, 'config': {
            'base_url': 'https://prices.super-pharm.co.il/'
        }
    },
    'be_pharm': {
        'name': 'Be Pharm', 'retailer_id': 150, 'config': {
            'base_url': 'https://prices.shufersal.co.il'
        }
    }
}

# ==============================================================================
# --- UTILITY FUNCTIONS ---
# ==============================================================================
def load_keywords_from_json(file_path):
    """Loads keywords from a JSON file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # Convert the list to a set for much faster 'in' checks
        print(f"-> Successfully loaded {len(data.get('keywords', []))} keywords from {file_path}")
        return set(data.get('keywords', []))
    except FileNotFoundError:
        print(f"[CRITICAL] Keyword file not found at '{file_path}'. Please create it. Classification will be limited.")
        return set()
    except json.JSONDecodeError:
        print(f"[CRITICAL] Error decoding JSON from '{file_path}'. Please check its format. Classification will be limited.")
        return set()

# Build an absolute path to the keywords file relative to the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
PHARMA_KEYWORDS_FILE = os.path.join(script_dir, "pharma_keywords.json")

# Load keywords from the external JSON file
PHARMA_KEYWORDS = load_keywords_from_json(PHARMA_KEYWORDS_FILE)

def create_resilient_session():
    """Creates a requests session with automatic retries."""
    session = requests.Session()
    retry_strategy = Retry(total=3, status_forcelist=[429, 500, 502, 503, 504], 
                          allowed_methods=["HEAD", "GET", "POST"], backoff_factor=1)
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        return psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DATABASE, 
                               user=PG_USER, password=PG_PASSWORD)
    except psycopg2.Error as e:
        print(f"[DB CRITICAL] Unable to connect: {e}")
        raise

def get_xml_element_text(parent, tag, default=None):
    """Safely gets text from an XML element."""
    child = parent.find(tag)
    return child.text.strip() if child is not None and child.text else default

# ==============================================================================
# --- METADATA & URL FETCHERS ---
# ==============================================================================
def fetch_goodpharm_metadata(session, config):
    """Fetches file metadata JSON from Good Pharm's endpoint."""
    try:
        response = session.post(config['file_list_url'], headers=HEADERS, timeout=60)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"[CRITICAL] Failed to fetch Good Pharm file list: {e}")
        return []

def get_goodpharm_download_url(session, file_name, config):
    """Gets the direct download URL for a Good Pharm file."""
    params = {'FileNm': file_name}
    try:
        response = session.post(config['download_endpoint'], params=params, headers=HEADERS, timeout=60)
        response.raise_for_status()
        return response.json()[0]['SPath']
    except Exception:
        return None

def fetch_superpharm_metadata(session, config):
    """Scrapes file metadata from Super-Pharm's web portal."""
    all_files, page_num = [], 1
    last_page_filenames = set()
    while True:
        page_url = f"{config['base_url']}?page={page_num}"
        print(f"  -> Scraping Super-Pharm page: {page_num}")
        try:
            response = session.get(page_url, headers=HEADERS, timeout=30, verify=False)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'lxml')
            table = soup.find('table', class_='gzTable')
            if not table: break
            
            rows = table.find('tbody').find_all('tr') if table.find('tbody') else []
            current_filenames = {cells[6].get_text(strip=True) for row in rows if len(cells := row.find_all('td')) >= 7}
            
            if not current_filenames or current_filenames == last_page_filenames:
                print("    -> Ending scrape for Super-Pharm")
                break
            
            last_page_filenames = current_filenames
            for row in rows:
                cells = row.find_all('td')
                if len(cells) > 5 and (link_tag := cells[5].find('a', href=True)):
                    all_files.append({'FileNm': cells[1].get_text(strip=True), 'getlink_url': urljoin(config['base_url'], link_tag['href'])})
            page_num += 1
            time.sleep(0.5)
        except requests.RequestException as e:
            print(f"[ERROR] Failed to scrape Super-Pharm page {page_num}: {e}")
            break
    return all_files

def get_superpharm_download_url(session, download_url, config):
    """For Super-Pharm, the metadata URL is the download URL."""
    return download_url

def fetch_bepharm_metadata(session, config):
    """Scrapes Be Pharm file metadata from the parent Shufersal portal."""
    all_files = []
    for cat_name, cat_id in [("PricesFull", 2), ("PromosFull", 4)]:
        page_num = 1
        last_page_filenames = set()
        while True:
            url = f"{config['base_url']}/FileObject/UpdateCategory?catID={cat_id}&storeId=0&page={page_num}"
            print(f"  -> Scraping Be Pharm {cat_name} page: {page_num}")
            try:
                response = session.get(url, headers=HEADERS, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'lxml')
                table = soup.find('table', class_='webgrid')
                if not table: break

                rows = table.find('tbody').find_all('tr') if table.find('tbody') else []
                current_filenames = {cells[6].get_text(strip=True) for row in rows if len(cells := row.find_all('td')) >= 7}
                
                if not current_filenames or current_filenames == last_page_filenames:
                    print(f"    -> Ending scrape for Be Pharm {cat_name}")
                    break
                
                last_page_filenames = current_filenames
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 7:
                        store_name = cells[5].get_text(strip=True)
                        if "BE" in store_name.upper():
                            link_tag = cells[0].find('a', href=True)
                            file_name = cells[6].get_text(strip=True)
                            if link_tag and file_name:
                                all_files.append({'FileNm': file_name, 'download_url': link_tag['href']})
                page_num += 1
                time.sleep(0.5)
            except requests.RequestException as e:
                print(f"[ERROR] Failed to scrape Be Pharm page {page_num}: {e}")
                break
    print(f"  -> Found {len(all_files)} Be Pharm files")
    return all_files

def get_bepharm_download_url(session, download_url, config):
    """For Be Pharm, the metadata URL is the download URL."""
    return download_url

# ==============================================================================
# --- XML PARSERS ---
# ==============================================================================
def parse_price_xml_shufersal_portal(xml_string, store_id, file_timestamp):
    """Parser for price files from Shufersal-based portals (Good Pharm, Be Pharm)."""
    items = []
    try:
        if xml_string.startswith('\ufeff'): xml_string = xml_string[1:]
        root = ET.fromstring(xml_string)
    except ET.ParseError as e:
        print(f"    [XML PARSE ERROR] {e}")
        return []

    items_container = root.find('.//Items') or root.find('.//ITEMS')
    item_elements = items_container.findall('Item') if items_container is not None else root.findall('.//Item')
    if not item_elements: return []

    for item in item_elements:
        item_name = get_xml_element_text(item, 'ItemName')
        if not item_name: continue
        items.append({
            'ItemCode': get_xml_element_text(item, 'ItemCode'),
            'ItemName': item_name,
            'ItemPrice': float(get_xml_element_text(item, 'ItemPrice', '0')),
            'RetailerStoreId': store_id,
            'PriceUpdateDate': get_xml_element_text(item, 'PriceUpdateDate') or file_timestamp.isoformat(),
        })
    return items

def parse_price_xml_superpharm(xml_string, store_id, file_timestamp):
    """Parser for Super-Pharm's unique price file format."""
    items = []
    try:
        root = ET.fromstring(xml_string)
    except ET.ParseError: return []

    for item in root.findall('.//Line'):
        item_name = get_xml_element_text(item, 'ItemName')
        if not item_name: continue
        items.append({
            'ItemCode': get_xml_element_text(item, 'ItemCode'),
            'ItemName': item_name,
            'ItemPrice': float(get_xml_element_text(item, 'ItemPrice', '0')),
            'RetailerStoreId': store_id,
            'PriceUpdateDate': get_xml_element_text(item, 'PriceUpdateDate') or file_timestamp.isoformat(),
        })
    return items

def parse_goodpharm_promo_xml(xml_string, store_id, file_timestamp):
    """Parser for Good Pharm's promotion file format."""
    promotions = []
    try:
        root = ET.fromstring(xml_string)
    except ET.ParseError: return []

    for promo_element in root.findall('.//Promotion'):
        promo_code = get_xml_element_text(promo_element, 'PromotionId')
        if not promo_code: continue
        item_codes = {item.text for item in promo_element.findall('.//PromotionItems/Item/ItemCode') if item.text}
        promotions.append({
            'PromotionCode': promo_code,
            'Description': get_xml_element_text(promo_element, 'PromotionDescription'),
            'StartDate': get_xml_element_text(promo_element, 'PromotionStartDate'),
            'EndDate': get_xml_element_text(promo_element, 'PromotionEndDate'),
            'ItemCodes': list(item_codes)
        })
    return promotions

def parse_superpharm_promo_xml(xml_string, store_id, file_timestamp):
    """Parser for promotion files from Shufersal-based portals (Super-Pharm, Be Pharm)."""
    promotions = {}
    try:
        root = ET.fromstring(xml_string)
    except ET.ParseError: return []

    for line in root.findall('.//Envelope/Header/Details/Line'):
        promo_details = line.find('PromotionDetails')
        if promo_details is None: continue
        promo_id = get_xml_element_text(line, 'PromotionId')
        item_code = get_xml_element_text(line, 'ItemCode')
        if not promo_id or not item_code: continue
        
        if promo_id not in promotions:
            promotions[promo_id] = {
                'PromotionCode': promo_id,
                'Description': get_xml_element_text(promo_details, 'PromotionDescription'),
                'StartDate': get_xml_element_text(promo_details, 'PromotionStartDate'),
                'EndDate': get_xml_element_text(promo_details, 'PromotionEndDate'),
                'ItemCodes': set()
            }
        promotions[promo_id]['ItemCodes'].add(item_code)
    
    return [{**promo, 'ItemCodes': list(promo['ItemCodes'])} for promo in promotions.values()]

# ==============================================================================
# --- PRODUCT CLASSIFICATION & TRANSFORMATION ---
# ==============================================================================
def parse_product_details(name):
    """Extracts structured attributes like size and scent from a product name string."""
    attributes = {}
    name = str(name)
    
    size_pattern = r'(\d+(\.\d+)?)\s*(מ"ל|מל|גרם|ג"ר|גר|ליטר|ל\'|ק"ג|קג|יחידות|יח\')'
    size_match = re.search(size_pattern, name)
    if size_match:
        attributes['size_value'] = float(size_match.group(1))
        unit = size_match.group(3).replace('"',"").replace("'", "")
        unit_map = {'מל': 'ml', 'גר': 'g', 'גרם': 'g', 'ג"ר': 'g', 'ליטר': 'l', 'ל': 'l', 'קג': 'kg', 'ק"ג': 'kg', 'יחידות': 'units', 'יח': 'units'}
        attributes['size_unit'] = unit_map.get(unit, unit)
        name = name.replace(size_match.group(0), '')
        
    scent_match = re.search(r'בניחוח\s*([\w\s]+)', name)
    if scent_match:
        attributes['scent'] = scent_match.group(1).strip()
        name = name.replace(scent_match.group(0), '')

    cleaned_name = re.sub(r'\s{2,}', ' ', name).strip(' -')
    return cleaned_name, attributes

def get_known_pharma_barcodes(conn, pharma_retailer_id):
    """Fetches barcodes from a reference retailer (Good Pharm) for cross-classification."""
    print(f"    -> Fetching known pharma barcodes from retailer {pharma_retailer_id}...")
    with conn.cursor() as cursor:
        cursor.execute("SELECT retailer_item_code FROM retailer_products WHERE retailer_id = %s", (pharma_retailer_id,))
        barcodes = {row[0] for row in cursor.fetchall()}
        print(f"    -> Found {len(barcodes)} known barcodes")
        return barcodes

def get_llm_classification_verdict(product_name: str, client: OpenAI) -> dict:
    """Queries an LLM to classify a product, with retries."""
    prompt = f"""Is the following an item typically sold in a pharmacy/drugstore (health, beauty, baby, hygiene, OTC meds), not a general grocery item? Respond ONLY with a valid JSON object: {{"is_pharmacy_product": boolean}}. Product Name: "{product_name}" """
    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model=LLM_MODEL, 
                messages=[{"role": "user", "content": prompt}], 
                response_format={"type": "json_object"}, 
                temperature=0.0, 
                max_tokens=20
            )
            verdict = json.loads(response.choices[0].message.content)
            if isinstance(verdict.get("is_pharmacy_product"), bool): return verdict
        except Exception as e:
            print(f"    ! LLM Error (Attempt {attempt+1}/3): {e}"); time.sleep(2)
    return {"is_pharmacy_product": False} # Default to False on failure

def classify_using_llm(df_to_classify):
    """Groups similar products and uses an LLM to classify them efficiently."""
    print(f"    -> Stage 3: Classifying {len(df_to_classify)} remaining products using AI")
    if not OPENAI_API_KEY:
        print("    ! WARNING: OPENAI_API_KEY not set. Skipping AI classification.")
        return set()
    
    # Group similar product names to reduce API calls
    df_to_classify['tokens'] = df_to_classify['ItemName'].apply(lambda name: set(str(name).split()))
    product_map = df_to_classify.set_index('ItemCode').to_dict('index')
    adj = collections.defaultdict(list)
    if len(product_map) < 2000: # Optimization to avoid huge combination checks
        for code1, code2 in itertools.combinations(product_map.keys(), 2):
            set1, set2 = product_map[code1]['tokens'], product_map[code2]['tokens']
            if len(set1.union(set2)) > 0:
                if (len(set1.intersection(set2)) / len(set1.union(set2))) > JACCARD_THRESHOLD_FOR_GROUPING:
                    adj[code1].append(code2); adj[code2].append(code1)
    
    visited, groups = set(), []
    for code in product_map:
        if code not in visited:
            current_group, q = [], collections.deque([code])
            visited.add(code)
            while q:
                current = q.popleft()
                current_group.append(current)
                for neighbor in adj[current]:
                    if neighbor not in visited: visited.add(neighbor); q.append(neighbor)
            groups.append(current_group)
    
    print(f"      -> Grouped into {len(groups)} unique entities for AI analysis")
    
    # Load cached verdicts and query LLM for new ones
    try:
        with open(CLASSIFICATION_VERDICTS_FILE, 'r', encoding='utf-8') as f: verdicts = json.load(f)
    except FileNotFoundError: verdicts = {}
    
    llm_client = OpenAI(api_key=OPENAI_API_KEY)
    llm_approved_codes = set()
    
    for group in groups:
        rep_name = product_map[group[0]]['ItemName']
        if rep_name in verdicts:
            is_pharmacy = verdicts[rep_name].get("is_pharmacy_product")
        else:
            print(f"      - AI Query: '{rep_name}'")
            verdict = get_llm_classification_verdict(rep_name, llm_client)
            is_pharmacy = verdict.get('is_pharmacy_product', False)
            verdicts[rep_name] = verdict
            # Cache the new verdict
            with open(CLASSIFICATION_VERDICTS_FILE, 'w', encoding='utf-8') as f: json.dump(verdicts, f, indent=2, ensure_ascii=False)
        
        if is_pharmacy:
            llm_approved_codes.update(group)
            
    return llm_approved_codes

def advanced_classify_and_transform(df, retailer_name, conn):
    """Runs the full classification and transformation pipeline on a DataFrame of products."""
    print(f"  -> Starting advanced classification for {len(df)} items from {retailer_name}")
    df['ItemName'] = df['ItemName'].fillna('')
    df_unique_products = df.drop_duplicates(subset=['ItemCode']).copy()
    pharma_item_codes = set()

    # Retailers like Super-Pharm and Be Pharm require a more advanced filter
    if retailer_name in ['Super-Pharm', 'Be Pharm']:
        # Stage 1: Cross-reference with known Good Pharm barcodes
        good_pharm_barcodes = get_known_pharma_barcodes(conn, RETAILERS['good_pharm']['retailer_id'])
        approved_by_gp = df_unique_products['ItemCode'].isin(good_pharm_barcodes)
        pharma_item_codes.update(df_unique_products[approved_by_gp]['ItemCode'])
        remaining_products = df_unique_products[~approved_by_gp]
        print(f"    -> Stage 1 (Cross-Reference): Approved {len(pharma_item_codes)} products")
        
        # Stage 2: Filter remaining by keywords
        approved_by_keyword = remaining_products['ItemName'].apply(lambda name: any(keyword in name for keyword in PHARMA_KEYWORDS))
        keyword_approved_products = remaining_products[approved_by_keyword]
        pharma_item_codes.update(keyword_approved_products['ItemCode'])
        remaining_products = remaining_products[~approved_by_keyword]
        print(f"    -> Stage 2 (Keywords): Approved {len(keyword_approved_products)} additional products")
        
        # Stage 3: Use LLM for any ambiguous products that are left
        if not remaining_products.empty:
            llm_approved_codes = classify_using_llm(remaining_products)
            pharma_item_codes.update(llm_approved_codes)
    else: # For a dedicated pharma store like Good Pharm, a simple keyword filter is enough
        is_pharma = df_unique_products['ItemName'].apply(lambda name: any(keyword in name for keyword in PHARMA_KEYWORDS))
        pharma_item_codes.update(df_unique_products[is_pharma]['ItemCode'])

    print(f"  -> Total approved pharma products: {len(pharma_item_codes)}")
    pharma_df = df[df['ItemCode'].isin(pharma_item_codes)].copy()
    if pharma_df.empty: return pharma_df
    
    # Transform approved products
    pharma_df['brand'] = pharma_df['ItemName'].apply(lambda x: x.split(' ')[0] if x else '')
    parsed_data = pharma_df['ItemName'].apply(parse_product_details)
    pharma_df['canonical_name'] = parsed_data.apply(lambda x: x[0])
    pharma_df['attributes'] = parsed_data.apply(lambda x: x[1])
    
    return pharma_df

# ==============================================================================
# --- FILE PROCESSING ---
# ==============================================================================
def download_and_parse_xml(session, url, file_name, store_id, file_ts, parser_func):
    """Downloads a file, decompresses if needed, and passes content to a specific parser."""
    try:
        print(f"    -> Downloading: {url[:80]}...")
        response = session.get(url, headers=HEADERS, timeout=120, verify=False)
        response.raise_for_status()
        content = response.content
        print(f"    -> Downloaded {len(content)} bytes")
        
        xml_string = ""
        if content.startswith(b'PK\x03\x04'): # ZIP file
            with io.BytesIO(content) as bio, zipfile.ZipFile(bio) as zip_file:
                xml_name = [n for n in zip_file.namelist() if n.lower().endswith('.xml')][0]
                xml_string = zip_file.read(xml_name).decode('utf-8-sig')
        elif content.startswith(b'\x1f\x8b'): # GZIP file
            xml_string = gzip.decompress(content).decode('utf-8-sig')
        else: # Plain XML
            xml_string = content.decode('utf-8-sig')
            
        print(f"    -> Parsing XML with {parser_func.__name__}")
        result = parser_func(xml_string, store_id, file_ts)
        if isinstance(result, list): print(f"    -> Parsed {len(result)} items/promotions")
        return result
    except Exception as e:
        print(f"    -> Error downloading/parsing {file_name}: {e}")
        return []

# ==============================================================================
# --- DATABASE OPERATIONS ---
# ==============================================================================
def load_prices_to_db(processed_items, retailer_id, conn):
    """Upserts products and inserts prices into the database."""
    if processed_items.empty: return 0
    try:
        with conn.cursor() as cursor:
            # Upsert products and create a mapping
            products_to_upsert = processed_items[['canonical_name', 'brand', 'attributes']].drop_duplicates(subset=['canonical_name', 'brand'])
            product_map = {}
            for _, row in products_to_upsert.iterrows():
                cursor.execute("""
                    INSERT INTO products (canonical_name, brand, attributes) 
                    VALUES (%s, %s, %s) 
                    ON CONFLICT (LOWER(canonical_name), LOWER(brand)) 
                    DO UPDATE SET attributes = EXCLUDED.attributes RETURNING product_id;
                """, (row['canonical_name'], row['brand'], Json(row['attributes'])))
                product_map[(row['canonical_name'], row['brand'])] = cursor.fetchone()[0]
            
            processed_items['product_id'] = processed_items.apply(lambda row: product_map.get((row['canonical_name'], row['brand'])), axis=1)

            # Upsert retailer_products and create a mapping
            retailer_product_map = {}
            for _, row in processed_items[['product_id', 'ItemCode']].drop_duplicates().iterrows():
                cursor.execute("""
                    INSERT INTO retailer_products (product_id, retailer_id, retailer_item_code) 
                    VALUES (%s, %s, %s) 
                    ON CONFLICT (retailer_id, retailer_item_code) 
                    DO UPDATE SET product_id = EXCLUDED.product_id RETURNING retailer_product_id;
                """, (row['product_id'], retailer_id, row['ItemCode']))
                retailer_product_map[(row['product_id'], row['ItemCode'])] = cursor.fetchone()[0]
            
            processed_items['retailer_product_id'] = processed_items.apply(
                lambda row: retailer_product_map.get((row['product_id'], row['ItemCode'])), axis=1
            )
            
            # Get store mappings
            cursor.execute("SELECT retailerspecificstoreid, storeid FROM stores WHERE retailerid = %s;", (retailer_id,))
            store_map = {str(rs_id): s_id for rs_id, s_id in cursor.fetchall()}
            processed_items['store_id'] = processed_items['RetailerStoreId'].map(store_map)
            
            # Insert prices
            prices_to_insert = processed_items.dropna(subset=['store_id', 'retailer_product_id'])[
                ['retailer_product_id', 'store_id', 'ItemPrice', 'PriceUpdateDate']
            ].to_records(index=False)
            
            if len(prices_to_insert) > 0:
                execute_batch(cursor, """
                    INSERT INTO prices (retailer_product_id, store_id, price, price_timestamp) 
                    VALUES (%s, %s, %s, %s) 
                    ON CONFLICT (retailer_product_id, store_id, price_timestamp) DO NOTHING;
                """, prices_to_insert)
                
            print(f"  [DB] ✅ Loaded {len(prices_to_insert)} price items")
            return len(prices_to_insert)
    except Exception as e:
        print(f"  [DB ERROR] {e}")
        raise

def load_promotions_to_db(promotions, retailer_id, conn):
    """Upserts promotions and links them to products."""
    if not promotions:
        return 0
    try:
        with conn.cursor() as cursor:
            # Get a map of all relevant retailer_product_ids for this retailer
            cursor.execute("SELECT retailer_item_code, retailer_product_id FROM retailer_products WHERE retailer_id = %s", (retailer_id,))
            rp_map = {code: rp_id for code, rp_id in cursor.fetchall()}
            
            links_to_insert = []
            for promo in promotions:
                cursor.execute("""
                    INSERT INTO promotions (retailer_id, retailer_promotion_code, description, start_date, end_date)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (retailer_id, retailer_promotion_code) DO UPDATE SET description = EXCLUDED.description
                    RETURNING promotion_id;
                """, (retailer_id, promo['PromotionCode'], promo['Description'], promo['StartDate'], promo['EndDate']))
                promotion_id = cursor.fetchone()[0]

                # Create links for products that exist in our db
                for item_code in promo['ItemCodes']:
                    if retailer_product_id := rp_map.get(item_code):
                        links_to_insert.append((promotion_id, retailer_product_id))
            
            if links_to_insert:
                execute_batch(cursor, """
                    INSERT INTO promotion_product_links (promotion_id, retailer_product_id)
                    VALUES (%s, %s) ON CONFLICT DO NOTHING;
                """, links_to_insert)

            print(f"  [DB] ✅ Loaded {len(promotions)} promotions and {len(links_to_insert)} product links.")
            return len(links_to_insert)
    except Exception as e:
        print(f"  [DB ERROR] {e}")
        raise

def log_file_processing(retailer_id, filename, file_type, status, error_message=None, rows_added=0):
    """Logs the outcome of processing a file to the filesprocessed table."""
    conn_log = get_db_connection()
    try:
        with conn_log.cursor() as cursor:
            cursor.execute("""
                INSERT INTO filesprocessed (retailerid, filename, filetype, processingstatus, errormessage, rowsadded) 
                VALUES (%s, %s, %s, %s, %s, %s) 
                ON CONFLICT (retailerid, filename) DO UPDATE SET 
                processingstatus = EXCLUDED.processingstatus, 
                errormessage = EXCLUDED.errormessage,
                rowsadded = EXCLUDED.rowsadded, 
                processingendtime = NOW();
            """, (retailer_id, filename, file_type, status, str(error_message)[:1000] if error_message else None, rows_added))
        conn_log.commit()
    except Exception as e:
        print(f"  [LOGGING ERROR] {e}")
    finally:
        conn_log.close()

def get_processed_files(conn, retailer_id):
    """Gets a set of successfully processed filenames for a retailer to avoid reprocessing."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT filename FROM filesprocessed WHERE retailerid = %s AND processingstatus = 'SUCCESS'", (retailer_id,))
        return {row[0] for row in cursor.fetchall()}

def classify_and_load_data(conn, retailer_id, retailer_name, all_price_items, all_promotions):
    """
    Takes consolidated data, classifies it once, and loads it into the database.
    """
    try:
        # Process prices if any were found
        if all_price_items:
            print(f"-> Consolidating {len(all_price_items)} price items from all files...")
            df = pd.DataFrame(all_price_items)
            
            print("-> Starting unified classification...")
            pharma_df = advanced_classify_and_transform(df, retailer_name, conn)
            
            print("-> Loading price data into database...")
            load_prices_to_db(pharma_df, retailer_id, conn)

        # Process promotions if any were found
        if all_promotions:
            print(f"-> Consolidating and loading {len(all_promotions)} promotions...")
            load_promotions_to_db(all_promotions, retailer_id, conn)
            
        # Commit all database changes at the end
        conn.commit()
        print("\n[DB] ✅ All data successfully committed to the database.")

    except Exception as e:
        print(f"\n[FATAL ERROR] An error occurred during the final data loading stage: {e}")
        conn.rollback()
        print("[DB] ❌ Database transaction rolled back.")

# ==============================================================================
# --- MAIN PIPELINE ---
# ==============================================================================
def process_retailer(retailer_key, limit_files=None):
    """
    The main processing pipeline for a single retailer, refactored for efficiency.
    It parses all files first, then classifies and loads the data in a single batch.
    """
    if retailer_key not in RETAILERS:
        print(f"❌ Unknown retailer: {retailer_key}")
        return
        
    retailer_info = RETAILERS[retailer_key]
    retailer_name = retailer_info['name']
    retailer_id = retailer_info['retailer_id']
    config = retailer_info['config']
    
    print(f"\n--- 🚚 Processing {retailer_name} (ID: {retailer_id}) ---")
    
    session = create_resilient_session()
    conn = get_db_connection()
    
    try:
        # Step 1: Assign the correct functions for the specified retailer
        print("\n[Step 1] Fetching file list...")
        if retailer_key == 'good_pharm':
            all_files = fetch_goodpharm_metadata(session, config)
            url_fetcher = get_goodpharm_download_url
            price_parser = parse_price_xml_shufersal_portal
            promo_parser = parse_goodpharm_promo_xml
        elif retailer_key == 'super_pharm':
            all_files = fetch_superpharm_metadata(session, config)
            url_fetcher = get_superpharm_download_url
            price_parser = parse_price_xml_superpharm
            promo_parser = parse_superpharm_promo_xml
        elif retailer_key == 'be_pharm':
            all_files = fetch_bepharm_metadata(session, config)
            url_fetcher = get_bepharm_download_url
            price_parser = parse_price_xml_shufersal_portal
            promo_parser = parse_superpharm_promo_xml
        
        if not all_files:
            print("No files found!")
            return
            
        print(f"  -> Found {len(all_files)} total files")

        # Step 2: Filter out files that have already been successfully parsed
        processed_files = get_processed_files(conn, retailer_id)
        files_to_process = [f for f in all_files if f['FileNm'] not in processed_files]
        
        if limit_files:
            files_to_process = files_to_process[:limit_files]
            
        print(f"\n[Step 2] Parsing {len(files_to_process)} new files (skipping {len(processed_files)} already processed)")
        
        # Step 3: Loop through files to parse and accumulate data
        all_price_items = []
        all_promotions = []
        
        for i, meta in enumerate(files_to_process, 1):
            file_name = meta['FileNm']
            print(f"\n[{i}/{len(files_to_process)}] Parsing: {file_name}")
            
            store_match = re.search(r'-(\d+)-', file_name)
            ts_match = re.search(r'-(\d{12,14})(?:\.|$)', file_name)
            
            if not store_match or not ts_match:
                print("  [SKIP] Invalid filename format")
                continue
                
            store_id = store_match.group(1)
            ts_str = ts_match.group(1)
            file_ts = datetime.strptime(ts_str, '%Y%m%d%H%M%S' if len(ts_str) == 14 else '%Y%m%d%H%M')
            
            try:
                download_url_part = meta.get('getlink_url') or meta.get('download_url')
                download_url = url_fetcher(session, download_url_part or file_name, config)
                
                if not download_url:
                    print("  [SKIP] Could not retrieve download URL")
                    continue

                if 'PriceFull' in file_name:
                    parsed_items = download_and_parse_xml(session, download_url, file_name, store_id, file_ts, price_parser)
                    if parsed_items:
                        all_price_items.extend(parsed_items)
                    log_file_processing(retailer_id, file_name, 'Price', 'SUCCESS', rows_added=0)

                elif 'PromoFull' in file_name:
                    parsed_promos = download_and_parse_xml(session, download_url, file_name, store_id, file_ts, promo_parser)
                    if parsed_promos:
                        all_promotions.extend(parsed_promos)
                    log_file_processing(retailer_id, file_name, 'Promo', 'SUCCESS', rows_added=0)

                # Commit after each file is logged as parsed
                conn.commit()

            except Exception as e:
                print(f"  [FATAL ERROR] Failed during parsing of {file_name}: {e}")
                conn.rollback()
                log_file_processing(retailer_id, file_name, 'Price' if 'PriceFull' in file_name else 'Promo', 'FAILED', error_message=str(e))
        
        # Step 4: After parsing all files, classify and load the consolidated data
        if all_price_items or all_promotions:
            print("\n[Step 3] All files parsed. Now classifying and loading data in a single batch...")
            classify_and_load_data(conn, retailer_id, retailer_name, all_price_items, all_promotions)

    finally:
        conn.close()
        
    print(f"\n--- ✅ {retailer_name} Processing Complete ---")

def main():
    """Parses command-line arguments and runs the ETL for selected retailers."""
    parser = argparse.ArgumentParser(description='Unified Pharmacy ETL Pipeline')
    parser.add_argument('--retailers', nargs='+', choices=['good_pharm', 'super_pharm', 'be_pharm', 'all'], default=['all'], help='Retailers to process (default: all)')
    parser.add_argument('--limit', type=int, help='Limit number of files per retailer (for testing)')
    args = parser.parse_args()
    
    if 'all' in args.retailers:
        retailers_to_process = ['good_pharm', 'super_pharm', 'be_pharm']
    else:
        retailers_to_process = args.retailers
        
    print("=== 🏥 Unified Pharmacy ETL Pipeline ===")
    print(f"Processing retailers: {', '.join(retailers_to_process)}")
    if args.limit:
        print(f"Limiting to {args.limit} files per retailer")
        
    for retailer_key in retailers_to_process:
        try:
            process_retailer(retailer_key, limit_files=args.limit)
        except Exception as e:
            print(f"❌ A critical error occurred while processing {retailer_key}: {e}")
            continue
            
    print("\n=== 🎉 All Processing Complete ===")

if __name__ == "__main__":
    main()