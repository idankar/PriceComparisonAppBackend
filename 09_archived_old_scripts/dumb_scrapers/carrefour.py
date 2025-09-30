"""
Carrefour Israel Price Scraper

This script scrapes product price data from prices.carrefour.co.il
It handles the client-side pagination system by extracting all file URLs
from the JavaScript arrays (files and files_html) that contain all available data.

Key Features:
- Extracts ALL files from JavaScript arrays (4132 files across 414 pages)
- Downloads all price files from all Carrefour stores in Israel
- Handles UTF-16 encoded XML files
- Processes gzipped price files  
- Outputs standardized JSON format
- Progress tracking with batch processing

Usage:
- Set TEST_MODE = True for quick test (3-4 files)
- Set TEST_MODE = False to download all price files
"""

import time
import gzip
import os
import json
import re
import base64
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from lxml import etree

# --- Configuration ---
TEST_MODE = False  # Set to False to process all files
RETAILER_NAME = "Mega/Carrefour Israel"
RETAILER_ID = 1
BASE_URL = "https://prices.carrefour.co.il/"
STORES_FILE_URL = "https://prices.carrefour.co.il/20250630/Stores7290055700007-202506300001.xml"

OUTPUT_DIR = 'raw_scraper_output'
PRODUCTS_OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'mega_carrefour_israel_products.json')
PROMOTIONS_OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'mega_carrefour_israel_promotions.json')

store_code_to_store_id_map = {}

def extract_info_from_filename(filename):
    """Extracts store code from the filename."""
    store_code = None
    # Pattern: Price7290055700007-XXXX-YYYYMMDDHHmm.gz
    # We want the XXXX part (store code), not the timestamp
    match = re.search(r'-(\d+)-\d{12}\.(xml|gz)', filename)
    if match:
        store_code = match.group(1).lstrip("0") or "0"
    return {'store_code': store_code}

def parse_stores_xml_and_map(xml_bytes):
    """Parses the stores XML and populates the global store map."""
    global store_code_to_store_id_map
    if not xml_bytes: 
        print("  [ERROR] No XML bytes to parse")
        return
    
    try:
        # First, ensure we have bytes
        if isinstance(xml_bytes, str):
            xml_bytes = xml_bytes.encode('utf-8')
        
        # Check for UTF-16 BOM
        if xml_bytes.startswith(b'\xff\xfe') or xml_bytes.startswith(b'\xfe\xff'):
            print("  [INFO] Detected UTF-16 encoding")
            try:
                # UTF-16 LE (little endian)
                if xml_bytes.startswith(b'\xff\xfe'):
                    xml_string = xml_bytes.decode('utf-16-le')
                # UTF-16 BE (big endian)
                else:
                    xml_string = xml_bytes.decode('utf-16-be')
            except:
                # Fallback to UTF-16 with BOM handling
                xml_string = xml_bytes.decode('utf-16')
        # Check if it's gzipped
        elif xml_bytes.startswith(b'\x1f\x8b'):
            print("  [INFO] Decompressing gzipped XML")
            xml_bytes = gzip.decompress(xml_bytes)
            # After decompression, check encoding again
            if xml_bytes.startswith(b'\xff\xfe') or xml_bytes.startswith(b'\xfe\xff'):
                xml_string = xml_bytes.decode('utf-16')
            else:
                xml_string = xml_bytes.decode('utf-8', errors='ignore')
        else:
            # Try UTF-8 first
            try:
                xml_string = xml_bytes.decode('utf-8')
            except:
                xml_string = xml_bytes.decode('utf-8', errors='ignore')
        
        # Clean up any potential issues
        xml_string = xml_string.strip()
        if xml_string.startswith('\ufeff'):  # Remove BOM if present
            xml_string = xml_string[1:]
        
        print(f"  [DEBUG] XML starts with: {xml_string[:50]}...")
        
        # Parse the XML
        parser = etree.XMLParser(recover=True, encoding='utf-8', remove_blank_text=True)
        root = etree.fromstring(xml_string.encode('utf-8'), parser=parser)
        
        if root is None:
            print("  [ERROR] Failed to parse XML - root is None")
            return
        
        print(f"  [INFO] Root element: {root.tag}")
        
        # Try different paths to find stores
        stores = root.findall(".//Store") or root.findall(".//store") or root.findall(".//{*}Store")
        
        if not stores:
            print(f"  [WARNING] No Store elements found. Checking structure...")
            # Print first few child elements to understand structure
            for i, child in enumerate(root):
                if i < 5:
                    print(f"    Child {i}: {child.tag}")
                    # Check if this is a Stores container
                    if 'Store' in child.tag:
                        stores = [child]
                        break
                    # Check children of children
                    for subchild in child:
                        if 'Store' in subchild.tag:
                            stores = child.findall(".//Store")
                            break
        
        print(f"  [INFO] Found {len(stores)} Store elements")
        
        for store_elem in stores:
            # Try different variations of StoreId
            store_id = (store_elem.findtext('StoreId') or 
                       store_elem.findtext('storeId') or 
                       store_elem.findtext('StoreID') or
                       store_elem.findtext('StoreNr') or
                       store_elem.findtext('.//StoreId'))
            
            if store_id:
                # Clean the store ID (remove leading zeros)
                store_id_clean = store_id.lstrip('0') or '0'
                store_code_to_store_id_map[store_id_clean] = int(store_id)
                print(f"    Mapped store: {store_id} -> {store_id_clean}")
            else:
                # Try to get store ID from attributes
                store_id = store_elem.get('StoreId') or store_elem.get('id')
                if store_id:
                    store_id_clean = store_id.lstrip('0') or '0'
                    store_code_to_store_id_map[store_id_clean] = int(store_id)
    
    except etree.XMLSyntaxError as e:
        print(f"  [ERROR] XML Syntax Error: {e}")
        print(f"  [DEBUG] Error at line {e.lineno}, column {e.column}")
    except Exception as e:
        print(f"  [ERROR] Stores XML Parsing Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"  [INFO] Mapped {len(store_code_to_store_id_map)} stores from the Stores file.")

def parse_promos_xml(xml_bytes):
    """
    Parses the Promo XML file for promotion data using a namespace-aware approach.
    """
    promotions = []
    if not xml_bytes: 
        print("    [ERROR] No XML bytes to parse for promo")
        return promotions
    
    try:
        # --- File pre-processing (your existing code is good) ---
        if isinstance(xml_bytes, str):
            xml_bytes = xml_bytes.encode('utf-8')
        
        if xml_bytes.startswith(b'\x1f\x8b'):
            xml_bytes = gzip.decompress(xml_bytes)
        
        if xml_bytes.startswith(b'\xff\xfe') or xml_bytes.startswith(b'\xfe\xff'):
            xml_string = xml_bytes.decode('utf-16')
        else:
            xml_string = xml_bytes.decode('utf-8', errors='ignore')

        xml_string = xml_string.strip()
        if xml_string.startswith('\ufeff'):
            xml_string = xml_string[1:]

        parser = etree.XMLParser(recover=True, encoding='utf-8')
        root = etree.fromstring(xml_string.encode('utf-8'), parser=parser)
        
        if root is None:
            print("    [ERROR] Failed to parse promo XML - root is None")
            return promotions

        # --- NAMESPACE-AWARE PARSING LOGIC ---
        
        # 1. Determine the namespace, if any
        nsmap = root.nsmap
        # The default namespace often has no key, or a key like 'def'
        # We create a dictionary that lxml can use for queries.
        ns = {'ns': nsmap[None]} if nsmap and None in nsmap else None

        # 2. Find the <Promotions> container tag using the namespace
        promotions_container = root.find("ns:Promotions", namespaces=ns) if ns else root.find("Promotions")
        
        if promotions_container is None:
            print(f"    [DEBUG] Could not find a <Promotions> container tag. Root tag is <{root.tag}>.")
            print(f"    [INFO] Successfully parsed 0 promotions")
            return promotions
        
        # 3. Find all <Promotion> tags within that container
        promo_elements = promotions_container.findall("ns:Promotion", namespaces=ns) if ns else promotions_container.findall("Promotion")

        for promo_elem in promo_elements:
            # Helper to find text within a namespaced element
            def find_text(element, tag_name):
                return element.findtext(f"ns:{tag_name}", namespaces=ns) if ns else element.findtext(tag_name)

            # 4. Find the <PromotionItems> container for each promotion
            items_container = promo_elem.find("ns:PromotionItems", namespaces=ns) if ns else promo_elem.find("PromotionItems")
            items_list = []
            if items_container is not None:
                item_elements = items_container.findall("ns:Item", namespaces=ns) if ns else items_container.findall("Item")
                for item in item_elements:
                    item_code = find_text(item, 'ItemCode')
                    if item_code:
                        items_list.append(item_code)

            promo_data = {
                'PromotionID': find_text(promo_elem, 'PromotionId'), # Corrected from 'PromotionID' to match XML
                'PromotionDescription': (find_text(promo_elem, 'PromotionDescription') or "").strip(),
                'PromotionStartDate': find_text(promo_elem, 'PromotionStartDate'),
                'PromotionStartHour': find_text(promo_elem, 'PromotionStartHour'),
                'PromotionEndDate': find_text(promo_elem, 'PromotionEndDate'),
                'PromotionEndHour': find_text(promo_elem, 'PromotionEndHour'),
                'Items': items_list
            }
            if promo_data['PromotionID'] and promo_data['Items']:
                promotions.append(promo_data)
        
        print(f"    [INFO] Successfully parsed {len(promotions)} promotions")
        return promotions
        
    except Exception as e:
        print(f"    [ERROR] Promo XML Parsing Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
    
    return []

def parse_prices_xml(xml_bytes):
    """Parses the PriceFull XML file for item data."""
    items = []
    if not xml_bytes: 
        print("    [ERROR] No XML bytes to parse")
        return items
    
    try:
        # First, ensure we have bytes
        if isinstance(xml_bytes, str):
            xml_bytes = xml_bytes.encode('utf-8')
        
        # Check if it's gzipped
        if xml_bytes.startswith(b'\x1f\x8b'):
            print("    [INFO] Decompressing gzipped price data")
            xml_bytes = gzip.decompress(xml_bytes)
        
        # Check for UTF-16 encoding
        if xml_bytes.startswith(b'\xff\xfe') or xml_bytes.startswith(b'\xfe\xff'):
            print("    [INFO] Detected UTF-16 encoding in price file")
            xml_string = xml_bytes.decode('utf-16')
        else:
            # Try UTF-8
            try:
                xml_string = xml_bytes.decode('utf-8')
            except:
                xml_string = xml_bytes.decode('utf-8', errors='ignore')
        
        # Clean up any potential issues
        xml_string = xml_string.strip()
        if xml_string.startswith('\ufeff'):  # Remove BOM if present
            xml_string = xml_string[1:]
        
        # Parse the XML
        parser = etree.XMLParser(recover=True, encoding='utf-8', remove_blank_text=True)
        root = etree.fromstring(xml_string.encode('utf-8'), parser=parser)
        
        if root is None:
            print("    [ERROR] Failed to parse price XML - root is None")
            return items
        
        print(f"    [INFO] Price XML root element: {root.tag}")
        
        # Try different paths to find items
        item_elements = root.findall(".//Item") or root.findall(".//item") or root.findall(".//{*}Item")
        
        # If no items found, check the structure
        if not item_elements:
            print(f"    [WARNING] No Item elements found. Checking structure...")
            # Look for Items container
            items_container = root.find(".//Items") or root.find(".//{*}Items")
            if items_container is not None:
                item_elements = items_container.findall(".//Item")
            else:
                # Check first level children
                for child in root:
                    print(f"      Child element: {child.tag}")
                    if 'Item' in child.tag:
                        item_elements = [child]
                        break
                    # Check if this child contains items
                    sub_items = child.findall(".//Item")
                    if sub_items:
                        item_elements = sub_items
                        break
        
        print(f"    [INFO] Found {len(item_elements)} Item elements")
        
        # Debug: Show structure of first item
        if item_elements and len(item_elements) > 0:
            first_item = item_elements[0]
            print("    [DEBUG] First item structure:")
            for child in first_item:
                text = child.text[:50] if child.text else "None"
                print(f"      - {child.tag}: {text}")
        
        for elem in item_elements:
            item_data = {}
            
            # Try different field names (case variations)
            item_data['ItemCode'] = (elem.findtext('ItemCode') or 
                                    elem.findtext('itemCode') or 
                                    elem.findtext('ItemNr') or
                                    elem.findtext('Barcode') or
                                    elem.findtext('ItemBarcode') or
                                    elem.findtext('barcode'))
            
            item_data['ItemNm'] = (elem.findtext('ItemName') or 
                                  elem.findtext('ItemNm') or 
                                  elem.findtext('itemName') or
                                  elem.findtext('Name') or
                                  elem.findtext('ItemDesc') or
                                  elem.findtext('Description'))
            
            item_data['ItemPrice'] = (elem.findtext('ItemPrice') or 
                                     elem.findtext('itemPrice') or 
                                     elem.findtext('Price') or
                                     elem.findtext('price') or
                                     elem.findtext('UnitPrice'))
            
            # Only add if we have at least barcode and price
            if item_data['ItemCode'] and item_data['ItemPrice']:
                items.append(item_data)
            elif item_data['ItemCode'] or item_data['ItemPrice']:
                # Debug incomplete items
                print(f"      [WARNING] Incomplete item: Code={item_data['ItemCode']}, Price={item_data['ItemPrice']}")
        
        print(f"    [INFO] Successfully parsed {len(items)} complete items")
        
        # Debug: Show a sample item
        if items:
            sample = items[0]
            print(f"    [DEBUG] Sample item: Code={sample['ItemCode']}, Name={sample['ItemNm'][:30] if sample['ItemNm'] else 'None'}, Price={sample['ItemPrice']}")
        
        return items
        
    except etree.XMLSyntaxError as e:
        print(f"    [ERROR] Price XML Syntax Error: {e}")
    except Exception as e:
        print(f"    [ERROR] Price XML Parsing Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
    
    return []

def standardize_product(raw_item, store_id):
    """Converts a raw item into our standard JSON format."""
    try:
        barcode, price = raw_item.get('ItemCode'), raw_item.get('ItemPrice')
        if not barcode or not price: return None
        return {"retailer_name": RETAILER_NAME, "retailer_id": RETAILER_ID, "store_id": int(store_id), "product_name": raw_item.get('ItemNm', 'Unknown'), "price": float(price), "retailer_item_code": str(barcode), "image_url": None}
    except (ValueError, TypeError): return None

def standardize_promotion(raw_promo):
    """Converts a raw promo item into our standard JSON format."""
    try:
        start_hour = raw_promo.get('PromotionStartHour', '00:00:00')
        end_hour = raw_promo.get('PromotionEndHour', '23:59:59')
        
        return {
            "retailer_name": RETAILER_NAME,
            "retailer_id": RETAILER_ID,
            "promotion_id": raw_promo.get('PromotionID'),
            "promotion_description": raw_promo.get('PromotionDescription'),
            "start_date": f"{raw_promo.get('PromotionStartDate')} {start_hour}",
            "end_date": f"{raw_promo.get('PromotionEndDate')} {end_hour}",
            "items": raw_promo.get('Items', [])
        }
    except Exception:
        return None

def download_via_javascript(driver, url):
    """Download file using JavaScript fetch API"""
    filename = url.split('/')[-1]
    print(f"  Attempting JavaScript download for: {filename}")
    
    script = """
    async function downloadFile(url) {
        try {
            const response = await fetch(url, {
                method: 'GET',
                credentials: 'same-origin',
                headers: {
                    'Accept': '*/*',
                    'Accept-Encoding': 'gzip, deflate, br'
                }
            });
            
            if (!response.ok) {
                return {success: false, error: 'HTTP ' + response.status};
            }
            
            const blob = await response.blob();
            const arrayBuffer = await blob.arrayBuffer();
            const bytes = new Uint8Array(arrayBuffer);
            
            // Convert to base64 for transfer
            let binary = '';
            const len = bytes.byteLength;
            for (let i = 0; i < len; i++) {
                binary += String.fromCharCode(bytes[i]);
            }
            const base64 = window.btoa(binary);
            
            return {
                success: true,
                data: base64,
                type: response.headers.get('content-type'),
                size: bytes.byteLength
            };
        } catch (error) {
            return {success: false, error: error.toString()};
        }
    }
    
    return await downloadFile(arguments[0]);
    """
    
    try:
        result = driver.execute_script(script, url)
        
        if result and result.get('success'):
            # Decode base64 data
            data = base64.b64decode(result['data'])
            print(f"    Successfully downloaded {result['size']} bytes via JavaScript")
            
            # Debug: Check if it's gzipped
            if data[:2] == b'\x1f\x8b':
                print(f"    File appears to be gzipped")
            elif data[:5] == b'<?xml':
                print(f"    File appears to be XML")
            elif b'<html' in data[:1000].lower():
                print(f"    WARNING: File appears to be HTML (blocked)")
                return None
                
            return data
        else:
            print(f"    JavaScript download failed: {result.get('error', 'Unknown error')}")
            return None
    except Exception as e:
        print(f"    JavaScript download error: {e}")
        return None

def main():
    print(f"--- Starting Simple Scraper for: {RETAILER_NAME} ---")
    driver = None
    downloaded_files = {}
    
    try:
        options = uc.ChromeOptions()
        options.add_argument('--disable-blink-features=AutomationControlled')
        print("  [INFO] Launching Chrome browser...")
        try:
            driver = uc.Chrome(options=options, version_main=137)
        except Exception:
            from selenium import webdriver
            driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(60)

        print("\n--- Visiting the main page ---")
        driver.get(BASE_URL)
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        
        print("\n--- Extracting all file links from JavaScript arrays ---")
        try:
            all_files_data = driver.execute_script("return typeof files !== 'undefined' ? files.map((filename, i) => ({filename, html: files_html[i]})) : []")
            all_file_urls = []
            for file_data in all_files_data:
                soup = BeautifulSoup(file_data['html'], 'html.parser')
                link = soup.find('a', class_='downloadBtn')
                if link and link.get('href'):
                    url = link.get('href')
                    all_file_urls.append(url if url.startswith('http') else BASE_URL + url.lstrip('/'))
            
            if STORES_FILE_URL not in all_file_urls: all_file_urls.append(STORES_FILE_URL)
            print(f"--- Total unique file URLs found: {len(all_file_urls)} ---")
        except Exception as e:
            print(f"  [ERROR] Could not extract files from JS: {e}")
            return
        
        price_files_urls = [url for url in all_file_urls if 'Price' in url and 'Promo' not in url]
        promo_files_urls = [url for url in all_file_urls if 'Promo' in url]
        stores_files_urls = [url for url in all_file_urls if 'Stores' in url]
            
        print(f"  - Price files: {len(price_files_urls)}")
        print(f"  - Promo files: {len(promo_files_urls)}")
        print(f"  - Stores files: {len(stores_files_urls)}")
        
        files_to_download = stores_files_urls + price_files_urls + promo_files_urls
        
        if TEST_MODE:
            test_files = []
            if stores_files_urls: test_files.append(stores_files_urls[0])
            if price_files_urls: test_files.extend(price_files_urls[:2])
            if promo_files_urls: test_files.append(promo_files_urls[0])
            files_to_download = test_files
            print(f"  TEST MODE: Limited to {len(files_to_download)} essential files")
        
        print(f"\n--- Downloading {len(files_to_download)} files ---")
        for url in files_to_download:
            filename = url.split('/')[-1]
            if filename not in downloaded_files:
                content = download_via_javascript(driver, url)
                if content: downloaded_files[filename] = content
        
        print(f"\n--- Processing {len(downloaded_files)} downloaded files ---")
        
        # Process stores file first
        for filename, content in downloaded_files.items():
            if 'Stores' in filename:
                print(f"\n  Processing stores file: {filename}")
                parse_stores_xml_and_map(content)
                break
        
        # Process price files
        all_scraped_products = []
        price_filenames = sorted([f for f in downloaded_files if 'Price' in f and 'Promo' not in f], reverse=True)
        print(f"\n  Processing {len(price_filenames)} price files...")
        for filename in price_filenames:
            content = downloaded_files[filename]
            file_info = extract_info_from_filename(filename)
            store_code = file_info.get('store_code')
            current_store_id = store_code_to_store_id_map.get(store_code, store_code)
            if not current_store_id: continue
            
            parsed_products = parse_prices_xml(content)
            for item in parsed_products:
                if sp := standardize_product(item, current_store_id):
                    all_scraped_products.append(sp)

        # Process promo files
        all_scraped_promotions = []
        promo_filenames = sorted([f for f in downloaded_files if 'Promo' in f], reverse=True)
        print(f"\n  Processing {len(promo_filenames)} promo files...")
        for filename in promo_filenames:
            content = downloaded_files[filename]
            parsed_promos = parse_promos_xml(content)
            for promo in parsed_promos:
                if sp := standardize_promotion(promo):
                    all_scraped_promotions.append(sp)
            
        print(f"\n  Final tally: {len(all_scraped_products)} products, {len(all_scraped_promotions)} promotions")
        
        # Save results
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(PRODUCTS_OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_scraped_products, f, ensure_ascii=False, indent=4)
        with open(PROMOTIONS_OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_scraped_promotions, f, ensure_ascii=False, indent=4)
            
        print(f"\n > Finished scraping.")
        print(f" > Saved {len(all_scraped_products)} products to {PRODUCTS_OUTPUT_FILE}")
        print(f" > Saved {len(all_scraped_promotions)} promotions to {PROMOTIONS_OUTPUT_FILE}")
        
    finally:
        if driver:
            print("\n--- Closing browser ---")
            driver.quit()

if __name__ == "__main__":
    main()