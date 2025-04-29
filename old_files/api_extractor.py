#!/usr/bin/env python3
#!/usr/bin/env python3
# src/api_extractor.py - Extract product data directly from Shufersal API

import os
import sys
import json
import csv
import requests
import logging
from urllib.parse import quote

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(config.LOGS_DIR, "api_extractor.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def extract_products_from_api(query, page_limit=5):
    """
    Extract product data directly from Shufersal API
    
    Args:
        query (str): Product to search for (e.g., "נוטלה")
        page_limit (int): Maximum number of pages to fetch
        
    Returns:
        list: Extracted product information
    """
    logger.info(f"🔍 Extracting products for query: {query}")
    
    # Get query-specific paths
    paths = config.get_query_paths(query)
    
    # Ensure output directories exist
    config.ensure_dir(paths["ocr_dir"])
    config.ensure_dir(paths["screenshots_dir"])
    
    all_products = []
    current_page = 0
    encoded_query = quote(query)  # URL encode the query
    
    # Headers to mimic a browser request
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://www.shufersal.co.il/online/he/search",
        "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7"
    }
    
    while current_page < page_limit:
        # Build the API URL
        url = f"https://www.shufersal.co.il/online/he/search/results?q={encoded_query}&relevance&limit=10&page={current_page}"
        
        try:
            logger.info(f"Fetching page {current_page + 1}...")
            response = requests.get(url, headers=headers)
            
            # Save the raw API response for debugging
            response_path = os.path.join(paths["screenshots_dir"], f"api_response_page_{current_page + 1}.json")
            with open(response_path, 'w', encoding='utf-8') as f:
                json.dump(response.json(), f, ensure_ascii=False, indent=2)
            
            # Check if the request was successful
            if response.status_code == 200:
                data = response.json()
                
                # Extract products from this page
                products = data.get("results", [])
                
                if not products:
                    logger.info("No more products found.")
                    break
                
                # Add these products to our list
                all_products.extend(products)
                logger.info(f"Found {len(products)} products on page {current_page + 1}")
                
                # Check if there are more pages
                pagination = data.get("pagination", {})
                if current_page >= pagination.get("numberOfPages", 0) - 1:
                    logger.info("Reached the last page.")
                    break
                
                # Move to the next page
                current_page += 1
            else:
                logger.error(f"Failed to fetch page {current_page + 1}. Status code: {response.status_code}")
                break
                
        except Exception as e:
            logger.error(f"Error fetching data: {str(e)}")
            break
    
    # Process and extract products
    processed_products = []
    
    # Filter and process the products
    for product in all_products:
        # Check if it's actually a product related to the search query
        name = product.get("name", "").lower()
        brand = product.get("brandName", "").lower()
        description = product.get("description", "").lower()
        
        # First extract relevant fields
        processed_product = {
            "query": query,
            "code": product.get("code"),
            "product_name": product.get("name"),
            "description": product.get("description"),
            "price": product.get("price", {}).get("value"),
            "unit_description": product.get("unitDescription"),
            "brand": product.get("brandName"),
            "image_url": None
        }
        
        # Get the first product image (medium size)
        images = product.get("images", [])
        for image in images:
            if image.get("format") == "medium":
                processed_product["image_url"] = image.get("url")
                break
        
        # Download the product image if available
        if processed_product["image_url"]:
            try:
                image_name = f"{processed_product['code']}.png"
                image_path = os.path.join(paths["cropped_dir"], image_name)
                
                # Download image
                img_response = requests.get(processed_product["image_url"])
                if img_response.status_code == 200:
                    os.makedirs(os.path.dirname(image_path), exist_ok=True)
                    with open(image_path, 'wb') as img_file:
                        img_file.write(img_response.content)
                    processed_product["image_path"] = image_path
                    logger.info(f"Saved image: {image_path}")
            except Exception as e:
                logger.warning(f"Error downloading image: {str(e)}")
        
        processed_products.append(processed_product)
    
    # Save results to CSV
    output_csv = paths["ocr_csv"]
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    
    # Write to CSV
    with open(output_csv, mode="w", newline='', encoding='utf-8') as f:
        fieldnames = ["query", "code", "product_name", "description", "price", "unit_description", "brand", "image_url", "image_path"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(processed_products)
    
    logger.info(f"✅ Extracted {len(processed_products)} products, saved to {output_csv}")
    
    # Also append to master CSV
    append_to_master_csv(processed_products)
    
    return processed_products

def append_to_master_csv(products):
    """
    Append results to the master CSV file
    
    Args:
        products (list): Product information to append
    """
    # Check if master CSV exists
    master_csv_exists = os.path.exists(config.MASTER_OCR_CSV)
    
    # Open in append mode
    with open(config.MASTER_OCR_CSV, mode="a" if master_csv_exists else "w", newline='', encoding='utf-8') as f:
        fieldnames = ["query", "code", "product_name", "description", "price", "unit_description", "brand", "image_url", "image_path"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        # Write header only if the file is new
        if not master_csv_exists:
            writer.writeheader()
        
        # Write all results
        writer.writerows(products)
    
    logger.info(f"✅ Appended {len(products)} entries to master CSV: {config.MASTER_OCR_CSV}")

def process_query(query):
    """
    Process a query using direct API extraction
    
    Args:
        query (str): Search query
    
    Returns:
        list: Extracted product information
    """
    return extract_products_from_api(query)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("⚠️ Missing query. Usage: python api_extractor.py 'נוטלה'")
        sys.exit(1)
    
    query = sys.argv[1]
    products = process_query(query)
    logger.info(f"✅ Extracted {len(products)} products for query: {query}")
