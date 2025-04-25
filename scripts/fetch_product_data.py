# scripts/fetch_product_data.py
import os
import json
import time
import logging
import argparse
import requests
from urllib.parse import quote
from tqdm import tqdm

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("product_fetching.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def fetch_products(query, max_pages=5):
    """Fetch products from Shufersal API"""
    logger.info(f"Fetching products for query: {query}")
    
    all_products = []
    encoded_query = quote(query)
    
    # Headers to mimic a browser request
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://www.shufersal.co.il/online/he/search",
        "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7"
    }
    
    # Process each page
    for page in range(max_pages):
        try:
            logger.info(f"Fetching page {page + 1}...")
            
            # Build the API URL 
            url = f"https://www.shufersal.co.il/online/he/search/results?q={encoded_query}&relevance&limit=10&page={page}"
            
            # Make the request
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            # Parse the JSON response
            data = response.json()
            
            # Extract products from this page
            products = data.get("results", [])
            
            if not products:
                logger.info(f"No products found on page {page + 1}")
                break
                
            logger.info(f"Found {len(products)} products on page {page + 1}")
            all_products.extend(products)
            
            # Add delay between requests
            time.sleep(1)
                
        except Exception as e:
            logger.error(f"Error processing page {page + 1}: {str(e)}")
            break
    
    return all_products

def download_product_images(products, output_dir):
    """Download product images"""
    logger.info(f"Downloading images for {len(products)} products")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    downloaded = 0
    
    for product in tqdm(products, desc="Downloading Images"):
        try:
            # Get product image (medium size)
            image_url = None
            images = product.get("images", [])
            for image in images:
                if image.get("format") == "medium":
                    image_url = image.get("url")
                    break
            
            if not image_url:
                continue
                
            # Create filename
            product_id = product.get("code", "unknown")
            image_path = os.path.join(output_dir, f"{product_id}.jpg")
            
            # Skip if image already exists
            if os.path.exists(image_path):
                continue
                
            # Download image
            img_response = requests.get(image_url, timeout=10)
            img_response.raise_for_status()
            
            with open(image_path, 'wb') as img_file:
                img_file.write(img_response.content)
                
            downloaded += 1
                
        except Exception as e:
            logger.warning(f"Error downloading image for product {product.get('code', 'unknown')}: {str(e)}")
    
    logger.info(f"Downloaded {downloaded} images")

def extract_product_data(products, output_file):
    """Extract structured data from products"""
    logger.info(f"Extracting data from {len(products)} products")
    
    # Create output directory
    output_dir = os.path.dirname(output_file)
    os.makedirs(output_dir, exist_ok=True)
    
    structured_data = []
    
    for product in products:
        try:
            # Extract image URL
            image_url = None
            images = product.get("images", [])
            for image in images:
                if image.get("format") == "medium":
                    image_url = image.get("url")
                    break
            
            # Extract structured data
            item = {
                "product_id": product.get("code", ""),
                "product_name": product.get("name", ""),
                "brand": product.get("brandName", ""),
                "description": product.get("description", ""),
                "price": product.get("price", {}).get("value", ""),
                "formatted_price": product.get("price", {}).get("formattedValue", ""),
                "amount": None,
                "unit": product.get("unitDescription", ""),
                "category": None,  # Need more processing to extract category
                "image_url": image_url,
                "image_path": f"{product.get('code', 'unknown')}.jpg" if image_url else None
            }
            
            # Try to extract amount from product name or description
            # This is a simple heuristic and may need improvement
            name = item["product_name"]
            if " " in name and any(char.isdigit() for char in name):
                parts = name.split()
                for part in parts:
                    if any(char.isdigit() for char in part):
                        if "%" in part:  # Percentage
                            item["amount"] = part
                        elif "מ\"ל" in part or "מל" in part:  # Milliliters
                            item["amount"] = part.replace("מ\"ל", "").replace("מל", "").strip()
                            item["unit"] = "מ\"ל"
                        elif "גרם" in part:  # Grams
                            item["amount"] = part.replace("גרם", "").strip()
                            item["unit"] = "גרם"
                        elif "ק\"ג" in part or "קג" in part:  # Kilograms
                            item["amount"] = part.replace("ק\"ג", "").replace("קג", "").strip()
                            item["unit"] = "ק\"ג"
                        elif "ליטר" in part:  # Liters
                            item["amount"] = part.replace("ליטר", "").strip()
                            item["unit"] = "ליטר"
                        elif "יח" in part:  # Units
                            item["amount"] = part.replace("יח", "").strip()
                            item["unit"] = "יח"
            
            structured_data.append(item)
            
        except Exception as e:
            logger.warning(f"Error processing product {product.get('code', 'unknown')}: {str(e)}")
    
    # Save to file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(structured_data, f, ensure_ascii=False, indent=2)
    
    logger.info(f"Extracted data for {len(structured_data)} products, saved to {output_file}")
    
    return structured_data

def process_query(query, output_dir, max_pages=5):
    """Process a single query"""
    logger.info(f"Processing query: {query}")
    
    # Create query-specific directories
    query_dir = query.replace(" ", "_")
    data_dir = os.path.join(output_dir, "data", query_dir)
    images_dir = os.path.join(output_dir, "images", query_dir)
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(images_dir, exist_ok=True)
    
    # Fetch products
    products = fetch_products(query, max_pages)
    
    if not products:
        logger.warning(f"No products found for query: {query}")
        return []
    
    # Save raw API response
    raw_file = os.path.join(data_dir, "raw_response.json")
    with open(raw_file, 'w', encoding='utf-8') as f:
        json.dump(products, f, ensure_ascii=False, indent=2)
    
    # Extract structured data
    structured_file = os.path.join(data_dir, "structured_data.json")
    structured_data = extract_product_data(products, structured_file)
    
    # Download images
    download_product_images(products, images_dir)
    
    return structured_data

def process_queries(queries, output_dir, max_pages=5):
    """Process multiple queries"""
    logger.info(f"Processing {len(queries)} queries")
    
    all_products = []
    
    for query in queries:
        products = process_query(query, output_dir, max_pages)
        all_products.extend(products)
        
        # Add delay between queries
        time.sleep(2)
    
    # Save all products to a master file
    if all_products:
        master_file = os.path.join(output_dir, "all_products.json")
        with open(master_file, 'w', encoding='utf-8') as f:
            json.dump(all_products, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Total products: {len(all_products)}")
        logger.info(f"Master data saved to {master_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch product data from Shufersal API")
    parser.add_argument("--queries", nargs="+", required=True, help="Product queries to search for")
    parser.add_argument("--output-dir", default="data/fetched", help="Output directory")
    parser.add_argument("--max-pages", type=int, default=5, help="Maximum pages per query")
    
    args = parser.parse_args()
    
    process_queries(args.queries, args.output_dir, args.max_pages)