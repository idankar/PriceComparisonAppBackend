# scripts/prepare_annotation_dataset.py
import os
import json
import argparse
import logging
import time
import requests
from urllib.parse import quote
from tqdm import tqdm
import random

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("annotation_prep.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def fetch_products(query, max_pages=2):
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
            continue
    
    return all_products

def download_product_images(products, output_dir):
    """Download product images"""
    logger.info(f"Downloading images for {len(products)} products")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    downloaded = 0
    image_paths = []
    
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
                image_paths.append({
                    "product_id": product_id,
                    "image_path": image_path,
                    "api_data": {
                        "name": product.get("name", ""),
                        "brand": product.get("brandName", ""),
                        "description": product.get("description", ""),
                        "unit_description": product.get("unitDescription", "")
                    }
                })
                continue
                
            # Download image
            img_response = requests.get(image_url, timeout=10)
            img_response.raise_for_status()
            
            with open(image_path, 'wb') as img_file:
                img_file.write(img_response.content)
                
            downloaded += 1
            image_paths.append({
                "product_id": product_id,
                "image_path": image_path,
                "api_data": {
                    "name": product.get("name", ""),
                    "brand": product.get("brandName", ""),
                    "description": product.get("description", ""),
                    "unit_description": product.get("unitDescription", "")
                }
            })
                
        except Exception as e:
            logger.warning(f"Error downloading image for product {product.get('code', 'unknown')}: {str(e)}")
    
    logger.info(f"Downloaded {downloaded} new images, total: {len(image_paths)}")
    return image_paths

def prepare_annotation_set(all_product_data, output_dir, sample_size=100):
    """Create a sample set for manual annotation"""
    logger.info(f"Preparing annotation set from {len(all_product_data)} products")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Select a random sample of products
    if len(all_product_data) > sample_size:
        sampled_data = random.sample(all_product_data, sample_size)
    else:
        sampled_data = all_product_data
    
    # Prepare annotation template
    annotation_data = []
    
    for item in sampled_data:
        # Create an annotation template
        annotation_item = {
            "product_id": item["product_id"],
            "image_path": item["image_path"],
            "api_data": item["api_data"],
            "visible_info": {
                "product_name": "",
                "brand": "",
                "amount": "",
                "unit": ""
            },
            "notes": ""
        }
        
        annotation_data.append(annotation_item)
    
    # Save annotation template to JSON file
    annotation_file = os.path.join(output_dir, "annotation_template.json")
    with open(annotation_file, 'w', encoding='utf-8') as f:
        json.dump(annotation_data, f, ensure_ascii=False, indent=2)
    
    # Also save as CSV for easier editing
    csv_file = os.path.join(output_dir, "annotation_template.csv")
    with open(csv_file, 'w', encoding='utf-8') as f:
        f.write("product_id,image_file,api_name,api_brand,visible_product_name,visible_brand,visible_amount,visible_unit,notes\n")
        
        for item in annotation_data:
            api_data = item["api_data"]
            f.write(f"{item['product_id']},{os.path.basename(item['image_path'])},")
            f.write(f"\"{api_data['name']}\",\"{api_data['brand']}\",")
            f.write(",,,,\n")
    
    logger.info(f"Created annotation template with {len(annotation_data)} products")
    logger.info(f"JSON template: {annotation_file}")
    logger.info(f"CSV template: {csv_file}")
    
    return annotation_data

def process_queries(queries, output_dir, max_pages=2, sample_size=50):
    """Process multiple product queries to prepare annotation dataset"""
    logger.info(f"Processing {len(queries)} queries")
    
    # Create directories
    raw_dir = os.path.join(output_dir, "raw")
    images_dir = os.path.join(output_dir, "images")
    annotation_dir = os.path.join(output_dir, "annotation")
    
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(images_dir, exist_ok=True)
    os.makedirs(annotation_dir, exist_ok=True)
    
    all_product_data = []
    
    # Process each query
    for query in queries:
        query_dir = query.replace(" ", "_")
        query_raw_dir = os.path.join(raw_dir, query_dir)
        query_images_dir = os.path.join(images_dir, query_dir)
        
        os.makedirs(query_raw_dir, exist_ok=True)
        os.makedirs(query_images_dir, exist_ok=True)
        
        # Fetch products
        products = fetch_products(query, max_pages)
        
        if products:
            # Save raw data
            raw_file = os.path.join(query_raw_dir, "products.json")
            with open(raw_file, 'w', encoding='utf-8') as f:
                json.dump(products, f, ensure_ascii=False, indent=2)
            
            # Download images
            product_data = download_product_images(products, query_images_dir)
            all_product_data.extend(product_data)
        
        # Add delay between queries
        time.sleep(2)
    
    # Prepare annotation set
    annotation_data = prepare_annotation_set(all_product_data, annotation_dir, sample_size)
    
    logger.info("=" * 50)
    logger.info("Annotation dataset preparation complete!")
    logger.info(f"Total products: {len(all_product_data)}")
    logger.info(f"Annotation sample: {len(annotation_data)}")
    logger.info("=" * 50)
    logger.info("Next step: Manually annotate the dataset to identify visible information")
    logger.info(f"Edit the CSV file: {os.path.join(annotation_dir, 'annotation_template.csv')}")
    logger.info("=" * 50)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prepare product dataset for manual annotation")
    parser.add_argument("--queries", nargs="+", required=True, help="Product queries to search for")
    parser.add_argument("--output-dir", default="data/annotation", help="Output directory")
    parser.add_argument("--max-pages", type=int, default=2, help="Maximum pages per query")
    parser.add_argument("--sample-size", type=int, default=50, help="Sample size for annotation")
    
    args = parser.parse_args()
    
    process_queries(args.queries, args.output_dir, args.max_pages, args.sample_size)