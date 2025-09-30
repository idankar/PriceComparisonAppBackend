# scripts/create_visual_dataset.py
import os
import json
import base64
import argparse
import logging
import time
import requests
from urllib.parse import quote
from tqdm import tqdm

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("visual_dataset.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def fetch_products(query, max_pages=3):
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
                    "image_path": image_path
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
                "image_path": image_path
            })
                
        except Exception as e:
            logger.warning(f"Error downloading image for product {product.get('code', 'unknown')}: {str(e)}")
    
    logger.info(f"Downloaded {downloaded} new images, total: {len(image_paths)}")
    return image_paths

def analyze_image_with_gpt4(image_path, api_key):
    """Analyze image with GPT-4o to extract only visually present information"""
    logger.info(f"Analyzing image: {image_path}")
    
    # Encode image
    with open(image_path, "rb") as image_file:
        base64_image = base64.b64encode(image_file.read()).decode('utf-8')
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    payload = {
        "model": "gpt-4o",
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a precise product information extractor. Analyze the grocery product in the image and extract ONLY "
                    "information that is VISUALLY PRESENT in the image. Return a JSON object with these fields: "
                    "product_name, brand, amount, unit, ingredients. If you can't see a field in the image, use null. "
                    "IMPORTANT: Only include information you can actually see in the image. Do not guess or infer information "
                    "that isn't visible. Do not include any explanations, just the JSON."
                )
            },
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "What product information can you see in this image? Extract ONLY visually present details."},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}
                ]
            }
        ],
        "response_format": {"type": "json_object"}
    }
    
    max_retries = 5
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            
            response_data = response.json()
            if "choices" in response_data and len(response_data["choices"]) > 0:
                result = json.loads(response_data["choices"][0]["message"]["content"])
                return result
            else:
                logger.warning(f"Unexpected response format for {image_path}")
                return None
                
        except requests.exceptions.RequestException as e:
            if "429" in str(e) and attempt < max_retries - 1:
                logger.warning(f"Rate limited (attempt {attempt+1}): {str(e)}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            elif attempt < max_retries - 1:
                logger.warning(f"Request failed (attempt {attempt+1}): {str(e)}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to analyze {image_path} after {max_retries} attempts: {str(e)}")
                return None
        except json.JSONDecodeError:
            logger.error(f"Failed to parse response as JSON for {image_path}")
            return None
    
    return None

def create_visual_dataset(query, api_key, output_dir, max_pages=3, max_images=10):
    """Create a dataset with visually verified information"""
    logger.info(f"Creating visual dataset for query: {query}")
    
    # Create query-specific directories
    query_dir = query.replace(" ", "_")
    data_dir = os.path.join(output_dir, "data", query_dir)
    images_dir = os.path.join(output_dir, "images", query_dir)
    visual_dir = os.path.join(output_dir, "visual", query_dir)
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(images_dir, exist_ok=True)
    os.makedirs(visual_dir, exist_ok=True)
    
    # Fetch products from API
    products = fetch_products(query, max_pages)
    
    if not products:
        logger.warning(f"No products found for query: {query}")
        return []
    
    # Save raw API response
    raw_file = os.path.join(data_dir, "raw_response.json")
    with open(raw_file, 'w', encoding='utf-8') as f:
        json.dump(products, f, ensure_ascii=False, indent=2)
    
    # Download images
    image_paths = download_product_images(products, images_dir)
    
    # Limit number of images to analyze with GPT-4o
    if max_images and len(image_paths) > max_images:
        image_paths = image_paths[:max_images]
    
    # Analyze images with GPT-4o
    visual_data = []
    
    for img_info in tqdm(image_paths, desc="Analyzing Images"):
        product_id = img_info["product_id"]
        image_path = img_info["image_path"]
        
        # Skip if already analyzed
        output_file = os.path.join(visual_dir, f"{product_id}_visual.json")
        if os.path.exists(output_file):
            with open(output_file, 'r', encoding='utf-8') as f:
                visual_info = json.load(f)
                visual_data.append(visual_info)
            continue
        
        # Find product data
        product_data = None
        for product in products:
            if product.get("code") == product_id:
                product_data = product
                break
        
        # Add delay to avoid rate limiting
        time.sleep(2)
        
        # Analyze with GPT-4o
        visual_info = analyze_image_with_gpt4(image_path, api_key)
        
        if visual_info:
            # Add metadata
            visual_info["product_id"] = product_id
            visual_info["image_path"] = image_path
            
            # Add some API data for comparison (not for training)
            if product_data:
                visual_info["api_data"] = {
                    "name": product_data.get("name"),
                    "brand": product_data.get("brandName"),
                    "price": product_data.get("price", {}).get("formattedValue")
                }
            
            # Save individual result
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(visual_info, f, ensure_ascii=False, indent=2)
            
            visual_data.append(visual_info)
    
    # Save all visual data
    all_visual_file = os.path.join(visual_dir, "all_visual_data.json")
    with open(all_visual_file, 'w', encoding='utf-8') as f:
        json.dump(visual_data, f, ensure_ascii=False, indent=2)
    
    logger.info(f"Created visual dataset with {len(visual_data)} products for query: {query}")
    return visual_data

def process_queries(queries, api_key, output_dir, max_pages=3, max_images=10):
    """Process multiple queries"""
    logger.info(f"Processing {len(queries)} queries")
    
    all_data = []
    
    for query in queries:
        visual_data = create_visual_dataset(query, api_key, output_dir, max_pages, max_images)
        all_data.extend(visual_data)
        
        # Add delay between queries
        time.sleep(5)
    
    # Save all data to a master file
    if all_data:
        master_file = os.path.join(output_dir, "all_visual_data.json")
        with open(master_file, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Total products with visual data: {len(all_data)}")
        logger.info(f"Master visual data saved to {master_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create dataset with visually verified product information")
    parser.add_argument("--queries", nargs="+", required=True, help="Product queries to search for")
    parser.add_argument("--api-key", required=True, help="OpenAI API key")
    parser.add_argument("--output-dir", default="data/visual_dataset", help="Output directory")
    parser.add_argument("--max-pages", type=int, default=3, help="Maximum pages per query")
    parser.add_argument("--max-images", type=int, default=10, help="Maximum images to analyze per query")
    
    args = parser.parse_args()
    
    process_queries(args.queries, args.api_key, args.output_dir, args.max_pages, args.max_images)