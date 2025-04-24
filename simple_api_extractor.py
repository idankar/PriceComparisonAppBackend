# simple_api_extractor.py
import os
import sys
import json
import csv
import time
import requests
import logging
from urllib.parse import quote
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("simple_api_extractor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def extract_products(query, max_pages=5, delay=1):
    """
    Extract product data directly from Shufersal API with error handling
    
    Args:
        query (str): Search query
        max_pages (int): Maximum number of pages to fetch
        delay (int): Delay between page requests
        
    Returns:
        list: Extracted products
    """
    logger.info(f"Extracting products for query: {query}")
    
    # Create sanitized query folder name
    query_dir = query.replace(" ", "_")
    
    # Ensure base directories exist
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "data")
    results_dir = os.path.join(data_dir, "results", query_dir)
    images_dir = os.path.join(data_dir, "images", query_dir)
    
    os.makedirs(results_dir, exist_ok=True)
    os.makedirs(images_dir, exist_ok=True)
    
    # Generate timestamp for filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Prepare to collect products
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
            
            # Make the request with retry mechanism
            max_retries = 3
            for retry in range(max_retries):
                try:
                    response = requests.get(url, headers=headers, timeout=10)
                    response.raise_for_status()  # Raise exception for 4XX/5XX responses
                    break
                except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as e:
                    if retry < max_retries - 1:
                        logger.warning(f"Request failed (attempt {retry+1}): {str(e)}. Retrying...")
                        time.sleep(2)
                    else:
                        logger.error(f"Request failed after {max_retries} attempts: {str(e)}")
                        raise
            
            # Parse the JSON response
            data = response.json()
            
            # Save raw response for debugging
            response_file = os.path.join(results_dir, f"response_page_{page+1}_{timestamp}.json")
            with open(response_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            # Extract products from this page
            page_products = data.get("results", [])
            
            if not page_products:
                logger.info(f"No products found on page {page + 1}")
                break
                
            logger.info(f"Found {len(page_products)} products on page {page + 1}")
            all_products.extend(page_products)
            
            # Add delay between page requests
            if page < max_pages - 1:
                time.sleep(delay)
                
        except Exception as e:
            logger.error(f"Error processing page {page + 1}: {str(e)}")
            # Continue to next page despite errors
            continue
    
    # Process and save the extracted data
    processed_products = []
    
    for product in all_products:
        try:
            # Extract basic product info
            processed_product = {
                "query": query,
                "code": product.get("code", ""),
                "product_name": product.get("name", ""),
                "description": product.get("description", ""),
                "price": product.get("price", {}).get("value", ""),
                "formatted_price": product.get("price", {}).get("formattedValue", ""),
                "unit_description": product.get("unitDescription", ""),
                "brand": product.get("brandName", ""),
                "image_url": None,
                "image_path": None
            }
            
            # Get product image (medium size)
            images = product.get("images", [])
            for image in images:
                if image.get("format") == "medium":
                    processed_product["image_url"] = image.get("url")
                    break
            
            # Download the image if available
            if processed_product["image_url"]:
                try:
                    image_name = f"{processed_product['code']}.jpg"
                    image_path = os.path.join(images_dir, image_name)
                    
                    # Only download if the image doesn't already exist
                    if not os.path.exists(image_path):
                        img_response = requests.get(processed_product["image_url"])
                        if img_response.status_code == 200:
                            with open(image_path, 'wb') as img_file:
                                img_file.write(img_response.content)
                            logger.info(f"Saved image: {image_path}")
                    else:
                        logger.info(f"Image already exists: {image_path}")
                        
                    processed_product["image_path"] = image_path
                    
                except Exception as e:
                    logger.warning(f"Error downloading image: {str(e)}")
            
            processed_products.append(processed_product)
            
        except Exception as e:
            logger.warning(f"Error processing product: {str(e)}")
            continue
    
    # Save to CSV file
    if processed_products:
        csv_path = os.path.join(results_dir, f"products_{query_dir}_{timestamp}.csv")
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                "query", "code", "product_name", "description", "price", 
                "formatted_price", "unit_description", "brand", "image_url", "image_path"
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(processed_products)
            
        logger.info(f"Saved {len(processed_products)} products to {csv_path}")
        
        # Also save products to JSON
        json_path = os.path.join(results_dir, f"products_{query_dir}_{timestamp}.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(processed_products, f, ensure_ascii=False, indent=2)
            
        logger.info(f"Saved {len(processed_products)} products to {json_path}")
    else:
        logger.warning(f"No products extracted for query: {query}")
    
    return processed_products

def process_multiple_queries(queries, max_pages=5, delay_between_queries=5):
    """
    Process multiple product queries
    
    Args:
        queries (list): List of product queries
        max_pages (int): Maximum number of pages to fetch per query
        delay_between_queries (int): Delay between different queries
    """
    results = []
    
    for i, query in enumerate(queries):
        logger.info(f"Processing query {i+1}/{len(queries)}: {query}")
        
        try:
            products = extract_products(query, max_pages=max_pages)
            results.append({
                "query": query,
                "num_products": len(products)
            })
            
            if i < len(queries) - 1:
                logger.info(f"Waiting {delay_between_queries} seconds before next query...")
                time.sleep(delay_between_queries)
                
        except Exception as e:
            logger.error(f"Error processing query '{query}': {str(e)}")
            results.append({
                "query": query,
                "error": str(e),
                "num_products": 0
            })
    
    # Summarize results
    logger.info("=" * 50)
    logger.info("Processing summary:")
    total_products = sum(r.get("num_products", 0) for r in results)
    successful_queries = sum(1 for r in results if r.get("num_products", 0) > 0)
    logger.info(f"Processed {len(queries)} queries")
    logger.info(f"Successful queries: {successful_queries}")
    logger.info(f"Failed queries: {len(queries) - successful_queries}")
    logger.info(f"Total products collected: {total_products}")
    logger.info("=" * 50)
    
    return results

if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Process a single query from command line
        query = sys.argv[1]
        extract_products(query)
    else:
        # Process a list of queries
        queries = [
            "חלב", "גבינה", "יוגורט", "לחם", "פסטה",
            "אורז", "שמן", "קפה", "תה", "סוכר"
        ]
        process_multiple_queries(queries)