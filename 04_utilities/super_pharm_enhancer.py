import json
import logging
import os
import requests
from urllib.parse import urlparse

# --- Configuration ---
INPUT_FILE = "superpharm_products_final.jsonl"
OUTPUT_FILE = "superpharm_products_enriched.jsonl"
IMAGE_FOLDER = "product_images"
REQUEST_TIMEOUT = 15

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')
logger = logging.getLogger(__name__)

def get_processed_ids(output_file):
    """Reads the output file to get a set of already processed product IDs to allow resuming."""
    processed_ids = set()
    if not os.path.exists(output_file):
        return processed_ids
    with open(output_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                data = json.loads(line)
                if 'productId' in data:
                    processed_ids.add(data['productId'])
            except json.JSONDecodeError:
                continue
    return processed_ids

def parse_categories_from_url(url_string):
    """Parses a URL string to extract a clean list of categories from its path."""
    if not url_string:
        return []
    
    path = urlparse(url_string).path
    # Split the path by '/', remove empty strings from the result,
    # and filter out the part that contains the category code (e.g., 'c')
    categories = [part for part in path.split('/') if part and part != 'c' and not part.isdigit()]
    return categories

def download_product_image(product_data, save_folder):
    """Downloads a product's image and saves it with the product ID as its name."""
    image_url = product_data.get("imageUrl")
    product_id = product_data.get("productId")

    if not image_url or not product_id:
        logger.warning(f"Product {product_id} is missing image URL. Skipping image download.")
        return

    try:
        if not os.path.exists(save_folder):
            os.makedirs(save_folder)

        # Determine the file extension (.jpg, .png, etc.)
        file_extension = os.path.splitext(image_url)[1]
        if not file_extension:
            file_extension = ".jpg" # Default to .jpg if no extension is found
        
        file_name = f"{product_id}{file_extension}"
        save_path = os.path.join(save_folder, file_name)

        # Skip download if image already exists
        if os.path.exists(save_path):
            logger.info(f"Image {file_name} already exists. Skipping download.")
            return

        response = requests.get(image_url, timeout=REQUEST_TIMEOUT, stream=True)
        response.raise_for_status()

        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"Successfully saved image {file_name}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading image for product {product_id}: {e}")

def main():
    """Main function to enrich product data and download images."""
    if not os.path.exists(INPUT_FILE):
        logger.error(f"Input file not found: {INPUT_FILE}")
        return

    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        products_to_process = [json.loads(line) for line in f]
    
    total_products = len(products_to_process)
    logger.info(f"Found {total_products} products to process from {INPUT_FILE}.")
    
    processed_ids = get_processed_ids(OUTPUT_FILE)
    if processed_ids:
        logger.info(f"Found {len(processed_ids)} already processed products. Resuming script.")

    with open(OUTPUT_FILE, 'a', encoding='utf-8') as f_out:
        for i, product_data in enumerate(products_to_process):
            product_id = product_data.get('productId')
            if not product_id or product_id in processed_ids:
                continue

            logger.info(f"Processing product {i + 1}/{total_products} (ID: {product_id})")

            # Function 1: Get categories from the source URL
            source_url = product_data.get("scrapedFrom", "")
            categories = parse_categories_from_url(source_url)
            product_data['categories'] = categories
            logger.info(f"Assigned categories: {categories}")

            # Function 2: Download the product image
            download_product_image(product_data, IMAGE_FOLDER)

            # Write the newly enriched data to the output file
            f_out.write(json.dumps(product_data, ensure_ascii=False) + '\n')
            processed_ids.add(product_id)

    logger.info(f"Enrichment complete. Processed {len(processed_ids)} unique products.")
    logger.info(f"Enriched data saved to: {OUTPUT_FILE}")
    logger.info(f"Images saved in: {IMAGE_FOLDER}")

if __name__ == "__main__":
    main()