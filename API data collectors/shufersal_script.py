#!/usr/bin/env python3
"""
Shufersal Product Data Collection Script

This script collects product data from Shufersal's website using their public API.
It organizes product information and images into a structured format for training
a product recognition model.

Note: This script uses Shufersal's public web API which is unofficial,
so it may require updates if the API changes.
"""

import os
import json
import time
import argparse
import requests
import pandas as pd
from PIL import Image
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import hashlib
import re
import logging
import random
from urllib.parse import quote
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("shufersal_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Define Hebrew characters set for Hebrew text detection
HEBREW_CHARS = set('אבגדהוזחטיכלמנסעפצקרשתךםןףץ')

# Common search terms to expand the default set
COMMON_SEARCH_TERMS = [
    # Dairy Products (Hebrew)
    "חלב", "גבינה", "גבינה צהובה", "קוטג", "יוגורט", "שמנת", "חמאה", "מוצרי חלב", "גבינה לבנה", 
    "גבינת עיזים", "מעדן חלב", "מילקי", "דני", "מגדים", "לבן", "חלב עמיד", "חלב טרי", "חלב סויה",
    
    # Bakery (Hebrew)
    "לחם", "פיתות", "לחמניות", "חלות", "עוגות", "עוגיות", "מאפים", "בורקס", "קרואסון", "לחם אחיד",
    "לחם קל", "לחם מלא", "לחם שיפון", "לחם כוסמין", "עוגת שמרים", "בייגלה", "קרקרים",
    
    # Snacks (Hebrew)
    "חטיפים", "במבה", "ביסלי", "צ'יפס", "מרק נמס", "פופקורן", "חטיפי תירס", "אפרופו", "דוריטוס", 
    "תפוצ'יפס", "צ'יטוס", "עונג", "קליק", "שוקולד", "חטיף בריאות", "אנרגיה", "פיצוחים", "גרעינים",
    
    # Beverages (Hebrew)
    "משקאות", "מים", "סודה", "קולה", "ספרייט", "פאנטה", "מיץ", "תרכיז", "משקה אנרגיה", "קפה",
    "תה", "שוקו", "חלב סויה", "משקה שקדים", "פריגת", "נביעות", "מי עדן", "קפה שחור", "קפה נמס",
    
    # Add more categories as needed
]

# Dictionary of common product translations
PRODUCT_TRANSLATIONS = {
    "חלב טרי": "Fresh Milk",
    "חלב מועשר": "Enriched Milk",
    "גבינה לבנה": "White Cheese",
    "קוטג": "Cottage Cheese",
    "יוגורט": "Yogurt",
    "שמנת": "Cream",
    "חמאה": "Butter",
    "מוצרי חלב": "Dairy Products",
    "גבינת עיזים": "Goat Cheese",
    "לבן": "Labaneh",
    "חלב עמיד": "UHT Milk",
    "חלב סויה": "Soy Milk",
    "דל לקטוז": "Lactose-Free",
    "שוקו": "Chocolate Milk",
    # Add more translations as needed
}

class ShufersalScraper:
    """Scraper for Shufersal products"""
    
    def __init__(self, output_dir):
        """
        Initialize the scraper
        
        Args:
            output_dir: Directory to save scraped data
        """
        self.output_dir = output_dir
        self.products_dir = os.path.join(output_dir, "products")
        self.raw_dir = os.path.join(output_dir, "raw", "shufersal")
        
        # Create directories if they don't exist
        os.makedirs(self.products_dir, exist_ok=True)
        os.makedirs(self.raw_dir, exist_ok=True)
        
        # Store processed products to avoid duplicates
        self.processed_products = set()
        self.failed_products = set()
    
    def search_products(self, query, max_pages=3):
        """Fetch products from Shufersal API"""
        logger.info(f"Searching for products: {query}")
        
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
        
        logger.info(f"Found {len(all_products)} products for query '{query}'")
        logger.info(f"Sample product fields: {list(all_products[0].keys()) if all_products else 'No products'}")
        
        # Save raw data
        if all_products:
            query_hash = hashlib.md5(query.encode()).hexdigest()[:8]
            with open(os.path.join(self.raw_dir, f"search_{query_hash}_products.json"), "w", encoding="utf-8") as f:
                json.dump(all_products, f, ensure_ascii=False, indent=2)
        
        return all_products
    
    def extract_product_metadata(self, product_data):
        """Extract structured metadata from product data"""
        # Initialize basic metadata
        metadata = {
            "product_id": f"shufersal_{product_data.get('code', '')}",
            "name": None,  # Start with null for English name
            "name_he": "",
            "brand": product_data.get("brandName", ""),
            "price": float(product_data.get("price", {}).get("value", 0)),
            "amount": "",
            "unit": "",
            "retailer": "Shufersal",
            "source": "shufersal",
            "source_id": product_data.get("code", "")
        }
        
        # Extract product name
        full_name = product_data.get("name", "")
        
        # Separate Hebrew and non-Hebrew parts
        hebrew_chars = set('אבגדהוזחטיכלמנסעפצקרשתךםןףץ')
        name_he_parts = []
        name_en_parts = []
        
        for word in full_name.split():
            if any(c in hebrew_chars for c in word):
                name_he_parts.append(word)
            else:
                name_en_parts.append(word)
        
        # Set Hebrew name
        metadata["name_he"] = " ".join(name_he_parts)
        
        # Set English name only if meaningful English text was found
        if name_en_parts and not all(part.isdigit() or part == "%" for part in name_en_parts):
            metadata["name"] = " ".join(name_en_parts)
        # Otherwise, keep it as null
        
        # Extract amount and unit if present
        amount_match = re.search(r'(\d+(?:\.\d+)?)\s*(גרם|ג\'|ג|מ"ל|מל|ק"ג|קג|ליטר|ל|יח\'|ml|g|kg|l|liter)', full_name)
        if amount_match:
            metadata["amount"] = amount_match.group(1)
            metadata["unit"] = amount_match.group(2)
        
        # Standardize units
        unit_mapping = {
            'גרם': 'g', 'ג\'': 'g', 'ג': 'g',
            'מ"ל': 'ml', 'מל': 'ml',
            'ק"ג': 'kg', 'קג': 'kg',
            'ליטר': 'l', 'ל': 'l',
            'יח\'': 'unit',
            '%': '%'
        }
        if metadata["unit"] in unit_mapping:
            metadata["unit"] = unit_mapping[metadata["unit"]]
        
        return metadata
    
    def process_product(self, product_data):
        """
        Process a single product
        
        Args:
            product_data: Product data from search
            
        Returns:
            Boolean indicating if processing was successful
        """
        try:
            # Extract product ID
            product_id = product_data.get("code")
            
            if not product_id:
                logger.warning("Product without ID, skipping")
                return False
                
            # Skip if already processed or failed
            if product_id in self.processed_products or product_id in self.failed_products:
                return False
                    
            # Format as shufersal_{product_id} for our database
            formatted_id = f"shufersal_{product_id}"
            
            logger.info(f"Processing product: {formatted_id}")
            
            # Create product directory
            product_dir = os.path.join(self.products_dir, formatted_id)
            os.makedirs(product_dir, exist_ok=True)
            
            # Get product information
            name = product_data.get("name", "")
            logger.info(f"Product name: {name}")
            
            try:
                # Get images
                image_urls = []
                images = product_data.get("images", [])
                
                # Log image info
                logger.info(f"Found {len(images)} image entries")
                
                for image in images:
                    if image.get("format") == "medium" and image.get("url"):
                        image_urls.append(image.get("url"))
                        
                logger.info(f"Found {len(image_urls)} valid image URLs")
                
                # Download and save images
                self._download_product_images(image_urls, product_dir)
                
                # Extract product metadata
                metadata = {
                    "product_id": formatted_id,
                    "name": name,
                    "name_he": "", # Will be filled by extract_product_metadata
                    "brand": product_data.get("brandName", ""),
                    "price": float(product_data.get("price", {}).get("value", 0)),
                    "amount": "",
                    "unit": "",
                    "retailer": "Shufersal",
                    "source": "shufersal",
                    "source_id": product_id
                }
                
                try:
                    # Try enhanced metadata extraction
                    enhanced_metadata = self.extract_product_metadata(product_data)
                    # Update the metadata with enhanced fields
                    metadata.update(enhanced_metadata)
                    logger.info(f"Enhanced metadata successful: {metadata}")
                except Exception as meta_error:
                    logger.error(f"Error in enhanced metadata extraction: {meta_error}")
                
                # Save metadata
                with open(os.path.join(product_dir, "metadata.json"), 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=2)
                
                # Add to processed set
                self.processed_products.add(product_id)
                
                return True
                
            except Exception as inner_e:
                logger.error(f"Inner error processing product {product_id}: {inner_e}")
                return False
                
        except Exception as e:
            logger.error(f"Error processing product: {e}")
            
            if product_data and "code" in product_data:
                self.failed_products.add(product_data["code"])
            
            return False
    
    def _get_image_urls(self, product_data):
        """Get all available image URLs from product data"""
        image_urls = []

        # Get images from the API response
        images = product_data.get("images", [])
        logger.info(f"Found {len(images)} image entries")

        # Examine each image entry to understand structure
        for i, image in enumerate(images):
            logger.info(f"Image {i} data: {image}")

            # Check for URL field directly
            if isinstance(image, dict) and "url" in image:
                url = image["url"]
                if url and isinstance(url, str) and url.startswith("http"):
                    logger.info(f"Adding valid URL: {url}")
                    image_urls.append(url)
                else:
                    logger.info(f"Invalid URL format: {url}")
            # Some APIs nest the URL under format fields
            elif isinstance(image, dict) and "format" in image:
                format_info = image.get("format")
                url = image.get("url")
                logger.info(f"Format: {format_info}, URL: {url}")
                if url and isinstance(url, str) and url.startswith("http"):
                    logger.info(f"Adding valid format URL: {url}")
                    image_urls.append(url)

        logger.info(f"Found {len(image_urls)} valid image URLs")
        return image_urls
    
    def _download_product_images(self, image_urls, product_dir):
        """
        Download product images
        
        Args:
            image_urls: List of image URLs
            product_dir: Directory to save images
        """
        for i, img_url in enumerate(image_urls):
            try:
                response = requests.get(img_url, timeout=10)
                response.raise_for_status()
                
                # Verify we received an actual image (not a placeholder)
                content_type = response.headers.get('Content-Type', '')
                if not content_type.startswith('image/'):
                    logger.warning(f"URL returned non-image content: {img_url}")
                    continue
                
                # Verify image size isn't too small (placeholders are often very small)
                if len(response.content) < 1000:
                    logger.warning(f"Image too small, likely a placeholder: {img_url}")
                    continue
                
                img = Image.open(BytesIO(response.content))
                
                # Skip very small images
                if img.width < 50 or img.height < 50:
                    logger.warning(f"Image dimensions too small: {img_url}")
                    continue
                
                img = img.convert('RGB')  # Convert to RGB (in case of PNG with alpha)
                
                # Save image
                img_path = os.path.join(product_dir, f"{i+1:03d}.jpg")
                img.save(img_path, "JPEG", quality=95)
                
                # Small delay between downloads
                time.sleep(0.2)
                
            except Exception as e:
                logger.error(f"Error downloading image {img_url}: {e}")
    
    def scrape_search_terms(self, max_products=30000, max_per_search=100, workers=10):
        """
        Scrape products using common search terms
        
        Args:
            max_products: Maximum total products to scrape
            max_per_search: Maximum products per search
            workers: Number of worker threads
            
        Returns:
            Total number of products processed
        """
        # Calculate products per search term
        search_terms = COMMON_SEARCH_TERMS.copy()
        products_per_search = min(max_per_search, max_products // len(search_terms))
        
        # Shuffle search terms for more variety
        random.shuffle(search_terms)
        
        # Process each search term
        total_processed = 0
        
        for term in tqdm(search_terms, desc="Processing search queries"):
            # Stop if we've reached the maximum total
            if total_processed >= max_products:
                break
                
            # Search for products with this term
            products = self.search_products(term, max(1, products_per_search // 10))
            
            # Limit to max products per search
            products = products[:products_per_search]
            
            logger.info(f"Processing {len(products)} products for query '{term}'")
            
            # Try to process one product directly for debugging
            if products:
                success = self.process_product(products[0])
                logger.info(f"Test product processing result: {success}")
            
            # Process products with multiple threads
            with ThreadPoolExecutor(max_workers=workers) as executor:
                results = list(tqdm(
                    executor.map(self.process_product, products),
                    total=len(products),
                    desc=f"Processing products for '{term}'"
                ))
                
            # Count successful processings
            successful = sum(1 for r in results if r)
            total_processed += successful
            
            logger.info(f"Processed {successful}/{len(products)} products for search '{term}'")
            logger.info(f"Total processed so far: {total_processed}/{max_products}")
            
            # Add delay between search terms
            time.sleep(2)
            
        return total_processed
    
    def create_summary(self):
        """
        Create a summary of the scraped data
        
        Returns:
            Dictionary with summary information
        """
        # Count products and images
        product_count = len([p for p in os.listdir(self.products_dir) if p.startswith("shufersal_")])
        
        total_images = 0
        products_with_images = 0
        products_with_hebrew = 0
        
        for product_id in os.listdir(self.products_dir):
            if not product_id.startswith("shufersal_"):
                continue
                
            product_dir = os.path.join(self.products_dir, product_id)
            
            # Count images
            images = [f for f in os.listdir(product_dir) if f.endswith(('.jpg', '.jpeg', '.png'))]
            image_count = len(images)
            total_images += image_count
            
            if image_count > 0:
                products_with_images += 1
                
            # Check for Hebrew
            metadata_path = os.path.join(product_dir, "metadata.json")
            if os.path.exists(metadata_path):
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                    if metadata.get('name_he'):
                        products_with_hebrew += 1
        
        # Calculate averages
        avg_images_per_product = total_images / product_count if product_count > 0 else 0
        percent_with_images = (products_with_images / product_count * 100) if product_count > 0 else 0
        percent_with_hebrew = (products_with_hebrew / product_count * 100) if product_count > 0 else 0
        
        # Create summary
        summary = {
            "total_products": product_count,
            "total_images": total_images,
            "products_with_images": products_with_images,
            "products_with_hebrew": products_with_hebrew,
            "avg_images_per_product": avg_images_per_product,
            "percent_with_images": percent_with_images,
            "percent_with_hebrew": percent_with_hebrew
        }
        
        # Save summary
        with open(os.path.join(self.output_dir, "shufersal_summary.json"), 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
            
        return summary
    
    def create_manifest(self, split_ratio=0.8, min_images=1):
        """
        Create a training manifest
        
        Args:
            split_ratio: Train/val split ratio
            min_images: Minimum number of images required
            
        Returns:
            Path to manifest file
        """
        import random
        
        manifest = {
            "train": {},
            "val": {}
        }
        
        products_excluded = 0
        products_included = 0
        
        # Process each Shufersal product
        for product_id in tqdm(os.listdir(self.products_dir), desc="Creating manifest"):
            if not product_id.startswith("shufersal_"):
                continue
                
            product_dir = os.path.join(self.products_dir, product_id)
            
            # Find images
            images = [f for f in os.listdir(product_dir) if f.endswith(('.jpg', '.jpeg', '.png'))]
            
            # Skip if not enough images
            if len(images) < min_images:
                products_excluded += 1
                continue
                
            # Load metadata
            metadata_path = os.path.join(product_dir, "metadata.json")
            if not os.path.exists(metadata_path):
                products_excluded += 1
                continue
                
            with open(metadata_path, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            # Shuffle images (with deterministic seed for reproducibility)
            random.seed(hash(product_id) % 10000)
            random.shuffle(images)
            
            # Split images
            split_idx = max(1, int(len(images) * split_ratio))
            train_images = images[:split_idx]
            val_images = images[split_idx:]
            
            # Ensure at least one image in each split
            if not val_images and len(train_images) > 1:
                val_images = [train_images.pop()]
            
            # Add to manifest
            product_info = {
                "brand": metadata.get("brand", ""),
                "name": metadata.get("name", ""),
                "name_he": metadata.get("name_he", ""),
                "retailer": "Shufersal"
            }
            
            manifest["train"][product_id] = {
                **product_info,
                "images": train_images
            }
            
            if val_images:
                manifest["val"][product_id] = {
                    **product_info,
                    "images": val_images
                }
                
            products_included += 1
        
        # Save manifest
        manifest_path = os.path.join(self.output_dir, "train_manifests", "shufersal_manifest.json")
        os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
        
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Created manifest with {products_included} products (excluded {products_excluded})")
        logger.info(f"Train images: {sum(len(p['images']) for p in manifest['train'].values())}")
        logger.info(f"Val images: {sum(len(p['images']) for p in manifest['val'].values())}")
        
        return manifest_path


def main():
    parser = argparse.ArgumentParser(description="Shufersal Product Data Scraper")
    parser.add_argument("--output-dir", type=str, default="data", help="Output directory")
    parser.add_argument("--max-products", type=int, default=30000, help="Maximum number of products to scrape")
    parser.add_argument("--max-per-search", type=int, default=100, help="Maximum products per search term")
    parser.add_argument("--workers", type=int, default=10, help="Number of worker threads")
    parser.add_argument("--create-manifest", action="store_true", help="Create training manifest after scraping")
    
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = ShufersalScraper(args.output_dir)
    
    # Scrape products
    total_products = scraper.scrape_search_terms(
        max_products=args.max_products,
        max_per_search=args.max_per_search,
        workers=args.workers
    )
    
    # Create summary
    summary = scraper.create_summary()
    for key, value in summary.items():
        logger.info(f"{key}: {value}")
    
    # Create manifest if requested
    if args.create_manifest:
        manifest_path = scraper.create_manifest()
        logger.info(f"Manifest created at {manifest_path}")

if __name__ == "__main__":
    main()