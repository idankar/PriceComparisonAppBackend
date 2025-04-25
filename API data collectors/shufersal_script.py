#!/usr/bin/env python3
"""
Shufersal Product Data Collection Script

This script collects product data from the Shufersal API, including:
- Product details (name, brand, price, etc.)
- Product images
- Category information

The data is organized into a structured format for training a product recognition model.
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
from urllib.parse import urljoin, quote
import logging

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

# Constants
DEFAULT_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7"
}

# Shufersal API endpoints
BASE_URL = "https://www.shufersal.co.il"
CATEGORIES_URL = f"{BASE_URL}/online/he/A"
CATEGORY_URL = f"{BASE_URL}/online/he/C"
PRODUCT_URL = f"{BASE_URL}/online/he/P"
SEARCH_URL = f"{BASE_URL}/online/he/search"
PRODUCT_API_URL = f"{BASE_URL}/online/he/api/v1/products"

# Regular expression for Hebrew characters
HEBREW_PATTERN = re.compile(r'[\u0590-\u05FF\u200f\u200e]+')

class ShufersalScraper:
    """Scraper for Shufersal online store products"""
    
    def __init__(self, output_dir, session=None):
        """
        Initialize the scraper
        
        Args:
            output_dir: Directory to save the scraped data
            session: Optional requests session to use
        """
        self.output_dir = output_dir
        self.products_dir = os.path.join(output_dir, "products")
        self.raw_dir = os.path.join(output_dir, "raw", "shufersal")
        
        # Create directories if they don't exist
        os.makedirs(self.products_dir, exist_ok=True)
        os.makedirs(self.raw_dir, exist_ok=True)
        
        # Create or use session
        self.session = session if session else requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        
        # Store categories
        self.categories = {}
        
        # Store products to avoid duplicates
        self.processed_products = set()
        self.failed_products = set()
        
    def _make_request(self, url, method="GET", params=None, data=None, json_data=None, retry=3):
        """
        Make a request to the API with retries
        
        Args:
            url: URL to request
            method: HTTP method
            params: URL parameters
            data: Form data
            json_data: JSON data
            retry: Number of retries
            
        Returns:
            Response object or None if all retries failed
        """
        for attempt in range(retry):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    data=data,
                    json=json_data,
                    timeout=30
                )
                
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                wait_time = 2 ** attempt
                logger.warning(f"Request failed: {e}. Retrying in {wait_time}s... ({attempt+1}/{retry})")
                time.sleep(wait_time)
                
                if attempt == retry - 1:
                    logger.error(f"Failed to fetch {url} after {retry} attempts")
                    return None
    
    def get_categories(self):
        """
        Get all product categories from the Shufersal website
        
        Returns:
            Dictionary mapping category IDs to category info
        """
        logger.info("Fetching top-level categories")
        
        response = self._make_request(CATEGORIES_URL)
        if not response:
            logger.error("Failed to fetch top-level categories")
            return {}
        
        # Try to parse the JavaScript that contains categories
        # This is a simplistic approach - may need adjustment
        try:
            categories_raw = {}
            category_lines = re.findall(r'categoryId:"([^"]+)",categoryName:"([^"]+)"', response.text)
            
            for cat_id, cat_name in category_lines:
                categories_raw[cat_id] = {
                    'id': cat_id,
                    'name': cat_name.replace('\\', ''),
                    'subcategories': {}
                }
                
            logger.info(f"Found {len(categories_raw)} top-level categories")
            
            # Fetch subcategories for each category
            for cat_id, cat_info in tqdm(categories_raw.items(), desc="Fetching subcategories"):
                self._get_subcategories(cat_id, cat_info)
                
            self.categories = categories_raw
            
            # Save categories to file
            with open(os.path.join(self.raw_dir, "categories.json"), 'w', encoding='utf-8') as f:
                json.dump(categories_raw, f, ensure_ascii=False, indent=2)
                
            return categories_raw
            
        except Exception as e:
            logger.error(f"Error parsing categories: {e}")
            return {}
    
    def _get_subcategories(self, category_id, category_info):
        """
        Get subcategories for a given category
        
        Args:
            category_id: Category ID
            category_info: Category info dictionary to update
        """
        cat_url = f"{CATEGORY_URL}/{category_id}"
        response = self._make_request(cat_url)
        
        if not response:
            return
            
        subcategories = {}
        
        # Extract subcategories from the page
        subcategory_lines = re.findall(r'categoryId:"([^"]+)",categoryName:"([^"]+)"', response.text)
        
        for subcat_id, subcat_name in subcategory_lines:
            if subcat_id != category_id:  # Skip the main category
                subcategories[subcat_id] = {
                    'id': subcat_id,
                    'name': subcat_name.replace('\\', ''),
                    'parent_id': category_id
                }
        
        category_info['subcategories'] = subcategories
    
    def get_products_by_category(self, category_id, max_products=1000, page_size=100):
        """
        Get products for a specific category
        
        Args:
            category_id: Category ID
            max_products: Maximum number of products to fetch
            page_size: Number of products per page
            
        Returns:
            List of product data
        """
        logger.info(f"Fetching products for category {category_id}")
        
        products = []
        page = 0
        total_fetched = 0
        
        while total_fetched < max_products:
            # Construct API URL
            url = f"{PRODUCT_API_URL}?categoryCode={category_id}&page={page}&size={page_size}"
            
            response = self._make_request(url)
            if not response:
                break
                
            try:
                data = response.json()
                items = data.get('content', [])
                
                if not items:
                    break
                    
                products.extend(items)
                total_fetched += len(items)
                
                # Check if we're on the last page
                if data.get('last', False):
                    break
                    
                page += 1
                
            except Exception as e:
                logger.error(f"Error parsing product data: {e}")
                break
        
        logger.info(f"Fetched {len(products)} products for category {category_id}")
        
        # Save raw data
        with open(os.path.join(self.raw_dir, f"category_{category_id}_products.json"), 'w', encoding='utf-8') as f:
            json.dump(products, f, ensure_ascii=False, indent=2)
            
        return products
        
    def process_product(self, product_data):
        """
        Process a single product
        
        Args:
            product_data: Product data from API
            
        Returns:
            Boolean indicating if processing was successful
        """
        try:
            # Extract product ID
            product_id = product_data.get('code')
            
            # Skip if already processed or failed
            if product_id in self.processed_products or product_id in self.failed_products:
                return False
                
            # Format as shufersal_{product_id} for our database
            formatted_id = f"shufersal_{product_id}"
            
            # Create product directory
            product_dir = os.path.join(self.products_dir, formatted_id)
            os.makedirs(product_dir, exist_ok=True)
            
            # Extract relevant information
            name = product_data.get('productName', '')
            brand = product_data.get('brandName', '')
            price = product_data.get('price', {}).get('price', 0)
            unit = product_data.get('unitOfMeasure', '')
            category = product_data.get('hierarchy', [])[0]['hierarchyName'] if product_data.get('hierarchy') else ''
            subcategory = product_data.get('hierarchy', [])[-1]['hierarchyName'] if product_data.get('hierarchy') and len(product_data.get('hierarchy')) > 1 else ''
            
            # Extract Hebrew name if present
            name_he = ''
            if HEBREW_PATTERN.search(name):
                # Extract Hebrew characters
                hebrew_chars = HEBREW_PATTERN.findall(name)
                name_he = ''.join(hebrew_chars)
                
                # Remove Hebrew from English name
                name_en = HEBREW_PATTERN.sub('', name).strip()
                name_en = re.sub(r'\s+', ' ', name_en)
                
                if name_en:  # If we have remaining text after removing Hebrew
                    name = name_en
            
            # Create metadata
            metadata = {
                "product_id": formatted_id,
                "name": name,
                "name_he": name_he,
                "brand": brand,
                "price": price,
                "unit": unit,
                "retailer": "Shufersal",
                "category": category,
                "subcategory": subcategory,
                "source": "shufersal",
                "source_id": product_id
            }
            
            # Save metadata
            with open(os.path.join(product_dir, "metadata.json"), 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            # Download and save images
            self._download_product_images(product_data, product_dir)
            
            # Add to processed set
            self.processed_products.add(product_id)
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing product {product_data.get('code', 'unknown')}: {e}")
            self.failed_products.add(product_data.get('code', 'unknown'))
            return False
    
    def _download_product_images(self, product_data, product_dir):
        """
        Download product images
        
        Args:
            product_data: Product data
            product_dir: Directory to save images
        """
        # Get image URLs
        images = []
        
        # Main image
        if 'image' in product_data and product_data['image']:
            main_image = product_data['image']
            if not main_image.startswith('http'):
                main_image = urljoin(BASE_URL, main_image)
            images.append(main_image)
        
        # Additional images
        if 'additionalMarketingContent' in product_data:
            for img in product_data['additionalMarketingContent'].get('additionalImageLinks', []):
                if img and not img.startswith('http'):
                    img = urljoin(BASE_URL, img)
                if img:
                    images.append(img)
        
        # Download images
        for i, img_url in enumerate(images):
            try:
                response = self._make_request(img_url)
                if not response:
                    continue
                    
                img = Image.open(BytesIO(response.content))
                img = img.convert('RGB')  # Convert to RGB (in case of PNG with alpha)
                
                # Save image
                img_path = os.path.join(product_dir, f"{i+1:03d}.jpg")
                img.save(img_path, "JPEG", quality=95)
                
                time.sleep(0.1)  # Small delay to be gentle to the server
                
            except Exception as e:
                logger.error(f"Error downloading image {img_url}: {e}")
    
    def scrape_all_categories(self, max_products_per_category=500, max_total_products=50000, workers=10):
        """
        Scrape products from all categories
        
        Args:
            max_products_per_category: Maximum products per category
            max_total_products: Maximum total products
            workers: Number of worker threads
            
        Returns:
            Total number of products processed
        """
        # Get categories if not already loaded
        if not self.categories:
            self.get_categories()
            
        if not self.categories:
            logger.error("No categories found. Aborting.")
            return 0
            
        # Collect all category IDs (including subcategories)
        all_category_ids = []
        for cat_id, cat_info in self.categories.items():
            all_category_ids.append(cat_id)
            for subcat_id in cat_info['subcategories'].keys():
                all_category_ids.append(subcat_id)
        
        logger.info(f"Found {len(all_category_ids)} categories to scrape")
        
        total_processed = 0
        products_per_category = max(1, min(max_products_per_category, max_total_products // len(all_category_ids)))
        
        # Process each category
        for cat_id in tqdm(all_category_ids, desc="Processing categories"):
            # Stop if we've reached the maximum total
            if total_processed >= max_total_products:
                break
                
            # Fetch products for this category
            products = self.get_products_by_category(cat_id, max_products=products_per_category)
            
            # Process products with multiple threads
            with ThreadPoolExecutor(max_workers=workers) as executor:
                results = list(tqdm(
                    executor.map(self.process_product, products), 
                    total=len(products),
                    desc=f"Processing products for category {cat_id}"
                ))
                
            # Count successful processings
            successful = sum(1 for r in results if r)
            total_processed += successful
            
            logger.info(f"Processed {successful}/{len(products)} products for category {cat_id}")
            logger.info(f"Total processed so far: {total_processed}/{max_total_products}")
            
        return total_processed
    
    def search_products(self, query, max_products=1000):
        """
        Search for products
        
        Args:
            query: Search query
            max_products: Maximum number of products to fetch
            
        Returns:
            List of product data
        """
        logger.info(f"Searching for '{query}'")
        
        encoded_query = quote(query)
        url = f"{SEARCH_URL}?q={encoded_query}"
        
        response = self._make_request(url)
        if not response:
            return []
            
        # Extract product IDs from search results
        product_ids = re.findall(r'productId:"([^"]+)"', response.text)
        unique_ids = list(set(product_ids))
        
        logger.info(f"Found {len(unique_ids)} unique products for query '{query}'")
        
        # Limit to max_products
        unique_ids = unique_ids[:max_products]
        
        # Fetch detailed product data for each ID
        products = []
        for product_id in tqdm(unique_ids, desc=f"Fetching product details for '{query}'"):
            try:
                product_url = f"{PRODUCT_API_URL}/{product_id}"
                product_response = self._make_request(product_url)
                
                if product_response:
                    product_data = product_response.json()
                    products.append(product_data)
                    time.sleep(0.2)  # Small delay to be gentle to the server
            except Exception as e:
                logger.error(f"Error fetching product {product_id}: {e}")
        
        # Save raw data
        query_hash = hashlib.md5(query.encode()).hexdigest()[:8]
        with open(os.path.join(self.raw_dir, f"search_{query_hash}_products.json"), 'w', encoding='utf-8') as f:
            json.dump(products, f, ensure_ascii=False, indent=2)
            
        return products
    
    def scrape_popular_searches(self, searches, max_products_per_search=200, workers=10):
        """
        Scrape products from popular search terms
        
        Args:
            searches: List of search terms
            max_products_per_search: Maximum products per search
            workers: Number of worker threads
            
        Returns:
            Total number of products processed
        """
        total_processed = 0
        
        for query in tqdm(searches, desc="Processing search queries"):
            # Search for products
            products = self.search_products(query, max_products=max_products_per_search)
            
            # Process products with multiple threads
            with ThreadPoolExecutor(max_workers=workers) as executor:
                results = list(tqdm(
                    executor.map(self.process_product, products), 
                    total=len(products),
                    desc=f"Processing products for search '{query}'"
                ))
                
            # Count successful processings
            successful = sum(1 for r in results if r)
            total_processed += successful
            
            logger.info(f"Processed {successful}/{len(products)} products for search '{query}'")
            logger.info(f"Total processed so far: {total_processed}")
            
        return total_processed
    
    def create_summary(self):
        """
        Create a summary of the scraped data
        
        Returns:
            Dictionary with summary information
        """
        # Count products and images
        product_count = len(os.listdir(self.products_dir))
        
        total_images = 0
        products_with_images = 0
        products_with_hebrew = 0
        
        for product_id in os.listdir(self.products_dir):
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
        
        # Process each product
        for product_id in tqdm(os.listdir(self.products_dir), desc="Creating manifest"):
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
                "category": metadata.get("category", ""),
                "retailer": metadata.get("retailer", "")
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
    parser.add_argument("--max-products", type=int, default=50000, help="Maximum number of products to scrape")
    parser.add_argument("--max-per-category", type=int, default=500, help="Maximum products per category")
    parser.add_argument("--workers", type=int, default=10, help="Number of worker threads")
    parser.add_argument("--include-searches", action="store_true", help="Include popular search terms")
    parser.add_argument("--create-manifest", action="store_true", help="Create training manifest after scraping")
    
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = ShufersalScraper(args.output_dir)
    
    # Scrape all categories
    total_from_categories = scraper.scrape_all_categories(
        max_products_per_category=args.max_per_category,
        max_total_products=args.max_products,
        workers=args.workers
    )
    
    # If enabled, also scrape from popular searches
    if args.include_searches:
        # Popular grocery search terms in Hebrew and English
        popular_searches = [
            "חלב", "גבינה", "לחם", "ביצים", "עוף", "בשר", "דגים", "פירות", "ירקות",
            "חטיפים", "משקאות", "יין", "בירה", "קפה", "תה", "סוכר", "מלח", "שמן",
            "milk", "cheese", "bread", "eggs", "chicken", "meat", "fish", "fruits", "vegetables",
            "snacks", "drinks", "wine", "beer", "coffee", "tea", "sugar", "salt", "oil"
        ]
        
        remaining_products = max(0, args.max_products - total_from_categories)
        if remaining_products > 0:
            products_per_search = max(10, remaining_products // len(popular_searches))
            
            total_from_searches = scraper.scrape_popular_searches(
                popular_searches,
                max_products_per_search=products_per_search,
                workers=args.workers
            )
            
            logger.info(f"Total products scraped from searches: {total_from_searches}")
    
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