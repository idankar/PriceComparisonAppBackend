#!/usr/bin/env python3
"""
Carrefour Product Data Collection Script

This script collects product data from Carrefour's website using their public API.
It organizes product information and images into a structured format for training
a product recognition model.

Note: This script uses Carrefour's public web API which is unofficial,
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
from urllib.parse import quote_plus

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("carrefour_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Carrefour API Constants
API_BASE_URL = "https://www.carrefour.fr/api/graphql"
PRODUCT_SEARCH_QUERY = """
query SearchProducts($query: String!, $from: Int, $size: Int, $filter: [String], $sort: [SearchProductsSortInput!]) {
  searchProducts(query: $query, from: $from, size: $size, filter: $filter, sort: $sort) {
    products {
      id
      designation
      breadcrumb {
        label
        url
      }
      brand {
        label
      }
      attributes {
        label
        value
      }
      ean
      images {
        main {
          url
        }
        secondary {
          url
        }
      }
      variants {
        id
      }
      price {
        unit {
          price
          pricePerUnitText
          isPromoted
        }
      }
    }
    pagination {
      resultsPerPage
      totalResults
      totalPages
    }
  }
}
"""

PRODUCT_DETAILS_QUERY = """
query GetProductDetails($productId: ID!) {
  product(id: $productId) {
    id
    designation
    brand {
      label
    }
    attributes {
      label
      value
    }
    longDescription
    ingredients
    nutritionalInformation {
      nutritionalTable {
        headers
        lines {
          label
          values
        }
      }
    }
    images {
      main {
        url
      }
      secondary {
        url
      }
      packagingInformation {
        url
      }
    }
    price {
      unit {
        price
        pricePerUnitText
        isPromoted
      }
    }
    breadcrumb {
      label
      url
    }
  }
}
"""

CATEGORIES_QUERY = """
query GetCategories {
  categories {
    id
    label
    url
    subCategories {
      id
      label
      url
      subCategories {
        id
        label
        url
      }
    }
  }
}
"""

# Common search terms for groceries and household items
COMMON_SEARCH_TERMS = [
    # Food items in French
    "céréales", "pâtes", "riz", "pain", "lait", "yaourt", "fromage",
    "café", "thé", "jus", "soda", "eau", "snacks", "chocolat", 
    "biscuits", "chips", "noix", "fruits", "légumes", "conserves",
    "surgelés", "condiments", "épices", "sauce", "huile", "vinaigre",
    
    # Household items in French
    "lessive", "savon", "nettoyants", "papier toilette", "essuie-tout",
    "mouchoirs", "sacs poubelle", "ampoules", "piles", "ustensiles",
    "vaisselle", "verres", "casseroles", "boîtes", "stockage",
    
    # Personal care in French
    "shampooing", "après-shampooing", "dentifrice", "brosse à dents", "fil dentaire",
    "déodorant", "gel douche", "lotion", "crème solaire", "rasoir",
    "soins féminins", "premiers secours", "vitamines", "médicaments",
    
    # Baby products in French
    "couches", "lingettes", "nourriture bébé", "lait infantile", "soins bébé",
    
    # Pet products in French
    "nourriture chien", "nourriture chat", "friandises animaux", "jouets animaux", "soins animaux"
]


class CarrefourAPI:
    """Carrefour API client"""
    
    def __init__(self):
        """Initialize the Carrefour API client"""
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7"
        })
        
        # Request counter for rate limiting
        self.request_count = 0
        self.last_request_time = time.time()
    
    def _rate_limit(self):
        """
        Apply rate limiting to avoid too many requests
        """
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        # Be conservative with request rate to avoid blocking
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)
            
        self.last_request_time = time.time()
        self.request_count += 1
    
    def graphql_request(self, query, variables=None):
        """
        Make a GraphQL request to the Carrefour API
        
        Args:
            query: GraphQL query
            variables: Query variables
            
        Returns:
            Response data
        """
        # Apply rate limiting
        self._rate_limit()
        
        try:
            payload = {
                "query": query,
                "variables": variables or {}
            }
            
            response = self.session.post(
                API_BASE_URL,
                json=payload,
                timeout=30
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return None
    
    def search_products(self, query, page=0, size=24, filters=None, sort=None):
        """
        Search for products
        
        Args:
            query: Search query
            page: Page number (starts from 0)
            size: Results per page
            filters: List of filters
            sort: Sort order
            
        Returns:
            Search results
        """
        from_idx = page * size
        
        variables = {
            "query": query,
            "from": from_idx,
            "size": size,
            "filter": filters or [],
            "sort": sort or []
        }
        
        result = self.graphql_request(PRODUCT_SEARCH_QUERY, variables)
        
        if not result:
            return None
            
        try:
            return result["data"]["searchProducts"]
        except (KeyError, TypeError):
            logger.error("Error parsing search results")
            return None
    
    def get_product_details(self, product_id):
        """
        Get detailed information for a product
        
        Args:
            product_id: Product ID
            
        Returns:
            Product details
        """
        variables = {
            "productId": product_id
        }
        
        result = self.graphql_request(PRODUCT_DETAILS_QUERY, variables)
        
        if not result:
            return None
            
        try:
            return result["data"]["product"]
        except (KeyError, TypeError):
            logger.error(f"Error parsing product details for ID {product_id}")
            return None
    
    def get_categories(self):
        """
        Get category hierarchy
        
        Returns:
            List of categories
        """
        result = self.graphql_request(CATEGORIES_QUERY)
        
        if not result:
            return None
            
        try:
            return result["data"]["categories"]
        except (KeyError, TypeError):
            logger.error("Error parsing categories")
            return None


class CarrefourScraper:
    """Scraper for Carrefour products"""
    
    def __init__(self, output_dir):
        """
        Initialize the scraper
        
        Args:
            output_dir: Directory to save scraped data
        """
        self.api = CarrefourAPI()
        self.output_dir = output_dir
        self.products_dir = os.path.join(output_dir, "products")
        self.raw_dir = os.path.join(output_dir, "raw", "carrefour")
        
        # Create directories if they don't exist
        os.makedirs(self.products_dir, exist_ok=True)
        os.makedirs(self.raw_dir, exist_ok=True)
        
        # Store processed products to avoid duplicates
        self.processed_products = set()
        self.failed_products = set()
        
        # Store categories
        self.categories = []
    
    def get_categories(self):
        """
        Get Carrefour categories
        
        Returns:
            List of categories
        """
        logger.info("Getting categories")
        
        categories = self.api.get_categories()
        
        if not categories:
            logger.error("Failed to get categories")
            return []
            
        self.categories = categories
        
        # Save raw data
        with open(os.path.join(self.raw_dir, "categories.json"), "w", encoding="utf-8") as f:
            json.dump(categories, f, ensure_ascii=False, indent=2)
            
        # Process categories to flat list
        flat_categories = []
        
        for category in categories:
            flat_categories.append({
                "id": category.get("id", ""),
                "label": category.get("label", ""),
                "url": category.get("url", ""),
                "level": 1
            })
            
            for subcategory in category.get("subCategories", []):
                flat_categories.append({
                    "id": subcategory.get("id", ""),
                    "label": subcategory.get("label", ""),
                    "url": subcategory.get("url", ""),
                    "level": 2,
                    "parent": category.get("label", "")
                })
                
                for subsubcategory in subcategory.get("subCategories", []):
                    flat_categories.append({
                        "id": subsubcategory.get("id", ""),
                        "label": subsubcategory.get("label", ""),
                        "url": subsubcategory.get("url", ""),
                        "level": 3,
                        "parent": subcategory.get("label", ""),
                        "grandparent": category.get("label", "")
                    })
        
        logger.info(f"Found {len(flat_categories)} categories")
        
        return flat_categories
    
    def search_products(self, query, max_products=100):
        """
        Search for products by query
        
        Args:
            query: Search query
            max_products: Maximum number of products to fetch
            
        Returns:
            List of products
        """
        logger.info(f"Searching for '{query}'")
        
        products = []
        page = 0
        page_size = min(24, max_products)  # Carrefour uses 24 products per page
        
        while len(products) < max_products:
            # Search for products
            search_result = self.api.search_products(query, page=page, size=page_size)
            
            if not search_result or not search_result.get("products"):
                break
                
            # Add products
            products.extend(search_result["products"])
            
            # Check if there are more pages
            pagination = search_result.get("pagination", {})
            total_pages = pagination.get("totalPages", 0)
            
            if page >= total_pages - 1 or not pagination.get("totalResults", 0):
                break
                
            # Move to next page
            page += 1
            
            # Small delay between requests
            time.sleep(1.0)
        
        # Limit to max_products
        products = products[:max_products]
        
        logger.info(f"Found {len(products)} products for query '{query}'")
        
        # Save raw data
        if products:
            query_hash = hashlib.md5(query.encode()).hexdigest()[:8]
            with open(os.path.join(self.raw_dir, f"search_{query_hash}_products.json"), "w", encoding="utf-8") as f:
                json.dump(products, f, ensure_ascii=False, indent=2)
        
        return products
    
    def process_product(self, product_data, get_details=True):
        """
        Process a single product
        
        Args:
            product_data: Product data from search
            get_details: Whether to get detailed product information
            
        Returns:
            Boolean indicating if processing was successful
        """
        try:
            # Extract product ID
            product_id = product_data.get("id")
            
            if not product_id:
                logger.warning("Product without ID, skipping")
                return False
                
            # Skip if already processed or failed
            if product_id in self.processed_products or product_id in self.failed_products:
                return False
                
            # Format as carrefour_{product_id} for our database
            formatted_id = f"carrefour_{product_id}"
            
            # Create product directory
            product_dir = os.path.join(self.products_dir, formatted_id)
            os.makedirs(product_dir, exist_ok=True)
            
            # Get detailed information if requested
            product_details = None
            if get_details:
                product_details = self.api.get_product_details(product_id)
            
            # Extract product information
            name = product_data.get("designation", "")
            
            # Get brand
            brand_obj = product_data.get("brand", {})
            brand = brand_obj.get("label", "") if brand_obj else ""
            
            # Get price
            price_obj = product_data.get("price", {}).get("unit", {})
            price = price_obj.get("price", 0) if price_obj else 0
            
            # Get categories from breadcrumb
            breadcrumb = product_data.get("breadcrumb", [])
            categories = [item.get("label", "") for item in breadcrumb]
            
            category = categories[-1] if categories else ""
            subcategory = categories[-2] if len(categories) > 1 else ""
            department = categories[0] if categories else ""
            
            # Get images
            images_obj = product_data.get("images", {})
            image_urls = []
            
            # Main image
            main_image = images_obj.get("main", {})
            if main_image and main_image.get("url"):
                image_urls.append(main_image["url"])
                
            # Secondary images
            secondary_images = images_obj.get("secondary", [])
            for img in secondary_images:
                if img and img.get("url"):
                    image_urls.append(img["url"])
            
            # If we have detailed product info, use it to enrich the data
            if product_details:
                # Use detailed name if available
                if product_details.get("designation"):
                    name = product_details["designation"]
                    
                # Use detailed brand if available
                detailed_brand = product_details.get("brand", {})
                if detailed_brand and detailed_brand.get("label"):
                    brand = detailed_brand["label"]
                    
                # Get additional images
                detailed_images = product_details.get("images", {})
                
                # Main image
                main_image = detailed_images.get("main", {})
                if main_image and main_image.get("url") and main_image["url"] not in image_urls:
                    image_urls.append(main_image["url"])
                    
                # Secondary images
                secondary_images = detailed_images.get("secondary", [])
                for img in secondary_images:
                    if img and img.get("url") and img["url"] not in image_urls:
                        image_urls.append(img["url"])
                        
                # Packaging images
                packaging_images = detailed_images.get("packagingInformation", [])
                for img in packaging_images:
                    if img and img.get("url") and img["url"] not in image_urls:
                        image_urls.append(img["url"])
            
            # Create metadata
            metadata = {
                "product_id": formatted_id,
                "name": name,
                "brand": brand,
                "price": price,
                "currency": "EUR",
                "retailer": "Carrefour",
                "category": category,
                "subcategory": subcategory,
                "department": department,
                "source": "carrefour",
                "source_id": product_id
            }
            
            # Add EAN if available
            if product_data.get("ean"):
                metadata["ean"] = product_data["ean"]
            
            # Save metadata
            with open(os.path.join(product_dir, "metadata.json"), "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            # Download and save images
            self._download_product_images(image_urls, product_dir)
            
            # Add to processed set
            self.processed_products.add(product_id)
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing product: {e}")
            
            if product_data and "id" in product_data:
                self.failed_products.add(product_data["id"])
                
            return False
    
    def _download_product_images(self, image_urls, product_dir):
        """
        Download product images
        
        Args:
            image_urls: List of image URLs
            product_dir: Directory to save images
        """
        for i, img_url in enumerate(image_urls):
            try:
                # For Carrefour images, make sure we get a high-resolution version
                # Replace thumbnail size with larger size
                img_url = re.sub(r'/\d+x\d+/', '/1200x1200/', img_url)
                
                response = requests.get(img_url, timeout=30)
                response.raise_for_status()
                
                # Open and verify image
                try:
                    img = Image.open(BytesIO(response.content))
                    
                    # Skip very small images
                    if img.width < 100 or img.height < 100:
                        continue
                        
                    # Convert to RGB (in case of PNG with alpha)
                    img = img.convert("RGB")
                    
                    # Save image
                    img_path = os.path.join(product_dir, f"{i+1:03d}.jpg")
                    img.save(img_path, "JPEG", quality=95)
                    
                except Exception as e:
                    logger.error(f"Error processing image {img_url}: {e}")
                    continue
                
                # Small delay between downloads
                time.sleep(0.1)
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Error downloading image {img_url}: {e}")
    
    def scrape_categories(self, max_products=30000, max_per_category=500, workers=10):
        """
        Scrape products from categories
        
        Args:
            max_products: Maximum total products to scrape
            max_per_category: Maximum products per category
            workers: Number of worker threads
            
        Returns:
            Total number of products processed
        """
        # Get categories if not already loaded
        if not self.categories:
            flat_categories = self.get_categories()
        else:
            flat_categories = self.categories
            
        if not flat_categories:
            logger.error("No categories found. Aborting.")
            return 0
            
        # Filter to level 2 and 3 categories (more specific)
        product_categories = [cat for cat in flat_categories if cat["level"] in [2, 3]]
        
        if not product_categories:
            logger.warning("No product categories found. Using all categories.")
            product_categories = flat_categories
            
        logger.info(f"Found {len(product_categories)} product categories")
        
        # Shuffle categories for more variety
        random.shuffle(product_categories)
        
        # Calculate products per category
        products_per_category = min(max_per_category, max_products // len(product_categories))
        
        # Process each category
        total_processed = 0
        
        for category in tqdm(product_categories, desc="Processing categories"):
            # Stop if we've reached the maximum total
            if total_processed >= max_products:
                break
                
            category_name = category["label"]
            
            # Search for products in this category
            logger.info(f"Searching in category '{category_name}'")
            products = self.search_products(category_name, max_products=products_per_category)
            
            # Process products with multiple threads
            with ThreadPoolExecutor(max_workers=workers) as executor:
                results = list(tqdm(
                    executor.map(lambda p: self.process_product(p, get_details=True), products),
                    total=len(products),
                    desc=f"Processing products for '{category_name}'"
                ))
                
            # Count successful processings
            successful = sum(1 for r in results if r)
            total_processed += successful
            
            logger.info(f"Processed {successful}/{len(products)} products for category '{category_name}'")
            logger.info(f"Total processed so far: {total_processed}/{max_products}")
        
        return total_processed
    
    def scrape_search_terms(self, max_products=30000, max_per_search=100, workers=10):
        """
        Scrape products using common search terms
        
        Args:
            max_products: Maximum total products to scrape
            max_per_search: Maximum products per search term
            workers: Number of worker threads
            
        Returns:
            Total number of products processed
        """
        # Calculate products per search term
        products_per_search = min(max_per_search, max_products // len(COMMON_SEARCH_TERMS))
        
        # Shuffle search terms for more variety
        search_terms = COMMON_SEARCH_TERMS.copy()
        random.shuffle(search_terms)
        
        # Process each search term
        total_processed = 0
        
        for term in tqdm(search_terms, desc="Processing search terms"):
            # Stop if we've reached the maximum total
            if total_processed >= max_products:
                break
                
            # Search for products with this term
            products = self.search_products(term, max_products=products_per_search)
            
            # Process products with multiple threads
            with ThreadPoolExecutor(max_workers=workers) as executor:
                results = list(tqdm(
                    executor.map(lambda p: self.process_product(p, get_details=True), products),
                    total=len(products),
                    desc=f"Processing products for '{term}'"
                ))
                
            # Count successful processings
            successful = sum(1 for r in results if r)
            total_processed += successful
            
            logger.info(f"Processed {successful}/{len(products)} products for search '{term}'")
            logger.info(f"Total processed so far: {total_processed}/{max_products}")
        
        return total_processed
    
    def create_summary(self):
        """
        Create a summary of the scraped data
        
        Returns:
            Dictionary with summary information
        """
        # Count products and images
        product_count = len([p for p in os.listdir(self.products_dir) if p.startswith("carrefour_")])
        
        total_images = 0
        products_with_images = 0
        
        for product_id in os.listdir(self.products_dir):
            if not product_id.startswith("carrefour_"):
                continue
                
            product_dir = os.path.join(self.products_dir, product_id)
            
            # Count images
            images = [f for f in os.listdir(product_dir) if f.endswith(('.jpg', '.jpeg', '.png'))]
            image_count = len(images)
            total_images += image_count
            
            if image_count > 0:
                products_with_images += 1
        
        # Calculate averages
        avg_images_per_product = total_images / product_count if product_count > 0 else 0
        percent_with_images = (products_with_images / product_count * 100) if product_count > 0 else 0
        
        # Create summary
        summary = {
            "total_products": product_count,
            "total_images": total_images,
            "products_with_images": products_with_images,
            "avg_images_per_product": avg_images_per_product,
            "percent_with_images": percent_with_images
        }
        
        # Save summary
        with open(os.path.join(self.output_dir, "carrefour_summary.json"), "w", encoding="utf-8") as f:
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
        
        # Process each Carrefour product
        for product_id in tqdm(os.listdir(self.products_dir), desc="Creating manifest"):
            if not product_id.startswith("carrefour_"):
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
                
            with open(metadata_path, "r", encoding="utf-8") as f:
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
                "category": metadata.get("category", ""),
                "retailer": "Carrefour"
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
        manifest_path = os.path.join(self.output_dir, "train_manifests", "carrefour_manifest.json")
        os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
        
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Created manifest with {products_included} products (excluded {products_excluded})")
        logger.info(f"Train images: {sum(len(p['images']) for p in manifest['train'].values())}")
        logger.info(f"Val images: {sum(len(p['images']) for p in manifest['val'].values())}")
        
        return manifest_path


def main():
    parser = argparse.ArgumentParser(description="Carrefour Product Data Scraper")
    parser.add_argument("--output-dir", type=str, default="data", help="Output directory")
    parser.add_argument("--max-products", type=int, default=30000, help="Maximum number of products to scrape")
    parser.add_argument("--max-per-category", type=int, default=500, help="Maximum products per category")
    parser.add_argument("--max-per-search", type=int, default=100, help="Maximum products per search term")
    parser.add_argument("--workers", type=int, default=10, help="Number of worker threads")
    parser.add_argument("--create-manifest", action="store_true", help="Create training manifest after scraping")
    parser.add_argument("--strategy", type=str, default="both", choices=["categories", "search", "both"], 
                      help="Scraping strategy: categories, search terms, or both")
    
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = CarrefourScraper(args.output_dir)
    
    # Execute scraping strategy
    total_products = 0
    
    if args.strategy in ["categories", "both"]:
        # Set target for categories (if both, use half of max)
        target = args.max_products if args.strategy == "categories" else args.max_products // 2
        
        cat_products = scraper.scrape_categories(
            max_products=target,
            max_per_category=args.max_per_category,
            workers=args.workers
        )
        
        total_products += cat_products
        logger.info(f"Scraped {cat_products} products from categories")
    
    if args.strategy in ["search", "both"] and total_products < args.max_products:
        # Scrape remaining products using search terms
        remaining = args.max_products - total_products
        
        search_products = scraper.scrape_search_terms(
            max_products=remaining,
            max_per_search=args.max_per_search,
            workers=args.workers
        )
        
        total_products += search_products
        logger.info(f"Scraped {search_products} products from search terms")
    
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