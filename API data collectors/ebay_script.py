#!/usr/bin/env python3
"""
eBay Product Data Collection Script

This script collects product data from eBay using their Finding and Shopping APIs.
It organizes product information and images into a structured format for training
a product recognition model.

Prerequisites:
- eBay Developer account
- API credentials (App ID)
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
import xml.etree.ElementTree as ET
from urllib.parse import quote_plus

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ebay_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# eBay API Constants
FINDING_API_URL = "https://svcs.ebay.com/services/search/FindingService/v1"
SHOPPING_API_URL = "https://open.api.ebay.com/shopping"

# Product categories for browsing
PRODUCT_CATEGORIES = [
    # Groceries and Food
    "Food & Beverages",
    "Pantry Items",
    "Coffee, Tea & Cocoa",
    "Snacks",
    
    # Household items
    "Home & Garden",
    "Kitchen, Dining & Bar",
    "Household Supplies",
    "Bath",
    "Bedding",
    
    # Personal care
    "Health & Beauty",
    "Personal Care",
    "Baby",
    
    # Electronics (commonly found in households)
    "Consumer Electronics",
    "Cell Phones & Accessories",
    
    # Office supplies
    "Office Supplies",
    "School Supplies"
]

# Common search terms for groceries and household items
COMMON_SEARCH_TERMS = [
    # Food items
    "cereal", "pasta", "rice", "bread", "milk", "yogurt", "cheese", 
    "coffee", "tea", "juice", "soda", "water", "snacks", "chocolate",
    "cookies", "crackers", "chips", "nuts", "fruit", "canned food",
    "frozen food", "condiments", "spices", "sauce", "oil", "vinegar",
    
    # Household items
    "detergent", "soap", "cleaners", "paper towels", "toilet paper",
    "tissues", "trash bags", "light bulbs", "batteries", "kitchen tools",
    "utensils", "dishes", "glasses", "cookware", "storage containers",
    
    # Personal care
    "shampoo", "conditioner", "toothpaste", "toothbrush", "floss",
    "deodorant", "body wash", "lotion", "sunscreen", "shaving cream",
    "razor", "feminine care", "first aid", "vitamins", "medicine",
    
    # Baby products
    "diapers", "wipes", "baby food", "formula", "baby care",
    
    # Pet products
    "dog food", "cat food", "pet treats", "pet toys", "pet care"
]


class EbayAPI:
    """eBay API client for Finding and Shopping APIs"""
    
    def __init__(self, app_id, global_id="EBAY-US"):
        """
        Initialize the eBay API client
        
        Args:
            app_id: eBay App ID (Developer Key)
            global_id: eBay Global ID (site to search)
        """
        self.app_id = app_id
        self.global_id = global_id
        
        # Request counter for rate limiting
        self.request_count = 0
        self.last_request_time = time.time()
    
    def _rate_limit(self):
        """
        Apply rate limiting to avoid exceeding API limits
        """
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        # eBay API allows 5 requests per second, but we'll be more conservative
        if elapsed < 0.25:
            time.sleep(0.25 - elapsed)
            
        self.last_request_time = time.time()
        self.request_count += 1
    
    def find_items(self, keywords=None, category_id=None, page=1, items_per_page=100):
        """
        Search for items using the Finding API
        
        Args:
            keywords: Search keywords
            category_id: eBay category ID
            page: Page number
            items_per_page: Items per page
            
        Returns:
            List of item data
        """
        # Apply rate limiting
        self._rate_limit()
        
        # Build request parameters
        params = {
            "OPERATION-NAME": "findItemsAdvanced",
            "SERVICE-VERSION": "1.0.0",
            "SECURITY-APPNAME": self.app_id,
            "RESPONSE-DATA-FORMAT": "JSON",
            "REST-PAYLOAD": True,
            "paginationInput.pageNumber": page,
            "paginationInput.entriesPerPage": items_per_page,
            "GLOBAL-ID": self.global_id,
            "itemFilter(0).name": "HideDuplicateItems",
            "itemFilter(0).value": "true",
            "itemFilter(1).name": "ListingType",
            "itemFilter(1).value": "FixedPrice",
            "outputSelector(0)": "PictureURLLarge",
            "outputSelector(1)": "ItemSpecifics"
        }
        
        if keywords:
            params["keywords"] = keywords
            
        if category_id:
            params["categoryId"] = category_id
        
        # Make the request
        try:
            response = requests.get(FINDING_API_URL, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract items from response
            try:
                search_result = data["findItemsAdvancedResponse"][0]
                
                # Check if any items found
                if "searchResult" not in search_result or int(search_result["searchResult"][0]["@count"]) == 0:
                    return [], 0, 0
                    
                items = search_result["searchResult"][0]["item"]
                
                # Get pagination info
                total_pages = int(search_result["paginationOutput"][0]["totalPages"][0])
                total_entries = int(search_result["paginationOutput"][0]["totalEntries"][0])
                
                return items, total_pages, total_entries
                
            except (KeyError, IndexError) as e:
                logger.error(f"Error parsing search results: {e}")
                return [], 0, 0
                
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return [], 0, 0
    
    def get_item_details(self, item_id):
        """
        Get detailed information about an item using the Shopping API
        
        Args:
            item_id: eBay item ID
            
        Returns:
            Item data
        """
        # Apply rate limiting
        self._rate_limit()
        
        # Build request parameters
        params = {
            "callname": "GetSingleItem",
            "responseencoding": "JSON",
            "appid": self.app_id,
            "siteid": 0,  # US site
            "version": 967,
            "ItemID": item_id,
            "IncludeSelector": "Details,ItemSpecifics,Description,PictureURLs"
        }
        
        # Make the request
        try:
            response = requests.get(SHOPPING_API_URL, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for errors
            if "Errors" in data and data["Errors"]:
                error_msg = data["Errors"][0]["LongMessage"]
                logger.error(f"eBay API error: {error_msg}")
                return None
                
            # Extract item
            if "Item" in data:
                return data["Item"]
                
            return None
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return None
    
    def get_category_info(self, category_id=None):
        """
        Get category hierarchy information
        
        Args:
            category_id: eBay category ID (None for top level)
            
        Returns:
            Category data
        """
        # Apply rate limiting
        self._rate_limit()
        
        # Build request parameters
        params = {
            "callname": "GetCategoryInfo",
            "responseencoding": "JSON",
            "appid": self.app_id,
            "siteid": 0,  # US site
            "version": 967,
            "IncludeSelector": "ChildCategories"
        }
        
        if category_id:
            params["CategoryID"] = category_id
        
        # Make the request
        try:
            response = requests.get(SHOPPING_API_URL, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for errors
            if "Errors" in data and data["Errors"]:
                error_msg = data["Errors"][0]["LongMessage"]
                logger.error(f"eBay API error: {error_msg}")
                return None
                
            # Extract categories
            if "CategoryArray" in data:
                return data["CategoryArray"]["Category"]
                
            return None
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return None


class EbayScraper:
    """Scraper for eBay products"""
    
    def __init__(self, app_id, output_dir, global_id="EBAY-US"):
        """
        Initialize the scraper
        
        Args:
            app_id: eBay App ID
            output_dir: Directory to save scraped data
            global_id: eBay Global ID
        """
        self.api = EbayAPI(app_id, global_id)
        self.output_dir = output_dir
        self.products_dir = os.path.join(output_dir, "products")
        self.raw_dir = os.path.join(output_dir, "raw", "ebay")
        
        # Create directories if they don't exist
        os.makedirs(self.products_dir, exist_ok=True)
        os.makedirs(self.raw_dir, exist_ok=True)
        
        # Store processed products to avoid duplicates
        self.processed_products = set()
        self.failed_products = set()
        
        # Store category mapping
        self.categories = {}
        self.category_names = {}
    
    def get_categories(self):
        """
        Get eBay category hierarchy
        
        Returns:
            Dictionary of categories
        """
        logger.info("Getting eBay category hierarchy")
        
        # Get top-level categories
        top_categories = self.api.get_category_info()
        
        if not top_categories:
            logger.error("Failed to get top-level categories")
            return {}
            
        # Process categories
        for category in top_categories:
            category_id = category["CategoryID"]
            category_name = category["CategoryName"]
            
            self.categories[category_id] = {
                "id": category_id,
                "name": category_name,
                "level": 1,
                "parent_id": None
            }
            
            self.category_names[category_name] = category_id
            
            # Get child categories for common product categories
            if any(keyword.lower() in category_name.lower() for keyword in PRODUCT_CATEGORIES):
                logger.info(f"Getting subcategories for {category_name} ({category_id})")
                
                subcategories = self.api.get_category_info(category_id)
                
                if subcategories:
                    for subcategory in subcategories:
                        subcat_id = subcategory["CategoryID"]
                        subcat_name = subcategory["CategoryName"]
                        
                        self.categories[subcat_id] = {
                            "id": subcat_id,
                            "name": subcat_name,
                            "level": 2,
                            "parent_id": category_id
                        }
                        
                        self.category_names[subcat_name] = subcat_id
        
        logger.info(f"Found {len(self.categories)} categories")
        
        # Save categories to file
        with open(os.path.join(self.raw_dir, "categories.json"), "w", encoding="utf-8") as f:
            json.dump(self.categories, f, ensure_ascii=False, indent=2)
            
        return self.categories
    
    def search_products(self, keywords=None, category_name=None, category_id=None, max_products=100):
        """
        Search for products by keywords or category
        
        Args:
            keywords: Search keywords
            category_name: Category name
            category_id: Category ID
            max_products: Maximum number of products to fetch
            
        Returns:
            List of product data
        """
        # Resolve category ID from name if provided
        if category_name and not category_id:
            if not self.category_names:
                self.get_categories()
                
            category_id = self.category_names.get(category_name)
            
            if not category_id:
                # Try case-insensitive match
                for name, cat_id in self.category_names.items():
                    if name.lower() == category_name.lower():
                        category_id = cat_id
                        break
                        
            if not category_id:
                logger.warning(f"Category '{category_name}' not found")
        
        # Log search parameters
        search_desc = []
        if keywords:
            search_desc.append(f"keywords='{keywords}'")
        if category_name:
            search_desc.append(f"category='{category_name}'")
        if category_id:
            search_desc.append(f"category_id='{category_id}'")
            
        logger.info(f"Searching for products: {', '.join(search_desc)}")
        
        # Search for products
        products = []
        page = 1
        items_per_page = min(100, max_products)
        
        while len(products) < max_products:
            # Make API request
            items, total_pages, total_entries = self.api.find_items(
                keywords=keywords,
                category_id=category_id,
                page=page,
                items_per_page=items_per_page
            )
            
            if not items:
                logger.info(f"No more items found after {len(products)} products")
                break
                
            # Add items to products list
            products.extend(items)
            logger.info(f"Found {len(items)} products on page {page}/{total_pages}")
            
            # Check if we reached the last page
            if page >= total_pages:
                break
                
            # Move to next page
            page += 1
            
            # Small delay between requests
            time.sleep(0.5)
        
        # Limit to max_products
        products = products[:max_products]
        
        # Save raw data
        if products:
            # Create filename
            filename_parts = []
            if keywords:
                filename_parts.append(f"kw_{keywords.replace(' ', '_')}")
            if category_id:
                filename_parts.append(f"cat_{category_id}")
                
            if not filename_parts:
                filename_parts.append("search")
                
            filename = "_".join(filename_parts)
            filename = re.sub(r'[^\w\-_]', '', filename)
            
            if len(filename) > 50:
                filename = filename[:47] + "..."
                
            # Add hash to ensure uniqueness
            filename += f"_{hashlib.md5(str(time.time()).encode()).hexdigest()[:6]}"
            
            # Save to file
            with open(os.path.join(self.raw_dir, f"{filename}.json"), "w", encoding="utf-8") as f:
                json.dump(products, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Found total of {len(products)} products")
        return products
    
    def process_product(self, product_data, get_details=True):
        """
        Process a single product from search results
        
        Args:
            product_data: Product data from search API
            get_details: Whether to get detailed information
            
        Returns:
            Boolean indicating if processing was successful
        """
        try:
            # Extract item ID
            item_id = product_data.get("itemId", [""])[0]
            
            if not item_id:
                logger.warning("Product without ID, skipping")
                return False
                
            # Skip if already processed or failed
            if item_id in self.processed_products or item_id in self.failed_products:
                return False
                
            # Format as ebay_{item_id} for our database
            formatted_id = f"ebay_{item_id}"
            
            # Create product directory
            product_dir = os.path.join(self.products_dir, formatted_id)
            os.makedirs(product_dir, exist_ok=True)
            
            # Get additional details if requested
            item_details = None
            if get_details:
                item_details = self.api.get_item_details(item_id)
                
                # If details request failed, continue with search data
                if not item_details:
                    logger.warning(f"Failed to get details for item {item_id}, using search data only")
            
            # Extract product information
            title = product_data.get("title", [""])[0]
            
            # Get price
            price_info = product_data.get("sellingStatus", [{}])[0].get("currentPrice", [{}])[0]
            price = float(price_info.get("__value__", 0))
            currency = price_info.get("@currencyId", "USD")
            
            # Get category info
            category_info = product_data.get("primaryCategory", [{}])[0]
            category_id = category_info.get("categoryId", [""])[0]
            category_name = category_info.get("categoryName", [""])[0]
            
            # Try to extract brand from title
            brand = ""
            
            # If we have item details, use them to get more accurate information
            if item_details:
                # Check for brand in item specifics
                if "ItemSpecifics" in item_details:
                    for specific in item_details["ItemSpecifics"].get("NameValueList", []):
                        if specific["Name"] == "Brand":
                            brand = specific["Value"]
                            break
                
                # If not found in specifics, try to extract from title
                if not brand:
                    title = item_details.get("Title", title)
                    
                # Use more accurate price from details
                if "ConvertedCurrentPrice" in item_details:
                    price = float(item_details["ConvertedCurrentPrice"]["Value"])
                    currency = item_details["ConvertedCurrentPrice"]["CurrencyID"]
                    
                # Use more accurate category from details
                if "PrimaryCategoryName" in item_details:
                    category_name = item_details["PrimaryCategoryName"]
                    
                if "PrimaryCategoryID" in item_details:
                    category_id = item_details["PrimaryCategoryID"]
            
            # Extract brand from title if still not found
            if not brand:
                # Common brand patterns in titles
                brand_match = re.search(r'^([\w\-]+)\s', title)
                if brand_match:
                    brand = brand_match.group(1)
            
            # Get image URLs
            image_urls = []
            
            # Try large image first
            if "pictureURLLarge" in product_data:
                image_urls.extend(product_data["pictureURLLarge"])
            
            # Fall back to standard image
            if not image_urls and "galleryURL" in product_data:
                image_urls.append(product_data["galleryURL"][0])
                
            # If we have item details, use all available images
            if item_details and "PictureURL" in item_details:
                pic_urls = item_details["PictureURL"]
                if isinstance(pic_urls, list):
                    image_urls = pic_urls
                else:
                    image_urls = [pic_urls]
            
            # Create metadata
            metadata = {
                "product_id": formatted_id,
                "name": title,
                "brand": brand,
                "price": price,
                "currency": currency,
                "retailer": "eBay",
                "category": category_name,
                "category_id": category_id,
                "source": "ebay",
                "source_id": item_id,
                "url": product_data.get("viewItemURL", [""])[0]
            }
            
            # Save metadata
            with open(os.path.join(product_dir, "metadata.json"), "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            # Download and save images
            self._download_product_images(image_urls, product_dir)
            
            # Add to processed set
            self.processed_products.add(item_id)
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing product: {e}")
            
            try:
                if product_data and "itemId" in product_data:
                    item_id = product_data["itemId"][0]
                    self.failed_products.add(item_id)
            except:
                pass
                
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
    
    def scrape_categories(self, max_products=30000, max_per_category=1000, workers=10):
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
            self.get_categories()
            
        if not self.categories:
            logger.error("No categories found. Aborting.")
            return 0
            
        # Find relevant categories that match our product types
        relevant_categories = []
        
        for cat_id, cat_info in self.categories.items():
            cat_name = cat_info["name"]
            # Check if category matches our interests
            if any(keyword.lower() in cat_name.lower() for keyword in PRODUCT_CATEGORIES):
                relevant_categories.append((cat_id, cat_name))
        
        if not relevant_categories:
            logger.warning("No relevant categories found. Using all categories.")
            relevant_categories = [(cat_id, cat_info["name"]) for cat_id, cat_info in self.categories.items()]
            
        logger.info(f"Found {len(relevant_categories)} relevant categories")
        
        # Shuffle categories for more variety
        random.shuffle(relevant_categories)
        
        # Calculate products per category
        products_per_category = min(max_per_category, max_products // len(relevant_categories))
        
        # Process each category
        total_processed = 0
        
        for cat_id, cat_name in tqdm(relevant_categories, desc="Processing categories"):
            # Stop if we've reached the maximum total
            if total_processed >= max_products:
                break
                
            # Search for products in this category
            logger.info(f"Searching in category {cat_name} ({cat_id})")
            products = self.search_products(
                category_id=cat_id,
                max_products=products_per_category
            )
            
            # Process products with multiple threads
            with ThreadPoolExecutor(max_workers=workers) as executor:
                results = list(tqdm(
                    executor.map(lambda p: self.process_product(p, get_details=True), products),
                    total=len(products),
                    desc=f"Processing products for {cat_name}"
                ))
                
            # Count successful processings
            successful = sum(1 for r in results if r)
            total_processed += successful
            
            logger.info(f"Processed {successful}/{len(products)} products for category {cat_name}")
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
            logger.info(f"Searching for '{term}'")
            products = self.search_products(
                keywords=term,
                max_products=products_per_search
            )
            
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
        product_count = len([p for p in os.listdir(self.products_dir) if p.startswith("ebay_")])
        
        total_images = 0
        products_with_images = 0
        
        for product_id in os.listdir(self.products_dir):
            if not product_id.startswith("ebay_"):
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
        with open(os.path.join(self.output_dir, "ebay_summary.json"), "w", encoding="utf-8") as f:
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
        
        # Process each eBay product
        for product_id in tqdm(os.listdir(self.products_dir), desc="Creating manifest"):
            if not product_id.startswith("ebay_"):
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
                "retailer": "eBay"
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
        manifest_path = os.path.join(self.output_dir, "train_manifests", "ebay_manifest.json")
        os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
        
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Created manifest with {products_included} products (excluded {products_excluded})")
        logger.info(f"Train images: {sum(len(p['images']) for p in manifest['train'].values())}")
        logger.info(f"Val images: {sum(len(p['images']) for p in manifest['val'].values())}")
        
        return manifest_path


def main():
    parser = argparse.ArgumentParser(description="eBay Product Data Scraper")
    parser.add_argument("--app-id", type=str, required=True, help="eBay App ID (Developer Key)")
    parser.add_argument("--output-dir", type=str, default="data", help="Output directory")
    parser.add_argument("--max-products", type=int, default=30000, help="Maximum number of products to scrape")
    parser.add_argument("--max-per-category", type=int, default=1000, help="Maximum products per category")
    parser.add_argument("--max-per-search", type=int, default=100, help="Maximum products per search term")
    parser.add_argument("--workers", type=int, default=10, help="Number of worker threads")
    parser.add_argument("--global-id", type=str, default="EBAY-US", help="eBay Global ID")
    parser.add_argument("--create-manifest", action="store_true", help="Create training manifest after scraping")
    parser.add_argument("--strategy", type=str, default="both", choices=["categories", "search", "both"], 
                      help="Scraping strategy: categories, search terms, or both")
    
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = EbayScraper(
        app_id=args.app_id,
        output_dir=args.output_dir,
        global_id=args.global_id
    )
    
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