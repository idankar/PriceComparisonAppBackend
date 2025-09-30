#!/usr/bin/env python3
"""
Shufersal Product Data Collection Script with MongoDB Integration

This script collects product data from Shufersal's website using their public API.
It organizes product information and images into a structured format for training
a product recognition model and stores the data in MongoDB.

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
from datetime import datetime
from pymongo import MongoClient, ASCENDING, TEXT
import gridfs


# Custom JSON encoder for datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)
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

# MongoDB connection setup function
def setup_mongodb_connection(uri="mongodb://localhost:27017/", db_name="price_comparison"):
    """
    Setup MongoDB connection and required collections
    
    Args:
        uri: MongoDB connection URI
        db_name: Database name
        
    Returns:
        Dictionary with connection objects
    """
    try:
        client = MongoClient(uri)
        db = client[db_name]
        
        # Setup collections if they don't exist
        if "products" not in db.list_collection_names():
            # Create products collection and indexes
            products_collection = db["products"]
            products_collection.create_index([("product_id", ASCENDING)], unique=True)
            products_collection.create_index([("name", TEXT), ("name_he", TEXT)])
            logger.info("Created products collection with indexes")
        else:
            products_collection = db["products"]
        
        # Setup retailers collection if it doesn't exist
        if "retailers" not in db.list_collection_names():
            retailers_collection = db["retailers"]
            retailers_collection.create_index([("name", ASCENDING)], unique=True)
            
            # Add Shufersal retailer if it doesn't exist
            if not retailers_collection.find_one({"name": "Shufersal"}):
                retailers_collection.insert_one({
                    "name": "Shufersal",
                    "website": "https://www.shufersal.co.il/",
                    "country": "Israel",
                    "added_at": datetime.now()
                })
            logger.info("Created retailers collection with indexes")
        else:
            retailers_collection = db["retailers"]
        
        # Setup GridFS for image storage
        fs = gridfs.GridFS(db, collection="product_images")
        
        # Create MongoDB connection object
        mongodb = {
            "client": client,
            "db": db,
            "products": products_collection,
            "retailers": retailers_collection,
            "fs": fs
        }
        
        logger.info(f"Connected to MongoDB at {uri}, database: {db_name}")
        return mongodb
        
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise


class ShufersalScraper:
    """Scraper for Shufersal products with MongoDB integration"""
    
    def __init__(self, output_dir, mongodb_uri="mongodb://localhost:27017/", db_name="price_comparison"):
        """
        Initialize the scraper
        
        Args:
            output_dir: Directory to save scraped data (for compatibility and backup)
            mongodb_uri: MongoDB connection string
            db_name: MongoDB database name
        """
        self.output_dir = output_dir
        self.products_dir = os.path.join(output_dir, "products")
        self.raw_dir = os.path.join(output_dir, "raw", "shufersal")
        
        # Create directories if they don't exist (for backup/compatibility)
        os.makedirs(self.products_dir, exist_ok=True)
        os.makedirs(self.raw_dir, exist_ok=True)
        
        # Store processed products to avoid duplicates
        self.processed_products = set()
        self.failed_products = set()
        
        # Setup MongoDB
        self.mongodb = setup_mongodb_connection(mongodb_uri, db_name)
        
        # Setup headers for requests
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Referer": "https://www.shufersal.co.il/online/he/search",
            "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7"
        }
    
    def search_products(self, query, max_pages=3):
        """Fetch products from Shufersal API"""
        logger.info(f"Searching for products: {query}")
        
        all_products = []
        encoded_query = quote(query)
        
        # Process each page
        for page in range(max_pages):
            try:
                logger.info(f"Fetching page {page + 1}...")
                
                # Build the API URL
                url = f"https://www.shufersal.co.il/online/he/search/results?q={encoded_query}&relevance&limit=10&page={page}"
                
                # Make the request
                response = requests.get(url, headers=self.headers, timeout=10)
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
        
        # Save raw data (keep for backup/compatibility)
        if all_products:
            query_hash = hashlib.md5(query.encode()).hexdigest()[:8]
            with open(os.path.join(self.raw_dir, f"search_{query_hash}_products.json"), "w", encoding="utf-8") as f:
                json.dump(all_products, f, ensure_ascii=False, indent=2, cls=DateTimeEncoder)
        
        return all_products
    
    def extract_product_metadata(product_data):
    """Extract structured metadata with correct handling of percentages"""
    product_id = product_data.get("code", "")
    full_name = product_data.get("name", "")
    
    # Initialize metadata
    metadata = {
        "product_id": f"shufersal_{product_id}",
        "name": "",
        "name_he": "",
        "brand": product_data.get("brandName", ""),
        "price": float(product_data.get("price", {}).get("value", 0) or 0),
        "amount": None,  # Use None for MongoDB (will be omitted if null)
        "unit": None,
        "retailer": "Shufersal",
        "source": "shufersal",
        "source_id": product_id
    }
    
    # Early exit for empty name
    if not full_name:
        return metadata
    
    # STEP 1: Separate Hebrew and non-Hebrew parts
    hebrew_parts = []
    non_hebrew_parts = []
    
    for word in full_name.split():
        if any(c in HEBREW_CHARS for c in word):
            hebrew_parts.append(word)
        elif word.replace('%', '').replace('.', '').isdigit():
            # Keep percentages and numbers with non-Hebrew
            non_hebrew_parts.append(word)
        else:
            non_hebrew_parts.append(word)
    
    # STEP 2: Check for percentage patterns
    percentage_match = re.search(r'(\d+(?:\.\d+)?)%', full_name)
    is_dairy = any(term in ' '.join(hebrew_parts) for term in 
                  ["חלב", "יוגורט", "גבינה", "שמנת", "לבן"])
    
    if percentage_match and is_dairy:
        # DAIRY PRODUCT WITH FAT PERCENTAGE
        fat_percentage = percentage_match.group(1)
        
        # Clean Hebrew name (remove fat references)
        clean_hebrew = re.sub(r'\s*\d+(?:\.\d+)?%\s*שומן?\s*', ' ', ' '.join(hebrew_parts))
        clean_hebrew = re.sub(r'\s+', ' ', clean_hebrew).strip()
        
        # Set appropriate English name based on product type
        if "חלב" in clean_hebrew:
            if "דל לקטוז" in clean_hebrew:
                name_en = "Lactose-Free Milk"
            else:
                name_en = "Milk"
        elif "יוגורט" in clean_hebrew:
            name_en = "Yogurt"
        elif "גבינה" in clean_hebrew:
            if "לבנה" in clean_hebrew:
                name_en = "White Cheese"
            else:
                name_en = "Cheese"
        elif "שמנת" in clean_hebrew:
            name_en = "Cream"
        elif "לבן" in clean_hebrew:
            name_en = "Sour Milk"
        elif "קוטג" in clean_hebrew:
            name_en = "Cottage Cheese"
        else:
            name_en = "Dairy Product"
        
        # Set metadata
        metadata["name"] = name_en
        metadata["name_he"] = clean_hebrew
        metadata["amount"] = float(fat_percentage) if fat_percentage.replace('.', '').isdigit() else fat_percentage
        metadata["unit"] = "%"
    else:
        # REGULAR PRODUCT
        # Set names
        metadata["name_he"] = ' '.join(hebrew_parts) if hebrew_parts else None
        metadata["name"] = ' '.join(non_hebrew_parts) if non_hebrew_parts else full_name
        
        # Try to extract amount and unit
        amount_match = re.search(r'(\d+(?:\.\d+)?)\s*(גרם|ג\'|ג|מ"ל|מל|ק"ג|קג|ליטר|ל|יח\'|ml|g|kg|l|liter)', full_name)
        
        if amount_match:
            amount = amount_match.group(1)
            unit = amount_match.group(2)
            
            # Standardize units
            unit_mapping = {
                'גרם': 'g', 'ג\'': 'g', 'ג': 'g',
                'מ"ל': 'ml', 'מל': 'ml',
                'ק"ג': 'kg', 'קג': 'kg',
                'ליטר': 'l', 'ל': 'l',
                'יח\'': 'unit',
                '%': '%'
            }
            
            metadata["amount"] = float(amount) if amount.replace('.', '').isdigit() else amount
            metadata["unit"] = unit_mapping.get(unit, unit)
    
    # Make sure empty values are properly represented for MongoDB
    for key, value in metadata.items():
        if value == "":
            metadata[key] = None
    
    return metadata
def main():
    parser = argparse.ArgumentParser(description="Shufersal Product Data Scraper with MongoDB Integration")
    parser.add_argument("--output-dir", type=str, default="data", help="Output directory")
    parser.add_argument("--max-products", type=int, default=30000, help="Maximum number of products to scrape")
    parser.add_argument("--max-per-search", type=int, default=100, help="Maximum products per search term")
    parser.add_argument("--workers", type=int, default=10, help="Number of worker threads")
    parser.add_argument("--create-manifest", action="store_true", help="Create training manifest after scraping")
    parser.add_argument("--mongodb-uri", type=str, default="mongodb://localhost:27017/", help="MongoDB connection URI")
    parser.add_argument("--db-name", type=str, default="price_comparison", help="MongoDB database name")
    
    args = parser.parse_args()
    
    try:
        # Initialize scraper
        scraper = ShufersalScraper(
            args.output_dir,
            mongodb_uri=args.mongodb_uri,
            db_name=args.db_name
        )
        
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
    
    except Exception as e:
        logger.error(f"Error in main scraper execution: {e}")
    finally:
        if 'scraper' in locals():
            scraper.close()


if __name__ == "__main__":
    main()