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
    
    # Fruits and Vegetables (Hebrew)
    "פירות", "ירקות", "תפוחים", "בננות", "תפוזים", "לימון", "מלפפון", "עגבניות", "גזר", "בצל",
    "פלפל", "חסה", "כרוב", "תפוחי אדמה", "בטטה", "אבוקדו", "מנגו", "אבטיח", "מלון", "ענבים",
    "אפרסק", "נקטרינה", "תות", "תאנים", "תמרים", "צנונית", "סלק", "כרובית", "ברוקולי", "קישוא",
    
    # Meat and Poultry (Hebrew)
    "בשר", "עוף", "הודו", "שניצל", "המבורגר", "קציצות", "פרגיות", "כנפיים", "חזה עוף", "כבד",
    "בשר טחון", "בשר בקר", "סטייק", "צלי", "נקניקיות", "נקניק", "קבב", "פסטרמה", "בשר טלה",
    
    # Frozen Foods (Hebrew)
    "מוצרים קפואים", "פיצה קפואה", "גלידה", "ירקות קפואים", "מוקפא", "שלגונים", "אדממה", "בורקס קפוא",
    "לקט ירקות", "אפונה קפואה", "שעועית קפואה", "תירס קפוא", "בצק עלים", "בצק קפוא", "פסטה קפואה",
    
    # Cleaning Products (Hebrew)
    "מוצרי ניקוי", "סבון כלים", "אקונומיקה", "מרכך כביסה", "אבקת כביסה", "ג'ל כביסה", "מנקה רצפות",
    "סבון", "שמפו", "מרכך שיער", "תרסיס ניקוי", "מטליות", "מגבונים", "נייר טואלט", "מברשת שיניים",
    "משחת שיניים", "סבון נוזלי", "קרם רחצה", "תחליב רחצה", "דאודורנט",
    
    # Personal Care (Hebrew)
    "טיפוח אישי", "קרם לחות", "קרם גוף", "קרם פנים", "קרם ידיים", "שמפו", "מרכך שיער", "דאודורנט",
    "מי פה", "מברשת שיניים", "משחת שיניים", "תחליב גילוח", "קרם גילוח", "מוס לשיער", "ג'ל לשיער",
    "סכיני גילוח", "קרם הגנה", "תחבושות", "טמפונים", "מגבונים אינטימיים", "לק לציפורניים", "מסיר לק"
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

# Custom JSON encoder to handle datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

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
                json.dump(all_products, f, ensure_ascii=False, indent=2)
        
        return all_products
    
    def extract_product_metadata(self, product_data):
        """Extract structured metadata with improved handling of dairy products and percentages"""
        product_id = product_data.get("code", "")
        full_name = product_data.get("name", "")
        
        # Initialize metadata
        metadata = {
            "product_id": f"shufersal_{product_id}",
            "name": None,  # Default to NULL for English name
            "name_he": "",
            "brand": product_data.get("brandName", ""),
            "price": float(product_data.get("price", {}).get("value", 0) or 0),
            "amount": None,
            "unit": None,
            "retailer": "Shufersal",
            "source": "shufersal",
            "source_id": product_id
        }
        
        # Early exit for empty name
        if not full_name:
            return metadata
        
        # Separate Hebrew and non-Hebrew parts
        hebrew_chars = set('אבגדהוזחטיכלמנסעפצקרשתךםןףץ')
        hebrew_parts = []
        non_hebrew_parts = []
        
        for word in full_name.split():
            if any(c in hebrew_chars for c in word):
                hebrew_parts.append(word)
            elif word.replace('%', '').replace('.', '').isdigit():
                # Keep percentages and numbers with non-Hebrew
                non_hebrew_parts.append(word)
            else:
                non_hebrew_parts.append(word)
        
        # Check for percentage patterns in dairy products - more comprehensive matching
        # First try the standard pattern with % sign
        percentage_match = re.search(r'(\d+(?:\.\d+)?)%', full_name)
        
        # If not found, try alternate patterns like "X אחוז" (X percent in Hebrew)
        if not percentage_match:
            percentage_match = re.search(r'(\d+(?:\.\d+)?)\s*אחוז', full_name)
        
        # If still not found, look for numbers followed by fat-related words
        if not percentage_match:
            fat_terms = ['שומן', 'שמנת', 'שמן']  # fat-related terms
            for term in fat_terms:
                match = re.search(r'(\d+(?:\.\d+)?)\s*' + term, full_name)
                if match:
                    percentage_match = match
                    break
        
        # For dairy products, also check for isolated numbers that might be fat percentages
        if not percentage_match:
            # Look for standalone numbers between 0-9 which are common fat percentages
            standalone_number = re.search(r'(?<!\d)([0-9](?:\.[0-9])?)(?!\d)(?!\s*(?:גרם|ג\'|ג|מ"ל|מל|ק"ג|קג|ליטר|ל|יח\'|ml|g|kg|l|liter))', full_name)
            if standalone_number:
                value = float(standalone_number.group(1))
                if 0 <= value <= 9:  # Likely to be a fat percentage if between 0-9
                    percentage_match = standalone_number
        
        hebrew_text = ' '.join(hebrew_parts)
        is_dairy = any(term in hebrew_text for term in ["חלב", "יוגורט", "גבינה", "שמנת", "לבן", "קוטג", "לאבנה"])
        
        # Special case: if we found a percentage and the product seems to be dairy by name
        if percentage_match and is_dairy:
            # DAIRY PRODUCT WITH FAT PERCENTAGE
            fat_percentage = percentage_match.group(1)
            
            # Clean Hebrew name (remove fat references)
            clean_hebrew = re.sub(r'\s*\d+(?:\.\d+)?%\s*שומן?\s*', ' ', hebrew_text)
            clean_hebrew = re.sub(r'\s+', ' ', clean_hebrew).strip()
            
            # Set appropriate English name based on product type, BUT ONLY for confident matches
            if "חלב" in clean_hebrew and "טרי" in clean_hebrew:
                if "דל לקטוז" in clean_hebrew:
                    name_en = "Lactose-Free Milk"
                else:
                    name_en = "Fresh Milk"
            elif "חלב" in clean_hebrew and clean_hebrew.count(" ") < 2:  # Simple milk description
                name_en = "Milk"
            elif "יוגורט" in clean_hebrew and clean_hebrew.count(" ") < 2:
                name_en = "Yogurt"
            elif "גבינה" in clean_hebrew and "לבנה" in clean_hebrew:
                name_en = "White Cheese" 
            elif "שמנת" in clean_hebrew and clean_hebrew.count(" ") < 2:
                name_en = "Cream"
            elif "קוטג" in clean_hebrew and clean_hebrew.count(" ") < 2:
                name_en = "Cottage Cheese"
            else:
                # For other dairy products, leave English name as NULL
                name_en = None
                logger.info(f"Dairy product without confident English translation: {clean_hebrew}")
            
            # Set metadata
            metadata["name"] = name_en  # May be None
            metadata["name_he"] = clean_hebrew
            metadata["amount"] = float(fat_percentage) if fat_percentage.replace('.', '').isdigit() else fat_percentage
            metadata["unit"] = "%"
        else:
            # REGULAR PRODUCT
            # Set Hebrew name
            metadata["name_he"] = ' '.join(hebrew_parts) if hebrew_parts else ""
            
            # CHANGE: For non-dairy products, set English name to NULL by default
            # We'll let a future translation step handle this properly
            metadata["name"] = None
            
            # Only set English name if it's a well-defined English term 
            # (and not just numbers, brand names, or generic terms)
            english_words = [word for word in non_hebrew_parts 
                            if word.isalpha() and len(word) > 2 
                            and word.lower() not in ['the', 'and', 'for', 'with', 'new']]
            
            if english_words and len(english_words) >= 2:  # At least 2 meaningful English words
                metadata["name"] = ' '.join(non_hebrew_parts)
            
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
    
    def process_product(self, product_data):
        """
        Process a single product and store in MongoDB and file system
        
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
            
            # Create product directory (still keep files for backup/compatibility)
            product_dir = os.path.join(self.products_dir, formatted_id)
            os.makedirs(product_dir, exist_ok=True)
            
            # Extract product metadata using our improved function
            metadata = self.extract_product_metadata(product_data)
            
            # Log dairy products with missing percentage information
            if "חלב" in metadata.get("name_he", "") and metadata.get("unit") != "%" and metadata.get("amount") is None:
                logger.info(f"Possible dairy product missing percentage: {metadata.get('name_he')} - {formatted_id}")
            
            # Add timestamp for MongoDB
            metadata["last_updated"] = datetime.now()
            
            # Create a copy of metadata for JSON serialization (without datetime objects)
            metadata_for_json = metadata.copy()
            metadata_for_json["last_updated"] = metadata["last_updated"].isoformat()
            
            # Save metadata to file system (for backup/compatibility)
            with open(os.path.join(product_dir, "metadata.json"), 'w', encoding='utf-8') as f:
                json.dump(metadata_for_json, f, ensure_ascii=False, indent=2)
            
            # Download and save images, get image filenames
            image_filenames = self._download_product_images(product_data, product_dir)
            
            # Add image references to metadata
            metadata["images"] = image_filenames
            
            # Store in MongoDB
            result = self.mongodb["products"].update_one(
                {"product_id": formatted_id},
                {"$set": metadata},
                upsert=True
            )
            
            # Log MongoDB insertion results for troubleshooting
            if result.modified_count > 0 or result.upserted_id is not None:
                logger.debug(f"Successfully added/updated product {formatted_id} in MongoDB")
            else:
                logger.warning(f"Product {formatted_id} may not have been added to MongoDB properly")
            
            # Add to processed set
            self.processed_products.add(product_id)
            
            return True
        except Exception as e:
            logger.error(f"Error processing product: {e}")
            if product_data and "code" in product_data:
                self.failed_products.add(product_data["code"])
            return False
    
    def _download_product_images(self, product_data, product_dir):
        """
        Download product images and save to file system and MongoDB's GridFS
        
        Args:
            product_data: Product data
            product_dir: Directory to save images (for backup/compatibility)
            
        Returns:
            List of image filenames
        """
        image_filenames = []
        
        try:
            # Get image URLs
            image_urls = []
            images = product_data.get("images", [])
            
            # Filter for valid image URLs
            for image in images:
                if image.get("format") in ["medium", "zoom"] and image.get("url"):
                    url = image.get("url")
                    if url and url.startswith("http"):
                        image_urls.append(url)
            
            # Remove duplicates while preserving order
            seen = set()
            unique_urls = []
            for url in image_urls:
                if url not in seen:
                    seen.add(url)
                    unique_urls.append(url)
            
            # Download only unique, valid images
            for i, img_url in enumerate(unique_urls):
                try:
                    response = requests.get(img_url, headers=self.headers, timeout=10)
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
                    
                    # Save image to file system (for backup/compatibility)
                    img_filename = f"{i+1:03d}.jpg"
                    img_path = os.path.join(product_dir, img_filename)
                    img.save(img_path, "JPEG", quality=95)
                    image_filenames.append(img_filename)
                    
                    # Save image to MongoDB GridFS
                    product_id = product_data.get("code")
                    formatted_id = f"shufersal_{product_id}"
                    
                    # Convert image to bytes
                    img_bytes = BytesIO()
                    img.save(img_bytes, format="JPEG", quality=95)
                    img_bytes.seek(0)
                    
                    # Store in GridFS with metadata as separate, serializable elements
                    # Avoid putting datetime directly in the metadata
                    image_metadata = {
                        "product_id": formatted_id,
                        "filename": img_filename,
                        "source": "shufersal",
                        "width": img.width,
                        "height": img.height,
                        "content_type": "image/jpeg",
                        "uploaded_at": datetime.now().isoformat()  # Store as string
                    }
                    
                    # Check if image already exists in GridFS (using product_id and filename as key)
                    existing_file = self.mongodb["fs"].find_one({
                        "metadata.product_id": formatted_id, 
                        "metadata.filename": img_filename
                    })
                    
                    # If not exists, save to GridFS
                    if not existing_file:
                        self.mongodb["fs"].put(
                            img_bytes.getvalue(),
                            filename=img_filename,  # Include filename in GridFS doc
                            metadata=image_metadata  # Store metadata
                        )
                    
                    # Small delay between downloads
                    time.sleep(0.2)
                except Exception as e:
                    logger.error(f"Error downloading image {img_url}: {e}")
        except Exception as e:
            logger.error(f"Error extracting image URLs: {e}")
        return image_filenames
    
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
        # Count products and images from MongoDB
        product_count = self.mongodb["products"].count_documents({"source": "shufersal"})
        
        # Count products with images
        products_with_images_pipeline = [
            {"$match": {"source": "shufersal"}},
            {"$match": {"images": {"$exists": True, "$ne": []}}},
            {"$count": "count"}
        ]
        products_with_images_result = list(self.mongodb["products"].aggregate(products_with_images_pipeline))
        products_with_images = products_with_images_result[0]["count"] if products_with_images_result else 0
        
        # Count products with Hebrew
        products_with_hebrew_pipeline = [
            {"$match": {"source": "shufersal"}},
            {"$match": {"name_he": {"$exists": True, "$ne": None}}},
            {"$count": "count"}
        ]
        products_with_hebrew_result = list(self.mongodb["products"].aggregate(products_with_hebrew_pipeline))
        products_with_hebrew = products_with_hebrew_result[0]["count"] if products_with_hebrew_result else 0
        
        # Count total images
        total_images_pipeline = [
            {"$match": {"source": "shufersal"}},
            {
                "$project": {
                    "image_count": {
                        "$size": {"$ifNull": ["$images", []]}
                    }
                }
            },
            {"$group": {"_id": None, "total": {"$sum": "$image_count"}}}
        ]
        total_images_result = list(self.mongodb["products"].aggregate(total_images_pipeline))
        total_images = total_images_result[0]["total"] if total_images_result else 0
        
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
            "percent_with_hebrew": percent_with_hebrew,
            "generated_at": datetime.now().isoformat()  # Use string format for JSON
        }
        
        # Save summary (keep for backup/compatibility)
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
        
        # Query products from MongoDB
        products_cursor = self.mongodb["products"].find({
            "source": "shufersal",
            "images": {"$exists": True, "$not": {"$size": 0}}
        })
        
        # Process each product
        for product in tqdm(products_cursor, desc="Creating manifest"):
            product_id = product.get("product_id")
            
            # Get images
            images = product.get("images", [])
            
            # Skip if not enough images
            if len(images) < min_images:
                products_excluded += 1
                continue
            
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
                "brand": product.get("brand", ""),
                "name": product.get("name", ""),
                "name_he": product.get("name_he", ""),
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
    
    def close(self):
        """Close MongoDB connection"""
        if hasattr(self, 'mongodb') and 'client' in self.mongodb:
            self.mongodb["client"].close()
            logger.info("MongoDB connection closed")


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