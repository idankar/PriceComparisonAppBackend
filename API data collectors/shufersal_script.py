# Updated Shufersal Scraper
import os
import json
import time
import argparse
import requests
from PIL import Image
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import random
import hashlib
import re
import logging
from urllib.parse import quote

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

EXPANDED_SEARCH_TERMS = [
    # Dairy Products (Hebrew)
    "חלב", "גבינה", "גבינה צהובה", "קוטג", "יוגורט", "שמנת", "חמאה", "מוצרי חלב", "גבינה לבנה", 
    "גבינת עיזים", "מעדן חלב", "מילקי", "דני", "מגדים", "לבן", "חלב עמיד", "חלב טרי", "חלב סויה",
    
    # Bakery (Hebrew)
    "לחם", "פיתות", "לחמניות", "חלות", "עוגות", "עוגיות", "מאפים", "בורקס", "קרואסון", "לחם אחיד",
    "לחם קל", "לחם מלא", "לחם שיפון", "לחם כוסמין", "עוגת שמרים", "בייגלה", "קרקרים",
    
    # Meat and Poultry (Hebrew)
    "בשר", "עוף", "הודו", "בקר", "בשר טחון", "המבורגר", "שניצל", "חזה עוף", "כרעיים", "כנפיים",
    "נקניקיות", "פסטרמה", "מבשלים", "סטייק", "קבב", "בשר קפוא", "בשר טרי", "פרגיות",
    
    # Fish and Seafood (Hebrew)
    "דגים", "טונה", "סלמון", "דג מושט", "דג בקלה", "נסיכת הנילוס", "אמנון", "דניס", "פילה דג",
    "דגים קפואים", "שרימפס", "סרדינים",
    
    # Fruits and Vegetables (Hebrew)
    "פירות", "ירקות", "תפוחים", "אגסים", "בננות", "תפוזים", "לימון", "אבוקדו", "עגבניות", "מלפפונים",
    "פלפל", "גזר", "תפוח אדמה", "בצל", "שום", "חסה", "כרוב", "פירות יבשים", "פירות קפואים",
    
    # Snacks (Hebrew)
    "חטיפים", "במבה", "ביסלי", "צ'יפס", "מרק נמס", "פופקורן", "חטיפי תירס", "אפרופו", "דוריטוס", 
    "תפוצ'יפס", "צ'יטוס", "עונג", "קליק", "שוקולד", "חטיף בריאות", "אנרגיה", "פיצוחים", "גרעינים",
    
    # Beverages (Hebrew)
    "משקאות", "מים", "סודה", "קולה", "ספרייט", "פאנטה", "מיץ", "תרכיז", "משקה אנרגיה", "קפה",
    "תה", "שוקו", "חלב סויה", "משקה שקדים", "פריגת", "נביעות", "מי עדן", "קפה שחור", "קפה נמס",
    
    # Alcohol (Hebrew)
    "אלכוהול", "יין", "בירה", "וודקה", "ויסקי", "ליקר", "עראק", "יין אדום", "יין לבן", "גולדסטאר",
    "מכבי", "קרלסברג", "קורונה", "ערק", "ברנדי", "יין מתוק", "יין יבש", "יין מבעבע",
    
    # Pantry Staples (Hebrew)
    "אורז", "פסטה", "קמח", "סוכר", "מלח", "שמן", "קטניות", "עדשים", "חומץ", "רטבים", "תבלינים",
    "שעועית", "חומוס", "עדשים", "אפונה", "תירס", "טונה", "זיתים", "שימורים", "רוטב עגבניות",
    
    # Frozen Foods (Hebrew)
    "מוצרים קפואים", "ירקות קפואים", "פיצה קפואה", "גלידה", "שניצל קפוא", "אפונה קפואה", 
    "תירס קפוא", "בצק קפוא", "אצבעות דג", "ארטיק", "מוצרי תפוחי אדמה קפואים",
    
    # Baby Products (Hebrew)
    "מוצרי תינוקות", "חיתולים", "מטרנה", "סימילאק", "מחיות", "בקבוקים", "מוצצים", "מגבונים",
    
    # Cleaning Products (Hebrew)
    "מוצרי ניקוי", "אקונומיקה", "סבון כלים", "נוזל כביסה", "מרכך כביסה", "סנו", "פיירי", "קולון", 
    "אריאל", "סנובון", "נייר טואלט", "מגבונים", "מטליות", "סבון ידיים",
    
    # Personal Care (Hebrew)
    "טיפוח אישי", "שמפו", "מרכך שיער", "סבון", "דאודורנט", "משחת שיניים", "מברשת שיניים",
    "פנטן", "הד אנד שולדרס", "קרם גוף", "קרם פנים", "קרם ידיים", "תער", "קצף גילוח",
    
    # Major Israeli Brands
    "תנובה", "שטראוס", "אסם", "עלית", "תלמה", "יכין", "אוסם", "צבר", "זוגלובק", "טרה", "יפאורה",
    "מטרנה", "מאסטר שף", "פלוס", "בית השיטה", "יד מרדכי", "עוף טוב", "טבעול", "החברה המרכזית",
    
    # Dairy Products (English)
    "milk", "cheese", "cottage cheese", "yogurt", "cream", "butter", "dairy products", "milk drink",
    "goat cheese", "pudding", "dairy dessert", "labane", "tzfatit", "feta",
    
    # Bakery (English)
    "bread", "pita", "rolls", "challah", "cakes", "cookies", "pastries", "burekas", "croissant",
    "whole wheat bread", "rye bread", "spelt bread", "bagels", "crackers", "puff pastry",
    
    # Meat and Poultry (English)
    "meat", "chicken", "turkey", "beef", "ground meat", "hamburger", "schnitzel", "chicken breast",
    "sausages", "pastrami", "steak", "kabab", "frozen meat", "fresh meat", "liver", "hearts",
    
    # Fish and Seafood (English)
    "fish", "tuna", "salmon", "tilapia", "cod", "nile perch", "denis", "fish fillet",
    "frozen fish", "shrimp", "sardines", "herring", "canned fish",
    
    # Fruits and Vegetables (English)
    "fruits", "vegetables", "apples", "pears", "bananas", "oranges", "lemon", "avocado", "tomatoes",
    "cucumbers", "pepper", "carrot", "potato", "onion", "garlic", "lettuce", "cabbage", "dried fruits",
    
    # Snacks (English)
    "snacks", "chips", "pretzels", "popcorn", "corn snacks", "tortilla chips", "potato chips",
    "energy bars", "chocolate", "candy", "gum", "nuts", "seeds", "rice cakes", "protein bar",
    
    # Beverages (English)
    "drinks", "water", "soda", "cola", "sprite", "fanta", "juice", "concentrate", "energy drink",
    "coffee", "tea", "chocolate milk", "soy milk", "almond milk", "mineral water", "sparkling water",
    
    # Alcohol (English)
    "alcohol", "wine", "beer", "vodka", "whiskey", "liqueur", "arak", "red wine", "white wine",
    "goldstar", "maccabi", "carlsberg", "corona", "brandy", "sweet wine", "dry wine", "sparkling wine",
    
    # Pantry Staples (English)
    "rice", "pasta", "flour", "sugar", "salt", "oil", "legumes", "lentils", "vinegar", "sauces",
    "spices", "beans", "hummus", "chickpeas", "peas", "corn", "olives", "canned goods", "tomato sauce",
    
    # Frozen Foods (English)
    "frozen products", "frozen vegetables", "frozen pizza", "ice cream", "frozen schnitzel",
    "frozen peas", "frozen corn", "frozen dough", "fish fingers", "popsicle", "frozen potato products",
    
    # Baby Products (English)
    "baby products", "diapers", "formula", "baby food", "bottles", "pacifiers", "wipes",
    "baby cereal", "baby bath", "baby soap", "baby oil", "baby powder",
    
    # Cleaning Products (English)
    "cleaning products", "bleach", "dish soap", "laundry detergent", "fabric softener",
    "toilet paper", "wipes", "cloths", "hand soap", "disinfectant", "window cleaner",
    
    # Personal Care (English)
    "personal care", "shampoo", "conditioner", "soap", "deodorant", "toothpaste", "toothbrush",
    "body lotion", "face cream", "hand cream", "razor", "shaving cream", "toilet paper",
    
    # International Brands
    "Coca Cola", "Pepsi", "Nestlé", "Kellogg's", "Heinz", "Unilever", "Procter & Gamble", "Colgate",
    "Dove", "Gillette", "Pampers", "Huggies", "Danone", "Lipton", "Nescafé", "Sprite", "Fanta", "Oreo"
]

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
        
        # Headers to mimic a browser request
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Referer": "https://www.shufersal.co.il/online/he/search",
            "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7"
        }
    
    def search_products(self, query, max_pages=5):
        """
        Search for products by query
        
        Args:
            query: Search query
            max_pages: Maximum number of pages to fetch
            
        Returns:
            List of products
        """
        logger.info(f"Searching for '{query}'")
        
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
        
        # Save raw data
        if all_products:
            query_hash = hashlib.md5(query.encode()).hexdigest()[:8]
            with open(os.path.join(self.raw_dir, f"search_{query_hash}_products.json"), 'w', encoding='utf-8') as f:
                json.dump(all_products, f, ensure_ascii=False, indent=2)
        
        return all_products
    
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
            
            # Create product directory
            product_dir = os.path.join(self.products_dir, formatted_id)
            os.makedirs(product_dir, exist_ok=True)
            
            # Extract product information
            name = product_data.get("name", "")
            
            # Extract amount and unit information
            amount = None
            unit = product_data.get("unitDescription", "")
            
            # Try to extract amount from product name
            if " " in name and any(char.isdigit() for char in name):
                parts = name.split()
                for part in parts:
                    if any(char.isdigit() for char in part):
                        if "%" in part:  # Percentage
                            amount = part
                        elif "מ\"ל" in part or "מל" in part:  # Milliliters
                            amount = part.replace("מ\"ל", "").replace("מל", "").strip()
                            unit = "מ\"ל"
                        elif "גרם" in part:  # Grams
                            amount = part.replace("גרם", "").strip()
                            unit = "גרם"
                        elif "ק\"ג" in part or "קג" in part:  # Kilograms
                            amount = part.replace("ק\"ג", "").replace("קג", "").strip()
                            unit = "ק\"ג"
                        elif "ליטר" in part:  # Liters
                            amount = part.replace("ליטר", "").strip()
                            unit = "ליטר"
                        elif "יח" in part:  # Units
                            amount = part.replace("יח", "").strip()
                            unit = "יח"
            
            # Extract Hebrew name if present
            name_he = ""
            if any(c in HEBREW_CHARS for c in name):
                # Extract Hebrew characters
                name_he = ''.join(c for c in name if c in HEBREW_CHARS or c == ' ')
                name_he = re.sub(r'\s+', ' ', name_he).strip()
                
                # Remove Hebrew from English name
                name_en = ''.join(c for c in name if c not in HEBREW_CHARS)
                name_en = re.sub(r'\s+', ' ', name_en).strip()
                
                if name_en:  # If we have remaining text after removing Hebrew
                    name = name_en
                else:
                    name = name_he
            
            # Get price
            price_data = product_data.get("price", {})
            price = price_data.get("value", 0)
            
            # Get brand
            brand = product_data.get("brandName", "")
            
            # Create metadata
            metadata = {
                "product_id": formatted_id,
                "name": name,
                "name_he": name_he,
                "brand": brand,
                "price": price,
                "amount": amount,
                "unit": unit,
                "retailer": "Shufersal",
                "source": "shufersal",
                "source_id": product_id
            }
            
            # Remove empty metadata fields
            metadata = {k: v for k, v in metadata.items() if v not in (None, "")}
            
            # Save metadata
            with open(os.path.join(product_dir, "metadata.json"), 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            # Download and save images
            self._download_product_images(product_data, product_dir)
            
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
        Download product images
        
        Args:
            product_data: Product data
            product_dir: Directory to save images
        """
        try:
            # Get image URLs
            image_urls = []
            images = product_data.get("images", [])
            
            # Filter for valid image URLs
            for image in images:
                if image.get("format") in ["medium", "zoom"] and image.get("url"):
                    url = image.get("url")
                    # Check if URL looks valid
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
                    
                    # Save image
                    img_path = os.path.join(product_dir, f"{i+1:03d}.jpg")
                    img.save(img_path, "JPEG", quality=95)
                    
                    # Small delay between downloads
                    time.sleep(0.2)
                    
                except Exception as e:
                    logger.error(f"Error downloading image {img_url}: {e}")
                    
        except Exception as e:
            logger.error(f"Error extracting image URLs: {e}")
    
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
        search_terms =COMMON_SEARCH_TERMS=EXPANDED_SEARCH_TERMS.copy()
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
            products = self.search_products(term, max_pages=max(1, products_per_search // 10))
            
            # Limit to max products per search
            products = products[:products_per_search]
            
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


# Define Hebrew characters set for Hebrew text detection
HEBREW_CHARS = set('אבגדהוזחטיכלמנסעפצקרשתךםןףץ')


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