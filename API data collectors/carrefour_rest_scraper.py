#!/usr/bin/env python3
"""
Carrefour Product Data Collection Script (REST API Version)

This script collects product data from Carrefour's website using their REST APIs
rather than GraphQL. Based on network analysis, this approach should bypass
the 403 forbidden errors encountered with the GraphQL API.
"""

import os
import json
import time
import argparse
import requests
import random
import logging
import re
from urllib.parse import quote, unquote
from concurrent.futures import ThreadPoolExecutor
from PIL import Image
from io import BytesIO
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("carrefour_rest_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Common search terms for groceries in French
COMMON_SEARCH_TERMS = [
    "lait", "fromage", "yaourt", "beurre", "crème", "chocolat", 
    "café", "thé", "jus", "eau", "pain", "pâtes", "riz", "céréales",
    "viande", "poulet", "poisson", "légumes", "fruits", "conserves",
    "huile", "vinaigrette", "sucre", "farine", "sel", "épices",
    "biscuits", "chips", "bonbons", "glace", "surgelés", "pizza",
    "savon", "shampoing", "dentifrice", "papier toilette", "lessive"
]

class CarrefourRestAPI:
    """Carrefour REST API client"""
    
    def __init__(self, country="fr"):
        """
        Initialize the Carrefour API client
        
        Args:
            country: Country code (fr, es, it, etc.)
        """
        self.country = country
        self.base_url = f"https://www.carrefour.{country}"
        self.session = requests.Session()
        
        # Set up headers to look like a browser
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": f"{self.base_url}/",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin"
        })
        
        # Initialize session with cookies
        self._init_session()
        
        # Request counter for rate limiting
        self.request_count = 0
        self.last_request_time = time.time()
    
    def _init_session(self):
        """Initialize session with necessary cookies"""
        try:
            # First visit the homepage to get cookies
            logger.info(f"Initializing session with Carrefour {self.country.upper()}...")
            response = self.session.get(f"{self.base_url}/", timeout=30)
            response.raise_for_status()
            
            logger.info(f"Session initialized with {len(self.session.cookies)} cookies")
            return True
        except Exception as e:
            logger.error(f"Error initializing session: {e}")
            return False
    
    def _rate_limit(self):
        """Apply rate limiting to avoid too many requests"""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        # Be conservative with request rate to avoid blocking
        if elapsed < 1.5:
            time.sleep(1.5 - elapsed)
            
        self.last_request_time = time.time()
        self.request_count += 1
        
        # Add jitter to seem more human-like
        jitter = random.uniform(0.2, 0.8)
        time.sleep(jitter)
    
    def search_products(self, query, page=1, limit=24):
        """
        Search for products using the regular search page
        
        Args:
            query: Search query
            page: Page number
            limit: Results per page
            
        Returns:
            List of product data
        """
        self._rate_limit()
        
        # URL encode the query
        encoded_query = quote(query)
        
        try:
            # First approach: Try the universe JSON API
            # This endpoint works for category searches
            universe_url = f"{self.base_url}/univers={encoded_query}.json"
            
            logger.info(f"Trying universe endpoint: {universe_url}")
            response = self.session.get(universe_url, timeout=30)
            
            if response.status_code == 200 and len(response.content) > 500:
                try:
                    data = response.json()
                    # Process universe format data
                    products = self._extract_products_from_universe(data)
                    if products:
                        logger.info(f"Found {len(products)} products from universe endpoint")
                        return products
                except Exception as e:
                    logger.warning(f"Error parsing universe response: {e}")
                    # Continue to next approach
            
            # Second approach: Try the search panel API
            # This is directly from the observed network requests
            search_panel_url = f"{self.base_url}/search_panel?q={encoded_query}"
            
            logger.info(f"Trying search panel endpoint: {search_panel_url}")
            response = self.session.get(search_panel_url, timeout=30)
            
            if response.status_code == 200 and len(response.content) > 500:
                try:
                    data = response.json()
                    # Process search panel format data
                    products = self._extract_products_from_search_panel(data)
                    if products:
                        logger.info(f"Found {len(products)} products from search panel endpoint")
                        return products
                except Exception as e:
                    logger.warning(f"Error parsing search panel response: {e}")
                    # Continue to next approach
            
            # Third approach: Parse the HTML search results page
            # This is a fallback method if the APIs fail
            search_url = f"{self.base_url}/s?q={encoded_query}&page={page}"
            
            logger.info(f"Trying HTML search page: {search_url}")
            response = self.session.get(search_url, timeout=30)
            
            if response.status_code == 200:
                # Extract structured data from HTML
                products = self._extract_products_from_html(response.text)
                if products:
                    logger.info(f"Found {len(products)} products from HTML search page")
                    return products
                else:
                    logger.warning("No products found in HTML search page")
            
            # If we got here, all approaches failed
            logger.warning(f"All search approaches failed for query '{query}'")
            return []
            
        except Exception as e:
            logger.error(f"Error searching products: {e}")
            return []
    
    def get_product_details(self, product_id=None, ean=None):
        """
        Get detailed product information
        
        Args:
            product_id: Product ID
            ean: EAN barcode
            
        Returns:
            Product details
        """
        self._rate_limit()
        
        try:
            if ean:
                # Try direct EAN lookup
                # Based on the observed request: 3560071080914.json?u=E6...
                ean_url = f"{self.base_url}/{ean}.json"
                
                logger.info(f"Trying direct EAN lookup: {ean_url}")
                response = self.session.get(ean_url, timeout=30)
                
                if response.status_code == 200 and len(response.content) > 100:
                    try:
                        data = response.json()
                        return data
                    except Exception as e:
                        logger.warning(f"Error parsing EAN response: {e}")
            
            if product_id:
                # Try product detail page API
                # Based on the observed "pdp" request
                pdp_url = f"{self.base_url}/p/pdp?productId={product_id}"
                
                logger.info(f"Trying product detail API: {pdp_url}")
                response = self.session.get(pdp_url, timeout=30)
                
                if response.status_code == 200 and len(response.content) > 100:
                    try:
                        data = response.json()
                        return data
                    except Exception as e:
                        logger.warning(f"Error parsing PDP response: {e}")
                
                # Fallback: Try direct product URL with HTML parsing
                product_url = f"{self.base_url}/p/{product_id}"
                
                logger.info(f"Trying product detail page: {product_url}")
                response = self.session.get(product_url, timeout=30)
                
                if response.status_code == 200:
                    # Extract product data from HTML
                    return self._extract_product_from_html(response.text)
            
            logger.warning(f"Could not get product details for product_id={product_id}, ean={ean}")
            return None
            
        except Exception as e:
            logger.error(f"Error getting product details: {e}")
            return None
    
    def _extract_products_from_universe(self, data):
        """Extract products from universe API response"""
        products = []
        
        try:
            # Structure depends on the actual response format
            # This is a placeholder implementation
            if "products" in data:
                for product in data["products"]:
                    products.append({
                        "id": product.get("id", ""),
                        "name": product.get("name", ""),
                        "brand": product.get("brand", {}).get("name", ""),
                        "price": product.get("price", {}).get("value", 0),
                        "image_url": product.get("image", {}).get("url", ""),
                        "ean": product.get("ean", "")
                    })
            
            return products
        except Exception as e:
            logger.error(f"Error extracting products from universe data: {e}")
            return []
    
    def _extract_products_from_search_panel(self, data):
        """Extract products from search panel API response"""
        products = []
        
        try:
            # Structure depends on the actual response format
            # This is a placeholder implementation
            if "results" in data:
                for product in data["results"]:
                    products.append({
                        "id": product.get("id", ""),
                        "name": product.get("name", ""),
                        "brand": product.get("brand", ""),
                        "price": product.get("price", 0),
                        "image_url": product.get("image", ""),
                        "ean": product.get("ean", "")
                    })
            
            return products
        except Exception as e:
            logger.error(f"Error extracting products from search panel data: {e}")
            return []
    
    def _extract_products_from_html(self, html_content):
        """Extract products from HTML search results page"""
        products = []
        
        try:
            # Look for JSON data embedded in the HTML
            # Many e-commerce sites embed product data in JSON-LD format
            json_ld_match = re.search(r'<script type="application/ld\+json">(.*?)</script>', html_content, re.DOTALL)
            
            if json_ld_match:
                try:
                    json_data = json.loads(json_ld_match.group(1))
                    
                    # Check if it's a product list
                    if "@type" in json_data and json_data["@type"] == "ItemList":
                        for item in json_data.get("itemListElement", []):
                            if "item" in item:
                                product = item["item"]
                                products.append({
                                    "id": product.get("sku", ""),
                                    "name": product.get("name", ""),
                                    "brand": product.get("brand", {}).get("name", ""),
                                    "price": float(product.get("offers", {}).get("price", 0)),
                                    "image_url": product.get("image", ""),
                                    "ean": product.get("gtin13", "")
                                })
                except Exception as e:
                    logger.error(f"Error parsing JSON-LD: {e}")
            
            # If no JSON-LD data, try alternative methods
            if not products:
                # Look for product data in a different format
                # For example, some sites have a window.__INITIAL_STATE__ variable
                state_match = re.search(r'window\.__INITIAL_STATE__\s*=\s*(\{.*?\});\s*</script>', html_content, re.DOTALL)
                
                if state_match:
                    try:
                        # Warning: This is potentially unsafe
                        # In a production environment, use a proper JSON parser
                        state_data = json.loads(state_match.group(1))
                        
                        # Extract product data from state
                        # Structure depends on the actual site implementation
                        if "products" in state_data:
                            for product in state_data["products"]:
                                products.append({
                                    "id": product.get("id", ""),
                                    "name": product.get("name", ""),
                                    "brand": product.get("brand", ""),
                                    "price": product.get("price", 0),
                                    "image_url": product.get("image", ""),
                                    "ean": product.get("ean", "")
                                })
                    except Exception as e:
                        logger.error(f"Error parsing state data: {e}")
            
            # If all else fails, try regex-based extraction
            if not products:
                # Look for product cards in the HTML
                product_card_pattern = r'data-product-id=["\']([^"\']+)["\'].*?data-product-name=["\']([^"\']+)["\']'
                product_matches = re.finditer(product_card_pattern, html_content, re.DOTALL)
                
                for match in product_matches:
                    product_id = match.group(1)
                    product_name = match.group(2)
                    
                    # Look for price near this product
                    price_pattern = r'data-product-price=["\']([^"\']+)["\']'
                    price_match = re.search(price_pattern, html_content[match.start():match.start()+500])
                    price = float(price_match.group(1)) if price_match else 0
                    
                    # Look for image near this product
                    image_pattern = r'data-product-thumbnail=["\']([^"\']+)["\']'
                    image_match = re.search(image_pattern, html_content[match.start():match.start()+500])
                    image_url = image_match.group(1) if image_match else ""
                    
                    products.append({
                        "id": product_id,
                        "name": product_name,
                        "price": price,
                        "image_url": image_url
                    })
            
            return products
        except Exception as e:
            logger.error(f"Error extracting products from HTML: {e}")
            return []
    
    def _extract_product_from_html(self, html_content):
        """Extract product details from HTML product page"""
        try:
            # Look for JSON-LD data first
            json_ld_match = re.search(r'<script type="application/ld\+json">(.*?)</script>', html_content, re.DOTALL)
            
            if json_ld_match:
                try:
                    json_data = json.loads(json_ld_match.group(1))
                    
                    # Check if it's a product
                    if "@type" in json_data and json_data["@type"] == "Product":
                        return {
                            "id": json_data.get("sku", ""),
                            "name": json_data.get("name", ""),
                            "brand": json_data.get("brand", {}).get("name", ""),
                            "price": float(json_data.get("offers", {}).get("price", 0)),
                            "image_url": json_data.get("image", ""),
                            "ean": json_data.get("gtin13", ""),
                            "description": json_data.get("description", "")
                        }
                except Exception as e:
                    logger.error(f"Error parsing product JSON-LD: {e}")
            
            # Fallback to regex extraction
            product_id_match = re.search(r'data-product-id=["\']([^"\']+)["\']', html_content)
            product_name_match = re.search(r'data-product-name=["\']([^"\']+)["\']', html_content)
            product_price_match = re.search(r'data-product-price=["\']([^"\']+)["\']', html_content)
            product_brand_match = re.search(r'data-product-brand=["\']([^"\']+)["\']', html_content)
            product_image_match = re.search(r'data-zoom-image=["\']([^"\']+)["\']', html_content)
            
            # Also try to extract EAN (often in structured data)
            ean_match = re.search(r'"ean"\s*:\s*"(\d+)"', html_content)
            
            return {
                "id": product_id_match.group(1) if product_id_match else "",
                "name": product_name_match.group(1) if product_name_match else "",
                "brand": product_brand_match.group(1) if product_brand_match else "",
                "price": float(product_price_match.group(1)) if product_price_match else 0,
                "image_url": product_image_match.group(1) if product_image_match else "",
                "ean": ean_match.group(1) if ean_match else ""
            }
            
        except Exception as e:
            logger.error(f"Error extracting product from HTML: {e}")
            return {}


class CarrefourRestScraper:
    """Scraper for Carrefour products using REST APIs"""
    
    def __init__(self, output_dir, country="fr"):
        """
        Initialize the scraper
        
        Args:
            output_dir: Directory to save scraped data
            country: Country code (fr, es, it, etc.)
        """
        self.api = CarrefourRestAPI(country)
        self.output_dir = output_dir
        self.products_dir = os.path.join(output_dir, "products")
        self.raw_dir = os.path.join(output_dir, "raw", "carrefour")
        
        # Create directories if they don't exist
        os.makedirs(self.products_dir, exist_ok=True)
        os.makedirs(self.raw_dir, exist_ok=True)
        
        # Store processed products to avoid duplicates
        self.processed_products = set()
        self.failed_products = set()
    
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
        
        products = self.api.search_products(query)
        
        # Limit to max_products
        products = products[:max_products]
        
        logger.info(f"Found {len(products)} products for query '{query}'")
        
        # Save raw data
        if products:
            query_hash = hash(query) % 10000
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
                # Try to get details using EAN if available
                ean = product_data.get("ean", "")
                if ean:
                    product_details = self.api.get_product_details(ean=ean)
                
                # If that fails, try using product ID
                if not product_details:
                    product_details = self.api.get_product_details(product_id=product_id)
            
            # Extract product information, preferring details if available
            data_source = product_details if product_details else product_data
            
            name = data_source.get("name", "")
            brand = data_source.get("brand", "")
            price = data_source.get("price", 0)
            ean = data_source.get("ean", "")
            
            # Ensure brand is a string (it might be a nested object)
            if isinstance(brand, dict) and "name" in brand:
                brand = brand["name"]
            
            # Extract image URL
            image_url = data_source.get("image_url", "")
            
            # If image_url is a list, take the first one
            if isinstance(image_url, list) and len(image_url) > 0:
                image_url = image_url[0]
            
            # Create metadata
            metadata = {
                "product_id": formatted_id,
                "name": name,
                "brand": brand,
                "price": price,
                "currency": "EUR",
                "retailer": "Carrefour",
                "source": "carrefour",
                "source_id": product_id
            }
            
            # Add EAN if available
            if ean:
                metadata["ean"] = ean
            
            # Save metadata
            with open(os.path.join(product_dir, "metadata.json"), "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            # Download and save images
            if image_url:
                self._download_product_images([image_url], product_dir)
            
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
                # For some URLs, make sure we get a high-resolution version
                # Replace thumbnail size with larger size
                img_url = re.sub(r'/\d+x\d+/', '/1200x1200/', img_url)
                
                response = self.api.session.get(img_url, timeout=30)
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
                time.sleep(random.uniform(0.5, 1.0))
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Error downloading image {img_url}: {e}")
    
    def scrape_search_terms(self, max_products=30000, max_per_search=100, workers=5):
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
            
            # Add delay between search terms
            time.sleep(random.uniform(2.0, 4.0))
        
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
    parser = argparse.ArgumentParser(description="Carrefour Product Data Scraper (REST API Version)")
    parser.add_argument("--output-dir", type=str, default="data", help="Output directory")
    parser.add_argument("--country", type=str, default="fr", help="Country code (fr, es, it, etc.)")
    parser.add_argument("--max-products", type=int, default=30000, help="Maximum number of products to scrape")
    parser.add_argument("--max-per-search", type=int, default=100, help="Maximum products per search term")
    parser.add_argument("--workers", type=int, default=5, help="Number of worker threads")
    parser.add_argument("--create-manifest", action="store_true", help="Create training manifest after scraping")
    parser.add_argument("--test-mode", action="store_true", help="Run in test mode with only a few products")
    
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = CarrefourRestScraper(args.output_dir, args.country)
    
    # Run in test mode if requested
    if args.test_mode:
        logger.info("Running in test mode with limited products")
        test_terms = ["lait", "fromage", "pain"]
        total_products = 0
        
        for term in test_terms:
            products = scraper.search_products(term, max_products=5)
            processed = 0
            
            for product in products:
                if scraper.process_product(product, get_details=True):
                    processed += 1
            
            logger.info(f"Processed {processed}/{len(products)} products for '{term}'")
            total_products += processed
        
        logger.info(f"Test completed with {total_products} total products processed")
        return
    
    # Normal scraping mode
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
    main()#!/usr/bin/env python3
"""
Carrefour Product Data Collection Script (REST API Version)

This script collects product data from Carrefour's website using their REST APIs
rather than GraphQL. Based on network analysis, this approach should bypass
the 403 forbidden errors encountered with the GraphQL API.
"""

import os
import json
import time
import argparse
import requests
import random
import logging
import re
from urllib.parse import quote, unquote
from concurrent.futures import ThreadPoolExecutor
from PIL import Image
from io import BytesIO
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("carrefour_rest_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Common search terms for groceries in French
COMMON_SEARCH_TERMS = [
    "lait", "fromage", "yaourt", "beurre", "crème", "chocolat", 
    "café", "thé", "jus", "eau", "pain", "pâtes", "riz", "céréales",
    "viande", "poulet", "poisson", "légumes", "fruits", "conserves",
    "huile", "vinaigrette", "sucre", "farine", "sel", "épices",
    "biscuits", "chips", "bonbons", "glace", "surgelés", "pizza",
    "savon", "shampoing", "dentifrice", "papier toilette", "lessive"
]

class CarrefourRestAPI:
    """Carrefour REST API client"""
    
    def __init__(self, country="fr"):
        """
        Initialize the Carrefour API client
        
        Args:
            country: Country code (fr, es, it, etc.)
        """
        self.country = country
        self.base_url = f"https://www.carrefour.{country}"
        self.session = requests.Session()
        
        # Set up headers to look like a browser
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": f"{self.base_url}/",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin"
        })
        
        # Initialize session with cookies
        self._init_session()
        
        # Request counter for rate limiting
        self.request_count = 0
        self.last_request_time = time.time()
    
    def _init_session(self):
        """Initialize session with necessary cookies"""
        try:
            # First visit the homepage to get cookies
            logger.info(f"Initializing session with Carrefour {self.country.upper()}...")
            response = self.session.get(f"{self.base_url}/", timeout=30)
            response.raise_for_status()
            
            logger.info(f"Session initialized with {len(self.session.cookies)} cookies")
            return True
        except Exception as e:
            logger.error(f"Error initializing session: {e}")
            return False
    
    def _rate_limit(self):
        """Apply rate limiting to avoid too many requests"""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        # Be conservative with request rate to avoid blocking
        if elapsed < 1.5:
            time.sleep(1.5 - elapsed)
            
        self.last_request_time = time.time()
        self.request_count += 1
        
        # Add jitter to seem more human-like
        jitter = random.uniform(0.2, 0.8)
        time.sleep(jitter)
    
    def search_products(self, query, page=1, limit=24):
        """
        Search for products using the regular search page
        
        Args:
            query: Search query
            page: Page number
            limit: Results per page
            
        Returns:
            List of product data
        """
        self._rate_limit()
        
        # URL encode the query
        encoded_query = quote(query)
        
        try:
            # First approach: Try the universe JSON API
            # This endpoint works for category searches
            universe_url = f"{self.base_url}/univers={encoded_query}.json"
            
            logger.info(f"Trying universe endpoint: {universe_url}")
            response = self.session.get(universe_url, timeout=30)
            
            if response.status_code == 200 and len(response.content) > 500:
                try:
                    data = response.json()
                    # Process universe format data
                    products = self._extract_products_from_universe(data)
                    if products:
                        logger.info(f"Found {len(products)} products from universe endpoint")
                        return products
                except Exception as e:
                    logger.warning(f"Error parsing universe response: {e}")
                    # Continue to next approach
            
            # Second approach: Try the search panel API
            # This is directly from the observed network requests
            search_panel_url = f"{self.base_url}/search_panel?q={encoded_query}"
            
            logger.info(f"Trying search panel endpoint: {search_panel_url}")
            response = self.session.get(search_panel_url, timeout=30)
            
            if response.status_code == 200 and len(response.content) > 500:
                try:
                    data = response.json()
                    # Process search panel format data
                    products = self._extract_products_from_search_panel(data)
                    if products:
                        logger.info(f"Found {len(products)} products from search panel endpoint")
                        return products
                except Exception as e:
                    logger.warning(f"Error parsing search panel response: {e}")
                    # Continue to next approach
            
            # Third approach: Parse the HTML search results page
            # This is a fallback method if the APIs fail
            search_url = f"{self.base_url}/s?q={encoded_query}&page={page}"
            
            logger.info(f"Trying HTML search page: {search_url}")
            response = self.session.get(search_url, timeout=30)
            
            if response.status_code == 200:
                # Extract structured data from HTML
                products = self._extract_products_from_html(response.text)
                if products:
                    logger.info(f"Found {len(products)} products from HTML search page")
                    return products
                else:
                    logger.warning("No products found in HTML search page")
            
            # If we got here, all approaches failed
            logger.warning(f"All search approaches failed for query '{query}'")
            return []
            
        except Exception as e:
            logger.error(f"Error searching products: {e}")
            return []
    
    def get_product_details(self, product_id=None, ean=None):
        """
        Get detailed product information
        
        Args:
            product_id: Product ID
            ean: EAN barcode
            
        Returns:
            Product details
        """
        self._rate_limit()
        
        try:
            if ean:
                # Try direct EAN lookup
                # Based on the observed request: 3560071080914.json?u=E6...
                ean_url = f"{self.base_url}/{ean}.json"
                
                logger.info(f"Trying direct EAN lookup: {ean_url}")
                response = self.session.get(ean_url, timeout=30)
                
                if response.status_code == 200 and len(response.content) > 100:
                    try:
                        data = response.json()
                        return data
                    except Exception as e:
                        logger.warning(f"Error parsing EAN response: {e}")
            
            if product_id:
                # Try product detail page API
                # Based on the observed "pdp" request
                pdp_url = f"{self.base_url}/p/pdp?productId={product_id}"
                
                logger.info(f"Trying product detail API: {pdp_url}")
                response = self.session.get(pdp_url, timeout=30)
                
                if response.status_code == 200 and len(response.content) > 100:
                    try:
                        data = response.json()
                        return data
                    except Exception as e:
                        logger.warning(f"Error parsing PDP response: {e}")
                
                # Fallback: Try direct product URL with HTML parsing
                product_url = f"{self.base_url}/p/{product_id}"
                
                logger.info(f"Trying product detail page: {product_url}")
                response = self.session.get(product_url, timeout=30)
                
                if response.status_code == 200:
                    # Extract product data from HTML
                    return self._extract_product_from_html(response.text)
            
            logger.warning(f"Could not get product details for product_id={product_id}, ean={ean}")
            return None
            
        except Exception as e:
            logger.error(f"Error getting product details: {e}")
            return None
    
    def _extract_products_from_universe(self, data):
        """Extract products from universe API response"""
        products = []
        
        try:
            # Structure depends on the actual response format
            # This is a placeholder implementation
            if "products" in data:
                for product in data["products"]:
                    products.append({
                        "id": product.get("id", ""),
                        "name": product.get("name", ""),
                        "brand": product.get("brand", {}).get("name", ""),
                        "price": product.get("price", {}).get("value", 0),
                        "image_url": product.get("image", {}).get("url", ""),
                        "ean": product.get("ean", "")
                    })
            
            return products
        except Exception as e:
            logger.error(f"Error extracting products from universe data: {e}")
            return []
    
    def _extract_products_from_search_panel(self, data):
        """Extract products from search panel API response"""
        products = []
        
        try:
            # Structure depends on the actual response format
            # This is a placeholder implementation
            if "results" in data:
                for product in data["results"]:
                    products.append({
                        "id": product.get("id", ""),
                        "name": product.get("name", ""),
                        "brand": product.get("brand", ""),
                        "price": product.get("price", 0),
                        "image_url": product.get("image", ""),
                        "ean": product.get("ean", "")
                    })
            
            return products
        except Exception as e:
            logger.error(f"Error extracting products from search panel data: {e}")
            return []
    
    def _extract_products_from_html(self, html_content):
        """Extract products from HTML search results page"""
        products = []
        
        try:
            # Look for JSON data embedded in the HTML
            # Many e-commerce sites embed product data in JSON-LD format
            json_ld_match = re.search(r'<script type="application/ld\+json">(.*?)</script>', html_content, re.DOTALL)
            
            if json_ld_match:
                try:
                    json_data = json.loads(json_ld_match.group(1))
                    
                    # Check if it's a product list
                    if "@type" in json_data and json_data["@type"] == "ItemList":
                        for item in json_data.get("itemListElement", []):
                            if "item" in item:
                                product = item["item"]
                                products.append({
                                    "id": product.get("sku", ""),
                                    "name": product.get("name", ""),
                                    "brand": product.get("brand", {}).get("name", ""),
                                    "price": float(product.get("offers", {}).get("price", 0)),
                                    "image_url": product.get("image", ""),
                                    "ean": product.get("gtin13", "")
                                })
                except Exception as e:
                    logger.error(f"Error parsing JSON-LD: {e}")
            
            # If no JSON-LD data, try alternative methods
            if not products:
                # Look for product data in a different format
                # For example, some sites have a window.__INITIAL_STATE__ variable
                state_match = re.search(r'window\.__INITIAL_STATE__\s*=\s*(\{.*?\});\s*</script>', html_content, re.DOTALL)
                
                if state_match:
                    try:
                        # Warning: This is potentially unsafe
                        # In a production environment, use a proper JSON parser
                        state_data = json.loads(state_match.group(1))
                        
                        # Extract product data from state
                        # Structure depends on the actual site implementation
                        if "products" in state_data:
                            for product in state_data["products"]:
                                products.append({
                                    "id": product.get("id", ""),
                                    "name": product.get("name", ""),
                                    "brand": product.get("brand", ""),
                                    "price": product.get("price", 0),
                                    "image_url": product.get("image", ""),
                                    "ean": product.get("ean", "")
                                })
                    except Exception as e:
                        logger.error(f"Error parsing state data: {e}")
            
            # If all else fails, try regex-based extraction
            if not products:
                # Look for product cards in the HTML
                product_card_pattern = r'data-product-id=["\']([^"\']+)["\'].*?data-product-name=["\']([^"\']+)["\']'
                product_matches = re.finditer(product_card_pattern, html_content, re.DOTALL)
                
                for match in product_matches:
                    product_id = match.group(1)
                    product_name = match.group(2)
                    
                    # Look for price near this product
                    price_pattern = r'data-product-price=["\']([^"\']+)["\']'
                    price_match = re.search(price_pattern, html_content[match.start():match.start()+500])
                    price = float(price_match.group(1)) if price_match else 0
                    
                    # Look for image near this product
                    image_pattern = r'data-product-thumbnail=["\']([^"\']+)["\']'
                    image_match = re.search(image_pattern, html_content[match.start():match.start()+500])
                    image_url = image_match.group(1) if image_match else ""
                    
                    products.append({
                        "id": product_id,
                        "name": product_name,
                        "price": price,
                        "image_url": image_url
                    })
            
            return products
        except Exception as e:
            logger.error(f"Error extracting products from HTML: {e}")
            return []
    
    def _extract_product_from_html(self, html_content):
        """Extract product details from HTML product page"""
        try:
            # Look for JSON-LD data first
            json_ld_match = re.search(r'<script type="application/ld\+json">(.*?)</script>', html_content, re.DOTALL)
            
            if json_ld_match:
                try:
                    json_data = json.loads(json_ld_match.group(1))
                    
                    # Check if it's a product
                    if "@type" in json_data and json_data["@type"] == "Product":
                        return {
                            "id": json_data.get("sku", ""),
                            "name": json_data.get("name", ""),
                            "brand": json_data.get("brand", {}).get("name", ""),
                            "price": float(json_data.get("offers", {}).get("price", 0)),
                            "image_url": json_data.get("image", ""),
                            "ean": json_data.get("gtin13", ""),
                            "description": json_data.get("description", "")
                        }
                except Exception as e:
                    logger.error(f"Error parsing product JSON-LD: {e}")
            
            # Fallback to regex extraction
            product_id_match = re.search(r'data-product-id=["\']([^"\']+)["\']', html_content)
            product_name_match = re.search(r'data-product-name=["\']([^"\']+)["\']', html_content)
            product_price_match = re.search(r'data-product-price=["\']([^"\']+)["\']', html_content)
            product_brand_match = re.search(r'data-product-brand=["\']([^"\']+)["\']', html_content)
            product_image_match = re.search(r'data-zoom-image=["\']([^"\']+)["\']', html_content)
            
            # Also try to extract EAN (often in structured data)
            ean_match = re.search(r'"ean"\s*:\s*"(\d+)"', html_content)
            
            return {
                "id": product_id_match.group(1) if product_id_match else "",
                "name": product_name_match.group(1) if product_name_match else "",
                "brand": product_brand_match.group(1) if product_brand_match else "",
                "price": float(product_price_match.group(1)) if product_price_match else 0,
                "image_url": product_image_match.group(1) if product_image_match else "",
                "ean": ean_match.group(1) if ean_match else ""
            }
            
        except Exception as e:
            logger.error(f"Error extracting product from HTML: {e}")
            return {}


class CarrefourRestScraper:
    """Scraper for Carrefour products using REST APIs"""
    
    def __init__(self, output_dir, country="fr"):
        """
        Initialize the scraper
        
        Args:
            output_dir: Directory to save scraped data
            country: Country code (fr, es, it, etc.)
        """
        self.api = CarrefourRestAPI(country)
        self.output_dir = output_dir
        self.products_dir = os.path.join(output_dir, "products")
        self.raw_dir = os.path.join(output_dir, "raw", "carrefour")
        
        # Create directories if they don't exist
        os.makedirs(self.products_dir, exist_ok=True)
        os.makedirs(self.raw_dir, exist_ok=True)
        
        # Store processed products to avoid duplicates
        self.processed_products = set()
        self.failed_products = set()
    
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
        
        products = self.api.search_products(query)
        
        # Limit to max_products
        products = products[:max_products]
        
        logger.info(f"Found {len(products)} products for query '{query}'")
        
        # Save raw data
        if products:
            query_hash = hash(query) % 10000
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
                # Try to get details using EAN if available
                ean = product_data.get("ean", "")
                if ean:
                    product_details = self.api.get_product_details(ean=ean)
                
                # If that fails, try using product ID
                if not product_details:
                    product_details = self.api.get_product_details(product_id=product_id)
            
            # Extract product information, preferring details if available
            data_source = product_details if product_details else product_data
            
            name = data_source.get("name", "")
            brand = data_source.get("brand", "")
            price = data_source.get("price", 0)
            ean = data_source.get("ean", "")
            
            # Ensure brand is a string (it might be a nested object)
            if isinstance(brand, dict) and "name" in brand:
                brand = brand["name"]
            
            # Extract image URL
            image_url = data_source.get("image_url", "")
            
            # If image_url is a list, take the first one
            if isinstance(image_url, list) and len(image_url) > 0:
                image_url = image_url[0]
            
            # Create metadata
            metadata = {
                "product_id": formatted_id,
                "name": name,
                "brand": brand,
                "price": price,
                "currency": "EUR",
                "retailer": "Carrefour",
                "source": "carrefour",
                "source_id": product_id
            }
            
            # Add EAN if available
            if ean:
                metadata["ean"] = ean
            
            # Save metadata
            with open(os.path.join(product_dir, "metadata.json"), "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            # Download and save images
            if image_url:
                self._download_product_images([image_url], product_dir)
            
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
                # For some URLs, make sure we get a high-resolution version
                # Replace thumbnail size with larger size
                img_url = re.sub(r'/\d+x\d+/', '/1200x1200/', img_url)
                
                response = self.api.session.get(img_url, timeout=30)
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
                time.sleep(random.uniform(0.5, 1.0))
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Error downloading image {img_url}: {e}")
    
    def scrape_search_terms(self, max_products=30000, max_per_search=100, workers=5):
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
            
            # Add delay between search terms
            time.sleep(random.uniform(2.0, 4.0))
        
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
    parser = argparse.ArgumentParser(description="Carrefour Product Data Scraper (REST API Version)")
    parser.add_argument("--output-dir", type=str, default="data", help="Output directory")
    parser.add_argument("--country", type=str, default="fr", help="Country code (fr, es, it, etc.)")
    parser.add_argument("--max-products", type=int, default=30000, help="Maximum number of products to scrape")
    parser.add_argument("--max-per-search", type=int, default=100, help="Maximum products per search term")
    parser.add_argument("--workers", type=int, default=5, help="Number of worker threads")
    parser.add_argument("--create-manifest", action="store_true", help="Create training manifest after scraping")
    parser.add_argument("--test-mode", action="store_true", help="Run in test mode with only a few products")
    
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = CarrefourRestScraper(args.output_dir, args.country)
    
    # Run in test mode if requested
    if args.test_mode:
        logger.info("Running in test mode with limited products")
        test_terms = ["lait", "fromage", "pain"]
        total_products = 0
        
        for term in test_terms:
            products = scraper.search_products(term, max_products=5)
            processed = 0
            
            for product in products:
                if scraper.process_product(product, get_details=True):
                    processed += 1
            
            logger.info(f"Processed {processed}/{len(products)} products for '{term}'")
            total_products += processed
        
        logger.info(f"Test completed with {total_products} total products processed")
        return
    
    # Normal scraping mode
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