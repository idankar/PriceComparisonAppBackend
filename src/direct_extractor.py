# Create a new file: src/direct_extractor.py

import os
import sys
import time
import json
import csv
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Add the project root to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(config.LOGS_DIR, "direct_extractor.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def extract_product_info_directly(query):
    """
    Extract product information directly from Shufersal website HTML
    
    Args:
        query (str): Search query for products
        
    Returns:
        list: Extracted product information
    """
    logger.info(f"🔍 Directly extracting product info for: {query}")
    
    # Get query-specific paths
    paths = config.get_query_paths(query)
    
    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    driver = webdriver.Chrome(options=options)
    driver.set_window_size(1920, 1080)
    driver.get("https://www.shufersal.co.il/online/he/")
    
    products = []
    
    try:
        # Wait for search box to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "js-site-search-input"))
        )
        search_box = driver.find_element(By.ID, "js-site-search-input")
        search_box.clear()
        search_box.send_keys(query)
        search_box.send_keys(Keys.RETURN)
        
        # Wait for results to load
        time.sleep(5)
        
        # Take one screenshot for reference
        screenshot_path = os.path.join(paths["screenshots_dir"], f"search_page_{paths['run_id']}.png")
        driver.save_screenshot(screenshot_path)
        logger.info(f"📸 Search page screenshot saved to: {screenshot_path}")
        
        # Save page source for debugging
        page_source_path = os.path.join(paths["screenshots_dir"], f"page_source_{paths['run_id']}.html")
        with open(page_source_path, 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        logger.info(f"📄 Page source saved to: {page_source_path}")
        
        # Try different CSS selectors for product tiles
        product_tiles = []
        selectors = [
            ".productCardWrapper",              # Original guess
            ".product-card",                    # Common pattern
            ".product-item",                    # Common pattern
            ".js-product-card",                 # Common pattern
            ".product",                         # Simple fallback
            "li.product",                       # List items
            ".product-container",               # Container pattern
            ".card",                            # Generic card
            "[data-component='product']",       # Data attribute
            ".productBox",                      # Another common pattern
            ".product-box",                     # Hyphenated variant
            ".srch-rslt-grid li",               # Search results grid
            ".productsList li"                  # Products list
        ]
        
        for selector in selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            if elements:
                logger.info(f"Found {len(elements)} elements with selector: {selector}")
                product_tiles = elements
                break
        
        # If no selectors worked, try finding elements that contain certain text
        if not product_tiles:
            logger.info("No product tiles found with CSS selectors, trying text-based search")
            
            # Find all elements that might contain product info
            all_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'נוטלה') or contains(text(), 'Nutella')]")
            logger.info(f"Found {len(all_elements)} elements containing 'נוטלה' or 'Nutella'")
            
            # For each element, try to find a parent that looks like a product tile
            for element in all_elements:
                try:
                    # Try to navigate up to find a container
                    potential_tile = element
                    for _ in range(5):  # Look up to 5 levels up
                        potential_tile = potential_tile.find_element(By.XPATH, "..")
                        class_attr = potential_tile.get_attribute("class") or ""
                        if "product" in class_attr or "card" in class_attr or "item" in class_attr:
                            product_tiles.append(potential_tile)
                            break
                except:
                    continue
            
            # Remove duplicates
            unique_tiles = []
            seen_ids = set()
            for tile in product_tiles:
                tile_id = tile.id
                if tile_id not in seen_ids:
                    seen_ids.add(tile_id)
                    unique_tiles.append(tile)
            
            product_tiles = unique_tiles
        
        logger.info(f"Found {len(product_tiles)} product tiles")
        
        # If still no tiles, print more debug info
        if not product_tiles:
            # Get all div elements
            all_divs = driver.find_elements(By.TAG_NAME, "div")
            logger.info(f"Page contains {len(all_divs)} div elements")
            
            # Print classes of first 20 divs
            for i, div in enumerate(all_divs[:20]):
                class_attr = div.get_attribute("class") or ""
                logger.info(f"Div {i} classes: {class_attr}")
        
        for i, tile in enumerate(product_tiles):
            try:
                # First, take a screenshot of the tile
                product_img_path = os.path.join(paths["cropped_dir"], f"product_{paths['run_id']}_{i}.png")
                try:
                    tile.screenshot(product_img_path)
                    logger.info(f"Saved tile screenshot to {product_img_path}")
                except:
                    logger.warning(f"Could not screenshot tile {i}")
                
                # Get the HTML of this tile for debugging
                tile_html = tile.get_attribute("outerHTML")
                tile_html_path = os.path.join(paths["cropped_dir"], f"tile_{paths['run_id']}_{i}.html")
                with open(tile_html_path, 'w', encoding='utf-8') as f:
                    f.write(tile_html)
                
                # Try different strategies to extract the product name
                product_name = "Unknown Product"
                
                # Method 1: Try to find elements with specific selectors
                name_selectors = [
                    ".productDescription", ".name", "h3", ".title", 
                    ".product-name", ".product-title", "span.name", 
                    "[data-id='name']", "[data-test='product-name']"
                ]
                for selector in name_selectors:
                    elements = tile.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        product_name = elements[0].text.strip()
                        logger.info(f"Found name with selector {selector}: {product_name}")
                        break
                
                # Method 2: If no name found, try looking for text containing Nutella
                if product_name == "Unknown Product":
                    nutella_elements = tile.find_elements(By.XPATH, ".//*[contains(text(), 'נוטלה') or contains(text(), 'Nutella')]")
                    if nutella_elements:
                        product_name = nutella_elements[0].text.strip()
                        logger.info(f"Found name with Nutella text search: {product_name}")
                
                # For Nutella queries, filter out non-Nutella products
                if query.lower() in ["נוטלה", "nutella"]:
                    if "נוטלה" not in product_name.lower() and "nutella" not in product_name.lower():
                        logger.info(f"Skipping non-Nutella product: {product_name}")
                        continue
                
                # Extract price using multiple strategies
                price = 0.0
                price_selectors = [
                    ".pricingContainer .number", ".price", ".priceInfo",
                    ".product-price", "[data-test='price']", ".price-container",
                    "[data-id='price']", ".current-price", "span.price"
                ]
                
                for selector in price_selectors:
                    elements = tile.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        price_text = elements[0].text.strip()
                        # Clean up price text
                        for char in ["₪", "₹", "שח", ",", " "]:
                            price_text = price_text.replace(char, "")
                        try:
                            price = float(price_text)
                            logger.info(f"Found price with selector {selector}: {price}")
                            break
                        except ValueError:
                            continue
                
                # Method 2: If no price found, look for text that contains ₪ or numbers
                if price == 0.0:
                    # Look for elements with ₪ symbol
                    price_elements = tile.find_elements(By.XPATH, ".//*[contains(text(), '₪')]")
                    if price_elements:
                        price_text = price_elements[0].text.strip()
                        # Extract digits
                        import re
                        digits = re.findall(r'[\d.,]+', price_text)
                        if digits:
                            try:
                                price = float(digits[0].replace(',', ''))
                                logger.info(f"Found price with ₪ text search: {price}")
                            except ValueError:
                                pass
                
                # Get product weight/quantity if available
                quantity = ""
                quantity_selectors = [".quantity", ".weight", ".unit", "[data-test='quantity']"]
                for selector in quantity_selectors:
                    elements = tile.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        quantity = elements[0].text.strip()
                        break
                
                # Add product info to results
                products.append({
                    "query": query,
                    "product_name": product_name,
                    "price": price,
                    "quantity": quantity,
                    "image_path": product_img_path
                })
                
                logger.info(f"✅ Extracted product: {product_name}, price: {price}, quantity: {quantity}")
                
            except Exception as e:
                logger.warning(f"Error extracting product info from tile {i}: {str(e)}")
                continue
    
    except Exception as e:
        logger.error(f"Error during direct extraction: {str(e)}")
    finally:
        driver.quit()
    
    # Save results to CSV
    if products:
        output_csv = paths["ocr_csv"]
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(output_csv), exist_ok=True)
        
        # Write to CSV
        with open(output_csv, mode="w", newline='', encoding='utf-8') as f:
            fieldnames = ["query", "product_name", "price", "quantity", "image_path"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(products)
        
        logger.info(f"✅ Product info saved: {output_csv} ({len(products)} entries)")
        
        # Also append to master CSV
        append_to_master_csv(products)
    else:
        logger.warning(f"No valid product results found. Nothing saved.")
    
    return products

def append_to_master_csv(products):
    """
    Append results to the master CSV file
    
    Args:
        products (list): Product information to append
    """
    # Check if master CSV exists
    master_csv_exists = os.path.exists(config.MASTER_OCR_CSV)
    
    # Open in append mode
    with open(config.MASTER_OCR_CSV, mode="a" if master_csv_exists else "w", newline='', encoding='utf-8') as f:
        fieldnames = ["query", "product_name", "price", "quantity", "image_path"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        # Write header only if the file is new
        if not master_csv_exists:
            writer.writeheader()
        
        # Write all results
        writer.writerows(products)
    
    logger.info(f"✅ Appended {len(products)} entries to master CSV: {config.MASTER_OCR_CSV}")

def process_query(query):
    """
    Process a query using direct HTML extraction
    
    Args:
        query (str): Search query
        
    Returns:
        list: Extracted product information
    """
    return extract_product_info_directly(query)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("⚠️ Missing query. Usage: python direct_extractor.py 'נוטלה'")
        sys.exit(1)
    
    query = sys.argv[1]
    products = process_query(query)
    logger.info(f"✅ Extracted {len(products)} products for query: {query}")