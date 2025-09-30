#!/usr/bin/env python3
"""
Super-Pharm Commercial Website Scraper (Upgraded with undetected-chromedriver)

This script scrapes product data from the Super-Pharm website (shop.super-pharm.co.il).
As the primary catalog builder for the PharmMate system, this scraper populates the
canonical_products table with comprehensive product information.

Features:
- Headless browser with undetected-chromedriver to avoid bot detection
- Robust logging to console and file
- Checkpoint/resume functionality for interrupted scrapes
- Dry-run and Test modes for safe execution
"""

import time
import json
import argparse
import re
import logging
from datetime import datetime
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import undetected_chromedriver as uc
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from urllib.parse import urljoin

# --- Configuration & Logging ---
load_dotenv()
DB_NAME = os.getenv("DB_NAME", "price_comparison_app_v2")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "***REMOVED***")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
SUPER_PHARM_RETAILER_ID = 52
CHECKPOINT_FILE = 'super_pharm_scraper_state.json'
LOG_FILE = 'super_pharm_scraper.log'
BASE_URL = "https://shop.super-pharm.co.il/"

log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('SuperPharmScraper')
logger.setLevel(logging.INFO)

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)
    file_handler = logging.FileHandler(LOG_FILE, mode='a')
    file_handler.setFormatter(log_formatter)
    logger.addHandler(file_handler)


class SuperPharmScraper:
    """Scraper for Super-Pharm commercial website"""

    def __init__(self, dry_run=False, headless=True, test_mode=False, resume=False):
        self.dry_run = dry_run
        self.headless = headless
        self.test_mode = test_mode
        self.resume = resume
        self.conn = None
        self.cursor = None
        self.total_products_processed = 0
        self.scraped_data_collection = []
        self.completed_categories = []
        self.test_mode_limit = 10
        self.categories = []
        self.seen_barcodes = set()  # For deduplication
        self.duplicate_count = 0

    def _setup_driver(self):
        """Setup WebDriver - try undetected first, fallback to regular Selenium"""
        options = uc.ChromeOptions()
        if self.headless:
            options.add_argument("--headless=new")
        options.add_argument("--start-maximized")

        logger.info("🚀 Initializing undetected-chromedriver...")
        try:
            # Try undetected-chromedriver first
            driver = uc.Chrome(options=options, use_subprocess=False)
            logger.info("✅ Successfully initialized undetected-chromedriver")
            return driver
        except Exception as e:
            logger.warning(f"⚠️ Undetected-chromedriver failed: {e}")
            logger.info("🔄 Falling back to regular Selenium WebDriver...")

            # Fallback to regular Selenium
            from selenium import webdriver
            from selenium.webdriver.chrome.service import Service
            from webdriver_manager.chrome import ChromeDriverManager

            try:
                regular_options = webdriver.ChromeOptions()
                if self.headless:
                    regular_options.add_argument("--headless=new")
                regular_options.add_argument("--start-maximized")
                regular_options.add_argument("--disable-blink-features=AutomationControlled")
                regular_options.add_experimental_option("excludeSwitches", ["enable-automation"])
                regular_options.add_experimental_option('useAutomationExtension', False)
                regular_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=regular_options)
                driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                logger.info("✅ Successfully initialized regular Selenium WebDriver")
                return driver
            except Exception as e2:
                logger.error(f"❌ Both drivers failed. Error: {e2}")
                return None

    def _connect_db(self):
        if self.dry_run: return
        try:
            self.conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            logger.info("✅ Database connection successful.")
        except psycopg2.OperationalError as e:
            logger.error(f"❌ Could not connect to the database: {e}")
            self.conn = None

    def _close_db(self):
        if self.cursor: self.cursor.close()
        if self.conn: self.conn.close()
        logger.info("🔌 Database connection closed.")

    def load_checkpoint(self):
        if self.resume and os.path.exists(CHECKPOINT_FILE):
            try:
                with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                self.completed_categories = state.get('completed_categories', [])
                self.total_products_processed = state.get('total_products_processed', 0)
                self.categories = state.get('discovered_categories', [])
                logger.info(f"✅ Resuming scrape. Found {len(self.completed_categories)} completed categories.")
                return True
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"⚠️ Could not read checkpoint file, starting fresh. Error: {e}")
        return False

    def save_checkpoint(self, category_name=None):
        if category_name and category_name not in self.completed_categories:
            self.completed_categories.append(category_name)
        state = {'completed_categories': self.completed_categories, 'total_products_processed': self.total_products_processed, 'discovered_categories': self.categories, 'last_updated': datetime.now().isoformat()}
        with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=4, ensure_ascii=False)
        logger.info(f"💾 Checkpoint saved. {len(self.completed_categories)} categories completed.")

    def _get_pharmacy_categories(self):
        """Get hardcoded pharmacy-relevant categories"""
        logger.info("📋 Loading pharmacy-relevant categories...")

        # Hardcoded list of pharmacy-relevant categories as requested by user
        self.categories = [
            # Food & Drinks
            {"name": "קרקרים ועוגיות אורז", "url": "https://shop.super-pharm.co.il/food-and-drinks/crackers-and-rice-cakes/c/70190000"},
            {"name": "דגנים וחטיפים", "url": "https://shop.super-pharm.co.il/food-and-drinks/cereals-and-snacks/c/70140000"},
            {"name": "חטיפים וממתקים", "url": "https://shop.super-pharm.co.il/food-and-drinks/snacks-and-sweets/c/70100000"},
            {"name": "שוקולד", "url": "https://shop.super-pharm.co.il/food-and-drinks/chocolate/c/70200000"},
            {"name": "משקאות אנרגיה ואיזוטוניים", "url": "https://shop.super-pharm.co.il/food-and-drinks/drinks/energy-and-isotonic-drinks/c/70181300"},
            {"name": "קפה", "url": "https://shop.super-pharm.co.il/coffee"},
            {"name": "משקאות קלים", "url": "https://shop.super-pharm.co.il/food-and-drinks/drinks/soft-drinks/c/70181400"},

            # Home
            {"name": "ניקוי ותחזוקה", "url": "https://shop.super-pharm.co.il/home/cleaning-and-maintenance/c/10180000"},

            # Personal Care
            {"name": "טיפוח השיער", "url": "https://shop.super-pharm.co.il/care/hair-care/c/15170000"},
            {"name": "היגיינת הפה", "url": "https://shop.super-pharm.co.il/care/oral-hygiene/c/15160000"},
            {"name": "דאודורנטים", "url": "https://shop.super-pharm.co.il/care/deodorants/c/15150000"},
            {"name": "גילוח והסרת שיער", "url": "https://shop.super-pharm.co.il/care/shaving-and-hair-removal/c/15140000"},
            {"name": "אמבט והיגיינה", "url": "https://shop.super-pharm.co.il/care/bath-and-hygiene/c/15120000"},
            {"name": "מוצרי היגיינה נשיים", "url": "https://shop.super-pharm.co.il/care/feminine-hygiene-products/c/15210000"},
            {"name": "הגנה מהשמש", "url": "https://shop.super-pharm.co.il/care/sun-protection/c/15100000"},
            {"name": "לילדים", "url": "https://shop.super-pharm.co.il/care/for-children/c/15130000"},
            {"name": "טיפוח עור הפנים", "url": "https://shop.super-pharm.co.il/care/facial-skin-care/c/15230000"},
            {"name": "טיפוח הגוף", "url": "https://shop.super-pharm.co.il/care/body-care/c/15220000"},
            {"name": "טיפוח העיניים", "url": "https://shop.super-pharm.co.il/care/eye-care/c/15260000"},
            {"name": "טיפוח הזקן", "url": "https://shop.super-pharm.co.il/care/beard-care/c/15250000"},

            # Cosmetics
            {"name": "בשמים", "url": "https://shop.super-pharm.co.il/cosmetics/perfumes/c/20110000"},
            {"name": "מוצרי טיפוח של מותגי קוסמטיקה", "url": "https://shop.super-pharm.co.il/cosmetics/cosmetics-brands-care-products/c/20230000"},
            {"name": "איפור פנים", "url": "https://shop.super-pharm.co.il/cosmetics/facial-makeup/c/20180000"},
            {"name": "איפור עיניים", "url": "https://shop.super-pharm.co.il/cosmetics/eye-makeup/c/20170000"},
            {"name": "איפור שפתיים", "url": "https://shop.super-pharm.co.il/cosmetics/lip-makeup/c/20190000"},
            {"name": "איפור גבות", "url": "https://shop.super-pharm.co.il/cosmetics/eyebrows-makeup/c/20100000"},
            {"name": "ערכות איפור", "url": "https://shop.super-pharm.co.il/cosmetics/makeup-kits/c/20200000"},
            {"name": "אביזרי איפור", "url": "https://shop.super-pharm.co.il/cosmetics/makeup-tool/c/20140000"},
            {"name": "ספריי לפנים", "url": "https://shop.super-pharm.co.il/cosmetics/facial-spray/c/20220000"},
            {"name": "טיפוח ציפורניים", "url": "https://shop.super-pharm.co.il/cosmetics/nail-care/c/20130000"},

            # Baby & Toddlers
            {"name": "טיפוח התינוק", "url": "https://shop.super-pharm.co.il/infants-and-toddlers/baby-care/c/25130000"},
            {"name": "רחצת התינוק", "url": "https://shop.super-pharm.co.il/infants-and-toddlers/baby-wash/c/25200000"},
            {"name": "הנקה והאכלה", "url": "https://shop.super-pharm.co.il/infants-and-toddlers/nursing-and-feeding/c/25120000"},
            {"name": "מוצצים ונשכנים", "url": "https://shop.super-pharm.co.il/infants-and-toddlers/pacifiers-and-teethers/c/25140000"},
            {"name": "החתלה", "url": "https://shop.super-pharm.co.il/infants-and-toddlers/diapering/c/25110000"},
            {"name": "גמילה מחיתולים", "url": "https://shop.super-pharm.co.il/infants-and-toddlers/toilet-training/c/25220000"},
            {"name": "ערכות", "url": "https://shop.super-pharm.co.il/infants-and-toddlers/kits/c/25180000"},

            # Health
            {"name": "תרופות", "url": "https://shop.super-pharm.co.il/health/medicines/c/30140000"},
            {"name": "תוספי תזונה", "url": "https://shop.super-pharm.co.il/health/supplements/c/30300000"},
            {"name": "עזרה ראשונה", "url": "https://shop.super-pharm.co.il/health/first-aid/c/30100000"},
            {"name": "מדידה ובדיקה", "url": "https://shop.super-pharm.co.il/health/measuring-and-testing/c/30220000"},
            {"name": "בריאות מינית וצעצועי מין", "url": "https://shop.super-pharm.co.il/health/sexual-wellness-and-sex-toys/c/30210000"},
            {"name": "מכשור רפואי", "url": "https://shop.super-pharm.co.il/health/medical-devices/c/30230000"},
            {"name": "ציוד רפואי", "url": "https://shop.super-pharm.co.il/health/medical-equipment/c/30120000"},
            {"name": "כינים", "url": "https://shop.super-pharm.co.il/health/lice/c/30200000"},
            {"name": "טיפול נשימתי", "url": "https://shop.super-pharm.co.il/health/respiratory-therapy/c/30190000"},
            {"name": "בריאות טבעית", "url": "https://shop.super-pharm.co.il/health/natural-health/c/30170000"},
            {"name": "אורתופדיה", "url": "https://shop.super-pharm.co.il/health/orthopedics/c/30160000"},
            {"name": "בריחת שתן", "url": "https://shop.super-pharm.co.il/health/incontinence/c/30250000"},

            # Optics
            {"name": "עדשות מגע", "url": "https://shop.super-pharm.co.il/optics/contact-lenses/c/65120000"},
            {"name": "משקפי שמש", "url": "https://shop.super-pharm.co.il/optics/sunglasses/c/65110000"},
            {"name": "משקפיים", "url": "https://shop.super-pharm.co.il/optics/glasses/c/65100000"},
            {"name": "ניקוי, אחסון ואביזרים", "url": "https://shop.super-pharm.co.il/optics/cleaning-storage-and-accessories/c/65130000"}
        ]

        logger.info(f"✅ Loaded {len(self.categories)} pharmacy-relevant categories for scraping")

    def _handle_pagination(self, driver, category_url):
        """Handle pagination by checking for next page links"""
        # Super-Pharm uses standard pagination with a hidden next link
        # Look for: <a id="nextHiddenLink" class="sr-only" rel="next" href="...?page=1">nextPage</a>

        logger.info("  📄 Searching for pagination links...")

        try:
            # Primary method: Look for the specific hidden link
            next_link = driver.find_element(By.CSS_SELECTOR, "#nextHiddenLink")
            next_url = next_link.get_attribute('href')
            link_text = next_link.text.strip()
            link_rel = next_link.get_attribute('rel')

            logger.info(f"  📄 Found nextHiddenLink: href='{next_url}', text='{link_text}', rel='{link_rel}'")

            if next_url and next_url != category_url:
                logger.info(f"  📄 ✅ Valid next page found: {next_url}")
                return next_url
            else:
                logger.info(f"  📄 ❌ Next URL is same as current or invalid")

        except Exception as e:
            logger.debug(f"  📄 nextHiddenLink not found: {e}")

        # Fallback method: Look for any pagination links
        try:
            logger.info("  📄 Trying fallback pagination selectors...")

            pagination_selectors = [
                "a[rel='next']",
                ".pagination a[href*='page=']",
                "[class*='pagination'] a",
                ".next",
                "[class*='next']",
                "a[href*='page=2']"
            ]

            for selector in pagination_selectors:
                try:
                    links = driver.find_elements(By.CSS_SELECTOR, selector)
                    logger.info(f"  📄 Selector '{selector}': found {len(links)} links")

                    for link in links:
                        href = link.get_attribute('href')
                        text = link.text.strip()
                        if href and 'page=' in href and href != category_url:
                            logger.info(f"  📄 ✅ Fallback next page: {href} (text: '{text}')")
                            return href

                except Exception as e:
                    logger.debug(f"  📄 Selector '{selector}' failed: {e}")

        except Exception as e:
            logger.debug(f"  📄 Fallback pagination failed: {e}")

        # Last resort: Try to construct next page URL manually
        try:
            if 'page=' not in category_url:
                # If we're on the first page (no page parameter), try page=1
                next_url = f"{category_url}?page=1" if '?' not in category_url else f"{category_url}&page=1"
                logger.info(f"  📄 🔧 Constructed next page URL: {next_url}")
                return next_url
        except:
            pass

        logger.info("  📄 ❌ No next page found - pagination complete")
        return None

    def _scroll_to_bottom(self, driver):
        """Super-Pharm uses pagination, not infinite scroll - just scroll once to load page content"""
        logger.info("  📜 Scrolling once to ensure page content is loaded...")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        logger.info("  📜 Scroll complete")

    def _extract_products_from_page(self, driver, category_name):
        products = []
        logger.info("  🔍 Using reverse-engineered product selectors...")

        try:
            # Wait for page to load
            time.sleep(5)

            # Log page info
            logger.info(f"  📍 Page title: {driver.title}")
            logger.info(f"  📍 Current URL: {driver.current_url}")

            # USE THE EXACT SELECTORS FROM HTML ANALYSIS
            # Products are in: <div class="add-to-basket" data-ean="..." data-product-code="...">
            product_elements = driver.find_elements(By.CSS_SELECTOR, "div.add-to-basket[data-ean][data-product-code]")

            logger.info(f"  📦 Found {len(product_elements)} products using correct selectors")

            if not product_elements:
                logger.warning("  ⚠️ No products found with main selector. Trying backup selectors...")

                # Backup selectors based on the HTML structure
                backup_selectors = [
                    "div[data-ean]",  # Any div with data-ean
                    "div[data-product-code]",  # Any div with product code
                    "[class*='add-to-basket']"  # Any element with add-to-basket class
                ]

                for selector in backup_selectors:
                    product_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    if product_elements:
                        logger.info(f"  📦 Found {len(product_elements)} products with backup selector: {selector}")
                        break

            # Extract products using the discovered structure
            for i, element in enumerate(product_elements):
                try:
                    product_data = {}

                    # Extract barcode (EAN) - this is the primary identifier
                    barcode = element.get_attribute('data-ean')
                    if not barcode:
                        logger.debug(f"    ❌ Product {i+1}: No EAN found, skipping")
                        continue

                    product_data['barcode'] = barcode

                    # Extract product code (internal Super-Pharm ID)
                    product_code = element.get_attribute('data-product-code')

                    # Extract category from data-category attribute
                    data_category = element.get_attribute('data-category')
                    if data_category:
                        # Use the specific category from the data attribute
                        product_data['category'] = data_category
                    else:
                        product_data['category'] = category_name

                    # Extract position on page
                    position = element.get_attribute('data-position')

                    # Now find the actual product details by navigating up to the product container
                    # The add-to-basket div is nested inside the actual product container
                    try:
                        # Go up to find the product container (parent elements)
                        product_container = element.find_element(By.XPATH, "../..")  # Go up 2 levels

                        # Extract product name - look for common product name elements
                        name = "Unknown Product"
                        name_selectors = [
                            ".product-name",
                            ".name",
                            ".title",
                            "h1", "h2", "h3", "h4",
                            "a[title]",
                            "[class*='name']",
                            "[class*='title']"
                        ]

                        for selector in name_selectors:
                            try:
                                name_elem = product_container.find_element(By.CSS_SELECTOR, selector)
                                name = name_elem.text.strip() or name_elem.get_attribute('title')
                                if name and len(name) > 2:
                                    break
                            except:
                                continue

                        # If still no name, try extracting from any text in the container
                        if name == "Unknown Product":
                            container_text = product_container.text.strip()
                            if container_text:
                                # Take first meaningful line
                                lines = [line.strip() for line in container_text.split('\n')
                                        if len(line.strip()) > 2 and not line.strip().startswith('₪')]
                                if lines:
                                    name = lines[0][:100]

                        product_data['name'] = name

                        # Extract image URL
                        image_url = None
                        try:
                            img = product_container.find_element(By.TAG_NAME, "img")
                            image_url = img.get_attribute('src') or img.get_attribute('data-src')
                            if image_url and not image_url.startswith('http'):
                                image_url = urljoin(BASE_URL, image_url)
                        except:
                            pass
                        product_data['image_url'] = image_url

                        # Extract product URL
                        try:
                            link = product_container.find_element(By.CSS_SELECTOR, "a[href]")
                            product_url = link.get_attribute('href')
                            if product_url and not product_url.startswith('http'):
                                product_url = urljoin(BASE_URL, product_url)
                            product_data['url'] = product_url
                        except:
                            product_data['url'] = None

                        # Extract price (for reference, not stored in canonical_products)
                        price = None
                        try:
                            price_elem = product_container.find_element(By.XPATH, "//*[contains(text(), '₪')]")
                            price_text = price_elem.text.strip()
                            import re
                            price_match = re.search(r'([\d,]+\.?\d*)', price_text.replace(',', ''))
                            if price_match:
                                price = float(price_match.group())
                        except:
                            pass

                    except Exception as e:
                        # If we can't find the product container, use the element itself
                        logger.debug(f"    ❌ Couldn't find product container for product {i+1}: {e}")
                        product_data['name'] = f"Product {barcode}"
                        product_data['image_url'] = None
                        product_data['url'] = None

                    # Set standard fields
                    product_data['brand'] = None  # Would need deeper analysis to extract
                    product_data['source_retailer_id'] = SUPER_PHARM_RETAILER_ID
                    product_data['description'] = None

                    products.append(product_data)

                    # Log first few products for debugging
                    if i < 5:
                        logger.info(f"    Product {i+1}: {product_data['name'][:40]}... "
                                   f"(EAN: {barcode}, Code: {product_code}, Pos: {position})")

                except Exception as e:
                    logger.debug(f"    ❌ Failed to extract product {i}: {e}")
                    continue

        except Exception as e:
            logger.error(f"  ❌ Error during product extraction: {e}")

        logger.info(f"  ✅ Successfully extracted {len(products)} products")
        return products

    def _clean_product_data(self, product_data: dict) -> dict:
        """
        Parses the cluttered 'name' field to extract a clean name and brand.
        """
        name = product_data.get('name', '')
        brand = product_data.get('brand')

        # If brand is missing, try to extract it from the name
        if not brand and name:
            # --- NEW: Extended list of known brands ---
            known_brands = [
                "NEUTROGENA", "HAAN", "NIVEA", "MORAZ", "DOVE", "OLD SPICE", "LIFE", "URIAGE", "AVENE",
                "Drizzilicious", "NESTLE", "FITNESS", "לייף וולנס", "נסטלה", "אנרג'י", "סאפורה"
            ]

            brand_found = False
            # Pattern 1: Look for known brands at the start of the string (case-insensitive)
            for b in known_brands:
                if name.lower().startswith(b.lower()):
                    # Find the actual brand casing from the name
                    brand = name[:len(b)]
                    name = name[len(b):].strip()
                    brand_found = True
                    break
            
            # Pattern 2: Fallback to any all-caps word at the beginning
            if not brand_found:
                match = re.match(r'^([A-Z][A-Z\s&]{2,})', name)
                if match:
                    potential_brand = match.group(1).strip()
                    if len(potential_brand) <= 20: # Avoid capturing long uppercase titles
                        brand = potential_brand
                        name = name[len(brand):].strip()

        # --- NEW: More comprehensive cleaning patterns ---
        # Clean up size/unit info (e.g., "500 מ"ל", "113 גרם", "18 פרוסות")
        name = re.sub(r'\n?[\d\.,]+\s*(מ"ל|מ\'\'ל|גרם|ג\'|יחידות|פרוסות|אריזות|פ\'|\'|מ"ג|מ\'ג)', '', name, flags=re.IGNORECASE).strip()
        
        # Clean up unit price in parentheses (e.g., "(₪19.38 ל-100 גרם)")
        name = re.sub(r'\s*\(\s*₪?[\d\.,]+\s*ל-?\d*\s*(מ"ל|גרם|יח\')\s*\)', '', name, flags=re.IGNORECASE).strip()

        # Clean up remaining hyphens or separators at the beginning of the name
        name = re.sub(r'^\s*-\s*', '', name).strip()
        
        # Final cleanup for whitespace
        name = ' '.join(name.split())

        product_data['name'] = name
        product_data['brand'] = brand
        
        return product_data

    def _process_product(self, product_data):
        barcode = product_data.get('barcode')

        # Check for duplicates
        if barcode in self.seen_barcodes:
            self.duplicate_count += 1
            logger.debug(f"    ⏭️  Skipping duplicate barcode: {barcode}")
            return False

        # Mark as seen
        self.seen_barcodes.add(barcode)

        # Clean the product data before processing
        cleaned_data = self._clean_product_data(product_data)

        if self.dry_run:
            self.scraped_data_collection.append(cleaned_data)
            self.total_products_processed += 1
            return True
        else:
            try:
                upsert_query = """
                    INSERT INTO canonical_products (barcode, name, brand, image_url, category, description, source_retailer_id, last_scraped_at)
                    VALUES (%(barcode)s, %(name)s, %(brand)s, %(image_url)s, %(category)s, %(description)s, %(source_retailer_id)s, NOW())
                    ON CONFLICT (barcode) DO UPDATE SET
                        name = EXCLUDED.name, brand = EXCLUDED.brand, image_url = EXCLUDED.image_url,
                        category = EXCLUDED.category, source_retailer_id = EXCLUDED.source_retailer_id, last_scraped_at = NOW();
                """
                self.cursor.execute(upsert_query, cleaned_data)
                self.total_products_processed += 1
                return True
            except Exception as e:
                logger.error(f"    ❌ Database error for barcode {cleaned_data.get('barcode')}: {e}")
                self.conn.rollback()
                return False

    def scrape(self):
        logger.info(f"Starting Super-Pharm scraper... Mode: {'DRY RUN' if self.dry_run else 'DATABASE'}")
        if not self.dry_run: self._connect_db()
        if not self.dry_run and not self.conn: return

        checkpoint_loaded = self.load_checkpoint()
        driver = self._setup_driver()
        if not driver: return

        try:
            # Load pharmacy-relevant categories
            if not self.categories:
                self._get_pharmacy_categories()
                self.save_checkpoint()

            # Filter out completed categories
            categories_to_scrape = [cat for cat in self.categories if cat['name'] not in self.completed_categories]
            logger.info(f"📋 Categories to scrape: {len(categories_to_scrape)} (of {len(self.categories)} total)")

            # In test mode, limit to 1 small category for data cleaning verification
            if self.test_mode:
                categories_to_scrape = categories_to_scrape[:1]
                logger.info(f"🧪 TEST MODE: Limited to first {len(categories_to_scrape)} category with data cleaning verification")

            # --- FINAL PAGINATION LOOP ---
            # Production-ready loop with staleness check to prevent race conditions

            for cat_idx, category in enumerate(categories_to_scrape, 1):
                page_num = 1
                logger.info(f"\n{'='*60}\n🔍 Category {cat_idx}/{len(categories_to_scrape)}: {category['name']}\n{'='*60}")

                current_url = category['url']
                category_products = []
                max_pages = 20  # Allow up to 20 pages for all categories

                while current_url and page_num <= max_pages:
                    logger.info(f"   📄 Navigating to page {page_num}: {current_url}")
                    driver.get(current_url)

                    try:
                        # Wait for the product grid to be present on the current page
                        WebDriverWait(driver, 20).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "div.add-to-basket[data-ean]"))
                        )
                        logger.info("   ✅ Page loaded and product grid is present.")

                        # Get a reference to the first product on the CURRENT page (for staleness check)
                        first_product_on_page = driver.find_element(By.CSS_SELECTOR, "div.add-to-basket[data-ean]")

                    except TimeoutException:
                        logger.warning("   ⚠️ No products found on this page or page failed to load. Ending category.")
                        break

                    # Extract products from the current page
                    products_on_page = self._extract_products_from_page(driver, category['name'])
                    logger.info(f"   ✅ Extracted {len(products_on_page)} products from page {page_num}.")

                    # Process each product (deduplication happens here)
                    unique_products = 0
                    for product in products_on_page:
                        if self._process_product(product):
                            unique_products += 1
                            category_products.append(product)

                    logger.info(f"   📦 Added {unique_products} unique products (skipped {len(products_on_page) - unique_products} duplicates)")

                    # Find the next page link to determine the next URL
                    try:
                        # Look for the hidden next link element
                        next_page_element = driver.find_element(By.CSS_SELECTOR, "a#nextHiddenLink[href]")
                        next_url = next_page_element.get_attribute('href')

                        if next_url and next_url != current_url:
                            logger.info(f"   🔄 Found next page link: {next_url}")

                            # Navigate to next page
                            driver.get(next_url)

                            # --- THE FIX: Wait for old content to disappear ---
                            try:
                                logger.info("   ⏳ Waiting for old page content to disappear...")
                                WebDriverWait(driver, 15).until(
                                    EC.staleness_of(first_product_on_page)
                                )
                                logger.info("   ✅ Old content cleared, ready for new page")
                            except:
                                logger.info("   ⚠️ Staleness check timed out, proceeding anyway")

                            current_url = next_url
                            page_num += 1
                        else:
                            logger.info("   ✅ No valid next page found. Reached the last page.")
                            break

                    except NoSuchElementException:
                        logger.info("   ✅ No next page link found. Reached the last page for this category.")
                        break

                # Category complete - show statistics
                logger.info(f"\n   📊 Category '{category['name']}' complete:")
                logger.info(f"      - Pages scraped: {page_num}")
                logger.info(f"      - Unique products: {len(category_products)}")
                logger.info(f"      - Total processed: {self.total_products_processed}")

                # Commit and save checkpoint after each category
                if not self.dry_run and self.conn:
                    self.conn.commit()
                    logger.info(f"   💾 Committed to database")

                self.save_checkpoint(category['name'])
        except Exception as e:
            logger.error(f"❌ Error during scraping: {e}")
        finally:
            if driver:
                try:
                    driver.quit()
                    logger.info("🌐 Browser closed")
                except:
                    pass

            # Print final statistics
            logger.info(f"\n{'='*60}")
            logger.info(f"📊 FINAL STATISTICS")
            logger.info(f"{'='*60}")
            logger.info(f"  📁 Categories processed: {len(self.completed_categories)}/{len(self.categories)}")
            logger.info(f"  ✅ Unique products: {self.total_products_processed}")
            logger.info(f"  ⏭️  Duplicates skipped: {self.duplicate_count}")
            logger.info(f"  📈 Total items processed: {self.total_products_processed + self.duplicate_count}")
            logger.info(f"{'='*60}")

            if self.dry_run:
                self._save_dry_run_file()
            else:
                self._close_db()

            # Clean up checkpoint if all categories were processed
            if len(self.completed_categories) == len(self.categories):
                if os.path.exists(CHECKPOINT_FILE):
                    os.remove(CHECKPOINT_FILE)
                    logger.info("🧹 Checkpoint file removed (all categories completed)")

    def _save_dry_run_file(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"super_pharm_dry_run_{timestamp}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.scraped_data_collection, f, ensure_ascii=False, indent=4)
        logger.info(f"\n✅ Dry run complete. {len(self.scraped_data_collection)} products saved to '{filename}'")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Super-Pharm website for product catalog")
    parser.add_argument("--dry-run", action="store_true", help="Output to JSON file instead of database")
    parser.add_argument("--no-headless", action="store_true", help="Run browser in visible mode")
    parser.add_argument("--test-mode", action="store_true", help="Scrape only a few items/categories for testing")
    parser.add_argument("--resume", action="store_true", help="Resume from the last completed category")
    args = parser.parse_args()

    scraper = SuperPharmScraper(dry_run=args.dry_run, headless=not args.no_headless, test_mode=args.test_mode, resume=args.resume)
    scraper.scrape()