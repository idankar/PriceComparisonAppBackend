#!/usr/bin/env python3
"""
Good Pharm Website Diagnostics Script

This script performs diagnostic tests on the Good Pharm website to understand:
1. Basic anti-bot measures and response types
2. JavaScript rendering requirements
3. Product container structure and selectors
4. Barcode storage methods
5. Pagination mechanisms

Target URL: https://goodpharm.co.il/shop?wpf_filter_cat_0=44
"""

import requests
import time
import logging
from datetime import datetime
import json
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.options import Options

# Setup logging
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('GoodPharmDiagnostics')
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

file_handler = logging.FileHandler('good_pharm_diagnostics.log', mode='w')
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)

class GoodPharmDiagnostics:
    """Diagnostic tool for Good Pharm website structure analysis"""

    def __init__(self):
        self.target_url = "https://goodpharm.co.il/shop?wpf_filter_cat_0=44"
        self.results = {
            "timestamp": datetime.now().isoformat(),
            "target_url": self.target_url,
            "tests": {}
        }

    def test_simple_requests(self):
        """Test 1: Simple requests call to detect basic anti-bot measures"""
        logger.info("="*60)
        logger.info("TEST 1: Simple Requests Analysis")
        logger.info("="*60)

        test_result = {
            "test_name": "simple_requests",
            "success": False,
            "response_code": None,
            "content_type": None,
            "content_length": None,
            "has_products": False,
            "response_type": "unknown",
            "notes": []
        }

        try:
            # Use browser-like headers similar to existing scrapers
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'he-IL,he;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }

            logger.info(f"Making request to: {self.target_url}")
            response = requests.get(self.target_url, headers=headers, timeout=30)

            test_result["response_code"] = response.status_code
            test_result["content_type"] = response.headers.get('content-type', 'unknown')
            test_result["content_length"] = len(response.text)

            logger.info(f"Response Code: {response.status_code}")
            logger.info(f"Content Type: {test_result['content_type']}")
            logger.info(f"Content Length: {test_result['content_length']} characters")

            if response.status_code == 200:
                content = response.text.lower()

                # Analyze response type
                if 'application/json' in test_result['content_type']:
                    test_result["response_type"] = "json_response"
                    test_result["notes"].append("Site returned JSON response")
                elif 'blocked' in content or 'captcha' in content or 'access denied' in content:
                    test_result["response_type"] = "blocked_or_captcha"
                    test_result["notes"].append("Possible blocking or CAPTCHA detected")
                elif len(content) < 1000:
                    test_result["response_type"] = "minimal_content"
                    test_result["notes"].append("Very minimal content - possible decoy page")
                elif 'javascript' in content and 'noscript' in content:
                    test_result["response_type"] = "javascript_required"
                    test_result["notes"].append("Heavy JavaScript rendering detected")
                else:
                    test_result["response_type"] = "clean_html"
                    test_result["notes"].append("Clean HTML response received")

                # Look for product indicators
                product_indicators = [
                    'product', 'barcode', 'ean', 'price', '₪', 'add to cart',
                    'wp-block', 'woocommerce', 'shop', 'good pharm', 'goodpharm'
                ]

                found_indicators = []
                for indicator in product_indicators:
                    if indicator in content:
                        found_indicators.append(indicator)
                        test_result["has_products"] = True

                if found_indicators:
                    logger.info(f"Product indicators found: {found_indicators}")
                    test_result["notes"].append(f"Found indicators: {', '.join(found_indicators)}")
                else:
                    logger.info("No clear product indicators found")
                    test_result["notes"].append("No product indicators detected")

                test_result["success"] = True
                logger.info("✅ Simple requests test completed successfully")

            else:
                test_result["notes"].append(f"Non-200 status code: {response.status_code}")
                logger.warning(f"⚠️ Non-200 response: {response.status_code}")

        except requests.exceptions.Timeout:
            test_result["notes"].append("Request timeout")
            logger.error("❌ Request timeout")
        except requests.exceptions.RequestException as e:
            test_result["notes"].append(f"Request error: {str(e)}")
            logger.error(f"❌ Request error: {e}")
        except Exception as e:
            test_result["notes"].append(f"Unexpected error: {str(e)}")
            logger.error(f"❌ Unexpected error: {e}")

        self.results["tests"]["simple_requests"] = test_result
        return test_result["success"]

    def test_headless_browser(self):
        """Test 2: Headless browser inspection for JavaScript rendering and selectors"""
        logger.info("="*60)
        logger.info("TEST 2: Headless Browser Analysis")
        logger.info("="*60)

        test_result = {
            "test_name": "headless_browser",
            "success": False,
            "page_loaded": False,
            "products_found": False,
            "product_count": 0,
            "product_selector": None,
            "barcode_storage": None,
            "pagination_type": None,
            "key_selectors": {},
            "sample_products": [],
            "notes": []
        }

        driver = None
        try:
            # Try undetected-chromedriver first, fallback to regular Selenium
            logger.info("🚀 Attempting undetected-chromedriver...")

            try:
                options = uc.ChromeOptions()
                options.add_argument("--headless=new")
                options.add_argument("--start-maximized")
                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")

                driver = uc.Chrome(options=options, use_subprocess=False)
                logger.info("✅ Successfully initialized undetected-chromedriver")

            except Exception as e:
                logger.warning(f"⚠️ Undetected-chromedriver failed: {e}")
                logger.info("🔄 Falling back to regular Selenium WebDriver...")

                from selenium import webdriver
                from selenium.webdriver.chrome.service import Service
                from webdriver_manager.chrome import ChromeDriverManager

                regular_options = webdriver.ChromeOptions()
                regular_options.add_argument("--headless=new")
                regular_options.add_argument("--start-maximized")
                regular_options.add_argument("--no-sandbox")
                regular_options.add_argument("--disable-dev-shm-usage")
                regular_options.add_argument("--disable-blink-features=AutomationControlled")
                regular_options.add_experimental_option("excludeSwitches", ["enable-automation"])
                regular_options.add_experimental_option('useAutomationExtension', False)
                regular_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=regular_options)
                driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                logger.info("✅ Successfully initialized regular Selenium WebDriver")

            # Navigate to the page
            logger.info(f"Navigating to: {self.target_url}")
            driver.get(self.target_url)

            # Wait for initial page load
            time.sleep(5)

            test_result["page_loaded"] = True
            logger.info(f"📍 Page title: {driver.title}")
            logger.info(f"📍 Current URL: {driver.current_url}")

            # Wait for content to load (give it more time for JavaScript)
            logger.info("⏳ Waiting for content to load...")
            time.sleep(10)

            # Try multiple product selectors based on common patterns
            product_selectors = [
                # WooCommerce patterns
                ".product",
                ".wc-block-grid__product",
                ".wp-block-post",
                ".product-item",
                ".shop-item",

                # Generic product patterns
                "div[data-product-id]",
                "div[data-ean]",
                "div[data-barcode]",
                "[class*='product']",

                # Good Pharm specific
                ".goodpharm-product",
                ".good-pharm-item",

                # Common e-commerce patterns
                ".item",
                ".tile",
                "article",
                ".card",

                # WordPress block patterns
                ".wp-block",
                ".post"
            ]

            best_selector = None
            max_products = 0

            for selector in product_selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    count = len(elements)

                    if count > max_products:
                        max_products = count
                        best_selector = selector

                    logger.info(f"  Selector '{selector}': {count} elements found")

                    if count > 0:
                        test_result["key_selectors"][selector] = count

                except Exception as e:
                    logger.debug(f"  Selector '{selector}' failed: {e}")

            if best_selector and max_products > 0:
                test_result["products_found"] = True
                test_result["product_count"] = max_products
                test_result["product_selector"] = best_selector

                logger.info(f"✅ Best product selector: '{best_selector}' ({max_products} products)")

                # Analyze product structure using the best selector
                self._analyze_product_structure(driver, best_selector, test_result)

            else:
                logger.warning("⚠️ No product containers found")
                test_result["notes"].append("No product containers detected")

            # Check for pagination
            self._analyze_pagination(driver, test_result)

            # Save page source for manual inspection if needed
            try:
                with open('good_pharm_page_source.html', 'w', encoding='utf-8') as f:
                    f.write(driver.page_source)
                logger.info("📄 Page source saved to 'good_pharm_page_source.html'")
            except Exception as e:
                logger.warning(f"Could not save page source: {e}")

            test_result["success"] = True
            logger.info("✅ Headless browser test completed successfully")

        except Exception as e:
            test_result["notes"].append(f"Browser test error: {str(e)}")
            logger.error(f"❌ Browser test error: {e}")

        finally:
            if driver:
                try:
                    driver.quit()
                    logger.info("🌐 Browser closed")
                except:
                    pass

        self.results["tests"]["headless_browser"] = test_result
        return test_result["success"]

    def _analyze_product_structure(self, driver, selector, test_result):
        """Analyze the structure of product elements"""
        logger.info(f"🔍 Analyzing product structure with selector: {selector}")

        try:
            products = driver.find_elements(By.CSS_SELECTOR, selector)[:5]  # Analyze first 5 products

            for i, product in enumerate(products):
                sample_product = {
                    "index": i,
                    "outer_html_preview": product.get_attribute('outerHTML')[:200] + "..." if len(product.get_attribute('outerHTML')) > 200 else product.get_attribute('outerHTML'),
                    "text_content": product.text.strip()[:100] + "..." if len(product.text.strip()) > 100 else product.text.strip(),
                    "attributes": {}
                }

                # Check common attributes
                common_attrs = ['data-product-id', 'data-ean', 'data-barcode', 'data-id', 'id', 'class']
                for attr in common_attrs:
                    value = product.get_attribute(attr)
                    if value:
                        sample_product["attributes"][attr] = value

                # Look for barcode storage patterns
                barcode_patterns = [
                    ('data-ean', 'EAN in data-ean attribute'),
                    ('data-barcode', 'Barcode in data-barcode attribute'),
                    ('data-product-id', 'Product ID in data-product-id attribute'),
                ]

                for attr, description in barcode_patterns:
                    if product.get_attribute(attr):
                        if not test_result["barcode_storage"]:
                            test_result["barcode_storage"] = description
                            logger.info(f"📊 Barcode storage found: {description}")
                        break

                # Look for images
                try:
                    img = product.find_element(By.TAG_NAME, "img")
                    sample_product["has_image"] = True
                    sample_product["image_src"] = img.get_attribute('src')
                except:
                    sample_product["has_image"] = False

                # Look for links
                try:
                    link = product.find_element(By.TAG_NAME, "a")
                    sample_product["has_link"] = True
                    sample_product["link_href"] = link.get_attribute('href')
                except:
                    sample_product["has_link"] = False

                test_result["sample_products"].append(sample_product)

                logger.info(f"  Product {i+1}: {sample_product['text_content'][:50]}...")

        except Exception as e:
            logger.warning(f"Product structure analysis failed: {e}")
            test_result["notes"].append(f"Product structure analysis failed: {str(e)}")

    def _analyze_pagination(self, driver, test_result):
        """Analyze pagination mechanisms"""
        logger.info("🔍 Analyzing pagination...")

        pagination_selectors = [
            # Standard pagination
            ".pagination",
            ".page-numbers",
            ".nav-links",
            "[class*='pagination']",
            "[class*='paging']",

            # Next/Previous buttons
            ".next",
            ".previous",
            "[class*='next']",
            "[class*='prev']",

            # Load more buttons
            ".load-more",
            ".show-more",
            "[class*='load-more']",

            # WooCommerce pagination
            ".woocommerce-pagination",
        ]

        pagination_found = False

        for selector in pagination_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    pagination_found = True
                    logger.info(f"  Pagination selector '{selector}': {len(elements)} elements")

                    # Check if it's a "load more" pattern vs standard pagination
                    for element in elements:
                        text = element.text.lower()
                        if 'load more' in text or 'show more' in text:
                            test_result["pagination_type"] = "load_more_button"
                        elif 'next' in text or 'previous' in text or any(char.isdigit() for char in text):
                            test_result["pagination_type"] = "standard_pagination"

            except Exception as e:
                logger.debug(f"Pagination selector '{selector}' failed: {e}")

        if not pagination_found:
            # Check for infinite scroll indicators
            try:
                # Scroll down to see if more content loads
                initial_height = driver.execute_script("return document.body.scrollHeight")
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(3)
                new_height = driver.execute_script("return document.body.scrollHeight")

                if new_height > initial_height:
                    test_result["pagination_type"] = "infinite_scroll"
                    logger.info("  Detected infinite scroll behavior")
                else:
                    test_result["pagination_type"] = "single_page_or_unknown"
                    logger.info("  No pagination detected - single page or unknown mechanism")

            except Exception as e:
                test_result["pagination_type"] = "unknown"
                logger.warning(f"Pagination analysis failed: {e}")

        logger.info(f"📊 Pagination type: {test_result.get('pagination_type', 'unknown')}")

    def run_diagnostics(self):
        """Run all diagnostic tests"""
        logger.info("🔬 Starting Good Pharm Website Diagnostics")
        logger.info(f"Target URL: {self.target_url}")
        logger.info("="*60)

        # Test 1: Simple requests
        requests_success = self.test_simple_requests()

        # Test 2: Headless browser
        browser_success = self.test_headless_browser()

        # Generate final report
        self._generate_report()

        return requests_success and browser_success

    def _generate_report(self):
        """Generate comprehensive diagnostic report"""
        logger.info("="*60)
        logger.info("📋 DIAGNOSTIC REPORT")
        logger.info("="*60)

        # Save detailed results to JSON
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"good_pharm_diagnostics_{timestamp}.json"

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=4)

        logger.info(f"📄 Detailed results saved to: {filename}")

        # Print summary
        requests_test = self.results["tests"].get("simple_requests", {})
        browser_test = self.results["tests"].get("headless_browser", {})

        logger.info("\n🔍 SUMMARY:")
        logger.info(f"  Target URL: {self.target_url}")

        logger.info(f"\n📡 Simple Requests Test:")
        if requests_test.get("success"):
            logger.info(f"  ✅ Status: {requests_test.get('response_code')}")
            logger.info(f"  📝 Response Type: {requests_test.get('response_type')}")
            logger.info(f"  📦 Has Products: {requests_test.get('has_products')}")
        else:
            logger.info(f"  ❌ Failed")

        logger.info(f"\n🌐 Browser Test:")
        if browser_test.get("success"):
            logger.info(f"  ✅ Page Loaded: {browser_test.get('page_loaded')}")
            logger.info(f"  📦 Products Found: {browser_test.get('product_count')} products")
            logger.info(f"  🎯 Best Selector: {browser_test.get('product_selector')}")
            logger.info(f"  📊 Barcode Storage: {browser_test.get('barcode_storage', 'Not detected')}")
            logger.info(f"  📄 Pagination: {browser_test.get('pagination_type', 'Unknown')}")
        else:
            logger.info(f"  ❌ Failed")

        logger.info("\n🏗️  RECOMMENDED SCRAPER ARCHITECTURE:")

        if browser_test.get("products_found"):
            logger.info("  ✅ JavaScript rendering REQUIRED - use undetected-chromedriver")
            logger.info(f"  🎯 Primary selector: {browser_test.get('product_selector')}")

            if browser_test.get("barcode_storage"):
                logger.info(f"  📊 Barcode extraction: {browser_test.get('barcode_storage')}")
            else:
                logger.info("  ⚠️  Barcode extraction method needs investigation")

            pagination = browser_test.get("pagination_type")
            if pagination == "infinite_scroll":
                logger.info("  📜 Use infinite scroll handling (like Be Pharm scraper)")
            elif pagination == "standard_pagination":
                logger.info("  📄 Use standard pagination handling (like Super Pharm scraper)")
            elif pagination == "load_more_button":
                logger.info("  🔄 Use load-more button clicking mechanism")
            else:
                logger.info("  📄 Single page or unknown pagination - investigate manually")

        else:
            logger.info("  ⚠️  No products detected - manual investigation required")
            if requests_test.get("response_type") == "blocked_or_captcha":
                logger.info("  🛡️  Site may have anti-bot protection")
            elif requests_test.get("response_type") == "javascript_required":
                logger.info("  🔧 Heavy JavaScript rendering detected")

        logger.info("="*60)

if __name__ == "__main__":
    diagnostics = GoodPharmDiagnostics()
    success = diagnostics.run_diagnostics()

    if success:
        logger.info("✅ All diagnostic tests completed successfully")
    else:
        logger.warning("⚠️ Some diagnostic tests failed - check logs for details")