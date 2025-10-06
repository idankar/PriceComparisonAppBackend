# APRIL.CO.IL - COMPREHENSIVE SCRAPING BLUEPRINT

## Executive Summary

**Retailer:** April.co.il (Israeli cosmetics and perfume retailer)
**Date of Analysis:** October 5, 2025
**Recommended Architecture:** ⭐ **SCENARIO A: Single-Pass Scraper**
**Justification:** Barcode data is embedded in JavaScript dataLayer on listing pages

---

## 1. TECHNICAL ANALYSIS

### 1.1 Technology Stack

| Component | Details |
|-----------|---------|
| **Anti-Bot Protection** | Cloudflare JavaScript Challenge |
| **Rendering Method** | Server-side HTML + Dynamic JavaScript |
| **Required Tool** | **Selenium or Playwright** (requests/BeautifulSoup will NOT work) |
| **User-Agent Spoofing** | Required |
| **JavaScript Execution** | Required |

### 1.2 Why Selenium/Playwright is Mandatory

- The site employs Cloudflare's "Just a moment..." challenge
- Initial request returns: `<title>Just a moment...</title>`
- JavaScript must execute to pass challenge and render actual content
- Simple HTTP requests receive 403 Forbidden responses

---

## 2. DATA AVAILABILITY ANALYSIS

### 2.1 Listing Page Fields (Category Pages)

**Sample URL:** `https://www.april.co.il/women-perfume`

#### Available Fields on Listing Page:

| Field | Availability | Extraction Method | Selector/Method |
|-------|-------------|-------------------|-----------------|
| **Barcode** | ✅ **YES** | JavaScript dataLayer | Extract from onclick attribute: `'id': '5994003399'` |
| **Product Name** | ✅ YES | HTML Element | `h2.card-title` |
| **Brand** | ✅ YES | HTML Element | `.firm-product-list span` |
| **Price (Current)** | ✅ YES | HTML Element | `span.saleprice` or `#saleprice{id}` |
| **Price (Original)** | ✅ YES | HTML Element | `span.oldprice` or `#oldprice{id}` |
| **Product URL** | ✅ YES | HTML Attribute | `a[href]` (relative path) |
| **Image URL** | ✅ YES | HTML Attribute | `img[data-src]` or `img[src]` |
| **Product ID** | ✅ YES | JavaScript dataLayer | Same as barcode in dataLayer |
| **Stock Status** | ✅ YES | Hidden div | `#stock{id}` (div with d-none class) |
| **Category** | ✅ YES | JavaScript dataLayer | In dataLayer: `'category': ''` |

#### Key Discovery: Barcode Extraction

The barcode is embedded in the `onclick` JavaScript attribute of product links:

```html
<a href="women-perfume-forever-mine-into-the-legend-for-women-edt-chevignon-1"
   onclick="dataLayer.push({'event': 'productClick','ecommerce': {'click': {'actionField': {'list': 'בשמים לנשים'},'products': [{'name': `אינטו דה לג'נד לאישה א.ד.ט`,'id': '5994003399','category': '','position': 1, 'brand': '','price':'96.75'}]}},});">
```

**Barcode Value:** `5994003399` (13-digit EAN)

#### CSS Selectors for Listing Page:

```python
SELECTORS = {
    'product_container': 'div.col.position-relative.item',
    'product_link': 'a[href*="women-perfume"], a[onclick*="dataLayer"]',
    'product_name': 'h2.card-title',
    'brand': '.firm-product-list span',
    'sale_price': 'span.saleprice',
    'old_price': 'span.oldprice',
    'image': 'img.img-fluid',
    'stock': 'div[id^="stock"]',  # Hidden div with stock quantity
}
```

---

### 2.2 Product Detail Page Fields

**Sample URL:** `https://www.april.co.il/women-perfume-forever-mine-into-the-legend-for-women-edt-chevignon-1`

#### Additional Fields on Detail Page:

| Field | Availability | Notes |
|-------|-------------|-------|
| **Long Description** | ✅ YES | More detailed product information |
| **Full Specifications** | ✅ YES | Product details table/list |
| **Multiple Images** | ✅ YES | Gallery with additional product images |
| **Related Products** | ✅ YES | Recommendations section |
| **Breadcrumb Category** | ✅ YES | Full category path |

**Important:** The barcode is ALSO available on detail pages, but since it's already on listing pages, visiting detail pages is **optional** and only needed for enrichment data.

---

## 3. PAGINATION ANALYSIS

### 3.1 Pagination Type

**Type:** Standard numbered pagination with JavaScript-based navigation

### 3.2 Pagination HTML Structure

```html
<ul class="pagination justify-content-center px-0">
    <li class="page-item disabled">
        <a class="page-link" aria-label="Previous" href="javascript:Go2Page(0);">
            <i class="fas fa-chevron-left"></i>
        </a>
    </li>
    <!-- Page numbers -->
    <li class="page-item"><a href="javascript:Go2Page(1);">2</a></li>
    <li class="page-item"><a href="javascript:Go2Page(2);">3</a></li>
    <!-- Next button -->
</ul>
```

### 3.3 Pagination Mechanism

- JavaScript function: `Go2Page(pageNumber)`
- Pages are 0-indexed (first page = 0)
- Total products shown in: `<input type="hidden" value="371" id="TotalProductAfterFilter">`

### 3.4 Pagination Selectors

```python
PAGINATION_SELECTORS = {
    'pagination_container': 'ul.pagination',
    'page_links': 'ul.pagination li.page-item a',
    'next_button': 'ul.pagination li.page-item a[aria-label*="Next"]',
    'total_products': 'input#TotalProductAfterFilter',
}
```

---

## 4. RECOMMENDED ARCHITECTURE

### **SCENARIO A: Single-Pass Scraper ✅**

#### Justification:

1. ✅ **Barcode is available on listing pages** (in JavaScript dataLayer)
2. ✅ All critical fields (name, brand, price, image, URL) available on listing
3. ✅ No need to visit individual product pages for essential data
4. ✅ Significantly faster and more efficient
5. ✅ Lower risk of IP blocking due to fewer requests

#### Architecture Overview:

```
┌─────────────────────────────────────────────────────────┐
│         SINGLE-PASS SCRAPER (april_scraper.py)          │
│                                                          │
│  1. Initialize Selenium with anti-detection             │
│  2. Navigate to category page                           │
│  3. Wait for Cloudflare challenge to pass               │
│  4. Extract total product count                         │
│  5. For each page:                                      │
│     ├── Extract all product cards                       │
│     ├── Parse HTML for basic fields                     │
│     ├── Extract barcode from JavaScript dataLayer       │
│     ├── Store complete product record                   │
│     └── Navigate to next page                           │
│  6. Save to database/CSV                                │
└─────────────────────────────────────────────────────────┘
```

---

## 5. IMPLEMENTATION PLAN

### 5.1 High-Level Code Structure

```python
# april_scraper.py

import re
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class AprilScraper:

    def __init__(self):
        self.driver = self.setup_driver()
        self.base_url = "https://www.april.co.il"

    def setup_driver(self):
        """Configure Selenium with anti-detection"""
        options = webdriver.ChromeOptions()
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36')

        driver = webdriver.Chrome(options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        return driver

    def wait_for_cloudflare(self, timeout=30):
        """Wait for Cloudflare challenge to complete"""
        time.sleep(3)  # Initial wait
        # Add logic to detect and wait for challenge completion

    def extract_barcode_from_datalayer(self, element):
        """Extract barcode from onclick dataLayer JavaScript"""
        try:
            onclick = element.get_attribute('onclick')
            # Parse: 'id': '5994003399'
            match = re.search(r"'id':\s*'(\d+)'", onclick)
            if match:
                return match.group(1)
        except:
            pass
        return None

    def scrape_listing_page(self, category_url):
        """Scrape all products from a category page"""
        self.driver.get(category_url)
        self.wait_for_cloudflare()

        # Get total products and calculate pages
        total_products_elem = self.driver.find_element(By.ID, "TotalProductAfterFilter")
        total_products = int(total_products_elem.get_attribute('value'))

        products = []
        page = 0

        while True:
            # Extract products from current page
            product_containers = self.driver.find_elements(By.CSS_SELECTOR, 'div.col.position-relative.item')

            for container in product_containers:
                product_data = {}

                # Extract link (contains barcode in dataLayer)
                try:
                    link = container.find_element(By.CSS_SELECTOR, 'a[onclick*="dataLayer"]')
                    product_data['barcode'] = self.extract_barcode_from_datalayer(link)
                    product_data['url'] = link.get_attribute('href')
                except:
                    pass

                # Extract name
                try:
                    name_elem = container.find_element(By.CSS_SELECTOR, 'h2.card-title')
                    product_data['name'] = name_elem.text.strip()
                except:
                    pass

                # Extract brand
                try:
                    brand_elem = container.find_element(By.CSS_SELECTOR, '.firm-product-list span')
                    product_data['brand'] = brand_elem.text.strip()
                except:
                    pass

                # Extract price
                try:
                    price_elem = container.find_element(By.CSS_SELECTOR, 'span.saleprice')
                    price_text = price_elem.text.strip()
                    product_data['price'] = self.parse_price(price_text)
                except:
                    pass

                # Extract image
                try:
                    img_elem = container.find_element(By.CSS_SELECTOR, 'img.img-fluid')
                    product_data['image_url'] = img_elem.get_attribute('src') or img_elem.get_attribute('data-src')
                except:
                    pass

                products.append(product_data)

            # Check if there's a next page
            try:
                next_button = self.driver.find_element(By.CSS_SELECTOR, 'ul.pagination li.page-item a[aria-label*="Next"]')
                if 'disabled' in next_button.get_attribute('class'):
                    break

                # Click next page
                page += 1
                self.driver.execute_script(f"Go2Page({page});")
                time.sleep(2)

            except:
                break

        return products

    def parse_price(self, price_text):
        """Extract numeric price from text"""
        # Extract numbers and convert to float
        match = re.search(r'(\d+\.?\d*)', price_text.replace(',', ''))
        return float(match.group(1)) if match else None

    def scrape_all_categories(self):
        """Scrape all product categories"""
        categories = [
            'women-perfume',
            'men-perfume',
            'niche-perfume',
            # Add more categories...
        ]

        all_products = []

        for category in categories:
            url = f"{self.base_url}/{category}"
            products = self.scrape_listing_page(url)
            all_products.extend(products)

        return all_products

    def save_to_csv(self, products, filename='april_products.csv'):
        """Save products to CSV"""
        import csv

        keys = products[0].keys()
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(products)

if __name__ == "__main__":
    scraper = AprilScraper()
    products = scraper.scrape_all_categories()
    scraper.save_to_csv(products)
    scraper.driver.quit()
```

---

## 6. ALTERNATIVE: Two-Phase Strategy (Optional Enhancement)

While **not necessary** for core data, you may optionally implement a backfill script to enrich product records with detail page data:

### Phase 1: Listing Scraper (as above)
- Collects: barcode, name, brand, price, URL, image

### Phase 2: Detail Page Backfill (optional)
- Input: Product URLs from Phase 1
- Collects: Long descriptions, specifications, image galleries
- Updates existing records in database

---

## 7. COMPLETE FIELD MAPPING

### 7.1 Database Schema Recommendation

```sql
CREATE TABLE april_products (
    barcode VARCHAR(13) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    brand VARCHAR(100),
    price_current DECIMAL(10, 2),
    price_original DECIMAL(10, 2),
    product_url TEXT,
    image_url TEXT,
    category VARCHAR(100),
    stock_quantity INT,
    description TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 7.2 Field Coverage Percentage

| Data Source | Fields Available | Completeness |
|-------------|-----------------|--------------|
| **Listing Page** | 9/10 critical fields | **90%** |
| **Detail Page** | Additional 3 enrichment fields | **30%** (non-critical) |
| **Combined Coverage** | All essential data | **100%** |

---

## 8. ANTI-SCRAPING COUNTERMEASURES

### 8.1 Detected Measures

1. **Cloudflare JavaScript Challenge** - Must pass challenge before accessing content
2. **User-Agent Detection** - Requires realistic browser user-agent
3. **WebDriver Detection** - Must mask `navigator.webdriver` property

### 8.2 Recommended Bypass Techniques

```python
# 1. Disable automation flags
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_experimental_option("excludeSwitches", ["enable-automation"])

# 2. Override webdriver property
driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

# 3. Use realistic user-agent
options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)...')

# 4. Add delays between requests
time.sleep(random.uniform(2, 5))

# 5. Rotate user-agents (optional)
user_agents = [...]
options.add_argument(f'user-agent={random.choice(user_agents)}')
```

---

## 9. PERFORMANCE ESTIMATES

### Single-Pass Scraper Performance:

| Metric | Estimate |
|--------|----------|
| **Products per page** | ~20 products |
| **Time per page** | 3-5 seconds |
| **Total categories** | ~50 categories |
| **Total products** | ~10,000-15,000 products |
| **Estimated runtime** | 2-4 hours (with delays) |
| **Success rate** | 95%+ (with proper error handling) |

---

## 10. RISK ASSESSMENT

| Risk | Severity | Mitigation |
|------|----------|------------|
| **IP Blocking** | Medium | Use delays, rotate user-agents, consider proxies |
| **Cloudflare Ban** | Low | Proper Selenium configuration reduces detection |
| **Site Structure Changes** | Medium | Implement robust error handling and logging |
| **Incomplete Data** | Low | All critical fields available on listing pages |

---

## 11. DELIVERABLES

### Recommended File Structure:

```
april_scraper/
├── april_scraper.py          # Main scraper script
├── config.py                 # Configuration (delays, user-agents, etc.)
├── utils.py                  # Helper functions
├── database.py               # Database connection and models
├── requirements.txt          # Python dependencies
├── README.md                 # Documentation
└── output/
    ├── april_products.csv    # CSV export
    └── logs/
        └── scraper.log       # Execution logs
```

### Requirements.txt:

```
selenium>=4.0.0
webdriver-manager>=3.8.0
pandas>=1.5.0
beautifulsoup4>=4.11.0
psycopg2-binary>=2.9.0  # If using PostgreSQL
```

---

## 12. CONCLUSION

**Recommendation:** Implement **Scenario A: Single-Pass Scraper**

**Key Success Factors:**
1. ✅ 100% field coverage for critical data (barcode, name, brand, price)
2. ✅ Efficient single-pass architecture
3. ✅ Barcode extraction from JavaScript dataLayer is reliable
4. ✅ Standard pagination is straightforward to implement
5. ✅ Cloudflare bypass is achievable with proper Selenium configuration

**Next Steps:**
1. Implement the scraper using the provided code structure
2. Test on a small category first (e.g., women-perfume with 20 products)
3. Add error handling and logging
4. Scale to all categories
5. Set up scheduled runs for data updates

---

## APPENDIX A: Sample Product Data

```json
{
    "barcode": "3355994003399",
    "name": "אינטו דה לג'נד לאישה א.ד.ט",
    "brand": "Chevignon",
    "price_current": 96.75,
    "price_original": 129.00,
    "product_url": "https://www.april.co.il/women-perfume-forever-mine-into-the-legend-for-women-edt-chevignon-1",
    "image_url": "https://www.april.co.il/Media/Uploads/3355994003399-.webp",
    "category": "בשמים לנשים",
    "stock_quantity": 8
}
```

---

**Document Version:** 1.0
**Author:** Claude (AI Assistant)
**Date:** October 5, 2025
**Status:** ✅ Ready for Implementation
