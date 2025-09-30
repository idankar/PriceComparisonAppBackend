# Database Schema & Canonical Product Matching Guide

## Database Connection
```python
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="price_comparison_app_v2",  # CRITICAL: Use v2, not price_comparison_app
    user="postgres",
    password="***REMOVED***"
)
```

## Dual-Source Data Architecture

### The Two-Layer System

```
┌─────────────────────────────────────────────────────────────┐
│                    LAYER 1: CANONICAL DATA                   │
│                   (Commercial Websites)                      │
│  • Clean product names (marketing-friendly)                  │
│  • High-quality images                                       │
│  • Product descriptions                                      │
│  • Categories and brand information                          │
└─────────────────────────────────────────────────────────────┘
                              ↓
                    [Product Matching Engine]
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                    LAYER 2: PRICE DATA                       │
│                (Transparency Portals)                        │
│  • Complete price coverage (all stores)                      │
│  • Barcodes and item codes                                   │
│  • Promotions and discounts                                  │
│  • Historical price tracking                                 │
└─────────────────────────────────────────────────────────────┘
```

## Core Database Tables

### 1. `products` - The Canonical Product Catalog
```sql
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    canonical_name TEXT NOT NULL,           -- Clean name from commercial site
    brand TEXT,                              -- Brand/manufacturer
    description TEXT,                        -- Full product description
    image_url TEXT,                          -- High-quality image from commercial
    image_source VARCHAR(50),                -- 'commercial' or 'manual'
    attributes JSONB,                        -- Flexible data storage
    embedding TEXT,                          -- Vector for ML matching
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(lower(canonical_name), lower(brand))
);

-- Critical indexes for performance
CREATE INDEX idx_products_canonical_name ON products(lower(canonical_name));
CREATE INDEX idx_products_brand ON products(lower(brand));
CREATE INDEX idx_products_attributes_barcode ON products((attributes->>'barcode'));
```

### 2. `retailer_products` - Links Products to Retailers
```sql
CREATE TABLE retailer_products (
    retailer_product_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(product_id),
    retailer_id INTEGER REFERENCES retailers(retailerid),
    retailer_item_code VARCHAR(100) NOT NULL,  -- From transparency portal
    original_retailer_name TEXT,               -- Raw name from transparency
    -- Note: confidence_score, match_method, last_updated columns removed in current implementation
    UNIQUE(retailer_id, retailer_item_code)
);
```

### 3. `prices` - Store-Level Pricing
```sql
CREATE TABLE prices (
    price_id BIGSERIAL PRIMARY KEY,
    retailer_product_id INTEGER REFERENCES retailer_products(retailer_product_id),
    store_id INTEGER REFERENCES stores(storeid),
    price NUMERIC(10,2) NOT NULL,
    price_timestamp TIMESTAMP NOT NULL,
    scraped_at TIMESTAMP DEFAULT NOW(),
    promotion_id BIGINT REFERENCES promotions(promotion_id),
    UNIQUE(retailer_product_id, store_id, price_timestamp)
);

-- Critical for price lookups
CREATE INDEX idx_prices_timestamp ON prices(price_timestamp DESC);
CREATE INDEX idx_prices_retailer_product ON prices(retailer_product_id);
CREATE INDEX idx_prices_store ON prices(store_id);
```

### 4. `canonical_products` - Master Deduplicated Products
```sql
CREATE TABLE canonical_products (
    canonical_id VARCHAR(50) PRIMARY KEY,      -- Format: 'canon_[barcode]'
    canonical_name TEXT NOT NULL,              -- Best name from commercial
    canonical_brand TEXT,                      -- Standardized brand
    primary_barcode VARCHAR(20) UNIQUE,        -- Primary barcode
    secondary_barcodes TEXT[],                 -- Alternative barcodes
    category VARCHAR(100),                     -- Product category
    image_url TEXT,                            -- Best image available
    attributes JSONB,                          -- All metadata
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_canonical_barcode ON canonical_products(primary_barcode);
```

### 5. `product_matching_queue` - Products Awaiting Matching
```sql
CREATE TABLE product_matching_queue (
    queue_id SERIAL PRIMARY KEY,
    retailer_product_id INTEGER REFERENCES retailer_products(retailer_product_id),
    transparency_name TEXT,                    -- Raw name from portal
    transparency_barcode VARCHAR(20),          -- Barcode if available
    potential_matches JSONB,                   -- Array of possible matches
    status VARCHAR(20) DEFAULT 'pending',      -- pending/matched/failed/manual
    attempted_at TIMESTAMP,
    matched_at TIMESTAMP,
    notes TEXT
);
```

## Canonical Product Selection Algorithm

### Phase 1: Commercial Data Ingestion
```python
def ingest_commercial_product(product_data):
    """
    Ingest product from commercial website (the canonical source)
    """
    # 1. Clean and normalize the product name
    canonical_name = clean_product_name(product_data['name'])

    # 2. Extract or generate canonical ID
    if product_data.get('barcode'):
        canonical_id = f"canon_{product_data['barcode']}"
    else:
        canonical_id = generate_canonical_id(canonical_name, product_data['brand'])

    # 3. Insert into products table
    cursor.execute("""
        INSERT INTO products (
            canonical_name,
            brand,
            description,
            image_url,
            image_source,
            attributes
        ) VALUES (%s, %s, %s, %s, 'commercial', %s)
        ON CONFLICT (lower(canonical_name), lower(brand))
        DO UPDATE SET
            image_url = EXCLUDED.image_url,
            description = EXCLUDED.description,
            updated_at = NOW()
        RETURNING product_id
    """, (
        canonical_name,
        product_data['brand'],
        product_data['description'],
        product_data['image_url'],
        Json({'barcode': product_data.get('barcode')})
    ))

    return cursor.fetchone()[0]
```

### Phase 2: Transparency Data Matching
```python
def match_transparency_to_canonical(transparency_data, retailer_id):
    """
    Match transparency portal data to canonical products
    """
    # Priority 1: Exact barcode match
    if transparency_data.get('barcode'):
        match = find_product_by_barcode(transparency_data['barcode'])
        if match:
            return create_retailer_product_link(
                match['product_id'],
                retailer_id,
                transparency_data,
                confidence=1.0,
                method='barcode'
            )

    # Priority 2: Fuzzy name + brand match
    candidates = find_products_by_fuzzy_match(
        transparency_data['name'],
        transparency_data.get('brand', '')
    )

    if candidates and candidates[0]['score'] > 0.85:
        return create_retailer_product_link(
            candidates[0]['product_id'],
            retailer_id,
            transparency_data,
            confidence=candidates[0]['score'],
            method='fuzzy'
        )

    # Priority 3: ML/Embedding similarity
    if transparency_data.get('name'):
        embedding_matches = find_by_embedding_similarity(
            transparency_data['name']
        )

        if embedding_matches and embedding_matches[0]['score'] > 0.90:
            return create_retailer_product_link(
                embedding_matches[0]['product_id'],
                retailer_id,
                transparency_data,
                confidence=embedding_matches[0]['score'],
                method='ml'
            )

    # Priority 4: Queue for manual review
    add_to_matching_queue(transparency_data, retailer_id, candidates)
    return None
```

### Phase 3: Matching Priority System

```python
class ProductMatcher:
    """
    Comprehensive matching system with fallback strategies
    """

    def match_product(self, transparency_product, retailer_id):
        """
        Match transparency product to canonical product
        Returns: (product_id, confidence, method) or None
        """

        # Level 1: Perfect barcode match (confidence: 1.0)
        if transparency_product.get('barcode'):
            # Clean barcode (remove leading zeros, validate checksum)
            barcode = self.clean_barcode(transparency_product['barcode'])

            result = self.db.query("""
                SELECT product_id
                FROM products
                WHERE attributes->>'barcode' = %s
                LIMIT 1
            """, (barcode,))

            if result:
                return (result['product_id'], 1.0, 'barcode')

        # Level 2: Name + Brand exact match (confidence: 0.95)
        if transparency_product.get('name') and transparency_product.get('brand'):
            result = self.db.query("""
                SELECT product_id
                FROM products
                WHERE lower(canonical_name) = lower(%s)
                  AND lower(brand) = lower(%s)
                LIMIT 1
            """, (
                transparency_product['name'],
                transparency_product['brand']
            ))

            if result:
                return (result['product_id'], 0.95, 'exact_name_brand')

        # Level 3: Fuzzy matching with threshold (confidence: 0.7-0.9)
        fuzzy_matches = self.fuzzy_match(
            transparency_product['name'],
            transparency_product.get('brand')
        )

        if fuzzy_matches:
            best_match = fuzzy_matches[0]
            if best_match['score'] > 0.85:
                return (
                    best_match['product_id'],
                    best_match['score'],
                    'fuzzy'
                )

        # Level 4: ML embedding similarity (confidence: 0.6-0.85)
        if self.ml_enabled:
            embedding_matches = self.embedding_similarity(
                transparency_product['name']
            )

            if embedding_matches and embedding_matches[0]['score'] > 0.75:
                return (
                    embedding_matches[0]['product_id'],
                    embedding_matches[0]['score'] * 0.9,  # Slightly reduce confidence
                    'ml_embedding'
                )

        # Level 5: Category + partial name match (confidence: 0.5-0.7)
        if transparency_product.get('category'):
            category_matches = self.category_based_match(
                transparency_product
            )

            if category_matches:
                return (
                    category_matches[0]['product_id'],
                    category_matches[0]['score'] * 0.7,
                    'category_partial'
                )

        # No match found - add to manual review queue
        self.add_to_queue(transparency_product, retailer_id)
        return None
```

## Matching Utilities

### Barcode Cleaning and Validation
```python
def clean_barcode(barcode):
    """Clean and validate barcode"""
    if not barcode:
        return None

    # Remove non-digits
    barcode = re.sub(r'\D', '', str(barcode))

    # Remove leading zeros but keep at least 8 digits
    barcode = barcode.lstrip('0')

    # Validate length (8-13 digits for standard barcodes)
    if len(barcode) < 8 or len(barcode) > 13:
        return None

    # Validate checksum for EAN-13
    if len(barcode) == 13:
        if not validate_ean13_checksum(barcode):
            return None

    return barcode
```

### Fuzzy Name Matching
```python
def fuzzy_match_products(transparency_name, brand=None, threshold=0.85):
    """
    Fuzzy match against canonical products
    """
    from fuzzywuzzy import fuzz
    from fuzzywuzzy import process

    # Get all canonical products
    query = """
        SELECT product_id, canonical_name, brand
        FROM products
    """

    if brand:
        query += " WHERE lower(brand) = lower(%s)"
        products = db.query(query, (brand,))
    else:
        products = db.query(query)

    # Create match candidates
    candidates = []
    for product in products:
        # Combine name and brand for matching
        canonical_str = f"{product['canonical_name']} {product['brand'] or ''}"
        transparency_str = f"{transparency_name} {brand or ''}"

        # Calculate similarity scores
        ratio = fuzz.ratio(canonical_str, transparency_str)
        partial_ratio = fuzz.partial_ratio(canonical_str, transparency_str)
        token_sort = fuzz.token_sort_ratio(canonical_str, transparency_str)

        # Weighted average score
        score = (ratio * 0.4 + partial_ratio * 0.3 + token_sort * 0.3) / 100

        if score >= threshold:
            candidates.append({
                'product_id': product['product_id'],
                'canonical_name': product['canonical_name'],
                'score': score
            })

    # Sort by score descending
    return sorted(candidates, key=lambda x: x['score'], reverse=True)
```

### Manual Review Queue Processing
```python
def process_manual_queue():
    """
    Process products in manual matching queue
    """
    # Get pending items
    pending = db.query("""
        SELECT q.*, rp.original_retailer_name, r.retailername
        FROM product_matching_queue q
        JOIN retailer_products rp ON q.retailer_product_id = rp.retailer_product_id
        JOIN retailers r ON rp.retailer_id = r.retailerid
        WHERE q.status = 'pending'
        ORDER BY q.queue_id
        LIMIT 100
    """)

    for item in pending:
        print(f"\n{'='*60}")
        print(f"Retailer: {item['retailername']}")
        print(f"Product: {item['original_retailer_name']}")
        print(f"Barcode: {item.get('transparency_barcode', 'N/A')}")

        # Show potential matches
        if item['potential_matches']:
            print("\nPotential matches:")
            for i, match in enumerate(item['potential_matches'][:5]):
                print(f"{i+1}. {match['name']} ({match['score']:.2f})")

        # Manual selection
        choice = input("\nSelect match (1-5) or 's' to skip: ")

        if choice.isdigit():
            selected = item['potential_matches'][int(choice)-1]
            create_retailer_product_link(
                selected['product_id'],
                item['retailer_id'],
                item,
                confidence=0.99,  # High confidence for manual match
                method='manual'
            )

            # Update queue status
            db.execute("""
                UPDATE product_matching_queue
                SET status = 'matched', matched_at = NOW()
                WHERE queue_id = %s
            """, (item['queue_id'],))
```

## Data Quality Rules

### Canonical Product Requirements
1. **Must have** clean, user-friendly name from commercial site
2. **Must have** high-quality image URL
3. **Should have** barcode for matching
4. **Should have** brand/manufacturer information
5. **Should have** product description

### Transparency Data Requirements
1. **Must have** retailer item code
2. **Must have** price information
3. **Must have** store association
4. **Should have** barcode for matching
5. **Can have** raw product name (not shown to users)

## SQL Queries for Common Operations

### Find Products Across All Retailers
```sql
-- Get canonical product with all retailer prices
SELECT
    p.canonical_name,
    p.brand,
    p.image_url,
    r.retailername,
    rp.retailer_item_code,
    pr.price,
    s.storename,
    s.city
FROM products p
JOIN retailer_products rp ON p.product_id = rp.product_id
JOIN retailers r ON rp.retailer_id = r.retailerid
JOIN prices pr ON rp.retailer_product_id = pr.retailer_product_id
JOIN stores s ON pr.store_id = s.storeid
WHERE lower(p.canonical_name) LIKE '%advil%'
  AND pr.price_timestamp = (
    SELECT MAX(price_timestamp)
    FROM prices
    WHERE retailer_product_id = pr.retailer_product_id
  )
ORDER BY pr.price ASC;
```

### Get Unmatched Products for Review
```sql
-- Products from transparency portal without canonical match
SELECT
    rp.retailer_product_id,
    rp.original_retailer_name,
    rp.retailer_item_code,
    r.retailername,
    COUNT(DISTINCT pr.store_id) as num_stores,
    AVG(pr.price) as avg_price
FROM retailer_products rp
JOIN retailers r ON rp.retailer_id = r.retailerid
LEFT JOIN prices pr ON rp.retailer_product_id = pr.retailer_product_id
WHERE rp.product_id IS NULL
  OR rp.confidence_score < 0.7
GROUP BY rp.retailer_product_id, rp.original_retailer_name,
         rp.retailer_item_code, r.retailername
ORDER BY num_stores DESC;
```

### Update Canonical Product with Better Data
```sql
-- Update canonical product when better commercial data is found
UPDATE products p
SET
    image_url = COALESCE(commercial.image_url, p.image_url),
    description = COALESCE(commercial.description, p.description),
    updated_at = NOW()
FROM (
    SELECT product_id, image_url, description
    FROM commercial_scrape_results
    WHERE scrape_date > NOW() - INTERVAL '1 day'
) commercial
WHERE p.product_id = commercial.product_id
  AND (
    p.image_url IS NULL
    OR commercial.image_url IS NOT NULL
  );
```

## Performance Optimization

### Critical Indexes
```sql
-- For barcode matching (most important)
CREATE INDEX idx_products_barcode ON products((attributes->>'barcode'));
CREATE INDEX idx_canonical_primary_barcode ON canonical_products(primary_barcode);

-- For name matching
CREATE INDEX idx_products_canonical_name_trgm ON products
  USING gin(canonical_name gin_trgm_ops);  -- Requires pg_trgm extension

-- For price lookups
CREATE INDEX idx_prices_composite ON prices(retailer_product_id, store_id, price_timestamp DESC);

-- For unmatched products
CREATE INDEX idx_retailer_products_unmatched ON retailer_products(product_id)
  WHERE product_id IS NULL;
```

### Materialized Views for Fast Lookups
```sql
-- Latest prices per product per store
CREATE MATERIALIZED VIEW mv_latest_prices AS
SELECT DISTINCT ON (rp.retailer_product_id, pr.store_id)
    p.product_id,
    p.canonical_name,
    p.image_url,
    rp.retailer_product_id,
    r.retailername,
    s.storeid,
    s.storename,
    s.city,
    pr.price,
    pr.price_timestamp
FROM products p
JOIN retailer_products rp ON p.product_id = rp.product_id
JOIN retailers r ON rp.retailer_id = r.retailerid
JOIN prices pr ON rp.retailer_product_id = pr.retailer_product_id
JOIN stores s ON pr.store_id = s.storeid
ORDER BY rp.retailer_product_id, pr.store_id, pr.price_timestamp DESC;

-- Refresh periodically
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_latest_prices;
```

## Implementation Checklist

### Initial Setup
- [ ] Create all tables with proper constraints
- [ ] Add necessary indexes for performance
- [ ] Set up pg_trgm extension for fuzzy matching
- [ ] Create matching queue table

### Commercial Data Pipeline
- [x] Scrape Super-Pharm commercial site ✅ (14,237 products with images)
- [x] Scrape Be Pharm commercial site ✅ (2,188 products created)
- [x] Scrape Good Pharm commercial site ✅ (12,107 products created)
- [x] Store canonical products with images ✅

### Transparency Data Pipeline
- [x] Download Super-Pharm transparency XML ✅ (900/2,177 files - pagination issue at page 61)
- [x] Download Be Pharm transparency XML ✅ (499 files from Shufersal portal)
- [x] Download Good Pharm transparency XML ✅ (271,895 prices from binaprojects portal)
- [x] Parse and store price data ✅ (481,273 total price points after cleanup)

### Matching Pipeline
- [ ] Implement barcode matching
- [ ] Implement fuzzy name matching
- [ ] Implement ML embedding matching (optional)
- [ ] Create manual review interface
- [ ] Run matching for all products

### Quality Assurance
- [ ] Verify all canonical products have images
- [ ] Verify price coverage across stores
- [ ] Check matching confidence scores
- [ ] Review and resolve unmatched products

## Common Issues and Solutions

### Issue: Good Pharm Duplicate Stores (CRITICAL - FIXED)
**Problem**: 144 stores in database, but only 72 are real
**Details**:
- 72 original stores (IDs 9080-10783) with clean retailerspecificstoreid
- 72 duplicate stores (IDs 1670981-1908568) with store names in retailerspecificstoreid
- ALL 731,470 prices were linked to duplicate stores
**Root Cause**: ETL created duplicates with wrong ID format when processing Good Pharm files
**Solution Applied**:
```sql
-- Fix applied Sept 18, 2025
UPDATE prices SET store_id = (mapping to original stores);
DELETE FROM stores WHERE retailerid = 97 AND storeid > 100000;
```
**Result**: Perfect 100% coverage (72/72 stores)

### Issue: Super-Pharm Portal Blocking (CRITICAL - UNRESOLVED)
**Problem**: HTTP 492 "Access Denied" from prices.super-pharm.co.il
**Impact**: 0 products, 0 prices processed despite discovering 1,920 files
**Evidence**: Manual downloads return IP blocking message
**Workaround**: Local Super-Pharm files exist in /Users/noa/Downloads/
**Need**: Find new download method or use existing local files

### Issue: Duplicate Prices in Super-Pharm
**Problem**: Processing same files multiple times created 7.27M duplicate prices
**Solution**:
- Added deduplication tracking with `processed_products` set
- Fixed ON CONFLICT clause to use proper unique constraint
- Cleaned duplicates with SQL query keeping only latest price per product/store

### Issue: Be Pharm Limited Coverage (VERIFIED - PORTAL LIMITATION)
**Problem**: Only 46/136 stores (34%) have price data
**Investigation Results**:
- ETL correctly processes all available files on Shufersal portal
- Only 46 Be Pharm stores regularly upload data to transparency portal
- Most active stores are in 700-800 range (e.g., 790, 854, 765)
- Popular stores (26, 233, 641) have minimal data uploads
**Reality**: Not a bug - limited by actual data availability on portal
**Status**: 105,296 prices from 46 stores is correct for available data

### Issue: Super-Pharm Pagination Breaks at Page 61
**Problem**: HTTP 481 error causes entire pagination loop to break
**Solution**: Continue to next page instead of breaking loop on 481 errors
```python
# Old: break  # This stopped all pagination
# New: page += 1; continue  # Skip failed page, continue with next
```

### Issue: Duplicate Products
```sql
-- Find duplicate products by barcode
SELECT attributes->>'barcode' as barcode, COUNT(*)
FROM products
WHERE attributes->>'barcode' IS NOT NULL
GROUP BY attributes->>'barcode'
HAVING COUNT(*) > 1;
```

### Issue: Poor Matching Quality
**Solution**: Adjust matching thresholds, add manual review
```python
# Lower threshold for manual review
if confidence < 0.7:
    add_to_manual_queue(product)
elif confidence < 0.85:
    flag_for_review(product)
```

### Issue: Missing Images
**Solution**: Fallback to generic or search for alternatives
```python
if not product.image_url:
    # Try to find from other sources
    product.image_url = (
        search_google_images(product.name) or
        get_manufacturer_image(product.brand, product.barcode) or
        DEFAULT_PRODUCT_IMAGE
    )
```

This comprehensive guide ensures proper canonical product selection and matching across your dual-source architecture.