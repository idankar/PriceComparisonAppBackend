# Price Comparison App Database Schema - Detailed Breakdown

## Database: `price_comparison_app_v2`

## Table Structure Overview

The database consists of 20 tables organized into several functional groups:

### Core Product Tables
1. **products** - Master canonical products
2. **canonical_products** - Canonical product definitions
3. **canonical_products_clean** - Cleaned canonical products
4. **retailer_products** - Products specific to retailers

### Mapping & Matching Tables
5. **product_to_canonical** - Maps products to canonical products
6. **listing_to_canonical** - Maps listings to canonical products
7. **barcode_to_canonical_map** - Maps barcodes to canonical products
8. **product_matches** - Stores product matching results
9. **commercial_government_matches** - Maps commercial to government products
10. **product_groups** - Groups related products
11. **product_group_links** - Links products to groups
12. **unmatched_products** - Tracks unmatched products

### Pricing & Promotion Tables
13. **prices** - Product prices by store and time
14. **promotions** - Promotion definitions
15. **promotion_product_links** - Links products to promotions

### Business Entity Tables
16. **retailers** - Retailer information
17. **stores** - Physical store locations
18. **categories** - Product categories

### System Tables
19. **filesprocessed** - ETL file processing tracking
20. **spatial_ref_sys** - PostGIS spatial reference system

---

## Detailed Table Schemas

### 1. **products**
Master table for all canonical products in the system.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| product_id | integer | PK, NOT NULL, auto-increment | Unique product identifier |
| canonical_name | text | NOT NULL | Standardized product name |
| brand | text | | Product brand |
| description | text | | Product description |
| image_url | text | | Product image URL |
| created_at | timestamp with time zone | DEFAULT CURRENT_TIMESTAMP | Record creation time |
| updated_at | timestamp with time zone | DEFAULT CURRENT_TIMESTAMP | Last update time |
| attributes | jsonb | | Additional product attributes |
| image_source | varchar(50) | | Source of the image |
| image_updated_at | timestamp with time zone | | Image update timestamp |
| embedding | text | | Product embedding for ML |
| canonical_id | varchar(255) | FK → canonical_products | Link to canonical product |

**Indexes:**
- PRIMARY KEY: product_id
- UNIQUE: (lower(canonical_name), lower(brand))

**Foreign Keys:**
- canonical_id → canonical_products(canonical_id)

---

### 2. **canonical_products**
Defines canonical product representations.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| canonical_id | varchar(255) | PK, NOT NULL | Unique canonical identifier |
| canonical_name | text | NOT NULL | Canonical product name |
| canonical_brand | varchar(255) | | Brand name |
| primary_barcode | varchar(255) | UNIQUE | Primary barcode |
| category | varchar(255) | | Product category |
| image_url | text | | Image URL |
| attributes | jsonb | | Additional attributes |
| created_at | timestamp | DEFAULT CURRENT_TIMESTAMP | Creation time |
| updated_at | timestamp | DEFAULT CURRENT_TIMESTAMP | Update time |

**Indexes:**
- PRIMARY KEY: canonical_id
- UNIQUE: primary_barcode
- INDEX: primary_barcode, canonical_brand

---

### 3. **canonical_products_clean**
Cleaned version of canonical products.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| canonical_id | integer | PK, NOT NULL, auto-increment | Unique identifier |
| barcode | varchar(255) | UNIQUE | Product barcode |
| name | text | NOT NULL | Product name |
| brand | varchar(255) | | Brand name |
| category | varchar(255) | | Category |
| image_url | text | | Image URL |
| product_url | text | | Product page URL |
| description | text | | Description |
| created_at | timestamp | DEFAULT CURRENT_TIMESTAMP | Creation time |

**Indexes:**
- PRIMARY KEY: canonical_id
- UNIQUE: barcode
- INDEX: barcode

---

### 4. **retailer_products**
Products specific to each retailer.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| retailer_product_id | integer | PK, NOT NULL, auto-increment | Unique identifier |
| product_id | integer | NOT NULL, FK → products | Reference to master product |
| retailer_id | integer | NOT NULL, FK → retailers | Retailer reference |
| retailer_item_code | varchar(100) | NOT NULL | Retailer's product code |
| original_retailer_name | text | | Original name from retailer |

**Indexes:**
- PRIMARY KEY: retailer_product_id
- UNIQUE: (retailer_id, retailer_item_code)
- INDEX: retailer_id, retailer_item_code
- INDEX: product_id

**Foreign Keys:**
- product_id → products(product_id)
- retailer_id → retailers(retailerid)

---

### 5. **prices**
Historical and current price data.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| price_id | bigint | PK, NOT NULL, auto-increment | Unique price record ID |
| retailer_product_id | integer | NOT NULL, FK → retailer_products | Product reference |
| store_id | integer | NOT NULL, FK → stores | Store reference |
| price | numeric(10,2) | NOT NULL | Product price |
| price_timestamp | timestamp with time zone | NOT NULL | When price was valid |
| scraped_at | timestamp with time zone | DEFAULT CURRENT_TIMESTAMP | When data was scraped |
| promotion_id | bigint | FK → promotions | Associated promotion |

**Indexes:**
- PRIMARY KEY: price_id
- UNIQUE: (retailer_product_id, store_id, price_timestamp)
- INDEX: price_timestamp DESC

**Foreign Keys:**
- retailer_product_id → retailer_products(retailer_product_id)
- store_id → stores(storeid)
- promotion_id → promotions(promotion_id) ON DELETE SET NULL

---

### 6. **retailers**
Retailer/chain information.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| retailerid | integer | PK, NOT NULL, auto-increment | Unique retailer ID |
| retailername | varchar(100) | NOT NULL, UNIQUE | Retailer name |
| chainid | varchar(50) | UNIQUE | Chain identifier |
| pricetransparencyportalurl | varchar(255) | | Price transparency URL |
| fileformat | varchar(20) | DEFAULT 'XML' | Data file format |
| notes | text | | Additional notes |
| createdat | timestamp with time zone | DEFAULT CURRENT_TIMESTAMP | Creation time |
| updatedat | timestamp with time zone | DEFAULT CURRENT_TIMESTAMP | Update time |

**Indexes:**
- PRIMARY KEY: retailerid
- UNIQUE: retailername
- UNIQUE: chainid

---

### 7. **stores**
Physical store locations.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| storeid | integer | PK, NOT NULL, auto-increment | Unique store ID |
| retailerid | integer | NOT NULL, FK → retailers | Retailer reference |
| retailerspecificstoreid | varchar(50) | NOT NULL | Retailer's store ID |
| storename | varchar(255) | | Store name |
| address | text | | Street address |
| city | varchar(100) | | City |
| postalcode | varchar(20) | | Postal code |
| latitude | numeric(9,6) | | GPS latitude |
| longitude | numeric(9,6) | | GPS longitude |
| isactive | boolean | DEFAULT true | Store active status |
| rawstoredata | jsonb | | Raw store data |
| createdat | timestamp with time zone | DEFAULT CURRENT_TIMESTAMP | Creation time |
| updatedat | timestamp with time zone | DEFAULT CURRENT_TIMESTAMP | Update time |
| subchainid | text | | Sub-chain identifier |
| subchainname | varchar(255) | | Sub-chain name |
| storetype | text | | Type of store |
| lastupdatedfromstoresfile | text | | Last update source file |

**Indexes:**
- PRIMARY KEY: storeid
- UNIQUE: (retailerid, retailerspecificstoreid)
- INDEX: city
- INDEX: retailerid

**Foreign Keys:**
- retailerid → retailers(retailerid) ON DELETE RESTRICT

---

### 8. **promotions**
Promotion definitions.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| promotion_id | bigint | PK, NOT NULL, auto-increment | Unique promotion ID |
| retailer_id | integer | NOT NULL, FK → retailers | Retailer reference |
| retailer_promotion_code | varchar(100) | NOT NULL | Retailer's promo code |
| description | text | | Promotion description |
| start_date | timestamp with time zone | | Promotion start date |
| end_date | timestamp with time zone | | Promotion end date |

**Indexes:**
- PRIMARY KEY: promotion_id
- UNIQUE: (retailer_id, retailer_promotion_code)

**Foreign Keys:**
- retailer_id → retailers(retailerid)

---

### 9. **promotion_product_links**
Links products to promotions.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| link_id | bigint | PK, NOT NULL, auto-increment | Unique link ID |
| promotion_id | bigint | NOT NULL, FK → promotions | Promotion reference |
| retailer_product_id | integer | NOT NULL, FK → retailer_products | Product reference |

**Indexes:**
- PRIMARY KEY: link_id
- UNIQUE: (promotion_id, retailer_product_id)

**Foreign Keys:**
- promotion_id → promotions(promotion_id) ON DELETE CASCADE
- retailer_product_id → retailer_products(retailer_product_id) ON DELETE CASCADE

---

### 10. **product_matches**
Stores product matching results.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| match_id | integer | PK, NOT NULL, auto-increment | Unique match ID |
| master_product_id | integer | NOT NULL | Master product reference |
| retailer_product_ids | integer[] | NOT NULL | Array of matched products |
| match_confidence | numeric(3,2) | NOT NULL | Match confidence score |
| match_method | varchar(50) | NOT NULL | Matching method used |
| match_details | jsonb | | Matching details |
| created_at | timestamp with time zone | DEFAULT CURRENT_TIMESTAMP | Creation time |
| reviewed | boolean | DEFAULT false | Manual review status |

**Indexes:**
- PRIMARY KEY: match_id
- INDEX: match_method
- GIN INDEX: retailer_product_ids

---

### 11. **product_to_canonical**
Maps products to canonical products.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| product_id | integer | PK, NOT NULL | Product reference |
| canonical_id | integer | FK → canonical_products_clean | Canonical product |
| retailer_id | integer | | Retailer reference |
| match_method | varchar(50) | | Matching method used |

**Indexes:**
- PRIMARY KEY: product_id
- INDEX: canonical_id

**Foreign Keys:**
- product_id → products(product_id)
- canonical_id → canonical_products_clean(canonical_id)

---

### 12. **listing_to_canonical**
Maps listings to canonical products.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| listing_id | varchar(255) | PK, NOT NULL | Unique listing ID |
| canonical_id | varchar(255) | FK → canonical_products | Canonical product |
| source_type | varchar(50) | | Source type |
| retailer | varchar(255) | | Retailer name |
| confidence_score | double precision | | Match confidence |
| match_method | varchar(50) | | Matching method |
| created_at | timestamp | DEFAULT CURRENT_TIMESTAMP | Creation time |

**Indexes:**
- PRIMARY KEY: listing_id
- INDEX: canonical_id

**Foreign Keys:**
- canonical_id → canonical_products(canonical_id)

---

### 13. **barcode_to_canonical_map**
Maps barcodes to canonical products.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| barcode | varchar(50) | | Product barcode |
| canonical_masterproductid | integer | | Canonical product ID |

**Indexes:**
- INDEX: barcode

---

### 14. **product_groups**
Groups related products together.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| group_id | integer | PK, NOT NULL, auto-increment | Unique group ID |
| created_at | timestamp with time zone | DEFAULT CURRENT_TIMESTAMP | Creation time |
| canonical_name | text | | Canonical group name |

**Indexes:**
- PRIMARY KEY: group_id

---

### 15. **product_group_links**
Links products to groups.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| link_id | integer | PK, NOT NULL, auto-increment | Unique link ID |
| group_id | integer | NOT NULL, FK → product_groups | Group reference |
| product_id | integer | NOT NULL, UNIQUE, FK → products | Product reference |

**Indexes:**
- PRIMARY KEY: link_id
- UNIQUE: product_id
- INDEX: group_id
- INDEX: product_id

**Foreign Keys:**
- group_id → product_groups(group_id) ON DELETE CASCADE
- product_id → products(product_id) ON DELETE CASCADE

---

### 16. **categories**
Product categories with hierarchy.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| categoryid | integer | PK, NOT NULL, auto-increment | Unique category ID |
| categoryname | varchar(150) | NOT NULL, UNIQUE | Category name |
| parentcategoryid | integer | FK → categories | Parent category |
| description | text | | Category description |
| createdat | timestamp with time zone | DEFAULT CURRENT_TIMESTAMP | Creation time |
| updatedat | timestamp with time zone | DEFAULT CURRENT_TIMESTAMP | Update time |

**Indexes:**
- PRIMARY KEY: categoryid
- UNIQUE: categoryname
- INDEX: parentcategoryid

**Foreign Keys:**
- parentcategoryid → categories(categoryid) ON DELETE SET NULL (self-referential)

---

### 17. **commercial_government_matches**
Maps commercial products to government products.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| match_id | integer | PK, NOT NULL, auto-increment | Unique match ID |
| commercial_product_id | varchar(255) | | Commercial product ID |
| government_product_id | integer | | Government product ID |
| match_method | varchar(50) | | Matching method |
| confidence | double precision | | Match confidence |
| commercial_name | text | | Commercial product name |
| commercial_brand | varchar(255) | | Commercial brand |
| commercial_price | numeric(10,2) | | Commercial price |
| commercial_image_url | text | | Commercial image URL |
| government_name | text | | Government product name |
| government_brand | varchar(255) | | Government brand |
| government_price | numeric(10,2) | | Government price |
| price_difference | numeric(10,2) | | Price difference |
| created_at | timestamp | DEFAULT CURRENT_TIMESTAMP | Creation time |

**Indexes:**
- PRIMARY KEY: match_id

---

### 18. **filesprocessed**
Tracks ETL file processing.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| fileid | integer | PK, NOT NULL, auto-increment | Unique file ID |
| retailerid | integer | NOT NULL, FK → retailers | Retailer reference |
| storeid | integer | FK → stores | Store reference |
| filename | varchar(255) | NOT NULL | File name |
| filetype | varchar(50) | NOT NULL | File type |
| filehash | varchar(64) | | File hash for deduplication |
| filesize | bigint | | File size in bytes |
| filetimestamp | timestamp with time zone | | File timestamp |
| processingstatus | varchar(50) | DEFAULT 'PENDING' | Processing status |
| processingstarttime | timestamp with time zone | | Processing start time |
| processingendtime | timestamp with time zone | | Processing end time |
| rowsadded | integer | | Number of rows added |
| errormessage | text | | Error message if failed |
| createdat | timestamp with time zone | DEFAULT CURRENT_TIMESTAMP | Creation time |
| download_url | text | | File download URL |
| updated_at | timestamp with time zone | DEFAULT CURRENT_TIMESTAMP | Update time |
| lastattempttime | timestamp with time zone | | Last attempt time |

**Indexes:**
- PRIMARY KEY: fileid
- UNIQUE: (retailerid, filename)
- INDEX: filetimestamp DESC
- INDEX: retailerid
- INDEX: storeid

**Foreign Keys:**
- retailerid → retailers(retailerid) ON DELETE RESTRICT
- storeid → stores(storeid) ON DELETE SET NULL

**Triggers:**
- set_filesprocessed_updated_at BEFORE UPDATE

---

### 19. **unmatched_products**
Tracks products that couldn't be matched.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| product_id | integer | PK, NOT NULL | Product reference |
| retailer_id | integer | | Retailer reference |
| product_name | text | | Product name |
| brand | text | | Brand |
| attempted_methods | text[] | | Methods attempted for matching |
| last_attempt | timestamp with time zone | DEFAULT CURRENT_TIMESTAMP | Last attempt time |

**Indexes:**
- PRIMARY KEY: product_id

---

### 20. **spatial_ref_sys**
PostGIS spatial reference system table.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| srid | integer | PK, NOT NULL | Spatial reference ID |
| auth_name | varchar(256) | | Authority name |
| auth_srid | integer | | Authority SRID |
| srtext | varchar(2048) | | Well-known text representation |
| proj4text | varchar(2048) | | Proj4 text representation |

**Indexes:**
- PRIMARY KEY: srid

**Check Constraints:**
- srid > 0 AND srid <= 998999

---

## Key Relationships

### Product Hierarchy
```
canonical_products / canonical_products_clean
         ↓
     products
         ↓
 retailer_products
         ↓
      prices
```

### Matching & Mapping Flow
```
retailer_products → product_matches → products
retailer_products → product_to_canonical → canonical_products_clean
listings → listing_to_canonical → canonical_products
barcodes → barcode_to_canonical_map → canonical products
```

### Business Entity Relationships
```
retailers → stores → prices
retailers → retailer_products
retailers → promotions → promotion_product_links → retailer_products
```

### Product Grouping
```
products → product_group_links → product_groups
```

---

## Database Features

### Indexes Strategy
- **Primary Keys**: All tables have auto-incrementing primary keys
- **Unique Constraints**: Enforce business rules (e.g., one product per retailer-code combination)
- **Foreign Keys**: Maintain referential integrity with CASCADE/RESTRICT options
- **Performance Indexes**: On frequently queried columns (timestamps, lookups)
- **GIN Indexes**: For array and JSONB columns

### Data Types
- **JSONB**: Used for flexible attribute storage (products.attributes, stores.rawstoredata)
- **Arrays**: Used for storing multiple IDs (product_matches.retailer_product_ids)
- **Numeric**: For precise price calculations
- **Timestamps**: With timezone support for proper temporal tracking

### PostGIS Integration
- Spatial data support through spatial_ref_sys table
- Geographic coordinates in stores table (latitude/longitude)

### Triggers
- `set_filesprocessed_updated_at`: Automatically updates timestamp on filesprocessed table

### Custom Functions
- `match_to_canonical_product`: Custom function for product matching
- `trigger_set_timestamp`: Timestamp update trigger function

---

## ETL & Data Flow

1. **Data Ingestion**: Files tracked in `filesprocessed`
2. **Product Creation**: New products added to `products` and `retailer_products`
3. **Matching**: Products matched via various tables (product_matches, product_to_canonical)
4. **Pricing**: Price data stored in `prices` with temporal tracking
5. **Promotions**: Linked through `promotions` and `promotion_product_links`

---

## Performance Considerations

1. **Partitioning Candidates**:
   - `prices` table (by price_timestamp)
   - `filesprocessed` table (by filetimestamp)

2. **Index Optimization**:
   - Composite indexes on frequently joined columns
   - Partial indexes for filtered queries
   - GIN indexes for JSONB and array operations

3. **Data Retention**:
   - Historical price data management
   - File processing history cleanup
   - Unmatched products periodic review