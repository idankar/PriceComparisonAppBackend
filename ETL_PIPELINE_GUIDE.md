# ETL Pipeline Guide - Price Comparison App

## Overview
This guide documents the dual-source ETL pipeline architecture for extracting, transforming, and loading pharmacy data from 3 Israeli pharmacy chains into a unified PostgreSQL database.

## Two-Source Data Architecture

### Key Concept: Commercial + Transparency Data
Each retailer has **TWO data sources** that work together:

1. **Commercial Website** (High-Quality Canonical Data)
   - Product names (clean, marketing-friendly)
   - Product images
   - Product descriptions
   - Categories and organization
   - Limited to products currently for sale

2. **Price Transparency Portal** (Complete Price Coverage)
   - All products (including discontinued)
   - Price points for EVERY store location
   - Barcodes and item codes
   - Promotions and discounts
   - Government-mandated data format

### Data Flow Strategy
```
Commercial Site → Canonical Products (name + image + description)
                            ↓
                     Product Matching
                            ↓
Transparency Portal → Price Points (all stores, all variations)
```

## Database Structure

### Core Tables
The successful database schema consists of these primary tables:

1. **retailers** - Pharmacy chain information
2. **products** - Canonical product catalog (from commercial sites)
3. **retailer_products** - Retailer-specific product mappings
4. **prices** - Current and historical pricing data (from transparency portals)
5. **stores** - Physical store locations with GPS coordinates

### Database Connection
```python
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="price_comparison_app_v2",
    user="postgres",
    password="***REMOVED***"
)
```

## Retailer Data Sources

### 1. Super-Pharm

#### Commercial Site (Canonical Data) ✅
**File**: `04_utilities/super_pharm_updater.py`
- **URL**: `https://shop.super-pharm.co.il/`
- **Method**: Selenium web scraping
- **Data**: Product names, images, descriptions (49 categories)
- **Purpose**: Creates canonical product records with clean names and images

#### Transparency Portal (Price Data) ✅
**File**: `01_data_scraping_pipeline/super_pharm_transparency_etl_FIXED.py`
- **URL**: `https://prices.super-pharm.co.il/` (Super-Pharm's own transparency portal)
- **Method**: XML file downloads
- **Data**: Complete price points for all stores, barcodes, promotions
- **Purpose**: Provides comprehensive pricing data for every store location

### 2. Be Pharm

#### Commercial Site (Canonical Data)
**File**: Need to implement/find
- **URL**: `https://www.bepharm.co.il/`
- **Method**: Web scraping (needs implementation)
- **Data**: Product names, images, descriptions
- **Purpose**: Creates canonical product records

#### Transparency Portal (Price Data) ✅
**File**: `01_data_scraping_pipeline/be_good_pharm_etl.py`
- **URL**: `https://prices.shufersal.co.il/` (Shufersal transparency portal)
- **Chain ID**: `7290027600007`
- **Method**: XML file downloads from Shufersal portal
- **Data**: Complete price points, barcodes
- **Purpose**: Provides pricing data for all Be Pharm stores

### 3. Good Pharm

#### Commercial Site (Canonical Data)
**File**: Need to implement/find
- **URL**: `https://www.goodpharm.co.il/` (needs verification)
- **Method**: Web scraping (needs implementation)
- **Data**: Product names, images, descriptions
- **Purpose**: Creates canonical product records

#### Transparency Portal (Price Data) ✅
**File**: `01_data_scraping_pipeline/be_good_pharm_etl.py`
- **URL**: `https://goodpharm.binaprojects.com/MainIO_Hok.aspx`
- **Chain ID**: `7290058108879`
- **Method**: JSON/XML downloads from binaprojects portal
- **Data**: Complete price points for all stores
- **Purpose**: Provides pricing data for all Good Pharm stores

## Successful ETL Implementation Pattern

### Phase 1: Commercial Data Collection (Canonical Products)
```python
# For each retailer's commercial site:
1. Scrape product listings
2. Extract:
   - Clean product name
   - High-quality image URL
   - Description
   - Category
3. Store as canonical product in 'products' table
```

### Phase 2: Transparency Data Collection (Price Points)
```python
# For each retailer's transparency portal:
1. Download XML/JSON files
2. Extract:
   - Item codes/barcodes
   - Prices for each store
   - Promotions
3. Match to canonical products via:
   - Barcode matching
   - Name similarity matching
   - Manual matching for edge cases
4. Store in 'prices' table linked to canonical product
```

### Phase 3: Product Matching
```python
# Link transparency data to canonical products:
1. Try exact barcode match
2. Try fuzzy name matching
3. Use ML/embedding similarity
4. Manual review for unmatched items
```

## Working Implementation Files

### Commercial Data Scrapers (Canonical)
- ✅ `04_utilities/super_pharm_updater.py` - Super-Pharm commercial scraper
- ⚠️ Be Pharm commercial scraper - needs implementation
- ⚠️ Good Pharm commercial scraper - needs implementation

### Transparency Portal ETL (Prices)
- ✅ `01_data_scraping_pipeline/super_pharm_transparency_etl_FIXED.py` - Super-Pharm transparency data (fixed pagination)
- ✅ `01_data_scraping_pipeline/be_good_pharm_etl_FIXED.py` - Be Pharm & Good Pharm transparency data (fixed store extraction)

### Supporting Files
- `06_product_matching/unified_product_matcher.py` - Cross-retailer product matching
- `05_geocoding/store_geocoding.py` - GPS coordinate assignment
- `02_backend_api/backend_app.py` - Flask API with search endpoints

## Unified ETL Script Template

```python
#!/usr/bin/env python3
"""
Unified Dual-Source ETL Pipeline for All 3 Pharmacy Chains
Phase 1: Commercial data for canonical products
Phase 2: Transparency data for comprehensive pricing
"""

import os
import sys
import time
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional

import requests
import psycopg2
from psycopg2.extras import Json
from bs4 import BeautifulSoup
from selenium import webdriver

class UnifiedPharmacyETL:
    def __init__(self):
        """Initialize the dual-source ETL pipeline"""
        self.conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="price_comparison_app_v2",
            user="postgres",
            password="***REMOVED***"
        )
        self.cursor = self.conn.cursor()

        # Dual-source configuration for each retailer
        self.retailers = {
            'Super-Pharm': {
                'retailer_id': 52,
                'commercial': {
                    'url': 'https://shop.super-pharm.co.il/',
                    'scraper': '04_utilities/super_pharm_updater.py',
                    'method': 'selenium'
                },
                'transparency': {
                    'url': 'https://prices.super-pharm.co.il/',
                    'method': 'xml_download'
                }
            },
            'Be Pharm': {
                'retailer_id': 150,
                'commercial': {
                    'url': 'https://www.bepharm.co.il/',
                    'method': 'needs_implementation'
                },
                'transparency': {
                    'url': 'https://prices.shufersal.co.il/',
                    'chain_id': '7290027600007',
                    'method': 'xml_download'
                }
            },
            'Good Pharm': {
                'retailer_id': 97,
                'commercial': {
                    'url': 'https://www.goodpharm.co.il/',
                    'method': 'needs_implementation'
                },
                'transparency': {
                    'url': 'https://goodpharm.binaprojects.com/',
                    'method': 'json_api'
                }
            }
        }

    def run_phase1_commercial(self):
        """Phase 1: Collect canonical product data from commercial sites"""
        for retailer_name, config in self.retailers.items():
            logging.info(f"Phase 1 - {retailer_name}: Collecting commercial data")

            if config['commercial']['method'] == 'selenium':
                # Run existing Selenium scraper
                self.run_selenium_scraper(config['commercial']['scraper'])
            else:
                logging.warning(f"{retailer_name} commercial scraper not implemented")

    def run_phase2_transparency(self):
        """Phase 2: Collect comprehensive pricing from transparency portals"""
        for retailer_name, config in self.retailers.items():
            logging.info(f"Phase 2 - {retailer_name}: Collecting transparency data")

            if config['transparency']['method'] == 'xml_download':
                self.process_xml_transparency(config)
            elif config['transparency']['method'] == 'json_api':
                self.process_json_transparency(config)

    def run_phase3_matching(self):
        """Phase 3: Match transparency prices to canonical products"""
        logging.info("Phase 3: Running product matching")
        # Use unified_product_matcher.py logic here
        pass

if __name__ == "__main__":
    etl = UnifiedPharmacyETL()

    # Phase 1: Get canonical products with images
    etl.run_phase1_commercial()

    # Phase 2: Get all price points
    etl.run_phase2_transparency()

    # Phase 3: Match and link
    etl.run_phase3_matching()
```

## Critical Success Factors

### 1. **Data Source Priority**
- Commercial sites → Clean names & images (user-facing)
- Transparency portals → Complete price coverage (backend data)

### 2. **Matching Strategy**
- Never show transparency portal product names to users
- Always use commercial site names/images for display
- Link via barcodes when possible, fuzzy matching as fallback

### 3. **Database Safety** ⚠️
```sql
-- SAFE: Delete specific data
DELETE FROM table_name WHERE condition;

-- DANGEROUS: Never use CASCADE without careful consideration
-- TRUNCATE table_name CASCADE;  -- This deleted everything!
```

### 4. **Retailer ID Mapping**
Ensure correct retailer IDs in database:
- Super-Pharm: 52
- Be Pharm: 150
- Good Pharm: 97

## Data Quality Expectations

### From Commercial Sites (Canonical)
- **Super-Pharm**: ~15,000 active products with images
- **Be Pharm**: ~9,000 active products (estimated)
- **Good Pharm**: ~10,000 active products (estimated)

### From Transparency Portals (Prices)
- **Super-Pharm**: ~15,476 products × ~180 stores = ~2.8M price points
- **Be Pharm**: ~9,099 products × ~150 stores = ~1.4M price points
- **Good Pharm**: ~10,491 products × ~220 stores = ~2.3M price points
- **Total**: ~6.5M price points across 550+ stores

## User Experience Flow

1. **User searches**: "Advil"
2. **System returns**: Canonical product (clean name + image from commercial site)
3. **User sees**: "Advil Liqui-Gels 200mg" with product image
4. **System calculates**: Nearest stores with best prices (from transparency data)
5. **User gets**: Clean UI with professional product presentation + accurate real-time pricing

## Implementation Status & Next Steps

### Current Status ✅/⚠️

| Retailer | Commercial Scraper | Transparency ETL | Product Matching | Data Quality |
|----------|-------------------|------------------|------------------|--------------|
| Super-Pharm | ✅ Working (`super_pharm_updater.py`) | ✅ Fixed (`super_pharm_transparency_etl_FIXED.py`) | ⚠️ Partial | 175,535 prices (12.2/product) |
| Be Pharm | ⚠️ Needs implementation | ✅ Fixed (`be_good_pharm_etl_FIXED.py`) | ⚠️ Needs linking | 33,843 prices (0.8/product - needs re-run) |
| Good Pharm | ⚠️ Needs implementation | ✅ Working (`be_good_pharm_etl_FIXED.py`) | ⚠️ Needs linking | 271,895 prices (22.2/product) |

### Priority Implementation Order

1. **Complete Super-Pharm Transparency ETL**
   - Portal: `https://prices.super-pharm.co.il/`
   - Method: XML downloads similar to Shufersal format
   - Link to existing canonical products from commercial scraper

2. **Implement Be Pharm Commercial Scraper**
   - Site: `https://www.bepharm.co.il/`
   - Extract: Product names, images, descriptions
   - Store as canonical products

3. **Implement Good Pharm Commercial Scraper**
   - Site: `https://www.goodpharm.co.il/`
   - Extract: Product names, images, descriptions
   - Store as canonical products

4. **Run Product Matching Pipeline**
   - Use `06_product_matching/unified_product_matcher.py`
   - Match all transparency data to canonical products
   - Review unmatched products manually

5. **Data Quality Validation**
   - Ensure all canonical products have images
   - Verify price coverage across all stores
   - Check matching confidence scores

6. **API & Frontend Testing**
   - Verify search returns canonical data
   - Test location-based pricing
   - Ensure images display correctly

## Common Pitfalls to Avoid

1. **Don't display transparency portal product names** - they're often truncated/ugly
2. **Don't lose barcode data** - it's the best matching key
3. **Don't assume one source has everything** - commercial lacks prices, transparency lacks images
4. **Don't mix retailer chain IDs** - Be Pharm ≠ Shufersal even if sharing portal
5. **Don't forget store-level pricing** - each store has different prices

This dual-source architecture ensures users see professional product presentations while getting comprehensive, accurate pricing data for their specific location.