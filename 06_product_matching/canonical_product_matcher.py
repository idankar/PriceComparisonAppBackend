#!/usr/bin/env python3
"""
Canonical Product Matching System
Creates canonical products that unify commercial and transparency listings
Uses barcode matching as primary method, with fuzzy matching as fallback
"""

import psycopg2
import json
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple
from fuzzywuzzy import fuzz
import hashlib
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ProductListing:
    """Represents any product listing (commercial or transparency)"""
    listing_id: str
    source_type: str  # 'commercial' or 'transparency'
    retailer: str
    name: str
    brand: str
    barcode: Optional[str]
    price: float
    image_url: Optional[str]
    category: Optional[str]
    attributes: dict

@dataclass  
class CanonicalProduct:
    """Master product that multiple listings map to"""
    canonical_id: str
    canonical_name: str
    canonical_brand: str
    primary_barcode: Optional[str]
    category: str
    image_url: str
    min_price: float
    max_price: float
    avg_price: float
    listing_count: int
    retailer_coverage: List[str]

class CanonicalProductMatcher:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="price_comparison_app_v2",
            user="postgres",
            password="025655358"
        )
        self.canonical_products = {}
        self.barcode_to_canonical = {}
        self.listing_to_canonical = {}
        
    def create_canonical_products_table(self):
        """Create table for canonical products"""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS canonical_products (
                canonical_id VARCHAR(255) PRIMARY KEY,
                canonical_name TEXT NOT NULL,
                canonical_brand VARCHAR(255),
                primary_barcode VARCHAR(255),
                category VARCHAR(255),
                image_url TEXT,
                attributes JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_canonical_barcode ON canonical_products(primary_barcode);
            CREATE INDEX IF NOT EXISTS idx_canonical_brand ON canonical_products(canonical_brand);
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS listing_to_canonical (
                listing_id VARCHAR(255) PRIMARY KEY,
                canonical_id VARCHAR(255) REFERENCES canonical_products(canonical_id),
                source_type VARCHAR(50),
                retailer VARCHAR(255),
                confidence_score FLOAT,
                match_method VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_listing_canonical ON listing_to_canonical(canonical_id);
        """)
        
        self.conn.commit()
        cursor.close()
        logger.info("Created canonical products tables")
    
    def load_all_listings(self) -> List[ProductListing]:
        """Load all product listings from both sources"""
        listings = []
        cursor = self.conn.cursor()
        
        # Load transparency data with barcodes
        cursor.execute("""
            SELECT DISTINCT
                p.product_id::text,
                p.canonical_name,
                p.brand,
                p.attributes->>'barcode' as barcode,
                p.attributes->>'ean' as ean,
                MIN(pr.price) as min_price,
                r.retailername,
                p.attributes
            FROM products p
            JOIN retailer_products rp ON p.product_id = rp.product_id
            JOIN prices pr ON rp.retailer_product_id = pr.retailer_product_id
            JOIN retailers r ON rp.retailer_id = r.retailerid
            WHERE r.retailerid IN (52, 97, 150)  -- Pharmacy retailers
            GROUP BY p.product_id, p.canonical_name, p.brand, p.attributes, r.retailername
        """)
        
        for row in cursor.fetchall():
            listing = ProductListing(
                listing_id=f"trans_{row[0]}",
                source_type='transparency',
                retailer=row[6],
                name=row[1],
                brand=row[2] or '',
                barcode=row[3] or row[4],  # Use barcode or ean
                price=float(row[5]) if row[5] else 0,
                image_url=None,
                category=None,
                attributes=row[7] or {}
            )
            listings.append(listing)
        
        # Load commercial data from Super-Pharm scraper
        try:
            with open('/Users/noa/Desktop/PriceComparisonApp/04_utilities/superpharm_products_final.jsonl', 'r') as f:
                for line in f:
                    data = json.loads(line.strip())
                    listing = ProductListing(
                        listing_id=f"comm_{data.get('productId', '')}",
                        source_type='commercial',
                        retailer='Super-Pharm',
                        name=data.get('name', ''),
                        brand=data.get('brand', ''),
                        barcode=data.get('ean', ''),
                        price=float(str(data.get('price', 0) or 0).replace(',', '')),
                        image_url=data.get('imageUrl', ''),
                        category=self.extract_category(data.get('scrapedFrom', '')),
                        attributes={}
                    )
                    listings.append(listing)
        except Exception as e:
            logger.warning(f"Could not load commercial data: {e}")
        
        logger.info(f"Loaded {len(listings)} total listings")
        return listings
    
    def extract_category(self, url: str) -> str:
        """Extract category from URL"""
        category_map = {
            '15170000': 'Hair Care',
            '15160000': 'Oral Hygiene',
            '15150000': 'Deodorants',
            '15140000': 'Shaving',
            '15120000': 'Bath & Hygiene',
            '25130000': 'Baby Care',
            '30140000': 'Medicines',
            '30300000': 'Supplements'
        }
        
        for code, cat in category_map.items():
            if code in url:
                return cat
        return 'Other'
    
    def generate_canonical_id(self, brand: str, name: str, barcode: str = None) -> str:
        """Generate unique ID for canonical product"""
        if barcode:
            return f"canon_{barcode}"
        else:
            text = f"{brand}_{name}".lower().replace(' ', '_')
            return f"canon_{hashlib.md5(text.encode()).hexdigest()[:12]}"
    
    def create_canonical_products(self, listings: List[ProductListing]):
        """Create canonical products from listings"""
        
        # Phase 1: Group by barcode
        barcode_groups = {}
        no_barcode_listings = []
        
        for listing in listings:
            if listing.barcode and len(listing.barcode) >= 8:
                if listing.barcode not in barcode_groups:
                    barcode_groups[listing.barcode] = []
                barcode_groups[listing.barcode].append(listing)
            else:
                no_barcode_listings.append(listing)
        
        logger.info(f"Found {len(barcode_groups)} unique barcodes")
        
        # Create canonical products from barcode groups
        for barcode, group in barcode_groups.items():
            canonical_id = self.generate_canonical_id(
                group[0].brand, 
                group[0].name, 
                barcode
            )
            
            # Use best data from group
            best_listing = self.select_best_listing(group)
            
            canonical = CanonicalProduct(
                canonical_id=canonical_id,
                canonical_name=best_listing.name,
                canonical_brand=best_listing.brand,
                primary_barcode=barcode,
                category=best_listing.category or 'Other',
                image_url=best_listing.image_url or '',
                min_price=min(l.price for l in group if l.price > 0),
                max_price=max(l.price for l in group if l.price > 0),
                avg_price=sum(l.price for l in group if l.price > 0) / len([l for l in group if l.price > 0]),
                listing_count=len(group),
                retailer_coverage=list(set(l.retailer for l in group))
            )
            
            self.canonical_products[canonical_id] = canonical
            self.barcode_to_canonical[barcode] = canonical_id
            
            # Map listings to canonical
            for listing in group:
                self.listing_to_canonical[listing.listing_id] = (canonical_id, 1.0, 'barcode')
        
        # Phase 2: Group remaining by brand for faster fuzzy matching
        logger.info(f"Processing {len(no_barcode_listings)} listings without barcodes")
        
        # Group by brand to reduce search space
        brand_groups = {}
        for listing in no_barcode_listings:
            brand_key = listing.brand.lower() if listing.brand else 'unknown'
            if brand_key not in brand_groups:
                brand_groups[brand_key] = []
            brand_groups[brand_key].append(listing)
        
        # Process each brand group
        for brand_key, brand_listings in brand_groups.items():
            # Get canonical products for this brand
            brand_canonicals = {
                cid: cp for cid, cp in self.canonical_products.items()
                if cp.canonical_brand.lower() == brand_key
            }
            
            for listing in brand_listings[:100]:  # Limit to first 100 per brand for speed
                if brand_canonicals:
                    # Try to match within same brand
                    best_match = None
                    best_score = 0
                    
                    for canonical_id, canonical in brand_canonicals.items():
                        name_score = fuzz.token_sort_ratio(
                            listing.name.lower(), 
                            canonical.canonical_name.lower()
                        )
                        
                        if name_score > best_score and name_score > 85:
                            best_score = name_score
                            best_match = (canonical_id, name_score / 100, 'fuzzy')
                    
                    if best_match:
                        canonical_id, score, method = best_match
                        self.listing_to_canonical[listing.listing_id] = (canonical_id, score, method)
                        
                        # Update canonical product stats
                        canonical = self.canonical_products[canonical_id]
                        if listing.price > 0:
                            prices = [canonical.min_price, canonical.max_price, listing.price]
                            canonical.min_price = min(prices)
                            canonical.max_price = max(prices)
                        canonical.listing_count += 1
                        if listing.retailer not in canonical.retailer_coverage:
                            canonical.retailer_coverage.append(listing.retailer)
                        continue
                
                # Create new canonical product if no match
                canonical_id = self.generate_canonical_id(listing.brand, listing.name)
                
                canonical = CanonicalProduct(
                    canonical_id=canonical_id,
                    canonical_name=listing.name,
                    canonical_brand=listing.brand,
                    primary_barcode=None,
                    category=listing.category or 'Other',
                    image_url=listing.image_url or '',
                    min_price=listing.price if listing.price > 0 else 0,
                    max_price=listing.price if listing.price > 0 else 0,
                    avg_price=listing.price if listing.price > 0 else 0,
                    listing_count=1,
                    retailer_coverage=[listing.retailer]
                )
                
                self.canonical_products[canonical_id] = canonical
                brand_canonicals[canonical_id] = canonical  # Add to brand group
                self.listing_to_canonical[listing.listing_id] = (canonical_id, 1.0, 'new')
        
        logger.info(f"Created {len(self.canonical_products)} canonical products")
    
    def select_best_listing(self, listings: List[ProductListing]) -> ProductListing:
        """Select best listing from group (prefer commercial for images)"""
        # Prefer commercial listings for better data quality
        commercial = [l for l in listings if l.source_type == 'commercial']
        if commercial:
            return commercial[0]
        return listings[0]
    
    def find_best_canonical_match(self, listing: ProductListing) -> Optional[Tuple[str, float, str]]:
        """Find best matching canonical product using fuzzy matching"""
        best_match = None
        best_score = 0
        
        for canonical_id, canonical in self.canonical_products.items():
            # Brand must match reasonably well
            brand_score = fuzz.ratio(listing.brand.lower(), canonical.canonical_brand.lower())
            if brand_score < 70:
                continue
            
            # Name matching
            name_score = fuzz.token_sort_ratio(listing.name.lower(), canonical.canonical_name.lower())
            
            # Combined score
            combined_score = (brand_score * 0.4) + (name_score * 0.6)
            
            if combined_score > best_score and combined_score > 85:
                best_score = combined_score
                best_match = (canonical_id, combined_score / 100, 'fuzzy')
        
        return best_match
    
    def save_to_database(self):
        """Save canonical products and mappings to database"""
        cursor = self.conn.cursor()
        
        # Save canonical products
        for canonical in self.canonical_products.values():
            cursor.execute("""
                INSERT INTO canonical_products 
                (canonical_id, canonical_name, canonical_brand, primary_barcode, 
                 category, image_url, attributes)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (canonical_id) DO UPDATE SET
                    canonical_name = EXCLUDED.canonical_name,
                    image_url = EXCLUDED.image_url,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                canonical.canonical_id,
                canonical.canonical_name,
                canonical.canonical_brand,
                canonical.primary_barcode,
                canonical.category,
                canonical.image_url,
                json.dumps({
                    'min_price': canonical.min_price,
                    'max_price': canonical.max_price,
                    'avg_price': canonical.avg_price,
                    'listing_count': canonical.listing_count,
                    'retailer_coverage': canonical.retailer_coverage
                })
            ))
        
        # Save mappings
        for listing_id, (canonical_id, score, method) in self.listing_to_canonical.items():
            source_type = 'commercial' if listing_id.startswith('comm_') else 'transparency'
            
            cursor.execute("""
                INSERT INTO listing_to_canonical
                (listing_id, canonical_id, source_type, confidence_score, match_method)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (listing_id) DO UPDATE SET
                    canonical_id = EXCLUDED.canonical_id,
                    confidence_score = EXCLUDED.confidence_score
            """, (listing_id, canonical_id, source_type, score, method))
        
        self.conn.commit()
        cursor.close()
        logger.info("Saved canonical products to database")
    
    def print_statistics(self):
        """Print matching statistics"""
        print("\n" + "=" * 60)
        print("CANONICAL PRODUCT MATCHING RESULTS")
        print("=" * 60)
        
        # Count by match method
        methods = {}
        for _, (_, _, method) in self.listing_to_canonical.items():
            methods[method] = methods.get(method, 0) + 1
        
        print(f"Total canonical products: {len(self.canonical_products)}")
        print(f"Total listings mapped: {len(self.listing_to_canonical)}")
        print("\nMatch methods:")
        for method, count in methods.items():
            print(f"  {method}: {count}")
        
        # Products with images
        with_images = sum(1 for c in self.canonical_products.values() if c.image_url)
        print(f"\nCanonical products with images: {with_images} ({with_images/len(self.canonical_products)*100:.1f}%)")
        
        # Multi-retailer products
        multi_retailer = sum(1 for c in self.canonical_products.values() if len(c.retailer_coverage) > 1)
        print(f"Products available at multiple retailers: {multi_retailer}")
        
        # Sample products
        print("\nSample canonical products with good coverage:")
        samples = sorted(self.canonical_products.values(), 
                        key=lambda x: x.listing_count, reverse=True)[:5]
        
        for i, canonical in enumerate(samples, 1):
            print(f"\n{i}. {canonical.canonical_name} ({canonical.canonical_brand})")
            print(f"   Barcode: {canonical.primary_barcode or 'None'}")
            print(f"   Listings: {canonical.listing_count}")
            print(f"   Price range: ₪{canonical.min_price:.2f} - ₪{canonical.max_price:.2f}")
            print(f"   Retailers: {', '.join(canonical.retailer_coverage)}")
            print(f"   Has image: {'Yes' if canonical.image_url else 'No'}")
    
    def run_matching_pipeline(self):
        """Run the complete matching pipeline"""
        logger.info("Starting canonical product matching pipeline...")
        
        # Create tables
        self.create_canonical_products_table()
        
        # Load all listings
        listings = self.load_all_listings()
        
        # Create canonical products
        self.create_canonical_products(listings)
        
        # Save to database
        self.save_to_database()
        
        # Print statistics
        self.print_statistics()
        
        logger.info("Matching pipeline complete!")

if __name__ == "__main__":
    matcher = CanonicalProductMatcher()
    matcher.run_matching_pipeline()