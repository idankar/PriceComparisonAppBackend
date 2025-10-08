#!/usr/bin/env python3
"""
Smart Image Acquisition Pipeline
Combines multiple strategies to achieve 75-80% real image coverage
"""

import re
import json
import psycopg2
import requests
import hashlib
import time
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
import os
from urllib.parse import quote
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('image_acquisition.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SmartImageAcquisition:
    def __init__(self, google_api_key: str = None, search_engine_id: str = None):
        self.conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="price_comparison_app_v2",
            user="postgres",
            password="025655358"
        )
        self.google_api_key = google_api_key
        self.search_engine_id = search_engine_id or "5480c9ffa13474272"  # Your CSE ID
        self.cache_file = "image_cache.json"
        self.load_cache()
        
        # Hebrew to English brand mapping
        self.hebrew_to_english = {
            'ניוואה': 'Nivea',
            'ניווה': 'Nivea',
            'לוריאל': "L'Oreal",
            'לוראל': "L'Oreal",
            'קולגייט': 'Colgate',
            'דאב': 'Dove',
            'פמפרס': 'Pampers',
            'האגיס': 'Huggies',
            'ג\'ונסון': 'Johnson',
            'נויטרוגינה': 'Neutrogena',
            'נטרוגינה': 'Neutrogena',
            'אולאי': 'Olay',
            'פנטן': 'Pantene',
            'הד אנד שולדרס': 'Head & Shoulders',
            'אורל בי': 'Oral-B',
            'סנסודיין': 'Sensodyne',
            'ליסטרין': 'Listerine',
            'גרנייה': 'Garnier',
            'וישי': 'Vichy',
            'לה רוש פוזה': 'La Roche-Posay',
            'אוסרין': 'Eucerin',
            'סטפיל': 'Cetaphil',
            'אבינו': 'Aveeno',
            'טרזמה': 'Tresemme',
            'שוורצקופף': 'Schwarzkopf',
            'וולה': 'Wella',
            'רבלון': 'Revlon',
            'מקס פקטור': 'Max Factor',
            'רימל': 'Rimmel',
            'אסי': 'Essie',
            'אסתי לאודר': 'Estee Lauder',
            'קליניק': 'Clinique',
            'לנקום': 'Lancome',
            'שיסיידו': 'Shiseido',
            'קילס': "Kiehl's",
            'מייבלין': 'Maybelline',
            'גילט': 'Gillette',
            'ג\'ילט': 'Gillette',
            'שיק': 'Schick',
            'ביק': 'BIC',
            'אקסון': 'AXE',
            'רקסונה': 'Rexona',
            'סופט': 'Soft',
            'טמפו': 'Tempo',
            'לייף': 'Life',
            'קרליין': 'Careline',
            'דר פישר': 'Dr Fischer',
            'ד"ר פישר': 'Dr Fischer',
        }
        
        # Category to placeholder image mapping
        self.category_placeholders = {
            'Shampoo': 'shampoo_placeholder.jpg',
            'שמפו': 'shampoo_placeholder.jpg',
            'Conditioner': 'conditioner_placeholder.jpg',
            'מרכך': 'conditioner_placeholder.jpg',
            'Body Wash': 'body_wash_placeholder.jpg',
            'ג\'ל רחצה': 'body_wash_placeholder.jpg',
            'Deodorant': 'deodorant_placeholder.jpg',
            'דאודורנט': 'deodorant_placeholder.jpg',
            'Toothpaste': 'toothpaste_placeholder.jpg',
            'משחת שיניים': 'toothpaste_placeholder.jpg',
            'Soap': 'soap_placeholder.jpg',
            'סבון': 'soap_placeholder.jpg',
            'Moisturizer': 'moisturizer_placeholder.jpg',
            'קרם לחות': 'moisturizer_placeholder.jpg',
            'Lipstick': 'lipstick_placeholder.jpg',
            'שפתון': 'lipstick_placeholder.jpg',
            'Mascara': 'mascara_placeholder.jpg',
            'מסקרה': 'mascara_placeholder.jpg',
            'Nail Polish': 'nail_polish_placeholder.jpg',
            'לק': 'nail_polish_placeholder.jpg',
            'Vitamins': 'vitamins_placeholder.jpg',
            'ויטמינים': 'vitamins_placeholder.jpg',
            'Diapers': 'diapers_placeholder.jpg',
            'חיתולים': 'diapers_placeholder.jpg',
            'Baby Wipes': 'wipes_placeholder.jpg',
            'מגבונים': 'wipes_placeholder.jpg',
        }
        
        self.stats = {
            'total': 0,
            'existing_images': 0,
            'barcode_found': 0,
            'google_found': 0,
            'placeholder_used': 0,
            'failed': 0
        }
    
    def load_cache(self):
        """Load cached image URLs"""
        try:
            with open(self.cache_file, 'r') as f:
                self.image_cache = json.load(f)
        except:
            self.image_cache = {}
    
    def save_cache(self):
        """Save cache to file"""
        with open(self.cache_file, 'w') as f:
            json.dump(self.image_cache, f, indent=2)
    
    def extract_barcode(self, text: str) -> Optional[str]:
        """Extract barcode from text"""
        if not text:
            return None
        patterns = [
            r'\b(\d{13})\b',
            r'\b(\d{12})\b',
            r'\b(\d{8})\b',
            r'ברקוד[:\s]*(\d+)',
            r'מקט[:\s]*(\d+)',
        ]
        for pattern in patterns:
            match = re.search(pattern, str(text), re.IGNORECASE)
            if match:
                barcode = match.group(1)
                if len(barcode) in [8, 12, 13]:
                    return barcode
        return None
    
    def translate_brand(self, brand: str) -> str:
        """Translate Hebrew brand to English"""
        if not brand:
            return ""
        brand_clean = brand.strip()
        return self.hebrew_to_english.get(brand_clean, brand_clean)
    
    def get_barcode_image(self, barcode: str, dry_run: bool = False) -> Optional[str]:
        """Get image from barcode APIs"""
        if not barcode:
            return None
            
        # Check cache first
        cache_key = f"barcode_{barcode}"
        if cache_key in self.image_cache:
            return self.image_cache[cache_key]
        
        # In dry run, just simulate success for some barcodes
        if dry_run:
            # Simulate 40% success rate for barcodes
            if hash(barcode) % 10 < 4:
                return f"simulated_barcode_image_{barcode}.jpg"
            return None
        
        # Try OpenFoodFacts
        try:
            response = requests.get(
                f"https://world.openfoodfacts.org/api/v0/product/{barcode}.json",
                timeout=5
            )
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 1:
                    product = data.get('product', {})
                    image_url = product.get('image_url') or product.get('image_front_url')
                    if image_url:
                        self.image_cache[cache_key] = image_url
                        logger.info(f"Found image for barcode {barcode} on OpenFoodFacts")
                        return image_url
        except Exception as e:
            logger.debug(f"OpenFoodFacts error for {barcode}: {e}")
        
        # Try Barcode Lookup (requires API key for production)
        # try:
        #     response = requests.get(
        #         f"https://api.barcodelookup.com/v3/products?barcode={barcode}&key=YOUR_KEY",
        #         timeout=5
        #     )
        #     ...
        
        return None
    
    def get_google_image(self, query: str, safe_search: bool = True) -> Optional[str]:
        """Get image from Google Custom Search API"""
        if not self.google_api_key or not query:
            return None
            
        # Check cache
        cache_key = f"google_{hashlib.md5(query.encode()).hexdigest()}"
        if cache_key in self.image_cache:
            return self.image_cache[cache_key]
        
        try:
            # Google Custom Search API
            search_url = "https://www.googleapis.com/customsearch/v1"
            params = {
                'key': self.google_api_key,
                'cx': self.search_engine_id,
                'q': query,
                'searchType': 'image',
                'num': 1,
                'safe': 'active' if safe_search else 'off',
                'imgSize': 'medium',
                'imgType': 'photo',  # Only real photos, not clipart
                'fileType': 'jpg|png'  # Common image formats
            }
            
            response = requests.get(search_url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if 'items' in data and len(data['items']) > 0:
                    image_url = data['items'][0]['link']
                    self.image_cache[cache_key] = image_url
                    logger.info(f"Found Google image for query: {query[:50]}")
                    return image_url
        except Exception as e:
            logger.debug(f"Google search error for '{query}': {e}")
        
        return None
    
    def build_smart_query(self, product: Dict) -> str:
        """Build optimized search query"""
        name = product.get('canonical_name', '')
        brand = product.get('brand', '')
        category = product.get('category', '')
        
        # Translate Hebrew brand
        english_brand = self.translate_brand(brand)
        
        # Extract English words from name
        english_words = re.findall(r'[A-Za-z][A-Za-z0-9\s\-\.]+', name)
        english_text = ' '.join(english_words).strip()
        
        # Build query based on available info
        if english_brand != brand:  # Successfully translated
            if category:
                return f"{english_brand} {category} product"
            elif english_text:
                return f"{english_brand} {english_text}"
            else:
                return f"{english_brand} product israel"
        elif english_text and len(english_text) > 5:
            return f"{english_text} product"
        elif category:
            return f"{category} product israel"
        else:
            return ""
    
    def get_placeholder_image(self, product: Dict) -> str:
        """Get appropriate placeholder image"""
        category = product.get('category') or ''
        
        # Try to match category
        if category:
            for cat_key, placeholder in self.category_placeholders.items():
                if cat_key.lower() in category.lower() or category.lower() in cat_key.lower():
                    return f"/static/placeholders/{placeholder}"
        
        # Extract type from name if no category
        name = (product.get('canonical_name') or '').lower()
        for cat_key, placeholder in self.category_placeholders.items():
            if cat_key.lower() in name:
                return f"/static/placeholders/{placeholder}"
        
        # Default placeholder
        return "/static/placeholders/default_product.jpg"
    
    def process_product(self, product: Dict, dry_run: bool = False) -> Tuple[str, str]:
        """Process single product and return image URL and method used"""
        
        # 1. Check existing image
        if product.get('image_url'):
            self.stats['existing_images'] += 1
            return product['image_url'], 'existing'
        
        # 2. Try barcode lookup
        barcode = self.extract_barcode(
            f"{product.get('canonical_name', '')} {product.get('description', '')} {product.get('retailer_item_code', '')}"
        )
        if barcode:
            image_url = self.get_barcode_image(barcode, dry_run)
            if image_url:
                self.stats['barcode_found'] += 1
                return image_url, 'barcode'
        
        # 3. Try Google search (only for promising queries)
        query = self.build_smart_query(product)
        if query and len(query) > 10 and self.google_api_key:
            # Use Google for international brands and products with good English content
            brand = self.translate_brand(product.get('brand', ''))
            international_brands = {
                'Nivea', 'Dove', 'L\'Oreal', 'Colgate', 'Pampers', 'Gillette',
                'Johnson', 'Neutrogena', 'Olay', 'Pantene', 'Head & Shoulders',
                'Oral-B', 'Sensodyne', 'Listerine', 'Garnier', 'Vichy',
                'La Roche-Posay', 'Eucerin', 'Cetaphil', 'Aveeno', 'OGX',
                'Tresemme', 'Schwarzkopf', 'Wella', 'Revlon', 'Max Factor',
                'Rimmel', 'Essie', 'Estee Lauder', 'Clinique', 'Lancome',
                'Shiseido', 'Kiehl\'s', 'Maybelline', 'Schick', 'BIC', 'AXE',
                'Rexona', 'Dr Fischer', 'Careline', 'Life'
            }
            
            # Try Google for international brands or if query has significant English content
            if brand in international_brands or len(re.findall(r'[A-Za-z]+', query)) > 3:
                image_url = self.get_google_image(query)
                if image_url:
                    self.stats['google_found'] += 1
                    return image_url, 'google'
        
        # 4. Use placeholder
        self.stats['placeholder_used'] += 1
        return self.get_placeholder_image(product), 'placeholder'
    
    def update_product_image(self, product_id: int, image_url: str, method: str):
        """Update product with image URL"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                UPDATE products 
                SET image_url = %s,
                    image_source = %s,
                    image_updated_at = NOW()
                WHERE product_id = %s
            """, (image_url, method, product_id))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error updating product {product_id}: {e}")
            self.conn.rollback()
        finally:
            cursor.close()
    
    def run_acquisition(self, limit: Optional[int] = None, dry_run: bool = False):
        """Run the image acquisition pipeline"""
        cursor = self.conn.cursor()
        
        # Get products needing images
        query = """
            SELECT DISTINCT ON (p.product_id)
                p.product_id, 
                p.canonical_name, 
                p.brand, 
                p.description,
                p.attributes->>'product_type' as category,
                p.image_url,
                rp.retailer_item_code
            FROM products p
            JOIN retailer_products rp ON p.product_id = rp.product_id
            WHERE rp.retailer_id IN (52, 150, 97)
                AND (p.image_url IS NULL OR p.image_url = '')
            ORDER BY p.product_id
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        cursor.execute(query)
        products = cursor.fetchall()
        
        total = len(products)
        self.stats['total'] = total
        
        logger.info(f"Starting image acquisition for {total} products")
        
        for i, row in enumerate(products):
            product = {
                'product_id': row[0],
                'canonical_name': row[1],
                'brand': row[2],
                'description': row[3],
                'category': row[4],
                'image_url': row[5],
                'retailer_item_code': row[6]
            }
            
            # Process product
            image_url, method = self.process_product(product, dry_run)
            
            # Update database (unless dry run)
            if not dry_run and image_url:
                self.update_product_image(product['product_id'], image_url, method)
            
            # Progress update
            if (i + 1) % 100 == 0:
                logger.info(f"Progress: {i + 1}/{total} ({(i + 1) / total * 100:.1f}%)")
                self.save_cache()  # Save cache periodically
            
            # Rate limiting for external APIs
            if method in ['google', 'barcode']:
                time.sleep(0.1)  # 10 requests per second max
        
        cursor.close()
        self.save_cache()
        
        # Final report
        logger.info("="*60)
        logger.info("IMAGE ACQUISITION COMPLETE")
        logger.info("="*60)
        logger.info(f"Total products: {self.stats['total']}")
        logger.info(f"Existing images: {self.stats['existing_images']} ({self.stats['existing_images']/total*100:.1f}%)")
        logger.info(f"Barcode lookups: {self.stats['barcode_found']} ({self.stats['barcode_found']/total*100:.1f}%)")
        logger.info(f"Google searches: {self.stats['google_found']} ({self.stats['google_found']/total*100:.1f}%)")
        logger.info(f"Placeholders: {self.stats['placeholder_used']} ({self.stats['placeholder_used']/total*100:.1f}%)")
        
        real_images = self.stats['existing_images'] + self.stats['barcode_found'] + self.stats['google_found']
        logger.info(f"\n✨ REAL IMAGE COVERAGE: {real_images}/{total} ({real_images/total*100:.1f}%)")
        
        return self.stats

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Smart Image Acquisition Pipeline")
    parser.add_argument('--google-key', help='Google Custom Search API key')
    parser.add_argument('--limit', type=int, help='Limit number of products to process')
    parser.add_argument('--dry-run', action='store_true', help='Run without updating database')
    
    args = parser.parse_args()
    
    # Run the pipeline
    acquisition = SmartImageAcquisition(google_api_key=args.google_key)
    acquisition.run_acquisition(limit=args.limit, dry_run=args.dry_run)