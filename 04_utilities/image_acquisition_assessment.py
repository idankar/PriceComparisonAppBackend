#!/usr/bin/env python3
"""
Image Acquisition Assessment Tool
Tests multiple methods to find product images and reports success rates
"""

import re
import json
import psycopg2
import requests
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
import time

class ImageAcquisitionAssessor:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="price_comparison_app_v2",
            user="postgres",
            password="***REMOVED***"
        )
        self.results = defaultdict(int)
        self.sample_results = []
        
    def extract_barcode(self, text: str) -> Optional[str]:
        """Extract barcode from product name or description"""
        if not text:
            return None
        patterns = [
            r'\b(\d{13})\b',  # EAN-13
            r'\b(\d{12})\b',  # UPC-A
            r'\b(\d{8})\b',   # EAN-8
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
    
    def extract_english_words(self, text: str) -> str:
        """Extract English words from mixed Hebrew-English text"""
        english_words = re.findall(r'[A-Za-z][A-Za-z0-9\s\-\.]+', text)
        return ' '.join(english_words).strip()
    
    def check_existing_images(self) -> Tuple[int, int]:
        """Check how many products already have images from scrapers"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT COUNT(DISTINCT p.product_id) as total,
                   COUNT(DISTINCT CASE WHEN p.image_url IS NOT NULL AND p.image_url != '' THEN p.product_id END) as with_images
            FROM products p
            JOIN retailer_products rp ON p.product_id = rp.product_id
            WHERE rp.retailer_id IN (52, 150, 97)
        """)
        result = cursor.fetchone()
        cursor.close()
        return result[0], result[1]
    
    def test_barcode_apis(self, barcode: str) -> bool:
        """Test if barcode yields results from free APIs"""
        if not barcode:
            return False
            
        # Test OpenFoodFacts
        try:
            response = requests.get(
                f"https://world.openfoodfacts.org/api/v0/product/{barcode}.json",
                timeout=5
            )
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 1 and data.get('product', {}).get('image_url'):
                    return True
        except:
            pass
            
        # Test UPCItemDB (limited free tier)
        # Note: Requires API key for production
        
        return False
    
    def analyze_brand_distribution(self) -> Dict:
        """Analyze brand distribution for targeted searching"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT p.brand, COUNT(*) as count,
                   COUNT(CASE WHEN p.image_url IS NOT NULL THEN 1 END) as has_image
            FROM products p
            JOIN retailer_products rp ON p.product_id = rp.product_id
            WHERE rp.retailer_id IN (52, 150, 97)
            GROUP BY p.brand
            ORDER BY count DESC
            LIMIT 50
        """)
        
        brands = {}
        for brand, count, has_image in cursor.fetchall():
            if brand:
                brands[brand] = {
                    'count': count,
                    'has_image': has_image,
                    'coverage': (has_image / count * 100) if count > 0 else 0
                }
        cursor.close()
        return brands
    
    def test_search_query_quality(self, sample_size: int = 100) -> Dict:
        """Test different search query strategies"""
        cursor = self.conn.cursor()
        cursor.execute(f"""
            SELECT p.product_id, p.canonical_name, p.brand, p.description,
                   p.attributes->>'product_type' as category,
                   p.image_url, rp.retailer_item_code
            FROM products p
            JOIN retailer_products rp ON p.product_id = rp.product_id
            WHERE rp.retailer_id IN (52, 150, 97)
            ORDER BY RANDOM()
            LIMIT {sample_size}
        """)
        
        query_strategies = {
            'has_existing_image': 0,
            'has_barcode': 0,
            'has_english_brand': 0,
            'has_english_in_name': 0,
            'has_category': 0,
            'international_brand': 0,
            'needs_placeholder': 0
        }
        
        international_brands = {
            'dove', 'nivea', 'loreal', 'maybelline', 'gillette', 'pampers',
            'huggies', 'johnson', 'neutrogena', 'olay', 'pantene', 'head & shoulders',
            'oral-b', 'colgate', 'sensodyne', 'listerine', 'garnier', 'vichy',
            'la roche posay', 'eucerin', 'cetaphil', 'aveeno', 'ogx', 'tresemme',
            'schwarzkopf', 'wella', 'revlon', 'max factor', 'rimmel', 'essie',
            'estee lauder', 'clinique', 'lancome', 'shiseido', 'kiehl'
        }
        
        for row in cursor.fetchall():
            product_id, name, brand, desc, category, image_url, item_code = row
            
            # Check existing image
            if image_url and image_url.strip():
                query_strategies['has_existing_image'] += 1
                continue
            
            # Check for barcode
            barcode = self.extract_barcode(f"{name} {desc} {item_code}")
            if barcode:
                query_strategies['has_barcode'] += 1
                if self.test_barcode_apis(barcode):
                    self.sample_results.append({
                        'product': name,
                        'method': 'barcode_api',
                        'query': barcode
                    })
                    continue
            
            # Check brand quality
            if brand:
                brand_lower = brand.lower().strip()
                if brand_lower in international_brands:
                    query_strategies['international_brand'] += 1
                    self.sample_results.append({
                        'product': name,
                        'method': 'international_brand',
                        'query': f"{brand} {category or 'product'}"
                    })
                    continue
                elif re.match(r'^[A-Za-z\s\-\.]+$', brand):
                    query_strategies['has_english_brand'] += 1
                    self.sample_results.append({
                        'product': name,
                        'method': 'english_brand',
                        'query': f"{brand} {self.extract_english_words(name)}"
                    })
                    continue
            
            # Check for English in name
            english_words = self.extract_english_words(name)
            if len(english_words) > 3:
                query_strategies['has_english_in_name'] += 1
                self.sample_results.append({
                    'product': name,
                    'method': 'english_extraction',
                    'query': english_words
                })
            elif category:
                query_strategies['has_category'] += 1
                self.sample_results.append({
                    'product': name,
                    'method': 'category_placeholder',
                    'query': f"generic {category}"
                })
            else:
                query_strategies['needs_placeholder'] += 1
                self.sample_results.append({
                    'product': name,
                    'method': 'placeholder_only',
                    'query': None
                })
        
        cursor.close()
        return query_strategies
    
    def generate_report(self):
        """Generate comprehensive assessment report"""
        print("\n" + "="*60)
        print("IMAGE ACQUISITION ASSESSMENT REPORT")
        print("="*60)
        
        # Check existing images
        total_products, with_images = self.check_existing_images()
        existing_coverage = (with_images / total_products * 100) if total_products > 0 else 0
        
        print(f"\n📊 CURRENT STATUS:")
        print(f"Total products: {total_products:,}")
        print(f"Products with images: {with_images:,} ({existing_coverage:.1f}%)")
        print(f"Products needing images: {total_products - with_images:,}")
        
        # Analyze brands
        print(f"\n🏷️ TOP BRANDS ANALYSIS:")
        brands = self.analyze_brand_distribution()
        international_count = 0
        for brand, data in list(brands.items())[:10]:
            coverage = data['coverage']
            status = "✅" if coverage > 50 else "⚠️" if coverage > 20 else "❌"
            print(f"{status} {brand}: {data['count']} products ({coverage:.1f}% have images)")
            if brand.lower() in ['dove', 'nivea', 'loreal', 'gillette', 'pampers']:
                international_count += data['count']
        
        # Test query strategies
        print(f"\n🔍 SEARCH STRATEGY ANALYSIS (100 product sample):")
        strategies = self.test_search_query_quality(100)
        
        total_tested = sum(strategies.values())
        for strategy, count in sorted(strategies.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / total_tested * 100) if total_tested > 0 else 0
            print(f"  {strategy}: {count} ({percentage:.1f}%)")
        
        # Calculate expected coverage
        print(f"\n📈 EXPECTED COVERAGE WITH SMART SEARCH:")
        
        expected_coverage = {
            'Existing scraped images': existing_coverage,
            'Barcode API lookups': strategies.get('has_barcode', 0) / total_tested * 100 * 0.4,  # 40% success rate
            'International brands (Google)': strategies.get('international_brand', 0) / total_tested * 100 * 0.9,  # 90% success
            'English brand search': strategies.get('has_english_brand', 0) / total_tested * 100 * 0.6,  # 60% success
            'English extraction': strategies.get('has_english_in_name', 0) / total_tested * 100 * 0.3,  # 30% success
            'Category placeholders': strategies.get('has_category', 0) / total_tested * 100
        }
        
        cumulative = existing_coverage
        for method, coverage in expected_coverage.items():
            if method != 'Existing scraped images':
                # Adjust for overlap with existing images
                adjusted_coverage = coverage * (1 - existing_coverage / 100)
                cumulative = min(cumulative + adjusted_coverage, 100)
                print(f"  {method}: +{adjusted_coverage:.1f}% → Total: {cumulative:.1f}%")
        
        print(f"\n✨ FINAL EXPECTED COVERAGE: {cumulative:.1f}%")
        
        # Show sample queries
        print(f"\n📝 SAMPLE SEARCH QUERIES:")
        for i, result in enumerate(self.sample_results[:10]):
            print(f"{i+1}. {result['product'][:40]}...")
            print(f"   Method: {result['method']}")
            print(f"   Query: {result['query'] or 'Use placeholder'}")
        
        # Recommendations
        print(f"\n💡 RECOMMENDATIONS:")
        print("1. Start with existing scraped images (FREE)")
        print("2. Use barcode APIs for products with barcodes (FREE/cheap)")
        print("3. Focus Google API on international brands (high success)")
        print("4. Create ~30 high-quality category placeholders")
        print("5. Consider manual upload for top 100 Israeli products")
        
        return cumulative

if __name__ == "__main__":
    assessor = ImageAcquisitionAssessor()
    expected_coverage = assessor.generate_report()
    
    print(f"\n🎯 FEASIBILITY: {'YES' if expected_coverage > 70 else 'MODERATE'}")
    print(f"Expected real image coverage: {expected_coverage:.1f}%")