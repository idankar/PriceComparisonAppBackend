#!/usr/bin/env python3
"""
Product Matching Module for PharmMate
Matches products across different retailers using fuzzy matching and similarity scoring
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import re
from difflib import SequenceMatcher
import json

class ProductMatcher:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="price_comparison_app_v2",
            user="postgres",
            password="***REMOVED***"
        )
        
    def normalize_text(self, text):
        """Normalize Hebrew/English text for matching"""
        if not text:
            return ""
        # Remove extra spaces, lowercase
        text = re.sub(r'\s+', ' ', text.lower().strip())
        # Remove common filler words in Hebrew
        hebrew_stopwords = ['של', 'עם', 'את', 'על', 'מס', 'יח', "יח'"]
        for word in hebrew_stopwords:
            text = text.replace(f' {word} ', ' ')
        return text
    
    def extract_size(self, text):
        """Extract size/quantity from product name"""
        # Match patterns like: 100ml, 100 ml, 100 מ"ל, 500g, 24 יח
        patterns = [
            r'(\d+\.?\d*)\s*(ml|מ"ל|מל)',
            r'(\d+\.?\d*)\s*(mg|מ"ג|מג)',
            r'(\d+\.?\d*)\s*(g|גר|גרם)',
            r'(\d+\.?\d*)\s*(l|ליטר)',
            r'(\d+)\s*(יח|יחידות)',
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return f"{match.group(1)} {match.group(2)}"
        return None
    
    def calculate_similarity(self, prod1, prod2):
        """Calculate similarity score between two products"""
        score = 0.0
        
        # Brand match (40% weight)
        if prod1['brand'] and prod2['brand']:
            brand1 = self.normalize_text(prod1['brand'])
            brand2 = self.normalize_text(prod2['brand'])
            if brand1 == brand2:
                score += 0.4
            elif SequenceMatcher(None, brand1, brand2).ratio() > 0.8:
                score += 0.3
        
        # Name similarity (40% weight)
        name1 = self.normalize_text(prod1['canonical_name'])
        name2 = self.normalize_text(prod2['canonical_name'])
        name_similarity = SequenceMatcher(None, name1, name2).ratio()
        score += name_similarity * 0.4
        
        # Size match (20% weight)
        size1 = self.extract_size(prod1['canonical_name'])
        size2 = self.extract_size(prod2['canonical_name'])
        if size1 and size2 and size1 == size2:
            score += 0.2
        elif prod1.get('attributes') and prod2.get('attributes'):
            attr1 = prod1['attributes']
            attr2 = prod2['attributes']
            if (attr1.get('size_value') == attr2.get('size_value') and 
                attr1.get('size_unit') == attr2.get('size_unit')):
                score += 0.2
        
        return score
    
    def find_matches(self, min_similarity=0.75):
        """Find matching products across retailers"""
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        # Get all products grouped by retailer
        query = """
        SELECT 
            p.product_id,
            p.canonical_name,
            p.brand,
            p.attributes,
            rp.retailer_id,
            r.retailername
        FROM products p
        JOIN retailer_products rp ON p.product_id = rp.product_id
        JOIN retailers r ON rp.retailer_id = r.retailerid
        WHERE rp.retailer_id IN (52, 150, 97)
        ORDER BY p.brand, p.canonical_name
        """
        
        cur.execute(query)
        products = cur.fetchall()
        
        # Group products by retailer
        retailer_products = {}
        for prod in products:
            retailer_id = prod['retailer_id']
            if retailer_id not in retailer_products:
                retailer_products[retailer_id] = []
            retailer_products[retailer_id].append(prod)
        
        matches = []
        processed = set()
        
        # Compare products across retailers
        for r1_id, r1_products in retailer_products.items():
            for prod1 in r1_products:
                if prod1['product_id'] in processed:
                    continue
                    
                match_group = {
                    'products': [prod1],
                    'retailers': {r1_id: prod1},
                    'confidence': 1.0
                }
                
                # Check against other retailers
                for r2_id, r2_products in retailer_products.items():
                    if r2_id == r1_id:
                        continue
                        
                    best_match = None
                    best_score = 0
                    
                    for prod2 in r2_products:
                        if prod2['product_id'] in processed:
                            continue
                            
                        score = self.calculate_similarity(prod1, prod2)
                        if score > best_score and score >= min_similarity:
                            best_score = score
                            best_match = prod2
                    
                    if best_match:
                        match_group['products'].append(best_match)
                        match_group['retailers'][r2_id] = best_match
                        match_group['confidence'] = min(match_group['confidence'], best_score)
                        processed.add(best_match['product_id'])
                
                if len(match_group['retailers']) > 1:
                    matches.append(match_group)
                    processed.add(prod1['product_id'])
        
        return matches
    
    def create_match_table(self):
        """Create table to store product matches"""
        cur = self.conn.cursor()
        
        # Drop table if exists (for testing)
        cur.execute("DROP TABLE IF EXISTS product_matches")
        
        # Create matches table
        cur.execute("""
        CREATE TABLE product_matches (
            match_id SERIAL PRIMARY KEY,
            master_product_id INTEGER,
            product_ids TEXT,
            retailer_ids TEXT,
            match_confidence DECIMAL(3,2),
            match_data JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        self.conn.commit()
    
    def save_matches(self, matches):
        """Save matched products to database"""
        cur = self.conn.cursor()
        
        for match in matches:
            product_ids = [p['product_id'] for p in match['products']]
            retailer_ids = list(match['retailers'].keys())
            master_id = min(product_ids)  # Use lowest ID as master
            
            # Prepare match data
            match_data = {
                'products': [
                    {
                        'product_id': p['product_id'],
                        'name': p['canonical_name'],
                        'brand': p['brand'],
                        'retailer': p['retailername']
                    }
                    for p in match['products']
                ]
            }
            
            cur.execute("""
            INSERT INTO product_matches 
            (master_product_id, product_ids, retailer_ids, match_confidence, match_data)
            VALUES (%s, %s, %s, %s, %s)
            """, (master_id, ','.join(map(str, product_ids)), ','.join(map(str, retailer_ids)), match['confidence'], json.dumps(match_data)))
        
        self.conn.commit()
        print(f"Saved {len(matches)} product matches")
    
    def run_matching(self, save=True):
        """Run the complete matching process"""
        print("Starting product matching...")
        
        # Create table if needed
        if save:
            self.create_match_table()
        
        # Find matches
        matches = self.find_matches()
        print(f"Found {len(matches)} product groups with matches across retailers")
        
        # Show sample matches
        for match in matches[:5]:
            print(f"\nMatch (confidence: {match['confidence']:.2f}):")
            for prod in match['products']:
                print(f"  - {prod['retailername']}: {prod['canonical_name']} ({prod['brand']})")
        
        # Save to database
        if save and matches:
            self.save_matches(matches)
        
        return matches
    
    def close(self):
        """Close database connection"""
        self.conn.close()


if __name__ == "__main__":
    matcher = ProductMatcher()
    try:
        matches = matcher.run_matching(save=True)
        print(f"\nTotal matches found: {len(matches)}")
        
        # Statistics
        total_products_matched = sum(len(m['products']) for m in matches)
        print(f"Total products matched: {total_products_matched}")
        print(f"Average products per match: {total_products_matched/len(matches):.1f}")
        
    finally:
        matcher.close()