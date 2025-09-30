#!/usr/bin/env python3
"""
Quick Product Database Recognition Test
Creates HTML visualization of test results with a smaller sample for faster execution
"""

import os
import json
import random
import base64
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple
import logging

# Import our recognition system
from recognition_scripts.fixed_product_recognition import (
    TrainingDataProcessor, 
    CLIPEncoder,
    TesseractOCR
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QuickProductTester:
    """Quick test product recognition across the database"""
    
    def __init__(self, training_data_path: str, image_directories: List[str]):
        self.training_data_path = training_data_path
        self.image_directories = image_directories
        self.processor = TrainingDataProcessor(training_data_path, image_directories)
        self.clip_encoder = CLIPEncoder()
        self.ocr = TesseractOCR()
        
    def select_test_products(self, num_products: int = 15) -> List[Dict]:
        """Select random products from different stores for testing"""
        # Load training data
        items = self.processor.load_training_data()
        if not items:
            logger.error("No training data loaded")
            return []
        
        # Filter items with valid image paths
        valid_items = [item for item in items if os.path.exists(item.get('local_image_path', ''))]
        logger.info(f"Found {len(valid_items)} items with valid image paths")
        
        # Group by store
        by_store = {}
        for item in valid_items:
            store = item.get('source_supermarket', 'unknown')
            if store not in by_store:
                by_store[store] = []
            by_store[store].append(item)
        
        # Select products from each store
        test_products = []
        products_per_store = max(1, num_products // len(by_store))
        
        for store, store_items in by_store.items():
            num_to_select = min(products_per_store, len(store_items))
            selected = random.sample(store_items, num_to_select)
            test_products.extend(selected)
            logger.info(f"Selected {num_to_select} products from {store}")
        
        return test_products[:num_products]
    
    def create_small_index(self, test_products: List[Dict], num_index_items: int = 200) -> List[Dict]:
        """Create a small index for similarity search from a subset of products"""
        # Load all training data
        items = self.processor.load_training_data()
        valid_items = [item for item in items if os.path.exists(item.get('local_image_path', ''))]
        
        # Randomly sample items for index (excluding test products)
        test_ids = {p['training_item_id'] for p in test_products}
        index_candidates = [item for item in valid_items if item['training_item_id'] not in test_ids]
        
        # Sample items for index
        index_items = random.sample(index_candidates, min(num_index_items, len(index_candidates)))
        
        logger.info(f"Creating index with {len(index_items)} items...")
        
        indexed_data = []
        for i, item in enumerate(index_items):
            if i % 20 == 0:
                logger.info(f"Processing index item {i+1}/{len(index_items)}")
            
            try:
                # Extract visual features
                visual_features = self.clip_encoder.encode_image(item['local_image_path'])
                
                # Extract OCR text
                ocr_result = self.ocr.extract_text(item['local_image_path'])
                
                indexed_data.append({
                    'training_item_id': item['training_item_id'],
                    'source_supermarket': item['source_supermarket'],
                    'text_for_embedding': item['text_for_embedding'],
                    'local_image_path': item['local_image_path'],
                    'visual_features': visual_features.tolist(),
                    'ocr_text': ocr_result.get('text', ''),
                    'ocr_confidence': ocr_result.get('confidence', 0)
                })
                
            except Exception as e:
                logger.error(f"Error processing {item['local_image_path']}: {e}")
                continue
        
        return indexed_data
    
    def find_similar_products(self, query_image: str, index_data: List[Dict], top_k: int = 5) -> List[Dict]:
        """Find similar products using the small index"""
        try:
            import numpy as np
            from sklearn.metrics.pairwise import cosine_similarity
            
            # Extract features from query image
            query_features = self.clip_encoder.encode_image(query_image)
            
            # Calculate similarities
            similarities = []
            for item in index_data:
                stored_features = np.array(item['visual_features'])
                similarity = cosine_similarity([query_features], [stored_features])[0][0]
                
                similarities.append({
                    'item': item,
                    'similarity': float(similarity)
                })
            
            # Sort by similarity and return top-k
            similarities.sort(key=lambda x: x['similarity'], reverse=True)
            return similarities[:top_k]
            
        except Exception as e:
            logger.error(f"Similarity search error: {e}")
            return []
    
    def run_ocr_test(self, image_path: str) -> Dict:
        """Run OCR test on a single image"""
        try:
            result = self.ocr.extract_text(image_path)
            return {
                'success': True,
                'text': result.get('text', ''),
                'confidence': result.get('confidence', 0),
                'word_count': result.get('word_count', 0),
                'method': result.get('method', ''),
                'words': result.get('words', [])
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'text': '',
                'confidence': 0,
                'word_count': 0,
                'method': 'error',
                'words': []
            }
    
    def image_to_base64(self, image_path: str) -> str:
        """Convert image to base64 for HTML embedding"""
        try:
            with open(image_path, 'rb') as f:
                image_data = f.read()
            return base64.b64encode(image_data).decode('utf-8')
        except Exception as e:
            logger.error(f"Error converting image to base64: {e}")
            return ""
    
    def generate_html_report(self, test_results: List[Dict], output_path: str = "quick_product_test_results.html"):
        """Generate comprehensive HTML report"""
        
        # Calculate statistics
        total_tests = len(test_results)
        successful_ocr = sum(1 for r in test_results if r['ocr_result']['success'])
        avg_ocr_confidence = sum(r['ocr_result']['confidence'] for r in test_results) / total_tests if total_tests > 0 else 0
        
        # Count by store
        store_stats = {}
        for result in test_results:
            store = result['product']['source_supermarket']
            if store not in store_stats:
                store_stats[store] = {'total': 0, 'successful_ocr': 0}
            store_stats[store]['total'] += 1
            if result['ocr_result']['success']:
                store_stats[store]['successful_ocr'] += 1
        
        html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Quick Product Database Recognition Test</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #2c3e50;
            text-align: center;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 30px 0;
        }}
        .stat-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
        }}
        .stat-number {{
            font-size: 2em;
            font-weight: bold;
            display: block;
        }}
        .test-item {{
            border: 1px solid #ddd;
            border-radius: 8px;
            margin: 20px 0;
            padding: 20px;
            background: white;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }}
        .test-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 2px solid #ecf0f1;
            padding-bottom: 15px;
            margin-bottom: 15px;
        }}
        .product-info {{
            flex: 1;
        }}
        .store-badge {{
            background: #3498db;
            color: white;
            padding: 5px 10px;
            border-radius: 20px;
            font-size: 0.9em;
            font-weight: bold;
        }}
        .test-content {{
            display: grid;
            grid-template-columns: 300px 1fr;
            gap: 20px;
            align-items: start;
        }}
        .image-section {{
            text-align: center;
        }}
        .test-image {{
            max-width: 100%;
            max-height: 250px;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.2);
        }}
        .results-section {{
            display: grid;
            gap: 15px;
        }}
        .result-card {{
            background: #f8f9fa;
            border-left: 4px solid #3498db;
            padding: 15px;
            border-radius: 5px;
        }}
        .result-title {{
            font-weight: bold;
            color: #2c3e50;
            margin-bottom: 8px;
        }}
        .confidence-bar {{
            width: 100%;
            height: 20px;
            background: #ecf0f1;
            border-radius: 10px;
            overflow: hidden;
            margin: 5px 0;
        }}
        .confidence-fill {{
            height: 100%;
            border-radius: 10px;
            transition: width 0.3s ease;
        }}
        .confidence-high {{ background: linear-gradient(90deg, #2ecc71, #27ae60); }}
        .confidence-medium {{ background: linear-gradient(90deg, #f39c12, #e67e22); }}
        .confidence-low {{ background: linear-gradient(90deg, #e74c3c, #c0392b); }}
        .similarity-results {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 15px;
            margin-top: 15px;
        }}
        .similarity-item {{
            border: 1px solid #ddd;
            border-radius: 5px;
            padding: 10px;
            text-align: center;
            background: white;
        }}
        .similarity-image {{
            max-width: 100%;
            max-height: 120px;
            border-radius: 5px;
        }}
        .similarity-score {{
            font-weight: bold;
            color: #3498db;
            margin: 5px 0;
        }}
        .ocr-words {{
            display: flex;
            flex-wrap: wrap;
            gap: 5px;
            margin-top: 10px;
        }}
        .ocr-word {{
            background: #e8f4fd;
            color: #2980b9;
            padding: 3px 8px;
            border-radius: 15px;
            font-size: 0.9em;
        }}
        .error {{
            background: #ffebee;
            border-left-color: #f44336;
            color: #c62828;
        }}
        .success {{
            background: #e8f5e8;
            border-left-color: #4caf50;
        }}
        .store-stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }}
        .store-stat {{
            background: white;
            border: 1px solid #ddd;
            border-radius: 8px;
            padding: 15px;
            text-align: center;
        }}
        .store-name {{
            font-weight: bold;
            color: #2c3e50;
            margin-bottom: 10px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🛒 Quick Product Database Recognition Test</h1>
        <p style="text-align: center; color: #7f8c8d; font-size: 1.1em;">
            Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </p>
        
        <div class="stats-grid">
            <div class="stat-card">
                <span class="stat-number">{total_tests}</span>
                <span>Products Tested</span>
            </div>
            <div class="stat-card">
                <span class="stat-number">{successful_ocr}</span>
                <span>Successful OCR ({successful_ocr/total_tests*100:.1f}%)</span>
            </div>
            <div class="stat-card">
                <span class="stat-number">{avg_ocr_confidence:.1f}%</span>
                <span>Avg OCR Confidence</span>
            </div>
            <div class="stat-card">
                <span class="stat-number">{len(store_stats)}</span>
                <span>Stores Tested</span>
            </div>
        </div>
        
        <h2>📊 Results by Store</h2>
        <div class="store-stats">
"""
        
        for store, stats in store_stats.items():
            success_rate = stats['successful_ocr'] / stats['total'] * 100 if stats['total'] > 0 else 0
            html_content += f"""
            <div class="store-stat">
                <div class="store-name">{store}</div>
                <div>{stats['successful_ocr']}/{stats['total']} successful</div>
                <div>({success_rate:.1f}% success rate)</div>
            </div>
"""
        
        html_content += """
        </div>
        
        <h2>🔍 Individual Test Results</h2>
"""
        
        # Individual test results
        for i, result in enumerate(test_results, 1):
            product = result['product']
            ocr_result = result['ocr_result']
            similarity_results = result.get('similarity_results', [])
            
            # Convert image to base64
            image_b64 = self.image_to_base64(product['local_image_path'])
            
            # Determine confidence class
            confidence = ocr_result.get('confidence', 0)
            if confidence >= 70:
                conf_class = "confidence-high"
            elif confidence >= 40:
                conf_class = "confidence-medium"
            else:
                conf_class = "confidence-low"
            
            # OCR result class
            ocr_class = "success" if ocr_result['success'] else "error"
            
            html_content += f"""
        <div class="test-item">
            <div class="test-header">
                <div class="product-info">
                    <h3>Test #{i}: {product.get('training_item_id', 'Unknown ID')}</h3>
                    <p><strong>Description:</strong> {product.get('text_for_embedding', 'No description')}</p>
                    <p><strong>Product ID:</strong> {product.get('original_store_product_id', 'Unknown')}</p>
                </div>
                <div class="store-badge">{product.get('source_supermarket', 'Unknown Store')}</div>
            </div>
            
            <div class="test-content">
                <div class="image-section">
                    <img src="data:image/jpeg;base64,{image_b64}" alt="Product Image" class="test-image">
                    <p style="margin-top: 10px; font-size: 0.9em; color: #7f8c8d;">
                        {os.path.basename(product['local_image_path'])}
                    </p>
                </div>
                
                <div class="results-section">
                    <div class="result-card {ocr_class}">
                        <div class="result-title">🔤 OCR Results</div>
                        <p><strong>Status:</strong> {'✅ Success' if ocr_result['success'] else '❌ Failed'}</p>
                        <p><strong>Confidence:</strong> {confidence:.1f}%</p>
                        <div class="confidence-bar">
                            <div class="confidence-fill {conf_class}" style="width: {confidence}%"></div>
                        </div>
                        <p><strong>Method:</strong> {ocr_result.get('method', 'Unknown')}</p>
                        <p><strong>Extracted Text:</strong></p>
                        <div style="background: #f1f2f6; padding: 10px; border-radius: 5px; font-family: monospace;">
                            {ocr_result.get('text', 'No text extracted') or 'No text extracted'}
                        </div>
"""
            
            if ocr_result.get('words'):
                html_content += f"""
                        <p><strong>Detected Words ({len(ocr_result['words'])}):</strong></p>
                        <div class="ocr-words">
"""
                for word in ocr_result['words'][:15]:  # Limit to first 15 words
                    html_content += f'<span class="ocr-word">{word}</span>'
                
                if len(ocr_result['words']) > 15:
                    html_content += f'<span class="ocr-word">+{len(ocr_result["words"]) - 15} more...</span>'
                
                html_content += """
                        </div>
"""
            
            html_content += """
                    </div>
"""
            
            # Similarity results if available
            if similarity_results:
                html_content += f"""
                    <div class="result-card">
                        <div class="result-title">🔍 Top 5 Similar Products</div>
                        <div class="similarity-results">
"""
                for sim_result in similarity_results[:5]:
                    sim_item = sim_result['item']
                    similarity = sim_result['similarity']
                    sim_image_b64 = self.image_to_base64(sim_item['local_image_path'])
                    
                    html_content += f"""
                            <div class="similarity-item">
                                <img src="data:image/jpeg;base64,{sim_image_b64}" alt="Similar Product" class="similarity-image">
                                <div class="similarity-score">{similarity:.3f}</div>
                                <div style="font-size: 0.8em;">{sim_item['source_supermarket']}</div>
                                <div style="font-size: 0.8em; margin-top: 5px;">
                                    {sim_item['text_for_embedding'][:40]}{'...' if len(sim_item['text_for_embedding']) > 40 else ''}
                                </div>
                            </div>
"""
                
                html_content += """
                        </div>
                    </div>
"""
            
            html_content += """
                </div>
            </div>
        </div>
"""
        
        html_content += """
    </div>
</body>
</html>
"""
        
        # Write HTML file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"HTML report generated: {output_path}")
        return output_path

def run_quick_test():
    """Run quick product database test"""
    
    # Configuration
    training_data_path = "/Users/noa/Desktop/PriceComparisonApp/training_data.jsonl"
    image_directories = [
        "/Users/noa/Desktop/PriceComparisonApp/rami_levi_product_images",
        "/Users/noa/Desktop/PriceComparisonApp/shufersal_product_images", 
        "/Users/noa/Desktop/PriceComparisonApp/victory_product_images",
        "/Users/noa/Desktop/PriceComparisonApp/hatzi_hinam_product_images",
        "/Users/noa/Desktop/PriceComparisonApp/yochananof_product_images"
    ]
    
    # Initialize tester
    tester = QuickProductTester(training_data_path, image_directories)
    
    # Select test products
    print("Selecting test products...")
    test_products = tester.select_test_products(num_products=15)
    print(f"Selected {len(test_products)} test products")
    
    # Create small index for similarity search
    print("Creating small similarity index...")
    index_data = tester.create_small_index(test_products, num_index_items=100)
    print(f"Created index with {len(index_data)} items")
    
    # Run tests
    test_results = []
    print(f"Running tests on {len(test_products)} products...")
    
    for i, product in enumerate(test_products, 1):
        print(f"Testing product {i}/{len(test_products)}: {product.get('training_item_id', 'Unknown')}")
        
        # Run OCR test
        ocr_result = tester.run_ocr_test(product['local_image_path'])
        
        # Run similarity search
        similarity_results = tester.find_similar_products(product['local_image_path'], index_data, top_k=5)
        
        test_results.append({
            'product': product,
            'ocr_result': ocr_result,
            'similarity_results': similarity_results
        })
    
    # Generate HTML report
    print("Generating HTML report...")
    report_path = tester.generate_html_report(test_results)
    
    print(f"Quick test completed! Results saved to: {report_path}")
    return report_path

if __name__ == "__main__":
    run_quick_test()