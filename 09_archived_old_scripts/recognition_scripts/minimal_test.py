#!/usr/bin/env python3
"""
Minimal Product Recognition Test
Quick visual test without heavy OCR processing for demonstration
"""

import os
import json
import random
import base64
from datetime import datetime
from typing import List, Dict
import logging

# Import our recognition system
from recognition_scripts.fixed_product_recognition import TrainingDataProcessor, CLIPEncoder

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def select_sample_products(training_data_path: str, num_products: int = 10) -> List[Dict]:
    """Select a small sample of products for testing"""
    processor = TrainingDataProcessor(training_data_path, [])
    items = processor.load_training_data()
    
    # Filter valid items
    valid_items = [item for item in items if os.path.exists(item.get('local_image_path', ''))]
    
    # Group by store and select evenly
    by_store = {}
    for item in valid_items:
        store = item.get('source_supermarket', 'unknown')
        if store not in by_store:
            by_store[store] = []
        by_store[store].append(item)
    
    sample_products = []
    products_per_store = max(1, num_products // len(by_store))
    
    for store, store_items in by_store.items():
        selected = random.sample(store_items, min(products_per_store, len(store_items)))
        sample_products.extend(selected)
        logger.info(f"Selected {len(selected)} products from {store}")
    
    return sample_products[:num_products]

def image_to_base64(image_path: str) -> str:
    """Convert image to base64"""
    try:
        with open(image_path, 'rb') as f:
            image_data = f.read()
        return base64.b64encode(image_data).decode('utf-8')
    except:
        return ""

def run_visual_similarity_test(test_products: List[Dict], index_products: List[Dict]) -> List[Dict]:
    """Run visual similarity test using CLIP"""
    clip_encoder = CLIPEncoder()
    
    # Extract features for index products
    index_features = []
    logger.info("Extracting features for index...")
    for i, product in enumerate(index_products):
        try:
            features = clip_encoder.encode_image(product['local_image_path'])
            index_features.append({
                'features': features,
                'product': product
            })
            if (i + 1) % 5 == 0:
                logger.info(f"Processed {i + 1}/{len(index_products)} index items")
        except Exception as e:
            logger.error(f"Error processing {product['local_image_path']}: {e}")
    
    # Test each product
    results = []
    logger.info("Running similarity tests...")
    for i, test_product in enumerate(test_products):
        logger.info(f"Testing product {i+1}/{len(test_products)}")
        
        try:
            # Extract query features
            query_features = clip_encoder.encode_image(test_product['local_image_path'])
            
            # Calculate similarities
            similarities = []
            for index_item in index_features:
                from sklearn.metrics.pairwise import cosine_similarity
                similarity = cosine_similarity([query_features], [index_item['features']])[0][0]
                similarities.append({
                    'similarity': float(similarity),
                    'product': index_item['product']
                })
            
            # Sort by similarity
            similarities.sort(key=lambda x: x['similarity'], reverse=True)
            
            results.append({
                'query_product': test_product,
                'similar_products': similarities[:5]  # Top 5
            })
            
        except Exception as e:
            logger.error(f"Error testing {test_product['local_image_path']}: {e}")
            results.append({
                'query_product': test_product,
                'similar_products': [],
                'error': str(e)
            })
    
    return results

def generate_minimal_html_report(test_results: List[Dict], output_path: str = "minimal_test_results.html"):
    """Generate minimal HTML report"""
    
    html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Minimal Product Recognition Test</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
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
        .test-item {{
            border: 1px solid #ddd;
            border-radius: 8px;
            margin: 20px 0;
            padding: 20px;
            background: white;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }}
        .query-section {{
            display: grid;
            grid-template-columns: 250px 1fr;
            gap: 20px;
            margin-bottom: 20px;
            padding-bottom: 20px;
            border-bottom: 2px solid #ecf0f1;
        }}
        .query-image {{
            max-width: 100%;
            max-height: 200px;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.2);
        }}
        .query-info {{
            display: flex;
            flex-direction: column;
            justify-content: center;
        }}
        .store-badge {{
            background: #3498db;
            color: white;
            padding: 5px 10px;
            border-radius: 20px;
            font-size: 0.9em;
            font-weight: bold;
            display: inline-block;
            margin-bottom: 10px;
        }}
        .similar-products {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 15px;
        }}
        .similar-item {{
            border: 1px solid #ddd;
            border-radius: 5px;
            padding: 10px;
            text-align: center;
            background: #fafafa;
        }}
        .similar-image {{
            max-width: 100%;
            max-height: 120px;
            border-radius: 5px;
        }}
        .similarity-score {{
            font-weight: bold;
            color: #3498db;
            margin: 5px 0;
            font-size: 1.1em;
        }}
        .product-desc {{
            font-size: 0.8em;
            color: #666;
            margin-top: 5px;
        }}
        .error {{
            background: #ffebee;
            color: #c62828;
            padding: 10px;
            border-radius: 5px;
            margin: 10px 0;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🎯 Minimal Product Recognition Test</h1>
        <p style="text-align: center; color: #7f8c8d; font-size: 1.1em;">
            Visual Similarity Testing - Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </p>
        
        <div style="text-align: center; margin: 20px 0; padding: 15px; background: #e8f5e8; border-radius: 8px;">
            <strong>📊 Test Results: {len(test_results)} products tested</strong>
        </div>
"""
    
    for i, result in enumerate(test_results, 1):
        query_product = result['query_product']
        similar_products = result.get('similar_products', [])
        error = result.get('error')
        
        # Convert query image to base64
        query_image_b64 = image_to_base64(query_product['local_image_path'])
        
        html_content += f"""
        <div class="test-item">
            <h2>Test #{i}</h2>
            
            <div class="query-section">
                <div style="text-align: center;">
                    <img src="data:image/jpeg;base64,{query_image_b64}" alt="Query Product" class="query-image">
                    <p style="font-size: 0.8em; margin-top: 5px; color: #666;">
                        {os.path.basename(query_product['local_image_path'])}
                    </p>
                </div>
                
                <div class="query-info">
                    <div class="store-badge">{query_product.get('source_supermarket', 'Unknown Store')}</div>
                    <h3>{query_product.get('training_item_id', 'Unknown ID')}</h3>
                    <p><strong>Description:</strong> {query_product.get('text_for_embedding', 'No description')}</p>
                    <p><strong>Product ID:</strong> {query_product.get('original_store_product_id', 'Unknown')}</p>
                </div>
            </div>
"""
        
        if error:
            html_content += f'<div class="error">❌ Error: {error}</div>'
        elif similar_products:
            html_content += f"""
            <h4>🔍 Top {len(similar_products)} Most Similar Products:</h4>
            <div class="similar-products">
"""
            
            for sim_result in similar_products:
                sim_product = sim_result['product']
                similarity = sim_result['similarity']
                sim_image_b64 = image_to_base64(sim_product['local_image_path'])
                
                html_content += f"""
                <div class="similar-item">
                    <img src="data:image/jpeg;base64,{sim_image_b64}" alt="Similar Product" class="similar-image">
                    <div class="similarity-score">{similarity:.3f}</div>
                    <div style="font-size: 0.8em; font-weight: bold;">{sim_product['source_supermarket']}</div>
                    <div class="product-desc">
                        {sim_product['text_for_embedding'][:60]}{'...' if len(sim_product['text_for_embedding']) > 60 else ''}
                    </div>
                </div>
"""
            
            html_content += "</div>"
        else:
            html_content += '<div class="error">❌ No similar products found</div>'
        
        html_content += "</div>"
    
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

def main():
    """Run minimal test"""
    training_data_path = "/Users/noa/Desktop/PriceComparisonApp/training_data.jsonl"
    
    # Select test products
    print("Selecting test products...")
    test_products = select_sample_products(training_data_path, num_products=8)
    print(f"Selected {len(test_products)} test products")
    
    # Select index products (different from test products)
    print("Selecting index products...")
    index_products = select_sample_products(training_data_path, num_products=50)
    
    # Remove test products from index
    test_ids = {p['training_item_id'] for p in test_products}
    index_products = [p for p in index_products if p['training_item_id'] not in test_ids][:30]
    print(f"Selected {len(index_products)} index products")
    
    # Run visual similarity test
    print("Running visual similarity tests...")
    results = run_visual_similarity_test(test_products, index_products)
    
    # Generate HTML report
    print("Generating HTML report...")
    report_path = generate_minimal_html_report(results)
    
    print(f"Test completed! Results saved to: {report_path}")

if __name__ == "__main__":
    main()