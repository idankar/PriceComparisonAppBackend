#!/usr/bin/env python3
"""
OCR Sample Test
Test OCR on a small sample of products and generate HTML report
"""

import os
import json
import random
import base64
from datetime import datetime
from typing import List, Dict
import logging

# Import our recognition system
from recognition_scripts.fixed_product_recognition import TrainingDataProcessor, TesseractOCR

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_ocr_sample_test():
    """Run OCR test on a small sample"""
    
    training_data_path = "/Users/noa/Desktop/PriceComparisonApp/training_data.jsonl"
    
    # Load and select products
    processor = TrainingDataProcessor(training_data_path, [])
    items = processor.load_training_data()
    
    # Filter valid items
    valid_items = [item for item in items if os.path.exists(item.get('local_image_path', ''))]
    
    # Select a small random sample
    sample_products = random.sample(valid_items, 6)
    
    # Initialize OCR
    ocr = TesseractOCR()
    
    # Test each product
    results = []
    print(f"Testing OCR on {len(sample_products)} products...")
    
    for i, product in enumerate(sample_products, 1):
        print(f"Testing product {i}/{len(sample_products)}: {product.get('training_item_id')}")
        
        try:
            # Run OCR
            ocr_result = ocr.extract_text(product['local_image_path'])
            
            results.append({
                'product': product,
                'ocr_result': {
                    'success': True,
                    'text': ocr_result.get('text', ''),
                    'confidence': ocr_result.get('confidence', 0),
                    'words': ocr_result.get('words', []),
                    'method': ocr_result.get('method', ''),
                    'word_count': ocr_result.get('word_count', 0)
                }
            })
            
        except Exception as e:
            results.append({
                'product': product,
                'ocr_result': {
                    'success': False,
                    'error': str(e),
                    'text': '',
                    'confidence': 0,
                    'words': [],
                    'method': 'error',
                    'word_count': 0
                }
            })
    
    # Generate HTML report
    generate_ocr_html_report(results)
    print("OCR test completed! Results saved to: ocr_sample_results.html")

def image_to_base64(image_path: str) -> str:
    """Convert image to base64"""
    try:
        with open(image_path, 'rb') as f:
            image_data = f.read()
        return base64.b64encode(image_data).decode('utf-8')
    except:
        return ""

def generate_ocr_html_report(results: List[Dict], output_path: str = "ocr_sample_results.html"):
    """Generate HTML report for OCR results"""
    
    # Calculate stats
    total_tests = len(results)
    successful_ocr = sum(1 for r in results if r['ocr_result']['success'])
    avg_confidence = sum(r['ocr_result']['confidence'] for r in results) / total_tests if total_tests > 0 else 0
    
    html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>OCR Sample Test Results</title>
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
        .stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
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
            display: grid;
            grid-template-columns: 250px 1fr;
            gap: 20px;
            margin-bottom: 20px;
        }}
        .test-image {{
            max-width: 100%;
            max-height: 200px;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.2);
        }}
        .product-info h3 {{
            margin: 0 0 10px 0;
            color: #2c3e50;
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
        .ocr-results {{
            background: #f8f9fa;
            border: 1px solid #ddd;
            border-radius: 8px;
            padding: 20px;
        }}
        .success {{ border-left: 4px solid #28a745; }}
        .error {{ border-left: 4px solid #dc3545; background: #fff5f5; }}
        .confidence-bar {{
            width: 100%;
            height: 20px;
            background: #ecf0f1;
            border-radius: 10px;
            overflow: hidden;
            margin: 10px 0;
        }}
        .confidence-fill {{
            height: 100%;
            border-radius: 10px;
        }}
        .confidence-high {{ background: linear-gradient(90deg, #28a745, #20c997); }}
        .confidence-medium {{ background: linear-gradient(90deg, #ffc107, #fd7e14); }}
        .confidence-low {{ background: linear-gradient(90deg, #dc3545, #e83e8c); }}
        .extracted-text {{
            background: #f1f3f4;
            border: 1px solid #dadce0;
            border-radius: 5px;
            padding: 15px;
            font-family: 'Courier New', monospace;
            white-space: pre-wrap;
            margin: 10px 0;
            max-height: 150px;
            overflow-y: auto;
        }}
        .words {{
            display: flex;
            flex-wrap: wrap;
            gap: 5px;
            margin-top: 10px;
        }}
        .word {{
            background: #e3f2fd;
            color: #1976d2;
            padding: 3px 8px;
            border-radius: 15px;
            font-size: 0.9em;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🔤 OCR Sample Test Results</h1>
        <p style="text-align: center; color: #7f8c8d; font-size: 1.1em;">
            Hebrew & English Text Recognition - Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </p>
        
        <div class="stats">
            <div class="stat-card">
                <span class="stat-number">{total_tests}</span>
                <span>Products Tested</span>
            </div>
            <div class="stat-card">
                <span class="stat-number">{successful_ocr}</span>
                <span>Successful OCR</span>
            </div>
            <div class="stat-card">
                <span class="stat-number">{successful_ocr/total_tests*100:.1f}%</span>
                <span>Success Rate</span>
            </div>
            <div class="stat-card">
                <span class="stat-number">{avg_confidence:.1f}%</span>
                <span>Avg Confidence</span>
            </div>
        </div>
"""
    
    for i, result in enumerate(results, 1):
        product = result['product']
        ocr_result = result['ocr_result']
        
        # Convert image to base64
        image_b64 = image_to_base64(product['local_image_path'])
        
        # Determine confidence class
        confidence = ocr_result.get('confidence', 0)
        if confidence >= 70:
            conf_class = "confidence-high"
        elif confidence >= 40:
            conf_class = "confidence-medium"
        else:
            conf_class = "confidence-low"
        
        # Result class
        result_class = "success" if ocr_result['success'] else "error"
        
        html_content += f"""
        <div class="test-item">
            <div class="test-header">
                <div style="text-align: center;">
                    <img src="data:image/jpeg;base64,{image_b64}" alt="Product Image" class="test-image">
                    <p style="font-size: 0.8em; margin-top: 5px; color: #666;">
                        {os.path.basename(product['local_image_path'])}
                    </p>
                </div>
                
                <div class="product-info">
                    <div class="store-badge">{product.get('source_supermarket', 'Unknown')}</div>
                    <h3>{product.get('training_item_id', 'Unknown ID')}</h3>
                    <p><strong>Description:</strong> {product.get('text_for_embedding', 'No description')}</p>
                    <p><strong>Product ID:</strong> {product.get('original_store_product_id', 'Unknown')}</p>
                </div>
            </div>
            
            <div class="ocr-results {result_class}">
                <h4>🔤 OCR Results</h4>
                <p><strong>Status:</strong> {'✅ Success' if ocr_result['success'] else '❌ Failed'}</p>
"""
        
        if ocr_result['success']:
            html_content += f"""
                <p><strong>Confidence:</strong> {confidence:.1f}%</p>
                <div class="confidence-bar">
                    <div class="confidence-fill {conf_class}" style="width: {confidence}%"></div>
                </div>
                <p><strong>Method:</strong> {ocr_result.get('method', 'Unknown')}</p>
                <p><strong>Word Count:</strong> {ocr_result.get('word_count', 0)}</p>
                
                <p><strong>Extracted Text:</strong></p>
                <div class="extracted-text">{ocr_result.get('text', 'No text extracted') or 'No text extracted'}</div>
"""
            
            if ocr_result.get('words'):
                html_content += f"""
                <p><strong>Detected Words ({len(ocr_result['words'])}):</strong></p>
                <div class="words">
"""
                for word in ocr_result['words'][:20]:  # Limit to 20 words
                    html_content += f'<span class="word">{word}</span>'
                
                if len(ocr_result['words']) > 20:
                    html_content += f'<span class="word">+{len(ocr_result["words"]) - 20} more...</span>'
                
                html_content += "</div>"
        else:
            html_content += f'<p><strong>Error:</strong> {ocr_result.get("error", "Unknown error")}</p>'
        
        html_content += """
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
    
    logger.info(f"OCR HTML report generated: {output_path}")

if __name__ == "__main__":
    run_ocr_sample_test()