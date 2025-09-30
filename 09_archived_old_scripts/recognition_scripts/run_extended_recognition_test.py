#!/usr/bin/env python3
"""
Extended Product Recognition Test - 20 Products
Run improved OCR + Visual recognition on 20 products and generate detailed HTML report
"""

import os
import json
import sys
import random
from pathlib import Path
from typing import List, Dict
import base64
from datetime import datetime
import sqlite3

# Import the recognition system
from recognition_scripts.fixed_product_recognition import FixedProductRecognizer, TesseractOCR


def get_random_products_from_db(db_path: str, count: int = 20) -> List[Dict]:
    """Get random products from the main database that have images"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get all product IDs that have image directories
        cursor.execute('SELECT id, name_he, brand, price FROM products')
        all_products = cursor.fetchall()
        conn.close()
        
        # Filter products that have actual image files
        products_with_images = []
        for product_id, name_he, brand, price in all_products:
            image_dir = Path(f"data/products/{product_id}")
            if image_dir.exists():
                image_files = list(image_dir.glob("*.jpg"))
                if image_files:
                    products_with_images.append({
                        'id': product_id,
                        'name_he': name_he,
                        'brand': brand,
                        'price': price,
                        'image_path': str(image_files[0])  # Use first image
                    })
        
        # Randomly sample requested count
        if len(products_with_images) < count:
            print(f"Warning: Only {len(products_with_images)} products with images found, using all")
            return products_with_images
        
        selected_products = random.sample(products_with_images, count)
        return selected_products
        
    except Exception as e:
        print(f"Error accessing database: {e}")
        return []


def encode_image_to_base64(image_path: str) -> str:
    """Convert image to base64 for HTML embedding"""
    try:
        with open(image_path, 'rb') as img_file:
            return base64.b64encode(img_file.read()).decode('utf-8')
    except Exception as e:
        print(f"Error encoding image {image_path}: {e}")
        return ""


def run_recognition_on_products(products: List[Dict]) -> List[Dict]:
    """Run recognition on list of products"""
    print("Initializing product recognizer...")
    recognizer = FixedProductRecognizer()
    
    # Enable faster processing for testing
    recognizer.ocr.label_detection_enabled = True  # Keep region detection but limit fallback
    
    results = []
    
    print(f"Processing {len(products)} products...")
    
    for i, product in enumerate(products, 1):
        print(f"[{i}/{len(products)}] Processing: {product['id']}")
        
        try:
            # Run recognition
            result = recognizer.recognize_product(product['image_path'])
            
            # Encode image for HTML
            image_base64 = encode_image_to_base64(product['image_path'])
            
            # Combine all information
            result_data = {
                "product_id": product['id'],
                "image_path": product['image_path'],
                "image_base64": image_base64,
                "expected": {
                    "name_he": product['name_he'],
                    "brand": product['brand'],
                    "price": product['price'],
                    "product_id": product['id']
                },
                "recognized": {
                    "product_id": result.product_id,
                    "name": result.name,
                    "brand": result.brand,
                    "confidence": result.confidence,
                    "match_type": result.match_type,
                    "price": result.price,
                    "category": result.category,
                    "ocr_text": result.ocr_text,
                    "visual_similarity": result.visual_similarity,
                    "text_similarity": result.text_similarity
                },
                "is_correct": (result.product_id == product['id']),
                "timestamp": datetime.now().isoformat()
            }
            
            results.append(result_data)
            
            # Print progress
            confidence_color = "🟢" if result.confidence > 0.7 else "🟡" if result.confidence > 0.4 else "🔴"
            match_status = "✅" if result_data["is_correct"] else "❌"
            print(f"  {match_status} {confidence_color} Conf: {result.confidence:.3f} | Type: {result.match_type} | Recognized: {result.product_id}")
            
        except Exception as e:
            print(f"  ❌ Error processing {product['id']}: {e}")
            results.append({
                "product_id": product['id'],
                "image_path": product['image_path'],
                "image_base64": encode_image_to_base64(product['image_path']),
                "expected": product,
                "recognized": {"error": str(e)},
                "is_correct": False,
                "timestamp": datetime.now().isoformat()
            })
    
    return results


def generate_enhanced_html_report(results: List[Dict]) -> str:
    """Generate comprehensive HTML report with enhanced analysis"""
    
    # Calculate detailed statistics
    correct_matches = sum(1 for r in results if r.get("is_correct", False))
    total_products = len(results)
    accuracy = (correct_matches / total_products * 100) if total_products > 0 else 0
    
    # Analyze by match type
    match_types = {}
    confidence_ranges = {"high": 0, "medium": 0, "low": 0}
    
    for result in results:
        recognized = result.get("recognized", {})
        if "error" not in recognized:
            match_type = recognized.get("match_type", "unknown")
            match_types[match_type] = match_types.get(match_type, 0) + 1
            
            confidence = recognized.get("confidence", 0)
            if confidence > 0.7:
                confidence_ranges["high"] += 1
            elif confidence > 0.4:
                confidence_ranges["medium"] += 1
            else:
                confidence_ranges["low"] += 1
    
    # Calculate average confidence
    valid_confidences = [r["recognized"]["confidence"] for r in results 
                        if "error" not in r.get("recognized", {}) and r["recognized"].get("confidence")]
    avg_confidence = sum(valid_confidences) / len(valid_confidences) if valid_confidences else 0
    
    html_content = f"""
<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Extended Product Recognition Results - 20 Products</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            direction: rtl;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        .header {{
            background: rgba(255, 255, 255, 0.95);
            color: #333;
            padding: 40px;
            border-radius: 15px;
            margin-bottom: 30px;
            text-align: center;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        }}
        .header h1 {{
            margin: 0;
            font-size: 3em;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }}
        .header p {{
            font-size: 1.2em;
            margin-top: 10px;
            color: #666;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: rgba(255, 255, 255, 0.95);
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 8px 25px rgba(0,0,0,0.1);
            text-align: center;
            transition: transform 0.3s ease;
        }}
        .stat-card:hover {{
            transform: translateY(-5px);
        }}
        .stat-card.accuracy {{
            background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
            color: white;
        }}
        .stat-card.correct {{
            background: linear-gradient(135deg, #43e97b 0%, #38f9d7 100%);
            color: white;
        }}
        .stat-card.confidence {{
            background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);
            color: white;
        }}
        .stat-number {{
            font-size: 2.5em;
            font-weight: bold;
            margin-bottom: 10px;
        }}
        .stat-label {{
            font-size: 1.1em;
            opacity: 0.9;
        }}
        .analysis-section {{
            background: rgba(255, 255, 255, 0.95);
            padding: 30px;
            border-radius: 15px;
            margin-bottom: 30px;
            box-shadow: 0 8px 25px rgba(0,0,0,0.1);
        }}
        .analysis-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }}
        .chart-container {{
            background: #f8f9fa;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
        }}
        .product-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(450px, 1fr));
            gap: 25px;
        }}
        .product-card {{
            background: rgba(255, 255, 255, 0.95);
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 8px 25px rgba(0,0,0,0.1);
            transition: transform 0.3s ease;
            position: relative;
            overflow: hidden;
        }}
        .product-card:hover {{
            transform: translateY(-8px);
        }}
        .product-card.correct {{
            border-right: 6px solid #28a745;
        }}
        .product-card.incorrect {{
            border-right: 6px solid #dc3545;
        }}
        .product-card::before {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 4px;
            background: linear-gradient(90deg, #667eea, #764ba2);
        }}
        .product-image {{
            width: 100%;
            max-width: 180px;
            height: 140px;
            object-fit: contain;
            border-radius: 10px;
            border: 2px solid #e9ecef;
            margin-bottom: 20px;
            display: block;
            margin-left: auto;
            margin-right: auto;
        }}
        .product-header {{
            text-align: center;
            margin-bottom: 20px;
        }}
        .product-id {{
            font-weight: bold;
            color: #495057;
            font-size: 1.1em;
            margin-bottom: 10px;
        }}
        .status-badge {{
            display: inline-block;
            padding: 8px 16px;
            border-radius: 25px;
            font-weight: bold;
            font-size: 0.9em;
        }}
        .status-correct {{
            background: #d4edda;
            color: #155724;
        }}
        .status-incorrect {{
            background: #f8d7da;
            color: #721c24;
        }}
        .product-info {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }}
        .expected, .recognized {{
            padding: 20px;
            border-radius: 10px;
            position: relative;
        }}
        .expected {{
            background: linear-gradient(135deg, #e8f5e8 0%, #f0f8f0 100%);
            border: 2px solid #28a745;
        }}
        .recognized {{
            background: linear-gradient(135deg, #fff3cd 0%, #fef9e7 100%);
            border: 2px solid #ffc107;
        }}
        .recognized.error {{
            background: linear-gradient(135deg, #f8d7da 0%, #fce8ea 100%);
            border: 2px solid #dc3545;
        }}
        .section-title {{
            font-weight: bold;
            color: #333;
            font-size: 1.1em;
            margin-bottom: 15px;
            display: flex;
            align-items: center;
        }}
        .section-title::before {{
            content: '';
            display: inline-block;
            width: 4px;
            height: 20px;
            background: #667eea;
            margin-left: 10px;
            border-radius: 2px;
        }}
        .field {{
            margin-bottom: 12px;
        }}
        .field-label {{
            font-weight: bold;
            color: #555;
            font-size: 0.9em;
            margin-bottom: 5px;
        }}
        .field-value {{
            padding: 8px 12px;
            background: rgba(255,255,255,0.8);
            border-radius: 6px;
            font-family: 'Courier New', monospace;
            border: 1px solid #dee2e6;
        }}
        .confidence {{
            display: inline-block;
            padding: 6px 12px;
            border-radius: 20px;
            color: white;
            font-weight: bold;
            font-size: 0.9em;
        }}
        .confidence.high {{ background: linear-gradient(135deg, #28a745, #20c997); }}
        .confidence.medium {{ background: linear-gradient(135deg, #ffc107, #fd7e14); }}
        .confidence.low {{ background: linear-gradient(135deg, #dc3545, #e83e8c); }}
        .match-type {{
            display: inline-block;
            padding: 4px 10px;
            border-radius: 15px;
            background: #6c757d;
            color: white;
            font-size: 0.8em;
            margin-right: 10px;
        }}
        .ocr-details {{
            background: #f8f9fa;
            padding: 15px;
            border-radius: 8px;
            margin-top: 15px;
            font-size: 0.9em;
            border: 2px dashed #6c757d;
            max-height: 120px;
            overflow-y: auto;
        }}
        .timestamp {{
            text-align: center;
            color: rgba(255,255,255,0.8);
            font-size: 0.9em;
            margin-top: 40px;
            padding: 20px;
            background: rgba(255,255,255,0.1);
            border-radius: 10px;
        }}
        .performance-indicator {{
            width: 100%;
            height: 8px;
            background: #e9ecef;
            border-radius: 4px;
            overflow: hidden;
            margin-top: 10px;
        }}
        .performance-bar {{
            height: 100%;
            background: linear-gradient(90deg, #dc3545, #ffc107, #28a745);
            border-radius: 4px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔍 Extended Product Recognition Test</h1>
            <p>Advanced OCR + Visual Recognition Analysis - 10 Products</p>
            <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>

        <div class="stats-grid">
            <div class="stat-card accuracy">
                <div class="stat-number">{accuracy:.1f}%</div>
                <div class="stat-label">Overall Accuracy</div>
                <div class="performance-indicator">
                    <div class="performance-bar" style="width: {accuracy}%;"></div>
                </div>
            </div>
            <div class="stat-card correct">
                <div class="stat-number">{correct_matches}</div>
                <div class="stat-label">Correct Matches</div>
            </div>
            <div class="stat-card confidence">
                <div class="stat-number">{avg_confidence:.1f}%</div>
                <div class="stat-label">Average Confidence</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{total_products}</div>
                <div class="stat-label">Total Products</div>
            </div>
        </div>

        <div class="analysis-section">
            <h2>📊 Performance Analysis</h2>
            <div class="analysis-grid">
                <div class="chart-container">
                    <h3>Match Types</h3>
                    {"<br>".join([f"{match_type}: {count}" for match_type, count in match_types.items()])}
                </div>
                <div class="chart-container">
                    <h3>Confidence Distribution</h3>
                    High (>70%): {confidence_ranges["high"]}<br>
                    Medium (40-70%): {confidence_ranges["medium"]}<br>
                    Low (<40%): {confidence_ranges["low"]}
                </div>
            </div>
        </div>

        <div class="product-grid">
"""

    for i, result in enumerate(results, 1):
        is_correct = result.get("is_correct", False)
        card_class = "correct" if is_correct else "incorrect"
        status_class = "status-correct" if is_correct else "status-incorrect"
        status_text = "✅ Correct Match" if is_correct else "❌ Incorrect Match"
        
        expected = result["expected"]
        recognized = result.get("recognized", {})
        
        # Handle recognition errors
        if "error" in recognized:
            recognized_html = f'''
            <div class="recognized error">
                <div class="section-title">🤖 Recognition Result</div>
                <div class="field">
                    <div class="field-label">Error:</div>
                    <div class="field-value">{recognized["error"]}</div>
                </div>
            </div>
            '''
        else:
            confidence = recognized.get("confidence", 0)
            confidence_class = "high" if confidence > 0.7 else "medium" if confidence > 0.4 else "low"
            
            ocr_section = ""
            if recognized.get("ocr_text"):
                ocr_section = f'<div class="ocr-details"><strong>🔍 OCR Extracted:</strong><br>{recognized.get("ocr_text", "N/A")}</div>'
            
            similarity_section = ""
            if recognized.get("visual_similarity") is not None or recognized.get("text_similarity") is not None:
                similarity_section = f'''
                <div class="field">
                    <div class="field-label">Similarity Scores:</div>
                    <div class="field-value">
                        Visual: {recognized.get("visual_similarity", "N/A")}<br>
                        Text: {recognized.get("text_similarity", "N/A")}
                    </div>
                </div>
                '''
            
            recognized_html = f'''
            <div class="recognized">
                <div class="section-title">🤖 Recognition Result</div>
                <div class="field">
                    <div class="field-label">Product ID:</div>
                    <div class="field-value">{recognized.get('product_id', 'N/A')}</div>
                </div>
                <div class="field">
                    <div class="field-label">Name:</div>
                    <div class="field-value">{recognized.get('name', 'N/A')}</div>
                </div>
                <div class="field">
                    <div class="field-label">Brand:</div>
                    <div class="field-value">{recognized.get('brand', 'N/A')}</div>
                </div>
                <div class="field">
                    <div class="field-label">Confidence & Type:</div>
                    <div class="field-value">
                        <span class="confidence {confidence_class}">{confidence:.1f}%</span>
                        <span class="match-type">{recognized.get('match_type', 'N/A')}</span>
                    </div>
                </div>
                <div class="field">
                    <div class="field-label">Price:</div>
                    <div class="field-value">₪{recognized.get('price', 'N/A')}</div>
                </div>
                {similarity_section}
                {ocr_section}
            </div>
            '''
        
        html_content += f'''
        <div class="product-card {card_class}">
            <div class="product-header">
                <img src="data:image/jpeg;base64,{result['image_base64']}" alt="{result['product_id']}" class="product-image">
                <div class="product-id">#{i} - {result['product_id']}</div>
                <span class="status-badge {status_class}">{status_text}</span>
            </div>
            
            <div class="product-info">
                <div class="expected">
                    <div class="section-title">🎯 Expected Product</div>
                    <div class="field">
                        <div class="field-label">Product ID:</div>
                        <div class="field-value">{expected['product_id']}</div>
                    </div>
                    <div class="field">
                        <div class="field-label">Name:</div>
                        <div class="field-value">{expected['name_he']}</div>
                    </div>
                    <div class="field">
                        <div class="field-label">Brand:</div>
                        <div class="field-value">{expected['brand']}</div>
                    </div>
                    <div class="field">
                        <div class="field-label">Price:</div>
                        <div class="field-value">₪{expected['price']}</div>
                    </div>
                </div>
                
                {recognized_html}
            </div>
        </div>
        '''

    html_content += f'''
        </div>

        <div class="timestamp">
            📊 Extended Recognition Test completed on {datetime.now().strftime('%Y-%m-%d at %H:%M:%S')}<br>
            🔬 Test included {total_products} products with advanced OCR and visual analysis
        </div>
    </div>
</body>
</html>
'''

    return html_content


def main():
    """Main function"""
    print("🔍 Extended Product Recognition Test - 10 Products")
    print("=" * 60)
    
    # Get random products from database
    print("📂 Selecting random products from database...")
    db_path = "products_complete.db"
    products = get_random_products_from_db(db_path, count=10)
    
    if not products:
        print("❌ No products found or database error")
        return
    
    print(f"✅ Selected {len(products)} products for testing")
    
    # Run recognition
    results = run_recognition_on_products(products)
    
    # Generate HTML report
    print("\n📄 Generating enhanced HTML report...")
    html_content = generate_enhanced_html_report(results)
    
    # Save report
    report_path = "extended_recognition_test_10_products.html"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✅ Report saved: {report_path}")
    
    # Print summary
    correct_matches = sum(1 for r in results if r.get("is_correct", False))
    total_products = len(results)
    accuracy = (correct_matches / total_products * 100) if total_products > 0 else 0
    
    print(f"\n📊 Final Results:")
    print(f"   🎯 Total Products: {total_products}")
    print(f"   ✅ Correct Matches: {correct_matches}")
    print(f"   📈 Accuracy: {accuracy:.1f}%")
    
    # Show confidence breakdown
    valid_confidences = [r["recognized"]["confidence"] for r in results 
                        if "error" not in r.get("recognized", {}) and r["recognized"].get("confidence")]
    if valid_confidences:
        avg_confidence = sum(valid_confidences) / len(valid_confidences)
        print(f"   📊 Average Confidence: {avg_confidence:.1f}%")
    
    return report_path


if __name__ == "__main__":
    main()