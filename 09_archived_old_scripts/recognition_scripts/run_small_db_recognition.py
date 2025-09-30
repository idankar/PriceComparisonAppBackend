#!/usr/bin/env python3
"""
Run product recognition on small test dataset and generate HTML report
"""

import os
import json
import sys
from pathlib import Path
from typing import List, Dict
import base64
from datetime import datetime

# Import the recognition system
from recognition_scripts.fixed_product_recognition import FixedProductRecognizer, TesseractOCR


def encode_image_to_base64(image_path: str) -> str:
    """Convert image to base64 for HTML embedding"""
    try:
        with open(image_path, 'rb') as img_file:
            return base64.b64encode(img_file.read()).decode('utf-8')
    except Exception as e:
        print(f"Error encoding image {image_path}: {e}")
        return ""


def run_recognition_on_small_dataset() -> List[Dict]:
    """Run recognition on all products in small dataset"""
    dataset_path = Path("data/small_test_dataset")
    images_path = dataset_path / "images"
    metadata_path = dataset_path / "metadata.json"
    
    # Load metadata
    with open(metadata_path, 'r', encoding='utf-8') as f:
        metadata = json.load(f)
    
    # Initialize recognizer
    print("Initializing product recognizer...")
    recognizer = FixedProductRecognizer()
    
    results = []
    
    print(f"Processing {len(metadata)} products...")
    
    for image_filename, product_info in metadata.items():
        image_path = images_path / image_filename
        
        if not image_path.exists():
            print(f"Warning: Image not found: {image_path}")
            continue
        
        print(f"Processing: {image_filename}")
        
        # Run recognition
        try:
            result = recognizer.recognize_product(str(image_path))
            
            # Encode image for HTML
            image_base64 = encode_image_to_base64(str(image_path))
            
            # Combine all information
            result_data = {
                "image_filename": image_filename,
                "image_base64": image_base64,
                "expected": {
                    "name_he": product_info["name_he"],
                    "brand": product_info["brand"],
                    "product_id": product_info["product_id"]
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
                "is_correct": (result.product_id == product_info["product_id"]),
                "timestamp": datetime.now().isoformat()
            }
            
            results.append(result_data)
            
            # Print progress
            confidence_color = "green" if result.confidence > 0.7 else "orange" if result.confidence > 0.4 else "red"
            match_status = "✓" if result_data["is_correct"] else "✗"
            print(f"  {match_status} Confidence: {result.confidence:.3f} | Match: {result.match_type} | ID: {result.product_id}")
            
        except Exception as e:
            print(f"Error processing {image_filename}: {e}")
            results.append({
                "image_filename": image_filename,
                "image_base64": encode_image_to_base64(str(image_path)),
                "expected": product_info,
                "recognized": {"error": str(e)},
                "is_correct": False,
                "timestamp": datetime.now().isoformat()
            })
    
    return results


def generate_html_report(results: List[Dict]) -> str:
    """Generate HTML report from recognition results"""
    
    # Calculate accuracy
    correct_matches = sum(1 for r in results if r.get("is_correct", False))
    total_products = len(results)
    accuracy = (correct_matches / total_products * 100) if total_products > 0 else 0
    
    html_content = f"""
<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Product Recognition Results - Small Database</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
            direction: rtl;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
            text-align: center;
        }}
        .header h1 {{
            margin: 0;
            font-size: 2.5em;
        }}
        .summary {{
            background: white;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 30px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }}
        .summary-card {{
            text-align: center;
            padding: 15px;
            border-radius: 8px;
            background: #f8f9fa;
        }}
        .summary-card.accuracy {{
            background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
            color: white;
        }}
        .summary-card.correct {{
            background: linear-gradient(135deg, #43e97b 0%, #38f9d7 100%);
            color: white;
        }}
        .summary-card.incorrect {{
            background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);
            color: white;
        }}
        .summary-card .number {{
            font-size: 2em;
            font-weight: bold;
            margin-bottom: 5px;
        }}
        .product-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 20px;
        }}
        .product-card {{
            background: white;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            transition: transform 0.3s ease;
        }}
        .product-card:hover {{
            transform: translateY(-5px);
        }}
        .product-card.correct {{
            border-left: 5px solid #28a745;
        }}
        .product-card.incorrect {{
            border-left: 5px solid #dc3545;
        }}
        .product-image {{
            width: 100%;
            max-width: 200px;
            height: 150px;
            object-fit: contain;
            border-radius: 8px;
            border: 1px solid #ddd;
            margin-bottom: 15px;
        }}
        .product-info {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 15px;
        }}
        .expected, .recognized {{
            padding: 15px;
            border-radius: 8px;
        }}
        .expected {{
            background: #e8f5e8;
            border: 1px solid #28a745;
        }}
        .recognized {{
            background: #fff3cd;
            border: 1px solid #ffc107;
        }}
        .recognized.error {{
            background: #f8d7da;
            border: 1px solid #dc3545;
        }}
        .field {{
            margin-bottom: 10px;
        }}
        .field-label {{
            font-weight: bold;
            color: #555;
            font-size: 0.9em;
        }}
        .field-value {{
            margin-top: 5px;
            padding: 5px 10px;
            background: rgba(255,255,255,0.7);
            border-radius: 4px;
            font-family: monospace;
        }}
        .confidence {{
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            color: white;
            font-weight: bold;
        }}
        .confidence.high {{ background: #28a745; }}
        .confidence.medium {{ background: #ffc107; }}
        .confidence.low {{ background: #dc3545; }}
        .match-type {{
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            background: #6c757d;
            color: white;
            font-size: 0.8em;
        }}
        .timestamp {{
            text-align: center;
            color: #6c757d;
            font-size: 0.9em;
            margin-top: 30px;
        }}
        .ocr-details {{
            background: #f8f9fa;
            padding: 10px;
            border-radius: 4px;
            margin-top: 10px;
            font-size: 0.9em;
            border: 1px dashed #6c757d;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🔍 Product Recognition Results</h1>
        <p>Small Test Database Analysis</p>
        <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>

    <div class="summary">
        <h2>📊 Recognition Summary</h2>
        <div class="summary-grid">
            <div class="summary-card accuracy">
                <div class="number">{accuracy:.1f}%</div>
                <div>Overall Accuracy</div>
            </div>
            <div class="summary-card correct">
                <div class="number">{correct_matches}</div>
                <div>Correct Matches</div>
            </div>
            <div class="summary-card incorrect">
                <div class="number">{total_products - correct_matches}</div>
                <div>Incorrect Matches</div>
            </div>
            <div class="summary-card">
                <div class="number">{total_products}</div>
                <div>Total Products</div>
            </div>
        </div>
    </div>

    <div class="product-grid">
"""

    for result in results:
        is_correct = result.get("is_correct", False)
        card_class = "correct" if is_correct else "incorrect"
        status_icon = "✅" if is_correct else "❌"
        
        expected = result["expected"]
        recognized = result.get("recognized", {})
        
        # Handle recognition errors
        if "error" in recognized:
            recognized_html = f'<div class="recognized error"><div class="field-label">Error:</div><div class="field-value">{recognized["error"]}</div></div>'
        else:
            confidence = recognized.get("confidence", 0)
            confidence_class = "high" if confidence > 0.7 else "medium" if confidence > 0.4 else "low"
            
            recognized_html = f"""
            <div class="recognized">
                <div class="field-label">🤖 Recognized As:</div>
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
                    <div class="field-label">Confidence:</div>
                    <div class="field-value">
                        <span class="confidence {confidence_class}">{confidence:.3f}</span>
                        <span class="match-type">{recognized.get('match_type', 'N/A')}</span>
                    </div>
                </div>
                <div class="field">
                    <div class="field-label">Price:</div>
                    <div class="field-value">₪{recognized.get('price', 'N/A')}</div>
                </div>
                {f'<div class="ocr-details"><strong>OCR Text:</strong><br>{recognized.get("ocr_text", "N/A")}</div>' if recognized.get("ocr_text") else ''}
                {f'<div class="field"><div class="field-label">Visual Similarity:</div><div class="field-value">{recognized.get("visual_similarity", "N/A")}</div></div>' if recognized.get("visual_similarity") is not None else ''}
                {f'<div class="field"><div class="field-label">Text Similarity:</div><div class="field-value">{recognized.get("text_similarity", "N/A")}</div></div>' if recognized.get("text_similarity") is not None else ''}
            </div>
            """
        
        html_content += f"""
        <div class="product-card {card_class}">
            <div style="text-align: center;">
                <img src="data:image/jpeg;base64,{result['image_base64']}" alt="{result['image_filename']}" class="product-image">
                <h3>{status_icon} {result['image_filename']}</h3>
            </div>
            
            <div class="product-info">
                <div class="expected">
                    <div class="field-label">🎯 Expected:</div>
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
                </div>
                
                {recognized_html}
            </div>
        </div>
        """

    html_content += f"""
    </div>

    <div class="timestamp">
        Report generated on {datetime.now().strftime('%Y-%m-%d at %H:%M:%S')}
    </div>
</body>
</html>
"""

    return html_content


def main():
    """Main function"""
    print("🔍 Running Product Recognition on Small Database")
    print("=" * 60)
    
    # Run recognition
    results = run_recognition_on_small_dataset()
    
    # Generate HTML report
    print("\n📄 Generating HTML report...")
    html_content = generate_html_report(results)
    
    # Save report
    report_path = "product_recognition_small_db_report.html"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✅ Report saved: {report_path}")
    
    # Print summary
    correct_matches = sum(1 for r in results if r.get("is_correct", False))
    total_products = len(results)
    accuracy = (correct_matches / total_products * 100) if total_products > 0 else 0
    
    print(f"\n📊 Recognition Summary:")
    print(f"   Total Products: {total_products}")
    print(f"   Correct Matches: {correct_matches}")
    print(f"   Accuracy: {accuracy:.1f}%")
    
    return report_path


if __name__ == "__main__":
    main()