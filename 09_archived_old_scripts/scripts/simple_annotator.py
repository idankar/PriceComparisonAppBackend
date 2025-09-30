# scripts/direct_annotator.py
import os
import json
import csv
import webbrowser
from flask import Flask, render_template_string, request, jsonify, send_file

app = Flask(__name__)

# Global variables to store data
PRODUCTS = []
CURRENT_INDEX = 0
ANNOTATIONS = []

@app.route('/')
def index():
    """Main page for annotation"""
    if CURRENT_INDEX >= len(PRODUCTS):
        return render_template_string(COMPLETED_TEMPLATE, total=len(PRODUCTS))
    
    product = PRODUCTS[CURRENT_INDEX]
    
    # Get API data
    api_data = product.get('api_data', {})
    
    return render_template_string(
        ANNOTATION_TEMPLATE, 
        product=product,
        product_id=product['product_id'],
        api_data=api_data,
        index=CURRENT_INDEX,
        total=len(PRODUCTS),
        progress=int((CURRENT_INDEX / len(PRODUCTS)) * 100)
    )

@app.route('/image/<product_id>')
def get_image(product_id):
    """Serve image directly using send_file"""
    for product in PRODUCTS:
        if product['product_id'] == product_id:
            image_path = product['image_path']
            if os.path.exists(image_path):
                return send_file(image_path)
    
    return "Image not found", 404

@app.route('/submit', methods=['POST'])
def submit_annotation():
    """Handle annotation submission"""
    global CURRENT_INDEX, ANNOTATIONS
    
    # Get data from form
    data = request.json
    product_id = PRODUCTS[CURRENT_INDEX]['product_id']
    
    # Create annotation
    annotation = {
        "product_id": product_id,
        "image_path": PRODUCTS[CURRENT_INDEX]['image_path'],
        "api_data": PRODUCTS[CURRENT_INDEX].get('api_data', {}),
        "visible_info": {
            "product_name": data.get('product_name', ''),
            "brand": data.get('brand', ''),
            "amount": data.get('amount', ''),
            "unit": data.get('unit', '')
        },
        "notes": data.get('notes', '')
    }
    
    # Add or update annotation
    updated = False
    for i, ann in enumerate(ANNOTATIONS):
        if ann['product_id'] == product_id:
            ANNOTATIONS[i] = annotation
            updated = True
            break
    
    if not updated:
        ANNOTATIONS.append(annotation)
    
    # Save annotations
    save_annotations()
    
    # Move to next product
    CURRENT_INDEX += 1
    
    return jsonify({"success": True})

@app.route('/skip', methods=['POST'])
def skip_product():
    """Skip current product"""
    global CURRENT_INDEX
    CURRENT_INDEX += 1
    return jsonify({"success": True})

def save_annotations():
    """Save annotations to file"""
    # Create annotation directory if it doesn't exist
    annotation_dir = os.path.join(os.path.dirname(PRODUCTS[0]['image_path']), '..', '..', 'annotation')
    os.makedirs(annotation_dir, exist_ok=True)
    
    # Save to JSON
    json_file = os.path.join(annotation_dir, "annotations.json")
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(ANNOTATIONS, f, ensure_ascii=False, indent=2)
    
    # Save to CSV
    csv_file = os.path.join(annotation_dir, "annotations.csv")
    with open(csv_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        
        # Write header
        writer.writerow([
            "product_id", "image_file", "api_name", "api_brand", 
            "visible_product_name", "visible_brand", "visible_amount", "visible_unit", "notes"
        ])
        
        # Write data
        for ann in ANNOTATIONS:
            api_data = ann.get('api_data', {})
            visible_info = ann.get('visible_info', {})
            image_file = os.path.basename(ann['image_path'])
            
            writer.writerow([
                ann['product_id'],
                image_file,
                api_data.get('name', ''),
                api_data.get('brand', ''),
                visible_info.get('product_name', ''),
                visible_info.get('brand', ''),
                visible_info.get('amount', ''),
                visible_info.get('unit', ''),
                ann.get('notes', '')
            ])
    
    print(f"Saved annotations to {json_file} and {csv_file}")

def load_data(annotation_dir, images_dir):
    """Load product data and existing annotations"""
    global PRODUCTS, ANNOTATIONS
    
    # Load template data
    template_file = os.path.join(annotation_dir, "annotation_template.json")
    if os.path.exists(template_file):
        with open(template_file, 'r', encoding='utf-8') as f:
            PRODUCTS = json.load(f)
    else:
        print(f"Template file not found: {template_file}")
        return False
    
    # Load existing annotations if any
    annotations_file = os.path.join(annotation_dir, "annotations.json")
    if os.path.exists(annotations_file):
        with open(annotations_file, 'r', encoding='utf-8') as f:
            ANNOTATIONS = json.load(f)
    
    # Find the last annotated index
    global CURRENT_INDEX
    if ANNOTATIONS:
        annotated_ids = set(ann['product_id'] for ann in ANNOTATIONS)
        for i, product in enumerate(PRODUCTS):
            if product['product_id'] not in annotated_ids:
                CURRENT_INDEX = i
                break
        else:
            CURRENT_INDEX = len(PRODUCTS)
    
    print(f"Loaded {len(PRODUCTS)} products and {len(ANNOTATIONS)} existing annotations")
    print(f"Starting from index {CURRENT_INDEX}")
    
    return True

def main(annotation_dir, images_dir, port=5000):
    """Run the annotation tool"""
    if not load_data(annotation_dir, images_dir):
        print("Failed to load data")
        return
    
    print(f"Starting server on http://localhost:{port}/")
    webbrowser.open(f'http://localhost:{port}/')
    app.run(host='0.0.0.0', port=port)

# HTML Templates
ANNOTATION_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Product Annotation</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            display: flex;
            gap: 20px;
        }
        .image-container {
            flex: 1;
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .form-container {
            flex: 1;
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        img {
            max-width: 100%;
            height: auto;
            border: 1px solid #ddd;
        }
        .progress-bar {
            height: 20px;
            background-color: #e0e0e0;
            border-radius: 10px;
            margin-bottom: 20px;
            overflow: hidden;
        }
        .progress {
            height: 100%;
            background-color: #4CAF50;
            text-align: center;
            line-height: 20px;
            color: white;
        }
        .form-group {
            margin-bottom: 15px;
        }
        label {
            display: block;
            margin-bottom: 5px;
            font-weight: bold;
        }
        input[type="text"], textarea {
            width: 100%;
            padding: 8px;
            border: 1px solid #ddd;
            border-radius: 4px;
        }
        button {
            padding: 10px 15px;
            border: none;
            border-radius: 4px;
            background-color: #4CAF50;
            color: white;
            cursor: pointer;
            margin-right: 10px;
        }
        button.skip {
            background-color: #f44336;
        }
        .modal {
            display: none;
            position: fixed;
            z-index: 1;
            left: 0;
            top: 0;
            width: 100%;
            height: 100%;
            background-color: rgba(0,0,0,0.4);
        }
        .modal-content {
            background-color: white;
            margin: 15% auto;
            padding: 20px;
            border-radius: 8px;
            width: 50%;
            box-shadow: 0 4px 8px rgba(0,0,0,0.2);
        }
        .close {
            color: #aaa;
            float: right;
            font-size: 28px;
            font-weight: bold;
            cursor: pointer;
        }
        .api-data {
            background-color: #f9f9f9;
            padding: 10px;
            border-radius: 4px;
            margin-bottom: 15px;
        }
        .api-data p {
            margin: 5px 0;
        }
    </style>
</head>
<body>
    <h1>Product Annotation Tool</h1>
    
    <div class="progress-bar">
        <div class="progress" style="width: {{ progress }}%">{{ progress }}%</div>
    </div>
    
    <p>Product {{ index + 1 }} of {{ total }}</p>
    
    <div class="container">
        <div class="image-container">
            <h2>Product Image</h2>
            <img src="/image/{{ product_id }}" alt="Product Image">
        </div>
        
        <div class="form-container">
            <h2>Information Verification</h2>
            
            <div class="api-data">
                <h3>API Data (Reference Only)</h3>
                <p><strong>Product:</strong> {{ api_data.name }}</p>
                <p><strong>Brand:</strong> {{ api_data.brand }}</p>
                <p><strong>Description:</strong> {{ api_data.description }}</p>
                <p><strong>Unit:</strong> {{ api_data.unit_description }}</p>
            </div>
            
            <form id="annotation-form">
                <div class="form-group">
                    <label for="product_name">Visible Product Name:</label>
                    <input type="text" id="product_name" name="product_name">
                </div>
                
                <div class="form-group">
                    <label for="brand">Visible Brand:</label>
                    <input type="text" id="brand" name="brand">
                </div>
                
                <div class="form-group">
                    <label for="amount">Visible Amount:</label>
                    <input type="text" id="amount" name="amount">
                </div>
                
                <div class="form-group">
                    <label for="unit">Visible Unit:</label>
                    <input type="text" id="unit" name="unit">
                </div>
                
                <div class="form-group">
                    <label for="notes">Notes:</label>
                    <textarea id="notes" name="notes" rows="3"></textarea>
                </div>
                
                <button type="button" id="submit-btn">Submit</button>
                <button type="button" class="skip" id="skip-btn">Skip</button>
                <button type="button" id="missing-btn">Missing Information</button>
            </form>
        </div>
    </div>
    
    <div id="missing-modal" class="modal">
        <div class="modal-content">
            <span class="close">&times;</span>
            <h2>Add Missing Information</h2>
            
            <div class="form-group">
                <label for="missing-category">Category:</label>
                <select id="missing-category">
                    <option value="product_name">Product Name</option>
                    <option value="brand">Brand</option>
                    <option value="amount">Amount</option>
                    <option value="unit">Unit</option>
                </select>
            </div>
            
            <div class="form-group">
                <label for="missing-info">Information:</label>
                <input type="text" id="missing-info">
            </div>
            
            <button type="button" id="add-missing-btn">Add</button>
        </div>
    </div>
    
    <script>
        document.addEventListener('DOMContentLoaded', function() {
            const form = document.getElementById('annotation-form');
            const submitBtn = document.getElementById('submit-btn');
            const skipBtn = document.getElementById('skip-btn');
            const missingBtn = document.getElementById('missing-btn');
            const modal = document.getElementById('missing-modal');
            const closeModal = document.querySelector('.close');
            const addMissingBtn = document.getElementById('add-missing-btn');
            
            // Submit form
            submitBtn.addEventListener('click', function() {
                const formData = {
                    product_name: document.getElementById('product_name').value,
                    brand: document.getElementById('brand').value,
                    amount: document.getElementById('amount').value,
                    unit: document.getElementById('unit').value,
                    notes: document.getElementById('notes').value
                };
                
                fetch('/submit', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify(formData)
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        window.location.reload();
                    }
                });
            });
            
            // Skip button
            skipBtn.addEventListener('click', function() {
                if (confirm('Are you sure you want to skip this product?')) {
                    fetch('/skip', {
                        method: 'POST'
                    })
                    .then(response => response.json())
                    .then(data => {
                        if (data.success) {
                            window.location.reload();
                        }
                    });
                }
            });
            
            // Missing information modal
            missingBtn.addEventListener('click', function() {
                modal.style.display = 'block';
            });
            
            closeModal.addEventListener('click', function() {
                modal.style.display = 'none';
            });
            
            window.addEventListener('click', function(event) {
                if (event.target === modal) {
                    modal.style.display = 'none';
                }
            });
            
            // Add missing information
            addMissingBtn.addEventListener('click', function() {
                const category = document.getElementById('missing-category').value;
                const info = document.getElementById('missing-info').value;
                
                if (info.trim() !== '') {
                    document.getElementById(category).value = info;
                    modal.style.display = 'none';
                }
            });
        });
    </script>
</body>
</html>
"""

COMPLETED_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Annotation Complete</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            text-align: center;
            background-color: #f5f5f5;
        }
        .container {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-top: 40px;
        }
        h1 {
            color: #4CAF50;
        }
        p {
            font-size: 18px;
            margin: 20px 0;
        }
        button {
            padding: 10px 15px;
            border: none;
            border-radius: 4px;
            background-color: #4CAF50;
            color: white;
            cursor: pointer;
            font-size: 16px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🎉 Annotation Complete! 🎉</h1>
        <p>You have successfully annotated all {{ total }} products.</p>
        <p>Thank you for your contribution!</p>
        <p>The annotation data has been saved to CSV and JSON files.</p>
        <button onclick="window.close()">Close</button>
    </div>
</body>
</html>
"""

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run product annotation tool")
    parser.add_argument("--annotation-dir", required=True, help="Directory with annotation templates")
    parser.add_argument("--images-dir", required=True, help="Directory containing product images")
    parser.add_argument("--port", type=int, default=5000, help="Port to run the web server on")
    
    args = parser.parse_args()
    
    main(args.annotation_dir, args.images_dir, args.port)