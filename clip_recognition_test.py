# clip_recognition_test.py

import os
import csv
import json
import random
from sentence_transformers import SentenceTransformer, util
from PIL import Image
import base64
from io import BytesIO
import torch
import numpy as np

class ClipProductTester:
    def __init__(self, model_name='clip-ViT-B-16', results_path='data/results'):
        """Initialize the tester with the CLIP model and path to product data"""
        print(f"Initializing model: {model_name}")
        self.model = SentenceTransformer(model_name)
        self.results_path = results_path
        self.products = self.load_products()
        
    def load_products(self):
        """Load product data from CSV/JSON files by searching recursively"""
        products = []
        
        # Function to recursively search for files
        def search_files(directory, extensions):
            found_files = []
            if os.path.exists(directory):
                for root, _, files in os.walk(directory):
                    for file in files:
                        if any(file.endswith(ext) for ext in extensions):
                            found_files.append(os.path.join(root, file))
            return found_files
        
        # Search for CSV files
        csv_files = search_files(self.results_path, ['.csv'])
        for filepath in csv_files:
            try:
                print(f"Attempting to read CSV: {filepath}")
                with open(filepath, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        # Print a sample row to debug
                        if len(products) == 0:
                            print(f"Sample CSV row: {row}")
                        
                        # Check if this row has the necessary data
                        if 'product_name' in row and ('image_path' in row or 'image_url' in row):
                            products.append(row)
            except Exception as e:
                print(f"Error reading {filepath}: {e}")
        
        # Search for JSON files
        json_files = search_files(self.results_path, ['.json'])
        for filepath in json_files:
            try:
                print(f"Attempting to read JSON: {filepath}")
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Print sample data to debug
                    print(f"JSON structure: {type(data)}")
                    if isinstance(data, dict) and 'results' in data:
                        data = data['results']
                    
                    if isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict):
                                if len(products) == 0:
                                    print(f"Sample JSON item: {item}")
                                if 'product_name' in item and ('image_path' in item or 'image_url' in item):
                                    products.append(item)
            except Exception as e:
                print(f"Error reading {filepath}: {e}")
        
        print(f"Loaded {len(products)} products from data files")
        return products
    
    def create_sample_test_data(self):
        """Create sample test data if no products are found"""
        print("Creating sample test data...")
        base_dir = os.path.join(os.path.dirname(os.path.dirname(self.results_path)), "images")
        if not os.path.exists(base_dir):
            base_dir = "data/images"  # Fallback to direct path
            if not os.path.exists(base_dir):
                print(f"Image directory not found at {base_dir}")
                base_dir = "."  # Last resort - search everywhere
        
        print(f"Searching for images in {base_dir}")
        sample_products = []
        
        # Look for images in the data/images directory
        count = 0
        for root, _, files in os.walk(base_dir):
            for file in files:
                if file.lower().endswith(('.jpg', '.jpeg', '.png')):
                    image_path = os.path.join(root, file)
                    product_name = os.path.splitext(file)[0].replace('_', ' ').title()
                    
                    # Check if the image can be opened
                    try:
                        with Image.open(image_path) as img:
                            # Image opened successfully
                            product = {
                                'product_name': product_name,
                                'brand': 'Unknown',
                                'price': '0.00',
                                'image_path': image_path
                            }
                            sample_products.append(product)
                            count += 1
                            if count <= 5:
                                print(f"Found image: {image_path}")
                            
                            if len(sample_products) >= 50:  # Limit to 50 samples
                                break
                    except Exception as e:
                        print(f"Could not open image {image_path}: {e}")
                        continue
            
            if len(sample_products) >= 50:
                break
        
        print(f"Created {len(sample_products)} sample products from image directory")
        return sample_products
    
    def get_test_images(self, num_images=50):
        """Get a random sample of product images for testing"""
        if not self.products:
            print("No products found in database, creating samples from images")
            self.products = self.create_sample_test_data()
            
        if not self.products:
            print("Could not create any sample products. No images found.")
            return []
            
        # Filter products that have valid image paths
        products_with_images = [p for p in self.products if ('image_path' in p and p['image_path'] and os.path.exists(p['image_path']))]
        
        if not products_with_images:
            print("No products with valid image paths found")
            return []
            
        # Randomly sample images
        sample_size = min(num_images, len(products_with_images))
        test_products = random.sample(products_with_images, sample_size)
        
        print(f"Selected {len(test_products)} products for testing")
        return test_products
    
    def recognize_product(self, image_path, top_k=5):
        """Recognize a product from an image using CLIP"""
        try:
            # Load and encode the query image
            image = Image.open(image_path).convert('RGB')
            query_embedding = self.model.encode(image)
            
            # Create text descriptions for all products
            product_texts = []
            for product in self.products:
                name = product.get('product_name', '')
                brand = product.get('brand', '')
                if brand and name:
                    text = f"{brand} {name}"
                else:
                    text = name
                product_texts.append((text, product))
            
            # Get text embeddings for all products
            text_descriptions = [item[0] for item in product_texts]
            text_embeddings = self.model.encode(text_descriptions)
            
            # Calculate similarities
            similarities = util.cos_sim(query_embedding, text_embeddings)[0]
            
            # Get top matches
            top_indices = torch.topk(similarities, min(top_k, len(similarities))).indices
            
            results = []
            for idx in top_indices:
                similarity = similarities[idx].item()
                product = product_texts[idx][1]
                results.append((similarity, product))
            
            return results
            
        except Exception as e:
            print(f"Error recognizing product from {image_path}: {e}")
            return []
    
    def run_test(self, num_images=50):
        """Run the recognition test on a set of images"""
        test_products = self.get_test_images(num_images)
        
        if not test_products:
            return []
        
        test_results = []
        
        for i, product in enumerate(test_products):
            print(f"Testing product {i+1}/{len(test_products)}")
            
            # Get image path
            image_path = product.get('image_path', '')
            if not image_path and 'image_url' in product:
                # If using URL, you would need to download the image first
                # For simplicity we'll skip these in this example
                continue
            
            if not os.path.exists(image_path):
                print(f"Image not found: {image_path}")
                continue
            
            # Get ground truth
            ground_truth = {
                'product_name': product.get('product_name', ''),
                'brand': product.get('brand', ''),
                'price': product.get('price', ''),
                'image_path': image_path
            }
            
            # Recognize product
            recognition_results = self.recognize_product(image_path)
            
            # Create image data URI for HTML
            try:
                with open(image_path, 'rb') as img_file:
                    img_data = base64.b64encode(img_file.read()).decode('utf-8')
                    img_ext = os.path.splitext(image_path)[1].lower().replace('.', '')
                    if img_ext in ['jpg', 'jpeg']:
                        img_ext = 'jpeg'
                    img_uri = f"data:image/{img_ext};base64,{img_data}"
            except Exception as e:
                print(f"Error creating data URI for {image_path}: {e}")
                img_uri = ""
            
            # Store result
            test_results.append({
                'ground_truth': ground_truth,
                'recognition_results': recognition_results,
                'image_uri': img_uri
            })
        
        return test_results

    def generate_html_report(self, test_results, output_path='clip_test_results.html'):
        """Generate an interactive HTML report of the test results"""
        html_content = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>CLIP Product Recognition Test Results</title>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    margin: 0;
                    padding: 20px;
                    background-color: #f5f5f5;
                }
                .container {
                    max-width: 1200px;
                    margin: 0 auto;
                    background-color: white;
                    padding: 20px;
                    box-shadow: 0 0 10px rgba(0,0,0,0.1);
                    border-radius: 5px;
                }
                h1 {
                    color: #333;
                    text-align: center;
                }
                .test-result {
                    display: none;
                    margin-bottom: 30px;
                    padding: 20px;
                    border: 1px solid #ddd;
                    border-radius: 5px;
                }
                .test-result.active {
                    display: block;
                }
                .result-grid {
                    display: grid;
                    grid-template-columns: 1fr 2fr;
                    gap: 20px;
                }
                .product-image {
                    max-width: 100%;
                    max-height: 400px;
                    border: 1px solid #ddd;
                }
                .product-details h3 {
                    margin-top: 0;
                    color: #444;
                }
                .match-item {
                    margin-bottom: 10px;
                    padding: 10px;
                    background-color: #f9f9f9;
                    border-radius: 3px;
                }
                .match-item.correct {
                    background-color: #d4edda;
                }
                .similarity {
                    font-weight: bold;
                    color: #007bff;
                }
                .navigation {
                    display: flex;
                    justify-content: space-between;
                    margin: 20px 0;
                }
                button {
                    padding: 10px 20px;
                    background-color: #007bff;
                    color: white;
                    border: none;
                    border-radius: 3px;
                    cursor: pointer;
                }
                button:disabled {
                    background-color: #cccccc;
                }
                .page-indicator {
                    text-align: center;
                    font-size: 18px;
                    margin: 10px 0;
                }
                .accuracy-stats {
                    background-color: #e9ecef;
                    padding: 15px;
                    margin-bottom: 20px;
                    border-radius: 5px;
                    text-align: center;
                }
                .correct {
                    color: #28a745;
                }
                .incorrect {
                    color: #dc3545;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>CLIP Product Recognition Test Results</h1>
                
                <div class="accuracy-stats">
                    <h2>Test Summary</h2>
                    <p>Total Products Tested: <span id="total-tested">0</span></p>
                    <p>Top-1 Accuracy: <span class="correct" id="top1-accuracy">0%</span></p>
                    <p>Top-3 Accuracy: <span class="correct" id="top3-accuracy">0%</span></p>
                    <p>Top-5 Accuracy: <span class="correct" id="top5-accuracy">0%</span></p>
                </div>
                
                <div class="navigation">
                    <button id="prev-btn" disabled>Previous</button>
                    <div class="page-indicator">Product <span id="current-index">1</span> of <span id="total-count">0</span></div>
                    <button id="next-btn">Next</button>
                </div>
        """
        
        # Calculate accuracy statistics
        total_tested = len(test_results)
        correct_top1 = 0
        correct_top3 = 0
        correct_top5 = 0
        
        # Add each test result to the HTML
        for i, result in enumerate(test_results):
            ground_truth = result['ground_truth']
            recognition_results = result['recognition_results']
            image_uri = result['image_uri']
            
            # Check if the top matches include the correct product
            # We'll consider it a match if the product name contains the ground truth product name or vice versa
            ground_truth_name = ground_truth['product_name'].lower()
            
            is_correct_top1 = False
            is_correct_top3 = False
            is_correct_top5 = False
            
            if recognition_results:
                # Check top-1
                top1_name = recognition_results[0][1].get('product_name', '').lower()
                if ground_truth_name in top1_name or top1_name in ground_truth_name:
                    is_correct_top1 = True
                    correct_top1 += 1
                
                # Check top-3
                for j in range(min(3, len(recognition_results))):
                    match_name = recognition_results[j][1].get('product_name', '').lower()
                    if ground_truth_name in match_name or match_name in ground_truth_name:
                        is_correct_top3 = True
                        break
                
                if is_correct_top3:
                    correct_top3 += 1
                
                # Check top-5
                for j in range(min(5, len(recognition_results))):
                    match_name = recognition_results[j][1].get('product_name', '').lower()
                    if ground_truth_name in match_name or match_name in ground_truth_name:
                        is_correct_top5 = True
                        break
                
                if is_correct_top5:
                    correct_top5 += 1
            
            # Create HTML for this test result
            active_class = "active" if i == 0 else ""
            html_content += f"""
                <div class="test-result {active_class}" data-index="{i+1}">
                    <div class="result-grid">
                        <div>
                            <h3>Test Image</h3>
                            <img src="{image_uri}" alt="Product" class="product-image">
                            <div class="ground-truth">
                                <h3>Ground Truth</h3>
                                <p><strong>Product:</strong> {ground_truth['product_name']}</p>
                                <p><strong>Brand:</strong> {ground_truth['brand']}</p>
                                <p><strong>Price:</strong> {ground_truth['price']}</p>
                            </div>
                        </div>
                        <div class="product-details">
                            <h3>Recognition Results</h3>
            """
            
            if recognition_results:
                for j, (similarity, product) in enumerate(recognition_results):
                    match_name = product.get('product_name', '')
                    match_brand = product.get('brand', '')
                    match_price = product.get('price', '')
                    
                    # Check if this match is correct
                    is_match_correct = (ground_truth_name in match_name.lower() or match_name.lower() in ground_truth_name)
                    match_class = "correct" if is_match_correct else ""
                    
                    html_content += f"""
                            <div class="match-item {match_class}">
                                <h4>Match #{j+1} - <span class="similarity">{similarity:.4f}</span></h4>
                                <p><strong>Product:</strong> {match_name}</p>
                                <p><strong>Brand:</strong> {match_brand}</p>
                                <p><strong>Price:</strong> {match_price}</p>
                            </div>
                    """
            else:
                html_content += "<p>No recognition results</p>"
            
            html_content += """
                        </div>
                    </div>
                </div>
            """
        
        # Calculate accuracies
        top1_accuracy = (correct_top1 / total_tested) * 100 if total_tested > 0 else 0
        top3_accuracy = (correct_top3 / total_tested) * 100 if total_tested > 0 else 0
        top5_accuracy = (correct_top5 / total_tested) * 100 if total_tested > 0 else 0
        
        # Add JavaScript for interactivity
        html_content += f"""
                <div class="navigation">
                    <button id="prev-btn2" disabled>Previous</button>
                    <div class="page-indicator">Product <span id="current-index2">1</span> of <span id="total-count2">{total_tested}</span></div>
                    <button id="next-btn2">Next</button>
                </div>
            </div>
            
            <script>
                // Update summary statistics
                document.getElementById('total-tested').textContent = '{total_tested}';
                document.getElementById('top1-accuracy').textContent = '{top1_accuracy:.2f}%';
                document.getElementById('top3-accuracy').textContent = '{top3_accuracy:.2f}%';
                document.getElementById('top5-accuracy').textContent = '{top5_accuracy:.2f}%';
                
                // Navigation functionality
                const results = document.querySelectorAll('.test-result');
                const totalCount = {total_tested};
                let currentIndex = 1;
                
                document.getElementById('total-count').textContent = totalCount;
                document.getElementById('total-count2').textContent = totalCount;
                
                function updateDisplay() {{
                    results.forEach(result => {{
                        result.classList.remove('active');
                        if (parseInt(result.dataset.index) === currentIndex) {{
                            result.classList.add('active');
                        }}
                    }});
                    
                    document.getElementById('current-index').textContent = currentIndex;
                    document.getElementById('current-index2').textContent = currentIndex;
                    
                    document.getElementById('prev-btn').disabled = currentIndex === 1;
                    document.getElementById('prev-btn2').disabled = currentIndex === 1;
                    document.getElementById('next-btn').disabled = currentIndex === totalCount;
                    document.getElementById('next-btn2').disabled = currentIndex === totalCount;
                }}
                
                document.getElementById('prev-btn').addEventListener('click', () => {{
                    if (currentIndex > 1) {{
                        currentIndex--;
                        updateDisplay();
                    }}
                }});
                
                document.getElementById('next-btn').addEventListener('click', () => {{
                    if (currentIndex < totalCount) {{
                        currentIndex++;
                        updateDisplay();
                    }}
                }});
                
                document.getElementById('prev-btn2').addEventListener('click', () => {{
                    if (currentIndex > 1) {{
                        currentIndex--;
                        updateDisplay();
                    }}
                }});
                
                document.getElementById('next-btn2').addEventListener('click', () => {{
                    if (currentIndex < totalCount) {{
                        currentIndex++;
                        updateDisplay();
                    }}
                }});
            </script>
        </body>
        </html>
        """
        
        # Write the HTML file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"HTML report generated at {output_path}")
        return output_path

def main():
    # Initialize the tester
    tester = ClipProductTester()
    
    # Run the test
    test_results = tester.run_test(num_images=50)
    
    # Generate the HTML report
    tester.generate_html_report(test_results)
    
    print("Test completed and report generated!")

if __name__ == "__main__":
    main()