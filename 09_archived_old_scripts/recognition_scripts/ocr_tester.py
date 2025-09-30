#!/usr/bin/env python3
"""
Interactive OCR Tester - Test individual images easily
"""

import requests
import json
import base64
import sys
import os
from pathlib import Path

def image_to_base64(image_path):
    """Convert image to base64 string"""
    try:
        with open(image_path, 'rb') as img_file:
            img_data = base64.b64encode(img_file.read()).decode()
            return f"data:image/jpeg;base64,{img_data}"
    except Exception as e:
        print(f"❌ Error reading image: {e}")
        return None

def test_ocr(image_path):
    """Test OCR on a single image"""
    print(f"\n🔍 Testing OCR on: {image_path}")
    print("=" * 60)
    
    # Check if file exists
    if not os.path.exists(image_path):
        print(f"❌ File not found: {image_path}")
        return
    
    # Convert to base64
    image_data = image_to_base64(image_path)
    if not image_data:
        return
    
    # Test OCR endpoint
    url = "http://localhost:5001/api/ocr/extract"
    
    try:
        print("📡 Sending request to OCR server...")
        response = requests.post(
            url, 
            json={"image": image_data}, 
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            
            print(f"\n✅ OCR Results:")
            print(f"  📝 Extracted Text: '{result.get('text', '').strip()}'")
            print(f"  🎯 Confidence: {result.get('confidence', 0):.1f}%")
            print(f"  📊 Word Count: {result.get('word_count', 0)}")
            print(f"  🔧 Method Used: {result.get('method', 'unknown')}")
            print(f"  🔧 PSM Mode: {result.get('psm_mode', 0)}")
            print(f"  ⭐ High Quality: {'Yes' if result.get('high_quality', False) else 'No'}")
            
            if result.get('words'):
                print(f"  🔤 Individual Words: {result.get('words', [])}")
            
            if result.get('confidence', 0) >= 90:
                print("  🎉 Excellent recognition quality!")
            elif result.get('confidence', 0) >= 70:
                print("  👍 Good recognition quality")
            else:
                print("  ⚠️  Poor recognition quality - image may be unclear")
                
        else:
            print(f"❌ Server error {response.status_code}: {response.text}")
            
    except requests.exceptions.ConnectionError:
        print("❌ Connection failed!")
        print("   Make sure the server is running:")
        print("   python backend_app.py")
    except Exception as e:
        print(f"❌ Error: {e}")

def create_test_image():
    """Create a test image with custom text"""
    from PIL import Image, ImageDraw, ImageFont
    
    text = input("\n📝 Enter text for test image (Hebrew/English): ").strip()
    if not text:
        text = "חלב תנובה 3%"  # Default Hebrew text
    
    try:
        img = Image.new('RGB', (400, 100), color='white')
        draw = ImageDraw.Draw(img)
        
        # Try to use a larger font
        try:
            font = ImageFont.truetype("/System/Library/Fonts/Arial.ttf", 24)
        except:
            font = ImageFont.load_default()
        
        # Center the text
        bbox = draw.textbbox((0, 0), text, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        
        x = (400 - text_width) // 2
        y = (100 - text_height) // 2
        
        draw.text((x, y), text, fill='black', font=font)
        
        test_path = "/tmp/custom_test.jpg"
        img.save(test_path)
        print(f"✅ Created test image: {test_path}")
        
        return test_path
        
    except Exception as e:
        print(f"❌ Error creating test image: {e}")
        return None

def browse_product_images():
    """Browse and select from existing product images"""
    data_dir = Path("data/products")
    
    if not data_dir.exists():
        print("❌ No product images directory found (data/products)")
        return None
    
    product_dirs = [d for d in data_dir.iterdir() if d.is_dir()]
    if not product_dirs:
        print("❌ No product directories found")
        return None
    
    print(f"\n📂 Found {len(product_dirs)} product directories")
    print("First 10 products:")
    
    for i, product_dir in enumerate(product_dirs[:10]):
        image_files = list(product_dir.glob("*.jpg"))
        if image_files:
            print(f"  {i+1}. {product_dir.name} ({len(image_files)} images)")
    
    try:
        choice = int(input(f"\nSelect product (1-{min(10, len(product_dirs))}): ")) - 1
        if 0 <= choice < min(10, len(product_dirs)):
            selected_dir = product_dirs[choice]
            image_files = list(selected_dir.glob("*.jpg"))
            if image_files:
                return str(image_files[0])  # Return first image
        
        print("❌ Invalid selection")
        return None
        
    except (ValueError, IndexError):
        print("❌ Invalid input")
        return None

def main():
    """Main interactive function"""
    print("🧪 Interactive OCR Tester")
    print("=" * 40)
    
    while True:
        print("\nOptions:")
        print("1. Test specific image file")
        print("2. Create and test custom text image") 
        print("3. Browse and test product images")
        print("4. Exit")
        
        choice = input("\nSelect option (1-4): ").strip()
        
        if choice == "1":
            image_path = input("Enter image path: ").strip()
            if image_path:
                test_ocr(image_path)
                
        elif choice == "2":
            image_path = create_test_image()
            if image_path:
                test_ocr(image_path)
                # Clean up
                try:
                    os.unlink(image_path)
                except:
                    pass
                    
        elif choice == "3":
            image_path = browse_product_images()
            if image_path:
                test_ocr(image_path)
                
        elif choice == "4":
            print("👋 Goodbye!")
            break
            
        else:
            print("❌ Invalid option")

if __name__ == "__main__":
    # Check if image path provided as command line argument
    if len(sys.argv) > 1:
        image_path = sys.argv[1]
        test_ocr(image_path)
    else:
        main()