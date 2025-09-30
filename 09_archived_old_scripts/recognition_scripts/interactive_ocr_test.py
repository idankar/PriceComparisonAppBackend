#!/usr/bin/env python3
"""
Interactive Hebrew OCR Test
Allows you to test OCR on any image file or create custom test images
"""

import os
import sys
from pathlib import Path
import tempfile

def test_custom_image():
    """Test OCR on a custom image file"""
    from recognition_scripts.fixed_product_recognition import TesseractOCR
    
    image_path = input("Enter image path (or press Enter for example): ").strip()
    
    if not image_path:
        # Try to find a sample image
        sample_images = list(Path("data/products").glob("*/001.jpg"))
        if sample_images:
            image_path = str(sample_images[0])
            print(f"Using sample image: {image_path}")
        else:
            print("No sample images found and no path provided")
            return
    
    if not os.path.exists(image_path):
        print(f"❌ Image not found: {image_path}")
        return
    
    print(f"\n🔍 Testing OCR on: {image_path}")
    print("-" * 50)
    
    ocr = TesseractOCR()
    result = ocr.extract_text(image_path)
    
    print(f"📊 OCR Results:")
    print(f"   Text extracted: '{result['text']}'")
    print(f"   Confidence: {result['confidence']:.1f}%")
    print(f"   Best method: {result['method']}")
    print(f"   PSM mode: {result['psm']}")
    print(f"   High-confidence words: {result['words']}")
    print(f"   Word count: {result['word_count']}")
    print(f"   Quality rating: {'🟢 Excellent' if result['confidence'] >= 90 else '🟡 Good' if result['confidence'] >= 70 else '🔴 Poor'}")

def test_custom_text():
    """Create and test OCR on custom Hebrew text"""
    from recognition_scripts.fixed_product_recognition import TesseractOCR
    
    text = input("Enter Hebrew/English text to test (or press Enter for 'חלב 3%'): ").strip()
    
    if not text:
        text = "חלב 3%"
    
    print(f"\n🧪 Creating test image with text: '{text}'")
    print("-" * 50)
    
    with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as temp_file:
        temp_path = temp_file.name
    
    try:
        ocr = TesseractOCR()
        
        if ocr.create_test_image(text, temp_path):
            print(f"✅ Test image created: {temp_path}")
            
            # Test OCR
            result = ocr.extract_text(temp_path)
            
            print(f"📊 OCR Results:")
            print(f"   Original text: '{text}'")
            print(f"   Extracted text: '{result['text']}'")
            print(f"   Match: {'✅' if text.strip() in result['text'] or result['text'].strip() in text else '❌'}")
            print(f"   Confidence: {result['confidence']:.1f}%")
            print(f"   Best method: {result['method']}")
            print(f"   PSM mode: {result['psm']}")
            print(f"   Quality: {'🟢 Excellent' if result['confidence'] >= 90 else '🟡 Good' if result['confidence'] >= 70 else '🔴 Poor'}")
            
            # Ask if user wants to keep the image
            keep = input(f"\nKeep test image? (y/N): ").strip().lower()
            if keep == 'y':
                final_path = f"test_image_{text.replace(' ', '_')}.jpg"
                os.rename(temp_path, final_path)
                print(f"Image saved as: {final_path}")
                temp_path = None  # Don't delete
        else:
            print("❌ Failed to create test image")
            
    finally:
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)

def compare_methods():
    """Compare different OCR preprocessing methods"""
    from recognition_scripts.fixed_product_recognition import TesseractOCR
    
    text = input("Enter text for method comparison (or press Enter for 'יוגורט'): ").strip()
    if not text:
        text = "יוגורט"
    
    print(f"\n🔬 Comparing preprocessing methods for: '{text}'")
    print("=" * 60)
    
    with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as temp_file:
        temp_path = temp_file.name
    
    try:
        ocr = TesseractOCR()
        
        if ocr.create_test_image(text, temp_path):
            print("Method Comparison Results:")
            print("-" * 40)
            
            # Test each preprocessing method individually
            methods = ["high_contrast", "inverted_high_contrast", "enhanced_contrast", 
                      "adaptive_gaussian", "denoised", "original"]
            
            for method in methods:
                # Temporarily override the method list to test just one
                old_methods = ocr.extract_text.__code__.co_names
                
                # Create a version that tests just this method
                import cv2
                import numpy as np
                import pytesseract
                
                image = cv2.imread(temp_path)
                processed = ocr._preprocess_image(image, method)
                
                # Test with best PSM mode (6)
                result = ocr._extract_with_config(processed, 6)
                
                print(f"   {method:<20}: {result['confidence']:5.1f}% - '{result['text']}'")
        
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)

def main():
    """Interactive main menu"""
    while True:
        print("\n🎯 Interactive Hebrew OCR Test")
        print("=" * 40)
        print("1. Test OCR on existing image")
        print("2. Create and test custom text")
        print("3. Compare preprocessing methods")
        print("4. Run full test suite")
        print("5. Exit")
        
        choice = input("\nSelect option (1-5): ").strip()
        
        try:
            if choice == "1":
                test_custom_image()
            elif choice == "2":
                test_custom_text()
            elif choice == "3":
                compare_methods()
            elif choice == "4":
                os.system("python test_hebrew_ocr.py")
            elif choice == "5":
                print("👋 Goodbye!")
                break
            else:
                print("❌ Invalid option. Please choose 1-5.")
                
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")
            print("Make sure all dependencies are installed and try again.")

if __name__ == "__main__":
    main()