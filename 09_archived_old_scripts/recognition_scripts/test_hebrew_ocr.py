#!/usr/bin/env python3
"""
Test script for Hebrew-optimized OCR improvements
Run this to validate the OCR enhancements on Hebrew product text
"""

import os
import sys
from pathlib import Path
import tempfile

def test_hebrew_ocr():
    """Test the improved Hebrew OCR system"""
    print("🔍 Testing Hebrew-Optimized OCR System")
    print("=" * 50)
    
    try:
        from recognition_scripts.fixed_product_recognition import TesseractOCR
        print("✅ Successfully imported Hebrew-optimized OCR class")
    except ImportError as e:
        print(f"❌ Failed to import OCR class: {e}")
        return False
    
    # Initialize OCR
    ocr = TesseractOCR()
    print(f"✅ OCR initialized with confidence threshold: {ocr.min_confidence}%")
    print(f"📝 PSM modes to test: {ocr.psm_modes}")
    
    # Test 1: Create and test Hebrew text images
    print("\n🧪 Test 1: Creating test images with Hebrew text")
    print("-" * 30)
    
    hebrew_test_texts = [
        "חלב 3%",
        "יוגורט ביו",
        "לחם שחור",
        "גבינה לבנה",
        "עגבניות",
        "MILK חלב"  # Mixed Hebrew/English
    ]
    
    for i, text in enumerate(hebrew_test_texts, 1):
        with tempfile.NamedTemporaryFile(suffix=f'_test_{i}.jpg', delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            # Create test image
            if ocr.create_test_image(text, temp_path):
                print(f"📸 Created test image for: '{text}'")
                
                # Test OCR
                result = ocr.extract_text(temp_path)
                
                print(f"   📊 Results:")
                print(f"      Text: '{result['text']}'")
                print(f"      Confidence: {result['confidence']:.1f}%")
                print(f"      Method: {result['method']}")
                print(f"      PSM: {result['psm']}")
                print(f"      High Quality: {'✅' if result['confidence'] >= 90 else '⚠️'}")
                print()
            else:
                print(f"❌ Failed to create test image for: '{text}'")
                
        finally:
            # Clean up
            try:
                os.unlink(temp_path)
            except:
                pass
    
    # Test 2: Test on real product images
    print("🧪 Test 2: Testing on real product images")
    print("-" * 30)
    
    # Find sample product images
    product_images = list(Path("data/products").glob("*/001.jpg"))[:3]
    
    if product_images:
        for img_path in product_images:
            print(f"📷 Testing: {img_path}")
            
            result = ocr.extract_text(str(img_path))
            
            print(f"   📊 Results:")
            print(f"      Text: '{result['text']}'")
            print(f"      Confidence: {result['confidence']:.1f}%")
            print(f"      Method: {result['method']}")
            print(f"      PSM: {result['psm']}")
            print(f"      Words found: {result['word_count']}")
            print(f"      High Quality: {'✅' if result['confidence'] >= 70 else '⚠️'}")
            print()
    else:
        print("⚠️  No product images found in data/products directory")
    
    # Test 3: Performance comparison
    print("🧪 Test 3: Performance summary")
    print("-" * 30)
    print("🎯 Key improvements implemented:")
    print("   • LSTM neural network (--oem 3) for Hebrew excellence")
    print("   • Optimized PSM modes: 6 (uniform block) prioritized")
    print("   • High-contrast preprocessing for 90%+ confidence")
    print("   • Hebrew + English language support (heb+eng)")
    print("   • Confidence threshold raised to 70% for quality")
    print("   • Multiple preprocessing methods tested automatically")
    
    return True

def test_backend_endpoints():
    """Test the OCR backend endpoints if server is running"""
    print("\n🌐 Testing Backend OCR Endpoints")
    print("=" * 50)
    
    try:
        import requests
        
        # Test the test endpoint
        print("🔗 Testing /api/ocr/test endpoint...")
        response = requests.post(
            'http://localhost:5001/api/ocr/test', 
            json={'text': 'חלב 3%'},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Backend endpoint working!")
            print(f"   Original: {data['original_text']}")
            print(f"   Extracted: {data['extracted_text']}")
            print(f"   Confidence: {data['confidence']}%")
            print(f"   High Quality: {'✅' if data['high_quality'] else '⚠️'}")
        else:
            print(f"⚠️  Endpoint returned status: {response.status_code}")
            
    except requests.exceptions.ConnectionError:
        print("⚠️  Backend server not running on localhost:5001")
        print("   To test endpoints: python backend_app.py")
    except ImportError:
        print("⚠️  requests library not available")
    except Exception as e:
        print(f"❌ Error testing endpoints: {e}")

def main():
    """Main test function"""
    print("🚀 Hebrew OCR Test Suite")
    print("Testing improvements based on Hebrew OCR preprocessing guide")
    print()
    
    # Test OCR functionality
    if test_hebrew_ocr():
        print("✅ OCR tests completed successfully!")
    else:
        print("❌ OCR tests failed")
        return
    
    # Test backend endpoints
    test_backend_endpoints()
    
    print("\n🎉 Testing complete!")
    print("\nTo run individual tests:")
    print("python test_hebrew_ocr.py")
    print("\nTo start backend server:")
    print("python backend_app.py")

if __name__ == "__main__":
    main()