#!/usr/bin/env python3
"""
Test script to verify the backend properly filters products
"""

import requests
import json

API_URL = "http://localhost:8000"

def test_search():
    """Test the search endpoint"""
    print("\n=== Testing Search Endpoint ===")

    try:
        response = requests.get(f"{API_URL}/api/search", params={"q": "vitamin"})

        if response.status_code == 200:
            products = response.json()
            print(f"Found {len(products)} products")

            # Check first few products
            for product in products[:3]:
                print(f"\nProduct: {product['name']}")
                print(f"  Brand: {product.get('brand', 'N/A')}")
                print(f"  Barcode: {product['barcode']}")

                if product['prices']:
                    min_price = min(p['price'] for p in product['prices'])
                    print(f"  Lowest Price: ₪{min_price}")
                    print(f"  Available at {len(product['prices'])} locations")
                else:
                    print("  ❌ NO PRICES - This shouldn't happen!")

            # Verify all products have prices
            products_without_prices = [p for p in products if not p['prices']]
            if products_without_prices:
                print(f"\n❌ ERROR: {len(products_without_prices)} products have no prices!")
                for p in products_without_prices[:3]:
                    print(f"  - {p['name']} (barcode: {p['barcode']})")
            else:
                print(f"\n✅ All {len(products)} products have valid prices!")

        else:
            print(f"❌ Search failed with status {response.status_code}")

    except Exception as e:
        print(f"❌ Error: {e}")

def test_health():
    """Test health endpoint"""
    print("\n=== Testing Health Endpoint ===")

    try:
        response = requests.get(f"{API_URL}/health")
        if response.status_code == 200:
            print("✅ Backend is healthy")
        else:
            print(f"❌ Health check failed with status {response.status_code}")
    except Exception as e:
        print(f"❌ Cannot connect to backend: {e}")
        print("Make sure the backend is running at http://localhost:8000")
        return False

    return True

if __name__ == "__main__":
    print("Testing PharmMate Backend")
    print("=" * 40)

    if test_health():
        test_search()

    print("\n" + "=" * 40)
    print("Test complete!")