#!/usr/bin/env python3
"""
Simple test for GET /api/recommendations/popular endpoint
"""

import requests
import time

BASE_URL = "http://localhost:8000"

print("Testing GET /api/recommendations/popular endpoint")
print("=" * 50)

print("\n1. Testing without authentication (public endpoint)...")
start_time = time.time()

try:
    response = requests.get(f"{BASE_URL}/api/recommendations/popular?limit=3", timeout=10)
    elapsed = time.time() - start_time

    if response.status_code == 200:
        recommendations = response.json()
        print(f"✅ Success! Got {len(recommendations)} recommendations in {elapsed:.2f}s")

        for i, product in enumerate(recommendations, 1):
            print(f"\n{i}. {product['name']}")
            print(f"   Barcode: {product['barcode']}")
            print(f"   Brand: {product.get('brand', 'N/A')}")

            if product.get('prices'):
                num_prices = len(product['prices'])
                print(f"   Prices available: {num_prices}")

            if product.get('promotions'):
                print(f"   🎉 Promotions: {len(product['promotions'])}")
    else:
        print(f"❌ Failed: {response.status_code}")
        print(f"   Error: {response.text}")

except requests.exceptions.Timeout:
    print(f"❌ Request timed out after 10 seconds")
except Exception as e:
    print(f"❌ Error: {e}")

print("\n" + "=" * 50)
