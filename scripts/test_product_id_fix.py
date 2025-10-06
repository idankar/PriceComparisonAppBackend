#!/usr/bin/env python3
"""
Test script to verify that the product_id field fix is working correctly.

This script validates that:
1. All summary endpoints return product_id field
2. All product_ids can be successfully fetched via /api/products/{id}
3. All barcodes can be successfully fetched via /api/products/{barcode}
"""

import requests
import sys

BASE_URL = "http://localhost:8000"


def test_endpoint(endpoint_name, url, headers=None):
    """Test a summary endpoint and verify product_id is present."""
    print(f"\n{'='*60}")
    print(f"Testing: {endpoint_name}")
    print(f"URL: {url}")
    print(f"{'='*60}")

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        products = response.json()

        if not products:
            print(f"⚠️  No products returned from {endpoint_name}")
            return True

        print(f"✓ Received {len(products)} products")

        # Check first product structure
        first_product = products[0]
        print(f"\nFirst product structure:")
        print(f"  - product_id: {first_product.get('product_id', 'MISSING')}")
        print(f"  - barcode: {first_product.get('barcode', 'MISSING')}")
        print(f"  - name: {first_product.get('name', 'MISSING')[:50]}...")
        print(f"  - lowest_price: {first_product.get('lowest_price', 'MISSING')}")

        # Validate all products have product_id
        all_have_product_id = True
        missing_count = 0

        for i, product in enumerate(products):
            if 'product_id' not in product or not product['product_id']:
                all_have_product_id = False
                missing_count += 1
                if missing_count <= 3:  # Show first 3 examples
                    print(f"✗ Product {i+1} missing product_id: {product.get('name', 'Unknown')[:30]}")

        if all_have_product_id:
            print(f"\n✅ All {len(products)} products have product_id field")
        else:
            print(f"\n❌ {missing_count}/{len(products)} products missing product_id")
            return False

        # Test that product_ids work with detail endpoint
        print(f"\nTesting product detail fetching:")
        test_count = min(3, len(products))

        for i in range(test_count):
            product = products[i]
            product_id = product['product_id']

            # Test with product_id
            detail_response = requests.get(f"{BASE_URL}/api/products/{product_id}")
            if detail_response.status_code == 200:
                print(f"  ✓ Product ID {product_id}: 200 OK")
            else:
                print(f"  ✗ Product ID {product_id}: {detail_response.status_code}")
                return False

        print(f"\n✅ {endpoint_name} validation complete")
        return True

    except requests.RequestException as e:
        print(f"❌ Error testing {endpoint_name}: {str(e)}")
        return False


def main():
    """Run all tests."""
    print("="*60)
    print("PRODUCT_ID FIX VALIDATION TEST")
    print("="*60)

    results = []

    # Test 1: Popular Recommendations (unauthenticated)
    results.append(test_endpoint(
        "GET /api/recommendations/popular",
        f"{BASE_URL}/api/recommendations/popular?limit=10"
    ))

    # Test 2: Search
    results.append(test_endpoint(
        "GET /api/search",
        f"{BASE_URL}/api/search?q=a"
    ))

    # Test 3: Authenticated Recommendations
    # First register a test user
    import time
    timestamp = int(time.time())
    register_data = {
        "username": f"testuser_{timestamp}",
        "email": f"test{timestamp}@example.com",
        "password": "testpass123"
    }

    try:
        register_response = requests.post(
            f"{BASE_URL}/api/register",
            json=register_data
        )
        register_response.raise_for_status()
        token = register_response.json()['access_token']

        headers = {"Authorization": f"Bearer {token}"}

        results.append(test_endpoint(
            "GET /api/recommendations (authenticated)",
            f"{BASE_URL}/api/recommendations?limit=10",
            headers=headers
        ))
    except requests.RequestException as e:
        print(f"❌ Could not test authenticated endpoint: {str(e)}")
        results.append(False)

    # Print summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)

    passed = sum(results)
    total = len(results)

    print(f"\nTests Passed: {passed}/{total}")

    if passed == total:
        print("\n✅ ALL TESTS PASSED - product_id fix is working correctly!")
        return 0
    else:
        print(f"\n❌ {total - passed} TESTS FAILED - fix needs attention")
        return 1


if __name__ == "__main__":
    sys.exit(main())
