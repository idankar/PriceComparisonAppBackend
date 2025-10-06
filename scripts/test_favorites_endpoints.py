#!/usr/bin/env python3
"""
Test script to verify that the favorites endpoints fix is working correctly.

This script validates that:
1. GET /api/favorites returns ProductSummary objects (not just strings)
2. DELETE /api/favorites/remove/{product_barcode} exists and works
3. Proper error handling for non-existent favorites
"""

import requests
import sys
import time

BASE_URL = "http://localhost:8000"


def test_favorites_endpoints():
    """Test the favorites endpoints."""
    print("="*60)
    print("FAVORITES ENDPOINTS FIX VALIDATION TEST")
    print("="*60)

    # Step 1: Register a test user
    print("\n[1/6] Registering test user...")
    timestamp = int(time.time())
    register_data = {
        "username": f"testuser_{timestamp}",
        "email": f"test{timestamp}@example.com",
        "password": "testpass123"
    }

    try:
        response = requests.post(f"{BASE_URL}/api/register", json=register_data)
        response.raise_for_status()
        token = response.json()['access_token']
        headers = {"Authorization": f"Bearer {token}"}
        print("✅ User registered successfully")
    except requests.RequestException as e:
        print(f"❌ Failed to register user: {str(e)}")
        return False

    # Step 2: Add products to favorites
    print("\n[2/6] Adding products to favorites...")
    test_products = [
        "3386461515671",
        "7290103445508",
        "8710428017000"
    ]

    for barcode in test_products:
        try:
            response = requests.post(
                f"{BASE_URL}/api/favorites/add",
                params={"product_barcode": barcode},
                headers=headers
            )
            response.raise_for_status()
            print(f"  ✓ Added product {barcode}")
        except requests.RequestException as e:
            print(f"  ✗ Failed to add product {barcode}: {str(e)}")
            return False

    print(f"✅ Added {len(test_products)} products to favorites")

    # Step 3: Test GET /api/favorites returns ProductSummary objects
    print("\n[3/6] Testing GET /api/favorites...")
    try:
        response = requests.get(f"{BASE_URL}/api/favorites", headers=headers)
        response.raise_for_status()
        favorites = response.json()

        if not isinstance(favorites, list):
            print("❌ Response is not an array")
            return False

        if len(favorites) == 0:
            print("❌ No favorites returned")
            return False

        print(f"✓ Received {len(favorites)} favorites")

        # Validate structure of first favorite
        first_fav = favorites[0]
        required_fields = ['product_id', 'barcode', 'name', 'image_url', 'lowest_price']
        missing_fields = [field for field in required_fields if field not in first_fav]

        if missing_fields:
            print(f"❌ Missing fields in response: {missing_fields}")
            return False

        print(f"\nFirst favorite structure:")
        print(f"  - product_id: {first_fav['product_id']}")
        print(f"  - barcode: {first_fav['barcode']}")
        print(f"  - name: {first_fav['name'][:30]}...")
        print(f"  - brand: {first_fav.get('brand', 'null')}")
        print(f"  - image_url: {first_fav['image_url'][:50]}...")
        print(f"  - lowest_price: {first_fav['lowest_price']}")

        # Validate all favorites have required fields
        for i, fav in enumerate(favorites):
            for field in required_fields:
                if field not in fav:
                    print(f"❌ Favorite {i+1} missing field: {field}")
                    return False

        print(f"\n✅ All {len(favorites)} favorites have correct structure")

    except requests.RequestException as e:
        print(f"❌ Failed to get favorites: {str(e)}")
        return False

    # Step 4: Test DELETE /api/favorites/remove
    print("\n[4/6] Testing DELETE /api/favorites/remove...")
    barcode_to_remove = test_products[1]  # Remove the middle one

    try:
        response = requests.delete(
            f"{BASE_URL}/api/favorites/remove/{barcode_to_remove}",
            headers=headers
        )
        response.raise_for_status()
        result = response.json()

        if result.get('status') != 'success':
            print(f"❌ Unexpected response: {result}")
            return False

        print(f"✓ Successfully removed product {barcode_to_remove}")
        print(f"  Response: {result['message']}")
        print("✅ DELETE endpoint works correctly")

    except requests.RequestException as e:
        print(f"❌ Failed to remove favorite: {str(e)}")
        return False

    # Step 5: Verify the favorite was removed
    print("\n[5/6] Verifying favorite was removed...")
    try:
        response = requests.get(f"{BASE_URL}/api/favorites", headers=headers)
        response.raise_for_status()
        favorites_after = response.json()

        if len(favorites_after) != len(test_products) - 1:
            print(f"❌ Expected {len(test_products) - 1} favorites, got {len(favorites_after)}")
            return False

        removed_barcodes = [fav['barcode'] for fav in favorites_after]
        if barcode_to_remove in removed_barcodes:
            print(f"❌ Product {barcode_to_remove} still in favorites!")
            return False

        print(f"✓ Favorites count correct: {len(favorites_after)}")
        print(f"✓ Removed product not in list")
        print("✅ Favorite successfully removed")

    except requests.RequestException as e:
        print(f"❌ Failed to verify removal: {str(e)}")
        return False

    # Step 6: Test error handling - remove non-existent favorite
    print("\n[6/6] Testing error handling...")
    fake_barcode = "9999999999999"

    try:
        response = requests.delete(
            f"{BASE_URL}/api/favorites/remove/{fake_barcode}",
            headers=headers
        )

        if response.status_code != 404:
            print(f"❌ Expected 404, got {response.status_code}")
            return False

        error_detail = response.json().get('detail')
        if 'not found' not in error_detail.lower():
            print(f"❌ Unexpected error message: {error_detail}")
            return False

        print(f"✓ Returns 404 for non-existent favorite")
        print(f"  Error message: {error_detail}")
        print("✅ Error handling works correctly")

    except requests.RequestException as e:
        print(f"❌ Failed to test error handling: {str(e)}")
        return False

    return True


def main():
    """Run all tests."""
    try:
        # Test if server is running
        response = requests.get(f"{BASE_URL}/health")
        response.raise_for_status()
    except requests.RequestException:
        print("❌ Backend server is not running on http://localhost:8000")
        print("   Please start the server first: python3 02_backend_api/backend.py")
        return 1

    success = test_favorites_endpoints()

    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)

    if success:
        print("\n✅ ALL TESTS PASSED")
        print("\nFavorites endpoints are working correctly:")
        print("  • GET /api/favorites returns ProductSummary objects")
        print("  • DELETE /api/favorites/remove/{barcode} works")
        print("  • Proper error handling for invalid requests")
        return 0
    else:
        print("\n❌ TESTS FAILED")
        print("\nPlease check the implementation and try again.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
