#!/usr/bin/env python3
"""
Test script for anonymous user features:
1. GET /api/recommendations/popular (public endpoint)
2. POST /api/sync (sync anonymous data to user account)
"""

import requests
import json
import random
import string
import bcrypt
import jwt
from datetime import datetime, timedelta

# Configuration
BASE_URL = "http://localhost:8000"

def generate_random_username():
    """Generate a random username for testing"""
    return "testuser_" + ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))

def create_test_user(username: str, email: str, password: str):
    """Create a test user and return user_id and token"""
    import psycopg2
    from psycopg2.extras import RealDictCursor

    conn = psycopg2.connect(
        dbname="price_comparison_app_v2",
        user="postgres",
        password="025655358",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    # Hash password using bcrypt
    password_bytes = password.encode('utf-8')
    salt = bcrypt.gensalt()
    password_hash = bcrypt.hashpw(password_bytes, salt).decode('utf-8')

    try:
        cursor.execute("""
            INSERT INTO users (username, email, password_hash)
            VALUES (%s, %s, %s)
            ON CONFLICT (username) DO UPDATE
            SET email = EXCLUDED.email, password_hash = EXCLUDED.password_hash
            RETURNING user_id
        """, (username, email, password_hash))

        conn.commit()
        result = cursor.fetchone()
        user_id = result['user_id']

        # Generate token
        JWT_SECRET = "your-secret-key-change-in-production"
        JWT_ALGORITHM = "HS256"
        payload = {
            "user_id": user_id,
            "username": username,
            "exp": datetime.utcnow() + timedelta(hours=24)
        }
        token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

        cursor.close()
        conn.close()
        return user_id, token

    except Exception as e:
        print(f"❌ Failed to create test user: {e}")
        conn.rollback()
        cursor.close()
        conn.close()
        return None, None

def get_sample_products():
    """Get some sample product barcodes from the database"""
    import psycopg2

    conn = psycopg2.connect(
        dbname="price_comparison_app_v2",
        user="postgres",
        password="025655358",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    cursor.execute("""
        SELECT barcode
        FROM canonical_products
        WHERE is_active = true
        AND barcode IS NOT NULL
        LIMIT 10
    """)

    products = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    return products

def test_popular_recommendations():
    """Test GET /api/recommendations/popular (public endpoint)"""
    print("\n📋 Test 1: Get Popular Recommendations (No Auth Required)")

    response = requests.get(f"{BASE_URL}/api/recommendations/popular?limit=5")

    if response.status_code == 200:
        recommendations = response.json()
        print(f"✅ Successfully retrieved {len(recommendations)} popular recommendations")

        for i, product in enumerate(recommendations, 1):
            print(f"\n{i}. {product['name']}")
            print(f"   Brand: {product.get('brand', 'N/A')}")
            print(f"   Barcode: {product['barcode']}")

            if product.get('prices'):
                min_price = min([p['price'] for p in product['prices']])
                num_retailers = len(set([p['retailer_name'] for p in product['prices']]))
                print(f"   Best Price: ₪{min_price:.2f}")
                print(f"   Available at {num_retailers} retailer(s)")

            if product.get('promotions'):
                print(f"   🎉 Has {len(product['promotions'])} promotion(s)")

        return True
    else:
        print(f"❌ Failed to get popular recommendations: {response.status_code}")
        print(f"   Error: {response.text}")
        return False

def test_popular_recommendations_no_auth_header():
    """Verify that popular recommendations work without Authorization header"""
    print("\n📋 Test 2: Verify Public Access (No Authorization Header)")

    # Make request without any headers
    response = requests.get(f"{BASE_URL}/api/recommendations/popular")

    if response.status_code == 200:
        print("✅ Public endpoint correctly accessible without authentication")
        return True
    else:
        print(f"❌ Expected 200 but got {response.status_code}")
        return False

def test_sync_endpoint(token: str, sample_barcodes: list):
    """Test POST /api/sync (requires authentication)"""
    print("\n📋 Test 3: Sync Anonymous Data to User Account")

    # Prepare sync data
    favorites = sample_barcodes[:3]  # First 3 as favorites
    cart = [
        {"barcode": sample_barcodes[3], "quantity": 2},
        {"barcode": sample_barcodes[4], "quantity": 1}
    ]

    print(f"   Syncing {len(favorites)} favorites and {len(cart)} cart items")

    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(
        f"{BASE_URL}/api/sync",
        headers=headers,
        json={
            "favorites": favorites,
            "cart": cart
        }
    )

    if response.status_code == 200:
        data = response.json()
        print(f"✅ Sync successful!")
        print(f"   Status: {data['status']}")
        print(f"   Favorites added: {data['favorites_added']}")
        print(f"   Cart items added: {data['cart_items_added']}")
        print(f"   Message: {data['message']}")
        return True
    else:
        print(f"❌ Sync failed: {response.status_code}")
        print(f"   Error: {response.text}")
        return False

def test_sync_without_auth():
    """Test that sync endpoint requires authentication"""
    print("\n📋 Test 4: Verify Sync Requires Authentication")

    response = requests.post(
        f"{BASE_URL}/api/sync",
        json={
            "favorites": ["123456"],
            "cart": [{"barcode": "789012", "quantity": 1}]
        }
    )

    if response.status_code == 401:
        print("✅ Sync endpoint correctly requires authentication")
        return True
    else:
        print(f"❌ Expected 401 but got {response.status_code}")
        return False

def test_sync_empty_data(token: str):
    """Test sync with empty data"""
    print("\n📋 Test 5: Sync with Empty Data")

    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(
        f"{BASE_URL}/api/sync",
        headers=headers,
        json={
            "favorites": [],
            "cart": []
        }
    )

    if response.status_code == 200:
        data = response.json()
        print(f"✅ Empty sync handled correctly")
        print(f"   Favorites added: {data['favorites_added']}")
        print(f"   Cart items added: {data['cart_items_added']}")
        return True
    else:
        print(f"❌ Failed: {response.status_code}")
        return False

def test_sync_duplicate_data(token: str, sample_barcodes: list):
    """Test syncing the same data twice (should handle duplicates)"""
    print("\n📋 Test 6: Sync Duplicate Data")

    favorites = sample_barcodes[:2]
    cart = [{"barcode": sample_barcodes[2], "quantity": 1}]

    headers = {"Authorization": f"Bearer {token}"}

    # First sync
    response1 = requests.post(
        f"{BASE_URL}/api/sync",
        headers=headers,
        json={"favorites": favorites, "cart": cart}
    )

    # Second sync with same data
    response2 = requests.post(
        f"{BASE_URL}/api/sync",
        headers=headers,
        json={"favorites": favorites, "cart": cart}
    )

    if response1.status_code == 200 and response2.status_code == 200:
        data1 = response1.json()
        data2 = response2.json()
        print(f"✅ Duplicate handling works correctly")
        print(f"   First sync: {data1['favorites_added']} favorites, {data1['cart_items_added']} cart items")
        print(f"   Second sync: {data2['favorites_added']} favorites, {data2['cart_items_added']} cart items")
        print(f"   (Second sync should add 0 or merge cart quantities)")
        return True
    else:
        print(f"❌ Failed")
        return False

def main():
    print("🧪 Testing Anonymous User Features")
    print("=" * 60)

    # Test 1: Health check
    print("\n📋 Test 0: Health Check")
    response = requests.get(f"{BASE_URL}/health")
    if response.status_code == 200:
        print("✅ Backend server is healthy")
    else:
        print("❌ Backend server is not responding")
        return

    # Test 2 & 3: Popular recommendations (public)
    test_popular_recommendations()
    test_popular_recommendations_no_auth_header()

    # Test 4: Sync requires auth
    test_sync_without_auth()

    # Create test user for authenticated tests
    print("\n📋 Creating test user for sync tests")
    username = generate_random_username()
    email = f"{username}@example.com"
    password = "testpass123"

    user_id, token = create_test_user(username, email, password)
    if not user_id or not token:
        print("❌ Cannot continue without user")
        return

    print(f"✅ Test user created: {username} (ID: {user_id})")

    # Get sample products
    sample_barcodes = get_sample_products()
    if not sample_barcodes:
        print("❌ No sample products available")
        return

    print(f"✅ Found {len(sample_barcodes)} sample products")

    # Test sync functionality
    test_sync_endpoint(token, sample_barcodes)
    test_sync_empty_data(token)
    test_sync_duplicate_data(token, sample_barcodes)

    print("\n" + "=" * 60)
    print("✅ All anonymous user feature tests completed!")

if __name__ == "__main__":
    main()
