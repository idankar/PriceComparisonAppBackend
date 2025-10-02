#!/usr/bin/env python3
"""
Test script for the recommendations API
This script demonstrates how to:
1. Create a test user
2. Add products to favorites and cart
3. Get personalized recommendations
"""

import requests
import json
import sys
import bcrypt

# Configuration
BASE_URL = "http://localhost:8000"

def create_test_user(username: str, email: str, password: str):
    """Create a test user directly in the database"""
    import psycopg2
    from psycopg2.extras import RealDictCursor

    conn = psycopg2.connect(
        dbname="price_comparison_app_v2",
        user="postgres",
        password="***REMOVED***",
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

        print(f"✅ Test user created: {username} (ID: {user_id})")
        cursor.close()
        conn.close()
        return user_id

    except Exception as e:
        print(f"❌ Failed to create test user: {e}")
        conn.rollback()
        cursor.close()
        conn.close()
        return None

def generate_token(user_id: int):
    """Generate a JWT token for a user"""
    import jwt
    from datetime import datetime, timedelta

    JWT_SECRET = "your-secret-key-change-in-production"
    JWT_ALGORITHM = "HS256"

    payload = {
        "user_id": user_id,
        "exp": datetime.utcnow() + timedelta(hours=24)
    }

    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return token

def get_sample_products():
    """Get some sample product barcodes from the database"""
    import psycopg2

    conn = psycopg2.connect(
        dbname="price_comparison_app_v2",
        user="postgres",
        password="***REMOVED***",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    cursor.execute("""
        SELECT barcode, name, brand, category
        FROM canonical_products
        WHERE is_active = true
        AND barcode IS NOT NULL
        AND category IS NOT NULL
        LIMIT 10
    """)

    products = cursor.fetchall()
    cursor.close()
    conn.close()

    return products

def test_add_to_favorites(token: str, barcode: str):
    """Test adding a product to favorites"""
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(
        f"{BASE_URL}/api/favorites/add",
        params={"product_barcode": barcode},
        headers=headers
    )

    if response.status_code == 200:
        print(f"✅ Added product {barcode} to favorites")
        return True
    else:
        print(f"❌ Failed to add to favorites: {response.text}")
        return False

def test_add_to_cart(token: str, barcode: str, quantity: int = 1):
    """Test adding a product to cart"""
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(
        f"{BASE_URL}/api/cart/add",
        params={"product_barcode": barcode, "quantity": quantity},
        headers=headers
    )

    if response.status_code == 200:
        print(f"✅ Added product {barcode} to cart")
        return True
    else:
        print(f"❌ Failed to add to cart: {response.text}")
        return False

def test_get_recommendations(token: str, limit: int = 10):
    """Test getting personalized recommendations"""
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(
        f"{BASE_URL}/api/recommendations",
        params={"limit": limit},
        headers=headers
    )

    if response.status_code == 200:
        recommendations = response.json()
        print(f"\n✅ Got {len(recommendations)} recommendations:")

        for i, product in enumerate(recommendations, 1):
            print(f"\n{i}. {product['name']}")
            print(f"   Brand: {product.get('brand', 'N/A')}")
            print(f"   Barcode: {product['barcode']}")

            if product.get('prices'):
                min_price = min([p['price'] for p in product['prices']])
                print(f"   Best Price: ₪{min_price:.2f}")

            if product.get('promotions'):
                print(f"   🎉 Has {len(product['promotions'])} promotion(s)")

        return recommendations
    else:
        print(f"❌ Failed to get recommendations: {response.text}")
        return None

def test_recommendations_without_auth():
    """Test that recommendations endpoint requires authentication"""
    response = requests.get(f"{BASE_URL}/api/recommendations")

    if response.status_code == 401:
        print("✅ Recommendations endpoint correctly requires authentication")
        return True
    else:
        print(f"❌ Expected 401 but got {response.status_code}")
        return False

def main():
    print("🧪 Testing Recommendations API\n")
    print("=" * 60)

    # Test 1: Verify endpoint requires authentication
    print("\n📋 Test 1: Verify authentication requirement")
    test_recommendations_without_auth()

    # Test 2: Create a test user
    print("\n📋 Test 2: Create test user")
    user_id = create_test_user("test_user", "test@example.com", "testpass123")

    if not user_id:
        print("❌ Cannot continue without a user")
        return

    # Test 3: Generate JWT token
    print("\n📋 Test 3: Generate JWT token")
    token = generate_token(user_id)
    print(f"✅ Token generated: {token[:20]}...")

    # Test 4: Get sample products
    print("\n📋 Test 4: Get sample products from database")
    products = get_sample_products()
    print(f"✅ Found {len(products)} sample products")

    if products:
        for barcode, name, brand, category in products[:3]:
            print(f"   - {name} ({brand}) - Category: {category}")

    # Test 5: Add products to favorites and cart
    print("\n📋 Test 5: Add products to favorites and cart")
    if products:
        # Add first 3 products to favorites
        for barcode, name, brand, category in products[:3]:
            test_add_to_favorites(token, barcode)

        # Add next 2 products to cart
        for barcode, name, brand, category in products[3:5]:
            test_add_to_cart(token, barcode)

    # Test 6: Get personalized recommendations
    print("\n📋 Test 6: Get personalized recommendations")
    recommendations = test_get_recommendations(token, limit=5)

    print("\n" + "=" * 60)
    print("✅ All tests completed!")

if __name__ == "__main__":
    main()
