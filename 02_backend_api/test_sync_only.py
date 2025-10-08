#!/usr/bin/env python3
"""
Quick test for POST /api/sync endpoint
"""

import requests
import bcrypt
import jwt
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor

BASE_URL = "http://localhost:8000"

def create_test_user():
    """Create a test user and return user_id and token"""
    conn = psycopg2.connect(
        dbname="price_comparison_app_v2",
        user="postgres",
        password="025655358",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    username = "synctest_user"
    email = "synctest@example.com"
    password = "testpass123"

    # Hash password
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
        payload = {
            "user_id": user_id,
            "username": username,
            "exp": datetime.utcnow() + timedelta(hours=24)
        }
        token = jwt.encode(payload, "your-secret-key-change-in-production", algorithm="HS256")

        cursor.close()
        conn.close()
        return user_id, token

    except Exception as e:
        print(f"❌ Error: {e}")
        conn.rollback()
        cursor.close()
        conn.close()
        return None, None

def get_sample_barcodes():
    """Get some sample barcodes"""
    conn = psycopg2.connect(
        dbname="price_comparison_app_v2",
        user="postgres",
        password="025655358",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    cursor.execute("""
        SELECT barcode FROM canonical_products
        WHERE is_active = true AND barcode IS NOT NULL
        LIMIT 5
    """)

    barcodes = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return barcodes

print("Testing POST /api/sync endpoint")
print("=" * 50)

# Create user and get token
print("\n1. Creating test user...")
user_id, token = create_test_user()
if not user_id:
    print("❌ Failed to create user")
    exit(1)
print(f"✅ User created (ID: {user_id})")

# Get sample barcodes
print("\n2. Getting sample barcodes...")
barcodes = get_sample_barcodes()
if not barcodes:
    print("❌ No barcodes found")
    exit(1)
print(f"✅ Found {len(barcodes)} barcodes")

# Test sync
print("\n3. Testing sync endpoint...")
headers = {"Authorization": f"Bearer {token}"}
data = {
    "favorites": barcodes[:2],
    "cart": [
        {"barcode": barcodes[2], "quantity": 2},
        {"barcode": barcodes[3], "quantity": 1}
    ]
}

print(f"   Syncing {len(data['favorites'])} favorites and {len(data['cart'])} cart items")
response = requests.post(f"{BASE_URL}/api/sync", headers=headers, json=data)

if response.status_code == 200:
    result = response.json()
    print(f"✅ Sync successful!")
    print(f"   Status: {result['status']}")
    print(f"   Favorites added: {result['favorites_added']}")
    print(f"   Cart items added: {result['cart_items_added']}")
    print(f"   Message: {result['message']}")
else:
    print(f"❌ Sync failed: {response.status_code}")
    print(f"   Error: {response.text}")

print("\n4. Testing sync without auth...")
response = requests.post(f"{BASE_URL}/api/sync", json=data)
if response.status_code == 401:
    print("✅ Correctly requires authentication")
else:
    print(f"❌ Expected 401, got {response.status_code}")

print("\n" + "=" * 50)
print("✅ Sync endpoint tests complete!")
