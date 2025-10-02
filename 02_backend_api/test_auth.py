#!/usr/bin/env python3
"""
Test script for authentication endpoints (register and login)
"""

import requests
import json
import random
import string

# Configuration
BASE_URL = "http://localhost:8000"

def generate_random_username():
    """Generate a random username for testing"""
    return "testuser_" + ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))

def test_register(username: str, email: str, password: str):
    """Test user registration"""
    print(f"\n📋 Testing Registration: {username}")

    response = requests.post(
        f"{BASE_URL}/api/register",
        json={
            "username": username,
            "email": email,
            "password": password
        }
    )

    if response.status_code == 200:
        data = response.json()
        print(f"✅ Registration successful!")
        print(f"   User ID: {data['user_id']}")
        print(f"   Username: {data['username']}")
        print(f"   Token: {data['access_token'][:30]}...")
        print(f"   Token Type: {data['token_type']}")
        return data
    else:
        print(f"❌ Registration failed: {response.status_code}")
        print(f"   Error: {response.text}")
        return None

def test_login(username: str, password: str):
    """Test user login"""
    print(f"\n📋 Testing Login: {username}")

    response = requests.post(
        f"{BASE_URL}/api/login",
        json={
            "username": username,
            "password": password
        }
    )

    if response.status_code == 200:
        data = response.json()
        print(f"✅ Login successful!")
        print(f"   User ID: {data['user_id']}")
        print(f"   Username: {data['username']}")
        print(f"   Token: {data['access_token'][:30]}...")
        print(f"   Token Type: {data['token_type']}")
        return data
    else:
        print(f"❌ Login failed: {response.status_code}")
        print(f"   Error: {response.text}")
        return None

def test_login_invalid_credentials(username: str):
    """Test login with invalid password"""
    print(f"\n📋 Testing Login with Invalid Password: {username}")

    response = requests.post(
        f"{BASE_URL}/api/login",
        json={
            "username": username,
            "password": "wrongpassword123"
        }
    )

    if response.status_code == 401:
        print(f"✅ Correctly rejected invalid credentials")
        return True
    else:
        print(f"❌ Expected 401 but got {response.status_code}")
        return False

def test_register_duplicate_username(username: str, email: str, password: str):
    """Test registering with an existing username"""
    print(f"\n📋 Testing Registration with Duplicate Username: {username}")

    response = requests.post(
        f"{BASE_URL}/api/register",
        json={
            "username": username,
            "email": "different_" + email,
            "password": password
        }
    )

    if response.status_code == 409:
        print(f"✅ Correctly rejected duplicate username")
        return True
    else:
        print(f"❌ Expected 409 but got {response.status_code}")
        print(f"   Response: {response.text}")
        return False

def test_register_invalid_data():
    """Test registration with invalid data"""
    print(f"\n📋 Testing Registration with Invalid Data")

    # Test short username
    response = requests.post(
        f"{BASE_URL}/api/register",
        json={
            "username": "ab",  # Too short
            "email": "test@example.com",
            "password": "password123"
        }
    )

    if response.status_code == 400:
        print(f"✅ Correctly rejected short username")
    else:
        print(f"❌ Expected 400 for short username but got {response.status_code}")

    # Test invalid email
    response = requests.post(
        f"{BASE_URL}/api/register",
        json={
            "username": "testuser",
            "email": "notanemail",  # Invalid
            "password": "password123"
        }
    )

    if response.status_code == 400:
        print(f"✅ Correctly rejected invalid email")
    else:
        print(f"❌ Expected 400 for invalid email but got {response.status_code}")

    # Test short password
    response = requests.post(
        f"{BASE_URL}/api/register",
        json={
            "username": "testuser",
            "email": "test@example.com",
            "password": "12345"  # Too short
        }
    )

    if response.status_code == 400:
        print(f"✅ Correctly rejected short password")
    else:
        print(f"❌ Expected 400 for short password but got {response.status_code}")

def test_protected_endpoint_with_token(token: str):
    """Test that the token works with a protected endpoint"""
    print(f"\n📋 Testing Token with Protected Endpoint (/api/recommendations)")

    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(
        f"{BASE_URL}/api/recommendations",
        headers=headers
    )

    if response.status_code == 200:
        print(f"✅ Token successfully authenticated with protected endpoint")
        recommendations = response.json()
        print(f"   Got {len(recommendations)} recommendations")
        return True
    else:
        print(f"❌ Token authentication failed: {response.status_code}")
        print(f"   Error: {response.text}")
        return False

def main():
    print("🧪 Testing Authentication Endpoints")
    print("=" * 60)

    # Generate random test credentials
    username = generate_random_username()
    email = f"{username}@example.com"
    password = "testpassword123"

    # Test 1: Health check
    print("\n📋 Test 1: Health Check")
    response = requests.get(f"{BASE_URL}/health")
    if response.status_code == 200:
        print("✅ Backend server is healthy")
    else:
        print("❌ Backend server is not responding")
        return

    # Test 2: Invalid data registration
    print("\n📋 Test 2: Registration with Invalid Data")
    test_register_invalid_data()

    # Test 3: New user registration
    print("\n📋 Test 3: New User Registration")
    register_result = test_register(username, email, password)
    if not register_result:
        print("❌ Cannot continue without successful registration")
        return

    # Test 4: Duplicate username registration
    print("\n📋 Test 4: Duplicate Username Registration")
    test_register_duplicate_username(username, email, password)

    # Test 5: Login with valid credentials
    print("\n📋 Test 5: Login with Valid Credentials")
    login_result = test_login(username, password)
    if not login_result:
        print("❌ Login failed")
        return

    # Test 6: Login with invalid credentials
    print("\n📋 Test 6: Login with Invalid Credentials")
    test_login_invalid_credentials(username)

    # Test 7: Use token with protected endpoint
    print("\n📋 Test 7: Use Token with Protected Endpoint")
    test_protected_endpoint_with_token(register_result['access_token'])

    # Test 8: Verify login token is different from register token
    print("\n📋 Test 8: Verify New Token on Each Login")
    if register_result['access_token'] != login_result['access_token']:
        print("✅ Each login generates a new token")
    else:
        print("⚠️  Same token returned (may be cached)")

    print("\n" + "=" * 60)
    print("✅ All authentication tests completed!")
    print(f"\nTest User Credentials:")
    print(f"  Username: {username}")
    print(f"  Email: {email}")
    print(f"  Password: {password}")
    print(f"  User ID: {register_result['user_id']}")

if __name__ == "__main__":
    main()
