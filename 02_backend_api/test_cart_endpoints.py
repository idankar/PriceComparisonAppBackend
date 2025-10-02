#!/usr/bin/env python3
"""
Comprehensive tests for the cart update and remove endpoints.
Tests both authentication requirements and database operations.
"""

import pytest
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
BASE_URL = "http://127.0.0.1:8000"
DB_NAME = os.getenv("DB_NAME", "price_comparison_app_v2")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "***REMOVED***")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

# Test user credentials
TEST_USERNAME = "test_cart_user"
TEST_EMAIL = "test_cart@example.com"
TEST_PASSWORD = "testpass123"

# Test product barcode (we'll use this for testing)
TEST_BARCODE = "078522030270"  # Valid barcode from database


def get_db_connection():
    """Create a database connection"""
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        cursor_factory=RealDictCursor
    )


def cleanup_test_user():
    """Remove test user and their data from database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Delete test user (CASCADE will delete cart and favorites)
        cursor.execute("DELETE FROM users WHERE username = %s", (TEST_USERNAME,))
        conn.commit()
    except Exception as e:
        print(f"Cleanup error (non-critical): {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def register_and_login():
    """Register a new test user and return the auth token"""
    # Clean up any existing test user
    cleanup_test_user()

    # Register new user
    register_response = requests.post(
        f"{BASE_URL}/api/register",
        json={
            "username": TEST_USERNAME,
            "email": TEST_EMAIL,
            "password": TEST_PASSWORD
        }
    )

    if register_response.status_code != 200:
        raise Exception(f"Failed to register test user: {register_response.text}")

    data = register_response.json()
    return data["access_token"], data["user_id"]


def add_product_to_cart(token, barcode, quantity=1):
    """Helper function to add a product to cart"""
    response = requests.post(
        f"{BASE_URL}/api/cart/add",
        params={"product_barcode": barcode, "quantity": quantity},
        headers={"Authorization": f"Bearer {token}"}
    )
    return response


def get_cart_from_db(user_id):
    """Get cart items directly from database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT product_barcode, quantity
            FROM user_cart
            WHERE user_id = %s
        """, (user_id,))
        results = cursor.fetchall()
        return {row['product_barcode']: row['quantity'] for row in results}
    finally:
        cursor.close()
        conn.close()


class TestCartUpdateEndpoint:
    """Tests for PUT /api/cart/update/{product_barcode}"""

    def test_update_requires_authentication(self):
        """Test that the update endpoint requires authentication"""
        response = requests.put(
            f"{BASE_URL}/api/cart/update/{TEST_BARCODE}",
            json={"quantity": 5}
        )
        assert response.status_code == 401, "Should require authentication"

    def test_update_with_invalid_token(self):
        """Test that the update endpoint rejects invalid tokens"""
        response = requests.put(
            f"{BASE_URL}/api/cart/update/{TEST_BARCODE}",
            json={"quantity": 5},
            headers={"Authorization": "Bearer invalid_token"}
        )
        assert response.status_code == 401, "Should reject invalid token"

    def test_update_quantity_success(self):
        """Test successfully updating item quantity"""
        token, user_id = register_and_login()

        # Add item to cart first
        add_response = add_product_to_cart(token, TEST_BARCODE, quantity=2)
        assert add_response.status_code == 200, "Should add item to cart"

        # Update quantity to 5
        update_response = requests.put(
            f"{BASE_URL}/api/cart/update/{TEST_BARCODE}",
            json={"quantity": 5},
            headers={"Authorization": f"Bearer {token}"}
        )

        assert update_response.status_code == 200, "Should update quantity successfully"
        data = update_response.json()
        assert data["status"] == "success"
        assert "updated" in data["message"].lower()

        # Verify in database
        cart = get_cart_from_db(user_id)
        assert cart[TEST_BARCODE] == 5, "Quantity should be updated to 5"

        # Cleanup
        cleanup_test_user()

    def test_update_quantity_to_zero_removes_item(self):
        """Test that setting quantity to 0 removes the item"""
        token, user_id = register_and_login()

        # Add item to cart
        add_response = add_product_to_cart(token, TEST_BARCODE, quantity=3)
        assert add_response.status_code == 200

        # Update quantity to 0
        update_response = requests.put(
            f"{BASE_URL}/api/cart/update/{TEST_BARCODE}",
            json={"quantity": 0},
            headers={"Authorization": f"Bearer {token}"}
        )

        assert update_response.status_code == 200
        data = update_response.json()
        assert data["status"] == "success"
        assert "removed" in data["message"].lower()

        # Verify item is removed from database
        cart = get_cart_from_db(user_id)
        assert TEST_BARCODE not in cart, "Item should be removed when quantity is 0"

        # Cleanup
        cleanup_test_user()

    def test_update_quantity_negative_removes_item(self):
        """Test that setting quantity to negative removes the item"""
        token, user_id = register_and_login()

        # Add item to cart
        add_response = add_product_to_cart(token, TEST_BARCODE, quantity=2)
        assert add_response.status_code == 200

        # Update quantity to -5
        update_response = requests.put(
            f"{BASE_URL}/api/cart/update/{TEST_BARCODE}",
            json={"quantity": -5},
            headers={"Authorization": f"Bearer {token}"}
        )

        assert update_response.status_code == 200
        data = update_response.json()
        assert data["status"] == "success"
        assert "removed" in data["message"].lower()

        # Verify item is removed from database
        cart = get_cart_from_db(user_id)
        assert TEST_BARCODE not in cart, "Item should be removed when quantity is negative"

        # Cleanup
        cleanup_test_user()

    def test_update_nonexistent_item(self):
        """Test updating an item that's not in the cart"""
        token, user_id = register_and_login()

        # Try to update item that's not in cart
        update_response = requests.put(
            f"{BASE_URL}/api/cart/update/{TEST_BARCODE}",
            json={"quantity": 5},
            headers={"Authorization": f"Bearer {token}"}
        )

        assert update_response.status_code == 404, "Should return 404 for item not in cart"
        data = update_response.json()
        assert "not found" in data["detail"].lower()

        # Cleanup
        cleanup_test_user()


class TestCartRemoveEndpoint:
    """Tests for DELETE /api/cart/remove/{product_barcode}"""

    def test_remove_requires_authentication(self):
        """Test that the remove endpoint requires authentication"""
        response = requests.delete(f"{BASE_URL}/api/cart/remove/{TEST_BARCODE}")
        assert response.status_code == 401, "Should require authentication"

    def test_remove_with_invalid_token(self):
        """Test that the remove endpoint rejects invalid tokens"""
        response = requests.delete(
            f"{BASE_URL}/api/cart/remove/{TEST_BARCODE}",
            headers={"Authorization": "Bearer invalid_token"}
        )
        assert response.status_code == 401, "Should reject invalid token"

    def test_remove_item_success(self):
        """Test successfully removing an item from cart"""
        token, user_id = register_and_login()

        # Add item to cart first
        add_response = add_product_to_cart(token, TEST_BARCODE, quantity=3)
        assert add_response.status_code == 200

        # Remove the item
        remove_response = requests.delete(
            f"{BASE_URL}/api/cart/remove/{TEST_BARCODE}",
            headers={"Authorization": f"Bearer {token}"}
        )

        assert remove_response.status_code == 200, "Should remove item successfully"
        data = remove_response.json()
        assert data["status"] == "success"
        assert "removed" in data["message"].lower()

        # Verify item is removed from database
        cart = get_cart_from_db(user_id)
        assert TEST_BARCODE not in cart, "Item should be removed from cart"

        # Cleanup
        cleanup_test_user()

    def test_remove_nonexistent_item(self):
        """Test removing an item that's not in the cart"""
        token, user_id = register_and_login()

        # Try to remove item that's not in cart
        remove_response = requests.delete(
            f"{BASE_URL}/api/cart/remove/{TEST_BARCODE}",
            headers={"Authorization": f"Bearer {token}"}
        )

        assert remove_response.status_code == 404, "Should return 404 for item not in cart"
        data = remove_response.json()
        assert "not found" in data["detail"].lower()

        # Cleanup
        cleanup_test_user()

    def test_remove_multiple_items(self):
        """Test removing multiple different items from cart"""
        token, user_id = register_and_login()

        # Add two different items to cart
        barcode1 = TEST_BARCODE
        barcode2 = "7290000803067"  # Different valid barcode from database

        add_response1 = add_product_to_cart(token, barcode1, quantity=2)
        add_response2 = add_product_to_cart(token, barcode2, quantity=3)

        # Verify both were added
        cart = get_cart_from_db(user_id)
        initial_count = len(cart)

        # Remove first item
        remove_response1 = requests.delete(
            f"{BASE_URL}/api/cart/remove/{barcode1}",
            headers={"Authorization": f"Bearer {token}"}
        )
        assert remove_response1.status_code == 200

        # Verify first item removed
        cart = get_cart_from_db(user_id)
        assert barcode1 not in cart
        assert len(cart) == initial_count - 1

        # Remove second item
        remove_response2 = requests.delete(
            f"{BASE_URL}/api/cart/remove/{barcode2}",
            headers={"Authorization": f"Bearer {token}"}
        )
        assert remove_response2.status_code == 200

        # Verify second item removed
        cart = get_cart_from_db(user_id)
        assert barcode2 not in cart
        assert len(cart) == 0

        # Cleanup
        cleanup_test_user()


class TestCartWorkflow:
    """Integration tests for complete cart workflows"""

    def test_complete_cart_workflow(self):
        """Test a complete workflow: add, update, remove"""
        token, user_id = register_and_login()

        # 1. Add item to cart
        add_response = add_product_to_cart(token, TEST_BARCODE, quantity=1)
        assert add_response.status_code == 200

        cart = get_cart_from_db(user_id)
        assert cart[TEST_BARCODE] == 1

        # 2. Update quantity to 3
        update_response = requests.put(
            f"{BASE_URL}/api/cart/update/{TEST_BARCODE}",
            json={"quantity": 3},
            headers={"Authorization": f"Bearer {token}"}
        )
        assert update_response.status_code == 200

        cart = get_cart_from_db(user_id)
        assert cart[TEST_BARCODE] == 3

        # 3. Update quantity to 10
        update_response = requests.put(
            f"{BASE_URL}/api/cart/update/{TEST_BARCODE}",
            json={"quantity": 10},
            headers={"Authorization": f"Bearer {token}"}
        )
        assert update_response.status_code == 200

        cart = get_cart_from_db(user_id)
        assert cart[TEST_BARCODE] == 10

        # 4. Remove item
        remove_response = requests.delete(
            f"{BASE_URL}/api/cart/remove/{TEST_BARCODE}",
            headers={"Authorization": f"Bearer {token}"}
        )
        assert remove_response.status_code == 200

        cart = get_cart_from_db(user_id)
        assert TEST_BARCODE not in cart

        # Cleanup
        cleanup_test_user()


if __name__ == "__main__":
    print("🧪 Running cart endpoint tests...")
    print("\nIMPORTANT: Make sure the backend server is running on http://127.0.0.1:8000")
    print("Run the server with: python 02_backend_api/backend.py\n")

    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])
