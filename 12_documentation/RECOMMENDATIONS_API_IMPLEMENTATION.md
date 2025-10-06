# Recommendations API Implementation

## Overview
This document describes the implementation of the personalized recommendations feature for the PharmMate price comparison application.

## What Was Built

### 1. Database Schema (`03_database/create_recommendations_system.sql`)

Created four new tables:

#### `users` table
- `user_id` (SERIAL PRIMARY KEY)
- `username` (VARCHAR, UNIQUE)
- `email` (VARCHAR, UNIQUE)
- `password_hash` (VARCHAR)
- `created_at`, `updated_at` (TIMESTAMP WITH TIME ZONE)

#### `user_favorites` table
- `favorite_id` (SERIAL PRIMARY KEY)
- `user_id` (INT, FK to users)
- `product_barcode` (VARCHAR)
- `added_at` (TIMESTAMP WITH TIME ZONE)
- UNIQUE constraint on (user_id, product_barcode)

#### `user_cart` table
- `cart_id` (SERIAL PRIMARY KEY)
- `user_id` (INT, FK to users)
- `product_barcode` (VARCHAR)
- `quantity` (INT)
- `added_at`, `updated_at` (TIMESTAMP WITH TIME ZONE)
- UNIQUE constraint on (user_id, product_barcode)

#### `user_preferences` table
- `preference_id` (SERIAL PRIMARY KEY)
- `user_id` (INT, FK to users)
- `preference_type` (VARCHAR) - 'category' or 'brand'
- `preference_value` (VARCHAR)
- `interaction_score` (INT)
- `last_updated` (TIMESTAMP WITH TIME ZONE)
- UNIQUE constraint on (user_id, preference_type, preference_value)

### 2. Backend API Updates (`02_backend_api/backend.py`)

#### Added Dependencies
- PyJWT for JWT authentication
- passlib[bcrypt] for password hashing

#### JWT Authentication
- `create_access_token()` - Creates JWT tokens
- `verify_token()` - Verifies and decodes JWT tokens
- `get_current_user()` - FastAPI dependency for protected routes

Configuration:
- `JWT_SECRET` - Secret key (defaults to environment variable)
- `JWT_ALGORITHM` - HS256
- `JWT_EXPIRATION_HOURS` - 24 hours

#### Helper Functions
- `track_user_interaction()` - Updates user preferences when products are added to favorites/cart
  - Increments category preference score
  - Increments brand preference score
  - Uses INSERT ... ON CONFLICT to update scores

#### New Endpoints

##### POST `/api/favorites/add`
- **Authentication**: Required (JWT Bearer token)
- **Parameters**: `product_barcode` (query param)
- **Response**: `{"status": "success", "message": "Product added to favorites"}`
- **Side Effect**: Tracks interaction in user_preferences table

##### POST `/api/cart/add`
- **Authentication**: Required (JWT Bearer token)
- **Parameters**:
  - `product_barcode` (query param)
  - `quantity` (query param, default=1)
- **Response**: `{"status": "success", "message": "Product added to cart"}`
- **Side Effect**: Tracks interaction in user_preferences table

##### GET `/api/recommendations`
- **Authentication**: Required (JWT Bearer token)
- **Parameters**: `limit` (query param, default=10, max=50)
- **Response**: Array of `ProductSearchResult` objects (same format as `/api/search`)
- **Logic**:
  1. Get user's top 2-3 categories and brands by interaction_score
  2. Find products matching those preferences
  3. Exclude products already in favorites or cart
  4. Filter for active products with valid prices
  5. Prioritize products with promotions
  6. Return randomized results for variety
  7. If user has no preferences, return popular random products

### 3. Migration Runner (`03_database/run_recommendations_migration.py`)
- Python script to execute the SQL migration
- Verifies tables were created successfully
- Displays schema information

### 4. Test Suite (`02_backend_api/test_recommendations.py`)
Comprehensive test script that:
1. Creates a test user
2. Generates JWT token
3. Adds products to favorites and cart
4. Gets personalized recommendations
5. Verifies authentication requirements

## How to Use

### Running the Migration
```bash
cd 03_database
python3 run_recommendations_migration.py
```

### Starting the Backend Server
```bash
cd 02_backend_api
python3 -m uvicorn backend:app --host 0.0.0.0 --port 8000
```

### API Usage Examples

#### 1. Create a User (Manual - Direct DB Insert)
```python
import bcrypt
import psycopg2

conn = psycopg2.connect(dbname="price_comparison_app_v2", ...)
cursor = conn.cursor()

password_hash = bcrypt.hashpw(b"password", bcrypt.gensalt()).decode('utf-8')
cursor.execute("""
    INSERT INTO users (username, email, password_hash)
    VALUES (%s, %s, %s)
    RETURNING user_id
""", ("username", "email@example.com", password_hash))
user_id = cursor.fetchone()[0]
conn.commit()
```

#### 2. Generate JWT Token
```python
import jwt
from datetime import datetime, timedelta

payload = {
    "user_id": user_id,
    "exp": datetime.utcnow() + timedelta(hours=24)
}
token = jwt.encode(payload, "your-secret-key", algorithm="HS256")
```

#### 3. Add Product to Favorites
```bash
curl -X POST "http://localhost:8000/api/favorites/add?product_barcode=123456789" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

#### 4. Add Product to Cart
```bash
curl -X POST "http://localhost:8000/api/cart/add?product_barcode=123456789&quantity=2" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

#### 5. Get Recommendations
```bash
curl -X GET "http://localhost:8000/api/recommendations?limit=10" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

### Response Format
The recommendations endpoint returns products in the same format as the search endpoint:

```json
[
  {
    "barcode": "7290006235657",
    "name": "Product Name",
    "brand": "Brand Name",
    "image_url": "https://...",
    "prices": [
      {
        "retailer_id": 1,
        "retailer_name": "Retailer",
        "store_id": 123,
        "store_name": "Store Name",
        "store_address": "Address",
        "price": 63.40,
        "last_updated": "2025-10-01T00:00:00Z",
        "in_stock": true
      }
    ],
    "promotions": [
      {
        "deal_id": 456,
        "title": "Special Offer",
        "description": "Details",
        "retailer_name": "Retailer",
        "store_id": 123
      }
    ]
  }
]
```

## Testing

Run the comprehensive test suite:
```bash
cd 02_backend_api
python3 test_recommendations.py
```

Expected output:
- ✅ Authentication requirement verified
- ✅ Test user created
- ✅ JWT token generated
- ✅ Products added to favorites and cart
- ✅ Personalized recommendations returned

## Security Considerations

1. **JWT Secret**: Change the default JWT_SECRET in production via environment variable
2. **HTTPS**: Use HTTPS in production to protect JWT tokens in transit
3. **Password Hashing**: Passwords are hashed with bcrypt (cost factor: default)
4. **Token Expiration**: Tokens expire after 24 hours
5. **Authorization**: All user-specific endpoints require valid JWT

## Frontend Integration

The frontend should:
1. Store the JWT token after user login (localStorage or secure cookie)
2. Include the token in the Authorization header for all protected requests:
   ```javascript
   headers: {
     'Authorization': `Bearer ${token}`
   }
   ```
3. Handle 401 Unauthorized responses by redirecting to login
4. Call `/api/favorites/add` when user favorites a product
5. Call `/api/cart/add` when user adds to cart
6. Display recommendations from `/api/recommendations` on the home screen

## Future Enhancements

1. **User Registration Endpoint**: Add POST `/api/register` for user signup
2. **Login Endpoint**: Add POST `/api/login` for authentication
3. **Password Reset**: Email-based password reset flow
4. **Refresh Tokens**: Longer-lived refresh tokens for better UX
5. **Collaborative Filtering**: Use similar users' preferences for better recommendations
6. **ML-based Recommendations**: Implement more sophisticated recommendation algorithms
7. **A/B Testing**: Test different recommendation strategies
8. **Performance Optimization**: Add caching for user preferences and recommendations

## Database Indexes

The following indexes were created for performance:
- `idx_users_username` - Fast username lookups
- `idx_users_email` - Fast email lookups
- `idx_user_favorites_user_id` - Fast favorites queries per user
- `idx_user_cart_user_id` - Fast cart queries per user
- `idx_user_preferences_user_id` - Fast preference queries per user
- `idx_user_preferences_score` - Fast top preferences queries

## Files Created/Modified

### New Files
1. `03_database/create_recommendations_system.sql` - Database schema
2. `03_database/run_recommendations_migration.py` - Migration runner
3. `02_backend_api/test_recommendations.py` - Test suite
4. `RECOMMENDATIONS_API_IMPLEMENTATION.md` - This documentation

### Modified Files
1. `02_backend_api/backend.py` - Added authentication and recommendations endpoints

## Dependencies Added
- PyJWT==2.10.1
- passlib==1.7.4
- bcrypt==5.0.0

## Summary

✅ Database schema created with 4 new tables
✅ JWT authentication implemented
✅ User interaction tracking implemented
✅ POST `/api/favorites/add` endpoint created
✅ POST `/api/cart/add` endpoint created
✅ GET `/api/recommendations` endpoint created
✅ All endpoints tested and working
✅ Returns product recommendations in same format as search endpoint
✅ Properly excludes already-interacted products
✅ Personalized based on user's top categories and brands

The recommendations system is now ready for frontend integration!
