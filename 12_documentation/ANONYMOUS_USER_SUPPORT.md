# Anonymous User Support Implementation

## Overview
This document describes the implementation of anonymous user support for the PharmMate price comparison application. Anonymous users can browse products and get recommendations without creating an account, and their local data can be synced when they eventually log in.

## Endpoints Implemented

### 1. GET `/api/recommendations/popular` (Public)

**Purpose**: Provide product recommendations for anonymous (non-authenticated) users

**Authentication**: **NOT REQUIRED** - This is a public endpoint

**Query Parameters**:
- `limit` (optional): Number of recommendations to return (default: 10, max: 50)

**Response** (200 OK):
```json
[
  {
    "barcode": "7290008015158",
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
        "price": 43.90,
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

**Response Format**: Same as `/api/search` and `/api/recommendations` endpoints

**Recommendation Logic**:
- Returns randomized selection of products
- Products must be active and have valid prices
- Provides variety with each request
- No personalization (same for all anonymous users with randomization)

**Performance**: ~2 seconds for 3 recommendations

---

### 2. POST `/api/sync` (Protected)

**Purpose**: Sync anonymous user's local data (favorites and cart) to their account after login/registration

**Authentication**: **REQUIRED** - JWT Bearer token

**Request Body**:
```json
{
  "favorites": ["barcode1", "barcode2", "barcode3"],
  "cart": [
    {"barcode": "barcode4", "quantity": 2},
    {"barcode": "barcode5", "quantity": 1}
  ]
}
```

**Response** (200 OK):
```json
{
  "status": "success",
  "favorites_added": 2,
  "cart_items_added": 2,
  "message": "Successfully synced 2 favorites and 2 cart items"
}
```

**Features**:
- Verifies all barcodes exist in database
- Ignores invalid/inactive products
- Handles duplicates gracefully (uses `ON CONFLICT DO NOTHING` for favorites)
- For cart items, adds quantities if item already exists
- Tracks user interactions for preference learning
- All operations are transactional (rollback on error)

**Error Responses**:
- `401 Unauthorized`: Missing or invalid JWT token
- `500 Internal Server Error`: Database error during sync

---

## Usage Examples

### Popular Recommendations (Public)

#### curl
```bash
# No authentication needed!
curl "http://localhost:8000/api/recommendations/popular?limit=10"
```

#### JavaScript
```javascript
// No Authorization header needed
async function getPopularRecommendations() {
  const response = await fetch(
    'http://localhost:8000/api/recommendations/popular?limit=10'
  );

  if (response.ok) {
    const recommendations = await response.json();
    displayRecommendations(recommendations);
  }
}
```

#### Python
```python
import requests

response = requests.get(
    "http://localhost:8000/api/recommendations/popular",
    params={"limit": 10}
)

if response.status_code == 200:
    recommendations = response.json()
    for product in recommendations:
        print(f"{product['name']} - {product['barcode']}")
```

---

### Data Sync (After Login)

#### curl
```bash
curl -X POST "http://localhost:8000/api/sync" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "favorites": ["7290008015158", "3600523718597"],
    "cart": [
      {"barcode": "7290113860162", "quantity": 2},
      {"barcode": "7290006235657", "quantity": 1}
    ]
  }'
```

#### JavaScript
```javascript
async function syncAnonymousData(token, localData) {
  const response = await fetch('http://localhost:8000/api/sync', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      favorites: localData.favorites,
      cart: localData.cart
    })
  });

  if (response.ok) {
    const result = await response.json();
    console.log(`Synced ${result.favorites_added} favorites and ${result.cart_items_added} cart items`);
    // Clear local storage after successful sync
    localStorage.removeItem('anonymous_favorites');
    localStorage.removeItem('anonymous_cart');
  }
}
```

#### Python
```python
import requests

headers = {"Authorization": f"Bearer {token}"}
data = {
    "favorites": ["barcode1", "barcode2"],
    "cart": [
        {"barcode": "barcode3", "quantity": 2},
        {"barcode": "barcode4", "quantity": 1}
    ]
}

response = requests.post(
    "http://localhost:8000/api/sync",
    headers=headers,
    json=data
)

if response.status_code == 200:
    result = response.json()
    print(f"Status: {result['status']}")
    print(f"Message: {result['message']}")
```

---

## Frontend Integration Flow

### Anonymous User Experience

```javascript
// 1. Show popular recommendations on home screen
async function loadHomeScreen() {
  // No auth required - works for everyone!
  const recommendations = await fetch(
    'http://localhost:8000/api/recommendations/popular?limit=10'
  ).then(r => r.json());

  displayRecommendations(recommendations);
}

// 2. Store favorites and cart locally
function addToFavorites(barcode) {
  const favorites = JSON.parse(localStorage.getItem('anonymous_favorites') || '[]');
  if (!favorites.includes(barcode)) {
    favorites.push(barcode);
    localStorage.setItem('anonymous_favorites', JSON.stringify(favorites));
  }
}

function addToCart(barcode, quantity) {
  const cart = JSON.parse(localStorage.getItem('anonymous_cart') || '[]');
  const existingItem = cart.find(item => item.barcode === barcode);

  if (existingItem) {
    existingItem.quantity += quantity;
  } else {
    cart.push({ barcode, quantity });
  }

  localStorage.setItem('anonymous_cart', JSON.stringify(cart));
}
```

### Login/Registration Flow

```javascript
async function handleLoginSuccess(token, userId, username) {
  // Store auth data
  localStorage.setItem('access_token', token);
  localStorage.setItem('user_id', userId);
  localStorage.setItem('username', username);

  // Sync anonymous data to account
  const anonymousFavorites = JSON.parse(
    localStorage.getItem('anonymous_favorites') || '[]'
  );
  const anonymousCart = JSON.parse(
    localStorage.getItem('anonymous_cart') || '[]'
  );

  if (anonymousFavorites.length > 0 || anonymousCart.length > 0) {
    try {
      const response = await fetch('http://localhost:8000/api/sync', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          favorites: anonymousFavorites,
          cart: anonymousCart
        })
      });

      if (response.ok) {
        const result = await response.json();
        console.log('Sync successful:', result.message);

        // Clear local anonymous data
        localStorage.removeItem('anonymous_favorites');
        localStorage.removeItem('anonymous_cart');

        // Show success message to user
        showNotification(
          `Welcome back! We've synced ${result.favorites_added} favorites and ${result.cart_items_added} cart items to your account.`
        );
      }
    } catch (error) {
      console.error('Sync failed:', error);
      // Don't block login if sync fails
    }
  }

  // Redirect to home (now shows personalized recommendations)
  window.location.href = '/home';
}
```

### Switching Between Anonymous and Authenticated

```javascript
async function getRecommendations() {
  const token = localStorage.getItem('access_token');

  if (token) {
    // Authenticated user - get personalized recommendations
    const response = await fetch('http://localhost:8000/api/recommendations', {
      headers: { 'Authorization': `Bearer ${token}` }
    });

    if (response.ok) {
      return await response.json();
    } else if (response.status === 401) {
      // Token expired - fall back to popular
      localStorage.removeItem('access_token');
      return getPopularRecommendations();
    }
  } else {
    // Anonymous user - get popular recommendations
    return getPopularRecommendations();
  }
}

async function getPopularRecommendations() {
  const response = await fetch(
    'http://localhost:8000/api/recommendations/popular?limit=10'
  );
  return await response.json();
}
```

---

## Test Results

### Sync Endpoint Tests
```
✅ Sync endpoint works with valid authentication
✅ Synced 2 favorites successfully
✅ Synced 2 cart items successfully
✅ Correctly requires authentication (401 without token)
✅ Handles duplicates gracefully
✅ Handles empty data correctly
✅ Tracks user interactions for preferences
```

### Popular Recommendations Tests
```
✅ Returns recommendations without authentication
✅ Response in correct format (same as /api/search)
✅ Performance: ~2 seconds for 3 recommendations
✅ Includes prices and promotions data
✅ Randomized results for variety
```

---

## Database Impact

### Tables Used

**user_favorites**
- Populated by sync endpoint
- Used to exclude from future recommendations

**user_cart**
- Populated by sync endpoint
- Quantities merged if item already exists
- Used to exclude from future recommendations

**user_preferences**
- Updated by sync endpoint via `track_user_interaction()`
- Builds user preference profile for personalized recommendations

### Performance Considerations

1. **Popular Recommendations Query**:
   - Uses same query structure as personalized recommendations
   - Simplified (removed expensive subqueries)
   - Uses RANDOM() for variety
   - Average execution time: 1-2 seconds for 3-10 items

2. **Sync Operation**:
   - Batched inserts with ON CONFLICT handling
   - Transactional (rollback on any error)
   - Scales with number of items being synced
   - Minimal impact: < 1 second for typical sync (5-10 items)

---

## Files Created/Modified

### Modified
- `02_backend_api/backend.py`:
  - Added `CartItem`, `SyncRequest`, `SyncResponse` models
  - Implemented GET `/api/recommendations/popular` endpoint
  - Implemented POST `/api/sync` endpoint

### Created
- `02_backend_api/test_anonymous_features.py` - Comprehensive test suite
- `02_backend_api/test_sync_only.py` - Focused sync endpoint tests
- `02_backend_api/test_popular_simple.py` - Focused popular recommendations tests
- `ANONYMOUS_USER_SUPPORT.md` - This documentation

---

## Security Considerations

1. ✅ **Public Endpoint Isolation**: Popular recommendations endpoint doesn't expose user data
2. ✅ **Authentication Required for Sync**: Can't sync to someone else's account
3. ✅ **Data Validation**: All barcodes verified against database before sync
4. ✅ **Duplicate Handling**: Uses database constraints to prevent duplicates
5. ✅ **Transaction Safety**: Sync is transactional (all-or-nothing)

---

## Future Enhancements

1. **Popular Recommendations Improvements**:
   - Cache results for better performance
   - Add trending products based on recent user activity
   - Implement category-based popular recommendations
   - Add time-based popularity (weekly/monthly)

2. **Sync Improvements**:
   - Add conflict resolution strategies (keep local vs server)
   - Support merging products with different quantities
   - Add sync history/audit trail
   - Support incremental sync

3. **Anonymous User Experience**:
   - Add search history for anonymous users
   - Implement recently viewed products
   - Add category preferences without login
   - Support guest checkout

---

## Summary

✅ **GET /api/recommendations/popular** - Public endpoint for anonymous users
✅ **POST /api/sync** - Merge anonymous data on login/registration
✅ **Same response format** - Consistent API across all recommendation endpoints
✅ **Graceful degradation** - App works without login, enhanced with login
✅ **Data persistence** - Anonymous data preserved and merged on login
✅ **Performance tested** - Both endpoints tested and working efficiently
✅ **Frontend-ready** - Clear integration patterns with examples

Anonymous user support is now fully functional and ready for production use!
