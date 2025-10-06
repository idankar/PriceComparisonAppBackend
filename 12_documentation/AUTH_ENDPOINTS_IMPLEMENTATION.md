# Authentication Endpoints Implementation

## Overview
This document describes the implementation of user registration and login endpoints for the PharmMate price comparison application.

## Endpoints Implemented

### POST `/api/register`
**Purpose**: Register a new user account

**Request Body**:
```json
{
  "username": "string",
  "email": "string",
  "password": "string"
}
```

**Validation Rules**:
- Username: Minimum 3 characters, must be unique
- Email: Must contain "@", must be unique
- Password: Minimum 6 characters

**Response** (200 OK):
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer",
  "user_id": 1,
  "username": "johndoe"
}
```

**Error Responses**:
- `400 Bad Request`: Invalid input data (username too short, invalid email, password too short)
- `409 Conflict`: Username or email already exists
- `500 Internal Server Error`: Server error during registration

**Features**:
- Passwords are hashed using bcrypt with auto-generated salt
- Automatically generates and returns JWT token
- User is immediately authenticated after registration

### POST `/api/login`
**Purpose**: Authenticate an existing user

**Request Body**:
```json
{
  "username": "string",
  "password": "string"
}
```

**Response** (200 OK):
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer",
  "user_id": 1,
  "username": "johndoe"
}
```

**Error Responses**:
- `401 Unauthorized`: Invalid username or password
- `500 Internal Server Error`: Server error during login

**Security Features**:
- Password verification using bcrypt
- Generic error message for invalid credentials (doesn't reveal if username or password is wrong)
- Returns fresh JWT token on each login

## Technical Implementation

### Password Hashing
```python
import bcrypt

# Hash password during registration
password_bytes = password.encode('utf-8')
salt = bcrypt.gensalt()
password_hash = bcrypt.hashpw(password_bytes, salt).decode('utf-8')

# Verify password during login
password_bytes = password.encode('utf-8')
stored_hash = password_hash.encode('utf-8')
is_valid = bcrypt.checkpw(password_bytes, stored_hash)
```

### JWT Token Generation
```python
import jwt
from datetime import datetime, timedelta

def create_access_token(data: dict) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(hours=24)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, JWT_SECRET, algorithm="HS256")
    return encoded_jwt
```

### Token Payload
The JWT token contains:
```json
{
  "user_id": 1,
  "username": "johndoe",
  "exp": 1696118400  // Expiration timestamp
}
```

## Usage Examples

### Register a New User (curl)
```bash
curl -X POST "http://localhost:8000/api/register" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "johndoe",
    "email": "john@example.com",
    "password": "securepass123"
  }'
```

### Register a New User (Python)
```python
import requests

response = requests.post(
    "http://localhost:8000/api/register",
    json={
        "username": "johndoe",
        "email": "john@example.com",
        "password": "securepass123"
    }
)

if response.status_code == 200:
    data = response.json()
    token = data["access_token"]
    user_id = data["user_id"]
    print(f"Registration successful! Token: {token}")
```

### Login (curl)
```bash
curl -X POST "http://localhost:8000/api/login" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "johndoe",
    "password": "securepass123"
  }'
```

### Login (Python)
```python
import requests

response = requests.post(
    "http://localhost:8000/api/login",
    json={
        "username": "johndoe",
        "password": "securepass123"
    }
)

if response.status_code == 200:
    data = response.json()
    token = data["access_token"]
    print(f"Login successful! Token: {token}")
else:
    print(f"Login failed: {response.json()['detail']}")
```

### Use Token with Protected Endpoints
```python
import requests

token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."

# Add product to favorites
response = requests.post(
    "http://localhost:8000/api/favorites/add",
    params={"product_barcode": "7290006235657"},
    headers={"Authorization": f"Bearer {token}"}
)

# Get recommendations
response = requests.get(
    "http://localhost:8000/api/recommendations",
    headers={"Authorization": f"Bearer {token}"}
)
```

## Frontend Integration

### Registration Flow
```javascript
async function register(username, email, password) {
  try {
    const response = await fetch('http://localhost:8000/api/register', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ username, email, password }),
    });

    if (response.ok) {
      const data = await response.json();
      // Store token in localStorage or secure cookie
      localStorage.setItem('access_token', data.access_token);
      localStorage.setItem('user_id', data.user_id);
      localStorage.setItem('username', data.username);

      // Redirect to home page
      window.location.href = '/home';
    } else {
      const error = await response.json();
      alert(`Registration failed: ${error.detail}`);
    }
  } catch (error) {
    console.error('Registration error:', error);
    alert('Registration failed. Please try again.');
  }
}
```

### Login Flow
```javascript
async function login(username, password) {
  try {
    const response = await fetch('http://localhost:8000/api/login', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ username, password }),
    });

    if (response.ok) {
      const data = await response.json();
      // Store token in localStorage or secure cookie
      localStorage.setItem('access_token', data.access_token);
      localStorage.setItem('user_id', data.user_id);
      localStorage.setItem('username', data.username);

      // Redirect to home page
      window.location.href = '/home';
    } else {
      const error = await response.json();
      alert('Invalid username or password');
    }
  } catch (error) {
    console.error('Login error:', error);
    alert('Login failed. Please try again.');
  }
}
```

### Making Authenticated Requests
```javascript
async function getRecommendations() {
  const token = localStorage.getItem('access_token');

  if (!token) {
    // Redirect to login page
    window.location.href = '/login';
    return;
  }

  try {
    const response = await fetch('http://localhost:8000/api/recommendations', {
      headers: {
        'Authorization': `Bearer ${token}`,
      },
    });

    if (response.ok) {
      const recommendations = await response.json();
      // Display recommendations
      displayRecommendations(recommendations);
    } else if (response.status === 401) {
      // Token expired or invalid, redirect to login
      localStorage.removeItem('access_token');
      window.location.href = '/login';
    }
  } catch (error) {
    console.error('Error fetching recommendations:', error);
  }
}
```

## Test Results

All tests passed successfully:

✅ **Test 1**: Health Check - Backend server is healthy
✅ **Test 2**: Registration with Invalid Data
  - Correctly rejected short username (< 3 chars)
  - Correctly rejected invalid email (no @)
  - Correctly rejected short password (< 6 chars)
✅ **Test 3**: New User Registration - Successfully created user and returned token
✅ **Test 4**: Duplicate Username Registration - Correctly rejected with 409 status
✅ **Test 5**: Login with Valid Credentials - Successfully authenticated and returned token
✅ **Test 6**: Login with Invalid Credentials - Correctly rejected with 401 status
✅ **Test 7**: Token Authentication - Token successfully works with protected endpoints
✅ **Test 8**: Token Generation - Each login generates a new token

## Security Considerations

### Implemented
1. ✅ **Password Hashing**: Passwords are hashed with bcrypt and never stored in plaintext
2. ✅ **Salt Generation**: Each password gets a unique salt via bcrypt.gensalt()
3. ✅ **Input Validation**: Username, email, and password requirements enforced
4. ✅ **Generic Error Messages**: Login errors don't reveal whether username or password is wrong
5. ✅ **JWT Expiration**: Tokens expire after 24 hours
6. ✅ **Unique Constraints**: Username and email must be unique

### Recommended for Production
1. ⚠️ **HTTPS Only**: Always use HTTPS in production to protect tokens in transit
2. ⚠️ **Environment Variables**: Store JWT_SECRET in environment variable, not in code
3. ⚠️ **Rate Limiting**: Implement rate limiting on login/register endpoints to prevent brute force
4. ⚠️ **Email Verification**: Add email verification step after registration
5. ⚠️ **Password Requirements**: Consider enforcing stronger password requirements (uppercase, numbers, special chars)
6. ⚠️ **Account Lockout**: Lock accounts after multiple failed login attempts
7. ⚠️ **Refresh Tokens**: Implement refresh tokens for better security and UX
8. ⚠️ **CORS Configuration**: Restrict CORS to specific domains in production

## Database Schema

The authentication system uses the `users` table created in the previous migration:

```sql
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(255) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

## Dependencies

- **PyJWT**: 2.10.1 - JWT token encoding/decoding
- **bcrypt**: 5.0.0 - Password hashing
- **FastAPI**: For API framework
- **psycopg2**: For PostgreSQL database connection

## Files Modified/Created

### Modified
- `02_backend_api/backend.py`:
  - Removed passlib dependency in favor of direct bcrypt usage
  - Added RegisterRequest, LoginRequest, and AuthResponse models
  - Implemented /api/register endpoint
  - Implemented /api/login endpoint

### Created
- `02_backend_api/test_auth.py` - Comprehensive test suite for authentication endpoints
- `AUTH_ENDPOINTS_IMPLEMENTATION.md` - This documentation file

## Summary

✅ **POST /api/register** - Create new user account with automatic JWT token
✅ **POST /api/login** - Authenticate existing user and return JWT token
✅ **bcrypt password hashing** - Secure password storage
✅ **JWT authentication** - Stateless authentication for protected endpoints
✅ **Input validation** - Username, email, and password requirements
✅ **Comprehensive testing** - All authentication flows tested and verified
✅ **Frontend-ready** - Clear API structure for easy integration

The authentication system is now fully functional and ready for production use (with the recommended security enhancements for production deployment).
