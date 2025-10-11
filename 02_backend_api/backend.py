import os
from typing import List, Optional
from urllib.parse import urlparse
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Depends, HTTPException, Query, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from datetime import datetime, timedelta
import uvicorn
import jwt
import bcrypt

# --- Configuration ---
load_dotenv()  # Load environment variables from .env

# Get the database URL from the environment
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    # Fallback to old variables for local development if DATABASE_URL is not set
    DB_NAME = os.getenv("DB_NAME", "price_comparison_app_v2")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "025655358")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
else:
    # Parse the DATABASE_URL for production
    result = urlparse(DATABASE_URL)
    DB_NAME = result.path[1:]
    DB_USER = result.username
    DB_PASSWORD = result.password
    DB_HOST = result.hostname
    DB_PORT = result.port

# JWT Configuration
JWT_SECRET = os.getenv("JWT_SECRET", "your-secret-key-change-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = 24

# --- FastAPI App Initialization ---
app = FastAPI(
    title="PharmMate API",
    description="API for the PharmMate price comparison application.",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Pydantic Models (Defines the JSON response structures) ---

class PricePoint(BaseModel):
    retailer_id: int
    retailer_name: str
    store_id: int
    store_name: str
    store_address: Optional[str] = None
    price: float
    last_updated: Optional[datetime] = None
    in_stock: bool = True

class Promotion(BaseModel):
    deal_id: int
    title: str
    description: Optional[str] = None
    retailer_name: str
    store_id: Optional[int] = None

class ProductSearchResult(BaseModel):
    product_id: str = Field(..., alias="barcode")
    name: str
    brand: Optional[str] = None
    image_url: Optional[str] = None
    prices: List[PricePoint] = []
    promotions: List[Promotion] = []

    class Config:
        orm_mode = True
        allow_population_by_field_name = True

class ProductSummary(BaseModel):
    """Simplified product model for list views with lowest price"""
    product_id: str
    barcode: str
    name: str
    brand: Optional[str] = None
    image_url: str  # Always non-null with fallback
    lowest_price: float  # Calculated minimum price across all retailers

class PaginatedProductResponse(BaseModel):
    """Paginated response for product search results"""
    total_results: int
    page: int
    page_size: int
    total_pages: int
    results: List[ProductSummary]

class NearbyStore(BaseModel):
    store_id: int
    retailer_name: str
    store_name: str
    address: Optional[str] = None
    distance_km: float

class Deal(BaseModel):
    deal_id: int
    retailer_name: str
    title: str
    description: Optional[str] = None
    product_id: Optional[str] = None  # Barcode for navigation
    product_name: Optional[str] = None
    product_brand: Optional[str] = None
    product_image_url: Optional[str] = None
    lowest_price: float  # Mandatory field - lowest price across all retailers
    original_price: Optional[float] = None
    discounted_price: Optional[float] = None
    image_url: Optional[str] = None  # For promotional banner if different from product

class StoreLocation(BaseModel):
    store_id: int
    retailer_name: str
    address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

# --- Authentication Models ---

class RegisterRequest(BaseModel):
    username: str
    email: str
    password: str

class LoginRequest(BaseModel):
    username: str
    password: str

class AuthResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user_id: int
    username: str

# --- Sync Models ---

class CartItem(BaseModel):
    barcode: str
    quantity: int

class SyncRequest(BaseModel):
    favorites: List[str] = []
    cart: List[CartItem] = []

class SyncResponse(BaseModel):
    status: str
    favorites_added: int
    cart_items_added: int
    message: str

class CartItemResponse(BaseModel):
    """Cart item with full product details and quantity"""
    product: ProductSummary
    quantity: int

# --- Cart Recommendation Models ---

class CartRecommendationRequest(BaseModel):
    """Request model for cart recommendation endpoint"""
    barcodes: List[str]

class MissingProduct(BaseModel):
    """Product that is not available at a retailer"""
    barcode: str
    name: str

class RetailerRecommendation(BaseModel):
    """Retailer recommendation with total price and missing products"""
    retailer_name: str
    total_price: float
    missing_products: List[MissingProduct]

class CartRecommendationResponse(BaseModel):
    """Response model for cart recommendation endpoint"""
    recommendation: RetailerRecommendation
    alternatives: List[RetailerRecommendation]

# --- Database Connection Dependency ---
def get_db():
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT,
        cursor_factory=RealDictCursor
    )
    try:
        yield conn.cursor()
    finally:
        conn.close()

# --- Authentication Utilities ---
def create_access_token(data: dict) -> str:
    """Create a JWT access token"""
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(hours=JWT_EXPIRATION_HOURS)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return encoded_jwt

def verify_token(token: str) -> dict:
    """Verify and decode a JWT token"""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Could not validate credentials")

def get_current_user(authorization: Optional[str] = Header(None), db: RealDictCursor = Depends(get_db)) -> int:
    """
    Dependency to get the current authenticated user from JWT token.
    Returns user_id.
    """
    print(f"[AUTH DEBUG] Received authorization header: {authorization[:50] if authorization else 'None'}...")

    if not authorization:
        print("[AUTH DEBUG] Authorization header is missing")
        raise HTTPException(status_code=401, detail="Authorization header missing")

    try:
        # Expected format: "Bearer <token>"
        scheme, token = authorization.split()
        print(f"[AUTH DEBUG] Scheme: {scheme}, Token preview: {token[:20]}...")

        if scheme.lower() != "bearer":
            print(f"[AUTH DEBUG] Invalid scheme: {scheme}")
            raise HTTPException(status_code=401, detail="Invalid authentication scheme")
    except ValueError as e:
        print(f"[AUTH DEBUG] Failed to split authorization header: {e}")
        raise HTTPException(status_code=401, detail="Invalid authorization header format")

    # Verify token
    try:
        payload = verify_token(token)
        print(f"[AUTH DEBUG] Token verified successfully. Payload: {payload}")
    except HTTPException as e:
        print(f"[AUTH DEBUG] Token verification failed: {e.detail}")
        raise

    user_id = payload.get("user_id")

    if user_id is None:
        print("[AUTH DEBUG] user_id not found in token payload")
        raise HTTPException(status_code=401, detail="Invalid token payload")

    # Verify user exists in database
    db.execute("SELECT user_id FROM users WHERE user_id = %s", (user_id,))
    user = db.fetchone()

    if not user:
        print(f"[AUTH DEBUG] User {user_id} not found in database")
        raise HTTPException(status_code=401, detail="User not found")

    print(f"[AUTH DEBUG] Successfully authenticated user_id: {user_id}")
    return user_id

# --- Helper Functions for User Preferences ---
def track_user_interaction(user_id: int, product_barcode: str, db: RealDictCursor):
    """
    Track user interaction and update user preferences based on product category and brand.
    This function is called when a user adds a product to favorites or cart.
    """
    # Get product details
    db.execute("""
        SELECT category, brand
        FROM canonical_products
        WHERE barcode = %s AND is_active = true
    """, (product_barcode,))

    product = db.fetchone()

    if not product:
        return  # Product not found or inactive

    category = product.get('category')
    brand = product.get('brand')

    # Update category preference
    if category:
        db.execute("""
            INSERT INTO user_preferences (user_id, preference_type, preference_value, interaction_score, last_updated)
            VALUES (%s, 'category', %s, 1, NOW())
            ON CONFLICT (user_id, preference_type, preference_value)
            DO UPDATE SET
                interaction_score = user_preferences.interaction_score + 1,
                last_updated = NOW()
        """, (user_id, category))

    # Update brand preference
    if brand:
        db.execute("""
            INSERT INTO user_preferences (user_id, preference_type, preference_value, interaction_score, last_updated)
            VALUES (%s, 'brand', %s, 1, NOW())
            ON CONFLICT (user_id, preference_type, preference_value)
            DO UPDATE SET
                interaction_score = user_preferences.interaction_score + 1,
                last_updated = NOW()
        """, (user_id, brand))

def get_full_cart(user_id: int, db: RealDictCursor) -> List[dict]:
    """
    Helper function to fetch the user's complete cart with full product details.
    Returns an array of CartItemResponse objects with product and quantity.
    """
    query = """
        SELECT
            cp.barcode as product_id,
            cp.barcode,
            cp.name,
            cp.brand,
            COALESCE(cp.image_url, 'https://via.placeholder.com/150?text=No+Image') as image_url,
            cp.lowest_price,
            uc.quantity
        FROM user_cart uc
        JOIN canonical_products cp ON uc.product_barcode = cp.barcode
        WHERE uc.user_id = %s
          AND cp.is_active = true
          AND cp.lowest_price IS NOT NULL
        ORDER BY uc.updated_at DESC;
    """
    db.execute(query, (user_id,))
    results = db.fetchall()

    # Format as CartItemResponse
    cart_items = [
        {
            "product": {
                "product_id": row['product_id'],
                "barcode": row['barcode'],
                "name": row['name'],
                "brand": row['brand'],
                "image_url": row['image_url'],
                "lowest_price": row['lowest_price']
            },
            "quantity": row['quantity']
        }
        for row in results
    ]

    return cart_items

# --- API Endpoints ---

@app.get("/health")
def health_check():
    """
    Health check endpoint to test backend connectivity.
    """
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

# --- Authentication Endpoints ---

@app.post("/api/register", response_model=AuthResponse, tags=["Authentication"])
def register_user(request: RegisterRequest, db: RealDictCursor = Depends(get_db)):
    """
    Register a new user account.

    Creates a new user with hashed password and returns a JWT token.
    """
    try:
        # Validate input
        if not request.username or len(request.username) < 3:
            raise HTTPException(status_code=400, detail="Username must be at least 3 characters")

        if not request.email or "@" not in request.email:
            raise HTTPException(status_code=400, detail="Invalid email address")

        if not request.password or len(request.password) < 6:
            raise HTTPException(status_code=400, detail="Password must be at least 6 characters")

        # Check if username already exists
        db.execute("SELECT user_id FROM users WHERE username = %s", (request.username,))
        if db.fetchone():
            raise HTTPException(status_code=409, detail="Username already exists")

        # Check if email already exists
        db.execute("SELECT user_id FROM users WHERE email = %s", (request.email,))
        if db.fetchone():
            raise HTTPException(status_code=409, detail="Email already exists")

        # Hash password using bcrypt
        password_bytes = request.password.encode('utf-8')
        salt = bcrypt.gensalt()
        password_hash = bcrypt.hashpw(password_bytes, salt).decode('utf-8')

        # Create user
        db.execute("""
            INSERT INTO users (username, email, password_hash)
            VALUES (%s, %s, %s)
            RETURNING user_id, username
        """, (request.username, request.email, password_hash))

        user = db.fetchone()
        db.connection.commit()

        # Generate JWT token
        token = create_access_token({"user_id": user['user_id'], "username": user['username']})

        return AuthResponse(
            access_token=token,
            token_type="bearer",
            user_id=user['user_id'],
            username=user['username']
        )

    except HTTPException:
        raise
    except Exception as e:
        db.connection.rollback()
        raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")

@app.post("/api/login", response_model=AuthResponse, tags=["Authentication"])
def login_user(request: LoginRequest, db: RealDictCursor = Depends(get_db)):
    """
    Authenticate a user and return a JWT token.

    Verifies username or email and password, returns token if valid.
    """
    try:
        # Find user by username OR email
        db.execute("""
            SELECT user_id, username, password_hash
            FROM users
            WHERE username = %s OR email = %s
        """, (request.username, request.username))

        user = db.fetchone()

        # Check if user exists
        if not user:
            raise HTTPException(status_code=401, detail="Invalid username or password")

        # Verify password using bcrypt
        password_bytes = request.password.encode('utf-8')
        stored_hash = user['password_hash'].encode('utf-8')
        if not bcrypt.checkpw(password_bytes, stored_hash):
            raise HTTPException(status_code=401, detail="Invalid username or password")

        # Generate JWT token
        token = create_access_token({"user_id": user['user_id'], "username": user['username']})

        return AuthResponse(
            access_token=token,
            token_type="bearer",
            user_id=user['user_id'],
            username=user['username']
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Login failed: {str(e)}")

@app.get("/api/search", response_model=PaginatedProductResponse, tags=["Products"])
def search_products(
    q: Optional[str] = None,
    category: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: RealDictCursor = Depends(get_db)
):
    """
    Performs a search on products with optional text and category filters.
    Returns paginated results.

    Parameters:
    - q: Optional text search on product names and brands
    - category: Optional exact category match (use Hebrew category strings)
    - page: Page number (default: 1)
    - page_size: Number of items per page (default: 20, max: 100)

    Returns paginated results with metadata.

    Examples:
    - /api/search?q=shampoo - Text search, first page
    - /api/search?q=shampoo&page=2&page_size=10 - Text search, second page with 10 items
    - /api/search?category=טיפוח/הגנה מהשמש - Category filter
    - /api/search?q=cream&category=טיפוח/טיפוח פנים/קרם פנים - Combined filter
    """

    # If neither parameter provided, return empty result
    if not q and not category:
        return PaginatedProductResponse(
            total_results=0,
            page=page,
            page_size=page_size,
            total_pages=0,
            results=[]
        )

    # Build dynamic query based on parameters
    query_conditions = [
        "is_active = true",
        "lowest_price IS NOT NULL",
        "image_url IS NOT NULL",
        "image_url NOT LIKE '%%placeholder%%'"
    ]
    count_params = []

    # Add text search condition if q is provided
    if q:
        search_query = f"%{q}%"
        query_conditions.append("(name ILIKE %s OR brand ILIKE %s)")
        count_params.extend([search_query, search_query])

    # Add category filter if category is provided
    # Use LIKE for prefix matching to support hierarchical categories (e.g., "טיפוח/הגנה מהשמש")
    if category:
        query_conditions.append("category LIKE %s")
        count_params.append(f"{category}%")

    # Construct WHERE clause
    where_clause = " AND ".join(query_conditions)

    # Get total count
    count_query = f"""
        SELECT COUNT(*) as total
        FROM canonical_products
        WHERE {where_clause};
    """
    db.execute(count_query, tuple(count_params))
    total_results = db.fetchone()['total']

    # Calculate pagination values
    total_pages = (total_results + page_size - 1) // page_size  # Ceiling division
    offset = (page - 1) * page_size

    # Build query parameters for main query
    query_params = count_params.copy()
    query_params.extend([page_size, offset])

    # Construct main query with pagination
    query = f"""
        SELECT
            barcode as product_id,
            barcode,
            name,
            brand,
            COALESCE(image_url, 'https://via.placeholder.com/150?text=No+Image') as image_url,
            lowest_price
        FROM canonical_products
        WHERE {where_clause}
        ORDER BY name
        LIMIT %s OFFSET %s;
    """

    db.execute(query, tuple(query_params))
    results = db.fetchall()

    return PaginatedProductResponse(
        total_results=total_results,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
        results=results
    )


@app.get("/api/products/by-barcode/{barcode}", response_model=ProductSearchResult, tags=["Products"])
def get_product_by_barcode(barcode: str, db: RealDictCursor = Depends(get_db)):
    """
    Used by the barcode scanner for an exact product match.
    Returns a single product with full price comparison data.
    Optimized with CTE and window function for fast performance.
    """
    query = """
        WITH latest_prices AS (
            SELECT
                p.*,
                ROW_NUMBER() OVER(
                    PARTITION BY p.retailer_product_id, p.store_id
                    ORDER BY p.price_timestamp DESC
                ) as rn
            FROM prices p
            JOIN retailer_products rp ON p.retailer_product_id = rp.retailer_product_id
            WHERE rp.barcode = %s
              AND p.price > 0
        )
        SELECT
            cp.barcode,
            cp.name,
            cp.brand,
            cp.image_url,
            (
                SELECT json_agg(
                    json_build_object(
                        'retailer_id', r.retailerid,
                        'retailer_name', r.retailername,
                        'store_id', s.storeid,
                        'store_name', s.storename,
                        'store_address', s.address,
                        'price', lp.price,
                        'last_updated', lp.scraped_at,
                        'in_stock', true
                    ) ORDER BY lp.price ASC
                )
                FROM latest_prices lp
                JOIN stores s ON lp.store_id = s.storeid
                JOIN retailer_products rp ON lp.retailer_product_id = rp.retailer_product_id
                JOIN retailers r ON s.retailerid = r.retailerid
                WHERE lp.rn = 1
                  AND s.isactive = true
            ) as prices,
            (
                SELECT json_agg(
                    json_build_object(
                        'deal_id', prom.promotion_id,
                        'title', prom.description,
                        'description', prom.remarks,
                        'retailer_name', r.retailername,
                        'store_id', prom.store_id
                    )
                )
                FROM promotions prom
                JOIN promotion_product_links ppl ON prom.promotion_id = ppl.promotion_id
                JOIN retailer_products rp ON ppl.retailer_product_id = rp.retailer_product_id
                JOIN retailers r ON prom.retailer_id = r.retailerid
                WHERE rp.barcode = cp.barcode
                  AND (prom.end_date IS NULL OR prom.end_date >= NOW())
            ) as promotions
        FROM canonical_products cp
        WHERE cp.barcode = %s
          AND cp.is_active = true;
    """
    db.execute(query, (barcode, barcode))
    result = db.fetchone()

    if not result:
        raise HTTPException(status_code=404, detail="Product not found for this barcode or is inactive.")

    # Convert None prices and promotions to empty lists
    if result['prices'] is None:
        result['prices'] = []
        # If no valid prices, don't return the product
        raise HTTPException(status_code=404, detail="Product has no valid prices available.")
    if result['promotions'] is None:
        result['promotions'] = []

    return result

@app.get("/api/products/{product_id}", response_model=ProductSearchResult, tags=["Products"])
def get_product_by_id(product_id: str, db: RealDictCursor = Depends(get_db)):
    """
    Fetches all information about a single product using its barcode as the ID.
    Returns detailed price comparison data from all retailers.
    Optimized with CTE and window function for fast performance.
    """
    query = """
        WITH latest_prices AS (
            SELECT
                p.*,
                ROW_NUMBER() OVER(
                    PARTITION BY p.retailer_product_id, p.store_id
                    ORDER BY p.price_timestamp DESC
                ) as rn
            FROM prices p
            JOIN retailer_products rp ON p.retailer_product_id = rp.retailer_product_id
            WHERE rp.barcode = %s
              AND p.price > 0
        )
        SELECT
            cp.barcode,
            cp.name,
            cp.brand,
            cp.image_url,
            (
                SELECT json_agg(
                    json_build_object(
                        'retailer_id', r.retailerid,
                        'retailer_name', r.retailername,
                        'store_id', s.storeid,
                        'store_name', s.storename,
                        'store_address', s.address,
                        'price', lp.price,
                        'last_updated', lp.scraped_at,
                        'in_stock', true
                    ) ORDER BY lp.price ASC
                )
                FROM latest_prices lp
                JOIN stores s ON lp.store_id = s.storeid
                JOIN retailer_products rp ON lp.retailer_product_id = rp.retailer_product_id
                JOIN retailers r ON s.retailerid = r.retailerid
                WHERE lp.rn = 1
                  AND s.isactive = true
            ) as prices,
            (
                SELECT json_agg(
                    json_build_object(
                        'deal_id', prom.promotion_id,
                        'title', prom.description,
                        'description', prom.remarks,
                        'retailer_name', r.retailername,
                        'store_id', prom.store_id
                    )
                )
                FROM promotions prom
                JOIN promotion_product_links ppl ON prom.promotion_id = ppl.promotion_id
                JOIN retailer_products rp ON ppl.retailer_product_id = rp.retailer_product_id
                JOIN retailers r ON prom.retailer_id = r.retailerid
                WHERE rp.barcode = cp.barcode
                  AND (prom.end_date IS NULL OR prom.end_date >= NOW())
            ) as promotions
        FROM canonical_products cp
        WHERE cp.barcode = %s
          AND cp.is_active = true;
    """
    db.execute(query, (product_id, product_id))
    result = db.fetchone()

    if not result:
        raise HTTPException(status_code=404, detail="Product not found or is inactive.")

    # Convert None prices and promotions to empty lists
    if result['prices'] is None:
        result['prices'] = []
        # If no valid prices, don't return the product
        raise HTTPException(status_code=404, detail="Product has no valid prices available.")
    if result['promotions'] is None:
        result['promotions'] = []

    return result

@app.get("/api/deals", response_model=List[Deal], tags=["Deals"])
def get_all_deals(limit: Optional[int] = 50, retailer_id: Optional[int] = None, db: RealDictCursor = Depends(get_db)):
    """Fetches a list of all currently active promotions with product information.

    Optimized query using CTE to pre-filter random promotions before joining,
    which dramatically improves performance by reducing the working set size.
    """

    # Build the WHERE clause for retailer filter
    retailer_filter = ""
    params = []
    if retailer_id:
        retailer_filter = "AND p.retailer_id = %s"
        # For the random_promotions CTE filter
        params.append(retailer_id)
        # For the main query filter (used in CTE)
        retailer_filter_cte = "AND retailer_id = %s"
    else:
        retailer_filter_cte = ""

    # Fetch 2x the limit to ensure we have enough after filtering for active products
    random_limit = limit * 2
    params_cte = []
    if retailer_id:
        params_cte.append(retailer_id)
    params_cte.append(random_limit)
    params_cte.append(limit)

    query = f"""
        WITH random_promotions AS (
            SELECT promotion_id
            FROM promotions
            WHERE (end_date IS NULL OR end_date >= NOW())
              {retailer_filter_cte}
            ORDER BY RANDOM()
            LIMIT %s
        ),
        deals_with_products AS (
            SELECT DISTINCT ON (cp.barcode)
                p.promotion_id AS deal_id,
                r.retailername AS retailer_name,
                p.description AS title,
                p.remarks AS description,
                cp.barcode AS product_id,
                cp.name AS product_name,
                cp.brand AS product_brand,
                cp.image_url AS product_image_url,
                cp.lowest_price,
                NULL::float AS original_price,
                NULL::float AS discounted_price,
                NULL AS image_url
            FROM random_promotions rp
            JOIN promotions p ON p.promotion_id = rp.promotion_id
            JOIN retailers r ON p.retailer_id = r.retailerid
            LEFT JOIN promotion_product_links ppl ON p.promotion_id = ppl.promotion_id
            LEFT JOIN retailer_products rp2 ON ppl.retailer_product_id = rp2.retailer_product_id
            LEFT JOIN canonical_products cp ON rp2.barcode = cp.barcode
            WHERE cp.is_active = true
              AND cp.barcode IS NOT NULL
              AND cp.lowest_price IS NOT NULL
            ORDER BY cp.barcode, p.promotion_id
        )
        SELECT * FROM deals_with_products
        ORDER BY RANDOM()
        LIMIT %s
    """

    db.execute(query, tuple(params_cte))
    results = db.fetchall()

    return results

@app.get("/api/stores", response_model=List[StoreLocation], tags=["Stores"])
def get_all_stores(db: RealDictCursor = Depends(get_db)):
    """Returns a list of all stores with their geographic coordinates."""
    query = """
        SELECT
            s.storeid AS store_id,
            r.retailername AS retailer_name,
            s.address,
            s.latitude,
            s.longitude
        FROM stores s
        JOIN retailers r ON s.retailerid = r.retailerid
        WHERE s.isactive = true;
    """
    db.execute(query)
    return db.fetchall()

@app.get("/api/stores/nearby", response_model=List[NearbyStore], tags=["Stores"])
def get_nearby_stores(lat: float, lon: float, limit: int = Query(5, ge=1, le=50), db: RealDictCursor = Depends(get_db)):
    """
    Returns a list of the closest stores to the user's location.
    """
    query = """
        SELECT
            s.storeid as store_id,
            r.retailername as retailer_name,
            s.storename as store_name,
            s.address,
            -- Haversine formula to calculate distance in Kilometers
            (6371 * acos(cos(radians(%s)) * cos(radians(s.latitude)) * cos(radians(s.longitude) - radians(%s)) + sin(radians(%s)) * sin(radians(s.latitude)))) AS distance_km
        FROM stores s
        JOIN retailers r ON s.retailerid = r.retailerid
        WHERE s.latitude IS NOT NULL AND s.longitude IS NOT NULL AND s.isactive = true
        ORDER BY distance_km
        LIMIT %s;
    """
    db.execute(query, (lat, lon, lat, limit))
    return db.fetchall()

# --- User Interaction Endpoints ---

@app.post("/api/favorites/add", tags=["User Interactions"])
def add_to_favorites(
    product_barcode: str,
    user_id: int = Depends(get_current_user),
    db: RealDictCursor = Depends(get_db)
):
    """
    Add a product to user's favorites.
    Protected endpoint - requires JWT authentication.
    """
    try:
        # Verify product exists
        db.execute("SELECT barcode FROM canonical_products WHERE barcode = %s AND is_active = true", (product_barcode,))
        product = db.fetchone()

        if not product:
            raise HTTPException(status_code=404, detail="Product not found or inactive")

        # Add to favorites
        db.execute("""
            INSERT INTO user_favorites (user_id, product_barcode)
            VALUES (%s, %s)
            ON CONFLICT (user_id, product_barcode) DO NOTHING
        """, (user_id, product_barcode))

        # Track interaction for preferences
        track_user_interaction(user_id, product_barcode, db)

        # Commit transaction
        db.connection.commit()

        return {"status": "success", "message": "Product added to favorites"}

    except HTTPException:
        raise
    except Exception as e:
        db.connection.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to add to favorites: {str(e)}")

@app.post("/api/cart/add", response_model=List[CartItemResponse], tags=["User Interactions"])
def add_to_cart(
    product_barcode: str,
    quantity: int = 1,
    user_id: int = Depends(get_current_user),
    db: RealDictCursor = Depends(get_db)
):
    """
    Add a product to user's cart.
    Protected endpoint - requires JWT authentication.

    Returns the complete updated cart with full product details.
    """
    try:
        # Verify product exists
        db.execute("SELECT barcode FROM canonical_products WHERE barcode = %s AND is_active = true", (product_barcode,))
        product = db.fetchone()

        if not product:
            raise HTTPException(status_code=404, detail="Product not found or inactive")

        # Add to cart or update quantity
        db.execute("""
            INSERT INTO user_cart (user_id, product_barcode, quantity)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, product_barcode)
            DO UPDATE SET quantity = user_cart.quantity + EXCLUDED.quantity, updated_at = NOW()
        """, (user_id, product_barcode, quantity))

        # Track interaction for preferences
        track_user_interaction(user_id, product_barcode, db)

        # Commit transaction
        db.connection.commit()

        # Return the full updated cart
        return get_full_cart(user_id, db)

    except HTTPException:
        raise
    except Exception as e:
        db.connection.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to add to cart: {str(e)}")

@app.get("/api/favorites", response_model=List[ProductSummary], tags=["User Interactions"])
def get_favorites(
    user_id: int = Depends(get_current_user),
    db: RealDictCursor = Depends(get_db)
):
    """
    Get the authenticated user's list of favorite products.
    Protected endpoint - requires JWT authentication.

    Returns an array of ProductSummary objects for all favorited products.
    """
    try:
        query = """
            SELECT
                cp.barcode as product_id,
                cp.barcode,
                cp.name,
                cp.brand,
                COALESCE(cp.image_url, 'https://via.placeholder.com/150?text=No+Image') as image_url,
                cp.lowest_price
            FROM user_favorites uf
            JOIN canonical_products cp ON uf.product_barcode = cp.barcode
            WHERE uf.user_id = %s
              AND cp.is_active = true
              AND cp.lowest_price IS NOT NULL
            ORDER BY uf.added_at DESC;
        """
        db.execute(query, (user_id,))
        results = db.fetchall()

        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch favorites: {str(e)}")

@app.delete("/api/favorites/remove/{product_barcode}", tags=["User Interactions"])
def remove_from_favorites(
    product_barcode: str,
    user_id: int = Depends(get_current_user),
    db: RealDictCursor = Depends(get_db)
):
    """
    Remove a product from the user's favorites.
    Protected endpoint - requires JWT authentication.

    Args:
        product_barcode: The barcode of the product to remove from favorites

    Returns:
        Success message confirming removal
    """
    try:
        # Check if item exists in favorites
        db.execute("""
            SELECT favorite_id FROM user_favorites
            WHERE user_id = %s AND product_barcode = %s
        """, (user_id, product_barcode))

        favorite_item = db.fetchone()

        if not favorite_item:
            raise HTTPException(status_code=404, detail="Product not found in favorites")

        # Delete the favorite
        db.execute("""
            DELETE FROM user_favorites
            WHERE user_id = %s AND product_barcode = %s
        """, (user_id, product_barcode))

        db.connection.commit()
        return {"status": "success", "message": "Product removed from favorites"}

    except HTTPException:
        raise
    except Exception as e:
        db.connection.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to remove from favorites: {str(e)}")

@app.get("/api/cart", response_model=List[CartItemResponse], tags=["User Interactions"])
def get_cart(
    user_id: int = Depends(get_current_user),
    db: RealDictCursor = Depends(get_db)
):
    """
    Get the authenticated user's shopping cart.
    Protected endpoint - requires JWT authentication.

    Returns an array of CartItemResponse objects with full product details and quantity.
    """
    try:
        return get_full_cart(user_id, db)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch cart: {str(e)}")

class UpdateCartRequest(BaseModel):
    quantity: int

@app.put("/api/cart/update/{product_barcode}", response_model=List[CartItemResponse], tags=["User Interactions"])
def update_cart_item(
    product_barcode: str,
    request: UpdateCartRequest,
    user_id: int = Depends(get_current_user),
    db: RealDictCursor = Depends(get_db)
):
    """
    Update the quantity of a product in the user's cart.
    Protected endpoint - requires JWT authentication.

    If quantity is 0 or negative, the item is removed from the cart.
    If quantity is positive, the item quantity is updated.

    Returns the complete updated cart with full product details.
    """
    try:
        # Check if item exists in cart
        db.execute("""
            SELECT cart_id FROM user_cart
            WHERE user_id = %s AND product_barcode = %s
        """, (user_id, product_barcode))

        cart_item = db.fetchone()

        if not cart_item:
            raise HTTPException(status_code=404, detail="Product not found in cart")

        # If quantity is 0 or negative, remove the item
        if request.quantity <= 0:
            db.execute("""
                DELETE FROM user_cart
                WHERE user_id = %s AND product_barcode = %s
            """, (user_id, product_barcode))

            db.connection.commit()
            # Return the full updated cart
            return get_full_cart(user_id, db)

        # Otherwise, update the quantity
        db.execute("""
            UPDATE user_cart
            SET quantity = %s, updated_at = NOW()
            WHERE user_id = %s AND product_barcode = %s
        """, (request.quantity, user_id, product_barcode))

        db.connection.commit()
        # Return the full updated cart
        return get_full_cart(user_id, db)

    except HTTPException:
        raise
    except Exception as e:
        db.connection.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to update cart item: {str(e)}")

@app.delete("/api/cart/remove/{product_barcode}", response_model=List[CartItemResponse], tags=["User Interactions"])
def remove_from_cart(
    product_barcode: str,
    user_id: int = Depends(get_current_user),
    db: RealDictCursor = Depends(get_db)
):
    """
    Remove a product from the user's cart.
    Protected endpoint - requires JWT authentication.

    Returns the complete updated cart with full product details.
    """
    try:
        # Check if item exists in cart
        db.execute("""
            SELECT cart_id FROM user_cart
            WHERE user_id = %s AND product_barcode = %s
        """, (user_id, product_barcode))

        cart_item = db.fetchone()

        if not cart_item:
            raise HTTPException(status_code=404, detail="Product not found in cart")

        # Delete the item
        db.execute("""
            DELETE FROM user_cart
            WHERE user_id = %s AND product_barcode = %s
        """, (user_id, product_barcode))

        db.connection.commit()
        # Return the full updated cart
        return get_full_cart(user_id, db)

    except HTTPException:
        raise
    except Exception as e:
        db.connection.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to remove from cart: {str(e)}")

# --- Recommendations Endpoint ---

@app.get("/api/recommendations", response_model=List[ProductSummary], tags=["Recommendations"])
def get_recommendations(
    limit: int = Query(10, ge=1, le=50),
    user_id: int = Depends(get_current_user),
    db: RealDictCursor = Depends(get_db)
):
    """
    Get personalized product recommendations for the authenticated user.
    Protected endpoint - requires JWT authentication.

    Returns products based on:
    1. User's top 2-3 categories and top 2-3 brands (by interaction score)
    2. Excludes products already in favorites or cart
    3. Returns active products with valid prices
    """

    # Step 1: Get user's top preferences
    db.execute("""
        SELECT preference_type, preference_value, interaction_score
        FROM user_preferences
        WHERE user_id = %s
        ORDER BY interaction_score DESC
    """, (user_id,))

    preferences = db.fetchall()

    if not preferences:
        # User has no preferences yet, return popular products
        query = """
            SELECT
                barcode as product_id,
                barcode,
                name,
                brand,
                COALESCE(image_url, 'https://via.placeholder.com/150?text=No+Image') as image_url,
                lowest_price
            FROM canonical_products
            WHERE is_active = TRUE
              AND lowest_price IS NOT NULL
              AND image_url IS NOT NULL
              AND image_url NOT LIKE '%%placeholder%%'
            ORDER BY RANDOM()
            LIMIT %s;
        """
        db.execute(query, (limit,))
        results = db.fetchall()
    else:
        # Extract top categories and brands
        top_categories = [p['preference_value'] for p in preferences if p['preference_type'] == 'category'][:3]
        top_brands = [p['preference_value'] for p in preferences if p['preference_type'] == 'brand'][:3]

        # Step 2: Get products user has already interacted with (to exclude)
        db.execute("""
            SELECT product_barcode FROM user_favorites WHERE user_id = %s
            UNION
            SELECT product_barcode FROM user_cart WHERE user_id = %s
        """, (user_id, user_id))

        interacted_barcodes = [row['product_barcode'] for row in db.fetchall()]

        # Step 3: Build recommendation query
        category_condition = ""
        brand_condition = ""
        query_params = []

        if top_categories:
            category_condition = "cp.category = ANY(%s)"
            query_params.append(top_categories)
        else:
            category_condition = "TRUE"
            # Don't append anything - TRUE doesn't need a parameter

        if top_brands:
            brand_condition = "cp.brand = ANY(%s)"
            query_params.append(top_brands)
        else:
            brand_condition = "TRUE"
            # Don't append anything - TRUE doesn't need a parameter

        # Add interacted_barcodes to params
        if interacted_barcodes:
            query_params.append(interacted_barcodes)
        else:
            query_params.append([])  # Empty array for NOT IN clause

        query_params.append(limit)

        query = f"""
            SELECT
                cp.barcode as product_id,
                cp.barcode,
                cp.name,
                cp.brand,
                COALESCE(cp.image_url, 'https://via.placeholder.com/150?text=No+Image') as image_url,
                cp.lowest_price
            FROM canonical_products cp
            WHERE cp.is_active = TRUE
              AND ({category_condition} OR {brand_condition})
              AND cp.barcode != ALL(%s)
              AND cp.lowest_price IS NOT NULL
              AND cp.image_url IS NOT NULL
              AND cp.image_url NOT LIKE '%%placeholder%%'
            ORDER BY
              -- Prioritize products with promotions
              CASE WHEN EXISTS (
                SELECT 1 FROM promotion_product_links ppl
                JOIN retailer_products rp ON ppl.retailer_product_id = rp.retailer_product_id
                WHERE rp.barcode = cp.barcode
              ) THEN 0 ELSE 1 END,
              -- Then by random for variety
              RANDOM()
            LIMIT %s;
        """

        db.execute(query, tuple(query_params))
        results = db.fetchall()

    return results

@app.get("/api/recommendations/popular", response_model=List[ProductSummary], tags=["Recommendations"])
def get_popular_recommendations(
    limit: int = Query(10, ge=1, le=50),
    db: RealDictCursor = Depends(get_db)
):
    """
    Get popular product recommendations for anonymous users.
    Public endpoint - does NOT require authentication.

    Returns trending, popular products with their pre-calculated lowest prices.
    """

    query = """
        SELECT
            barcode as product_id,
            barcode,
            name,
            brand,
            COALESCE(image_url, 'https://via.placeholder.com/150?text=No+Image') as image_url,
            lowest_price
        FROM canonical_products
        WHERE is_active = TRUE
          AND lowest_price IS NOT NULL
          AND image_url IS NOT NULL
          AND image_url NOT LIKE '%%placeholder%%'
        ORDER BY RANDOM()
        LIMIT %s;
    """

    db.execute(query, (limit,))
    results = db.fetchall()

    return results

@app.post("/api/sync", response_model=SyncResponse, tags=["User Interactions"])
def sync_anonymous_data(
    request: SyncRequest,
    user_id: int = Depends(get_current_user),
    db: RealDictCursor = Depends(get_db)
):
    """
    Sync anonymous user data (favorites and cart) to the user's account.
    Protected endpoint - requires JWT authentication.

    This is typically called when a user logs in for the first time
    and has local data that needs to be merged with their account.
    """
    try:
        favorites_added = 0
        cart_items_added = 0

        # Sync favorites
        for barcode in request.favorites:
            # Verify product exists
            db.execute("SELECT barcode FROM canonical_products WHERE barcode = %s AND is_active = true", (barcode,))
            product = db.fetchone()

            if product:
                # Add to favorites (ignore if already exists)
                db.execute("""
                    INSERT INTO user_favorites (user_id, product_barcode)
                    VALUES (%s, %s)
                    ON CONFLICT (user_id, product_barcode) DO NOTHING
                    RETURNING favorite_id
                """, (user_id, barcode))

                if db.fetchone():
                    favorites_added += 1
                    # Track interaction for preferences
                    track_user_interaction(user_id, barcode, db)

        # Sync cart items
        for item in request.cart:
            # Verify product exists
            db.execute("SELECT barcode FROM canonical_products WHERE barcode = %s AND is_active = true", (item.barcode,))
            product = db.fetchone()

            if product:
                # Add to cart or update quantity
                db.execute("""
                    INSERT INTO user_cart (user_id, product_barcode, quantity)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (user_id, product_barcode)
                    DO UPDATE SET quantity = user_cart.quantity + EXCLUDED.quantity, updated_at = NOW()
                    RETURNING cart_id
                """, (user_id, item.barcode, item.quantity))

                if db.fetchone():
                    cart_items_added += 1
                    # Track interaction for preferences
                    track_user_interaction(user_id, item.barcode, db)

        # Commit transaction
        db.connection.commit()

        return SyncResponse(
            status="success",
            favorites_added=favorites_added,
            cart_items_added=cart_items_added,
            message=f"Successfully synced {favorites_added} favorites and {cart_items_added} cart items"
        )

    except HTTPException:
        raise
    except Exception as e:
        db.connection.rollback()
        raise HTTPException(status_code=500, detail=f"Sync failed: {str(e)}")

@app.post("/api/cart/recommendation", response_model=CartRecommendationResponse, tags=["Cart"])
def get_cart_recommendation(
    request: CartRecommendationRequest,
    db: RealDictCursor = Depends(get_db)
):
    """
    Analyze a shopping cart and recommend the cheapest retailer for the entire purchase.

    This endpoint compares prices across major retailers (Super-Pharm, Be Pharm, Good Pharm)
    and returns the retailer with the lowest total price that has all products in stock.

    Request body:
    {
        "barcodes": ["7290019075271", "3600522251750", "7290014775510"]
    }

    Response includes:
    - recommendation: The cheapest retailer with all products available
    - alternatives: Other retailers with their totals and missing products
    """

    try:
        # Validate input
        if not request.barcodes or len(request.barcodes) == 0:
            raise HTTPException(status_code=400, detail="Barcodes list cannot be empty")

        # Define major retailers to compare (Super-Pharm, Good Pharm, Be Pharm)
        MAJOR_RETAILERS = [52, 97, 150]  # Super-Pharm, Good Pharm, Be Pharm

        # Step 1: Get all product names for the barcodes (for missing product info)
        product_names = {}
        placeholders = ','.join(['%s'] * len(request.barcodes))
        db.execute(f"""
            SELECT barcode, name
            FROM canonical_products
            WHERE barcode IN ({placeholders})
              AND is_active = true
        """, tuple(request.barcodes))

        for row in db.fetchall():
            product_names[row['barcode']] = row['name']

        # Check if any products were not found
        missing_from_db = [b for b in request.barcodes if b not in product_names]
        if missing_from_db:
            raise HTTPException(
                status_code=404,
                detail=f"Products not found in database: {', '.join(missing_from_db)}"
            )

        # Step 2: Fetch all latest prices for all barcodes across all major retailers
        # Optimized query using CTE with window function to avoid N+1 correlated subquery
        # This eliminates the performance bottleneck of executing a subquery for each row
        query = f"""
            WITH latest_prices_by_retailer AS (
                SELECT
                    rp.barcode,
                    r.retailerid,
                    r.retailername,
                    p.price,
                    ROW_NUMBER() OVER (
                        PARTITION BY rp.retailer_product_id
                        ORDER BY p.price_timestamp DESC
                    ) as rn
                FROM retailer_products rp
                JOIN retailers r ON rp.retailer_id = r.retailerid
                JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
                WHERE rp.barcode IN ({placeholders})
                  AND r.retailerid = ANY(%s)
                  AND p.price > 0
            )
            SELECT barcode, retailerid, retailername, price
            FROM latest_prices_by_retailer
            WHERE rn = 1
            ORDER BY barcode, retailerid, price ASC
        """

        db.execute(query, tuple(request.barcodes) + (MAJOR_RETAILERS,))
        all_prices = db.fetchall()

        # Step 3: Organize prices by retailer and barcode
        # Structure: {retailer_id: {retailer_name: str, prices: {barcode: price}}}
        retailer_data = {}
        for retailer_id in MAJOR_RETAILERS:
            retailer_data[retailer_id] = {
                'retailer_name': None,
                'prices': {}
            }

        for row in all_prices:
            retailer_id = row['retailerid']
            barcode = row['barcode']
            price = float(row['price'])
            retailer_name = row['retailername']

            if retailer_id in retailer_data:
                retailer_data[retailer_id]['retailer_name'] = retailer_name
                # Keep only the lowest price if there are multiple stores
                if barcode not in retailer_data[retailer_id]['prices']:
                    retailer_data[retailer_id]['prices'][barcode] = price
                else:
                    retailer_data[retailer_id]['prices'][barcode] = min(
                        retailer_data[retailer_id]['prices'][barcode],
                        price
                    )

        # Step 4: Calculate totals and identify missing products for each retailer
        retailer_results = []

        for retailer_id, data in retailer_data.items():
            retailer_name = data['retailer_name']

            # Get retailer name from database if not found in prices
            if not retailer_name:
                db.execute("SELECT retailername FROM retailers WHERE retailerid = %s", (retailer_id,))
                result = db.fetchone()
                retailer_name = result['retailername'] if result else f"Retailer {retailer_id}"

            total_price = 0.0
            missing_products = []

            for barcode in request.barcodes:
                if barcode in data['prices']:
                    total_price += data['prices'][barcode]
                else:
                    # Product is missing at this retailer
                    missing_products.append(MissingProduct(
                        barcode=barcode,
                        name=product_names.get(barcode, "Unknown Product")
                    ))

            retailer_results.append({
                'retailer_name': retailer_name,
                'total_price': round(total_price, 2),
                'missing_products': missing_products,
                'has_all_products': len(missing_products) == 0
            })

        # Step 5: Sort retailers - first by availability (all products), then by price
        retailer_results.sort(key=lambda x: (not x['has_all_products'], x['total_price']))

        # Step 6: Determine recommendation and alternatives
        recommendation = None
        alternatives = []

        for result in retailer_results:
            retailer_rec = RetailerRecommendation(
                retailer_name=result['retailer_name'],
                total_price=result['total_price'],
                missing_products=result['missing_products']
            )

            # The first retailer with all products is the recommendation
            if recommendation is None and result['has_all_products']:
                recommendation = retailer_rec
            else:
                alternatives.append(retailer_rec)

        # If no retailer has all products, recommend the one with fewest missing items and lowest price
        if recommendation is None:
            if retailer_results:
                recommendation = RetailerRecommendation(
                    retailer_name=retailer_results[0]['retailer_name'],
                    total_price=retailer_results[0]['total_price'],
                    missing_products=retailer_results[0]['missing_products']
                )
                alternatives = [
                    RetailerRecommendation(
                        retailer_name=r['retailer_name'],
                        total_price=r['total_price'],
                        missing_products=r['missing_products']
                    )
                    for r in retailer_results[1:]
                ]
            else:
                raise HTTPException(
                    status_code=404,
                    detail="No price data available for the requested products"
                )

        return CartRecommendationResponse(
            recommendation=recommendation,
            alternatives=alternatives
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cart recommendation failed: {str(e)}")

if __name__ == "__main__":
    print("🚀 Starting PharmMate Backend Server...")
    print("API documentation available at http://127.0.0.1:8000/docs")
    uvicorn.run("backend:app", host="0.0.0.0", port=8000, reload=True)