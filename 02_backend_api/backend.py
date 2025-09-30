import os
from typing import List, Optional
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from datetime import datetime
import uvicorn

# --- Configuration ---
load_dotenv()
DB_NAME = os.getenv("DB_NAME", "price_comparison_app_v2")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "***REMOVED***")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

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
    original_price: Optional[float] = None
    discounted_price: Optional[float] = None
    image_url: Optional[str] = None  # For promotional banner if different from product

class StoreLocation(BaseModel):
    store_id: int
    retailer_name: str
    address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

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

# --- API Endpoints ---

@app.get("/health")
def health_check():
    """
    Health check endpoint to test backend connectivity.
    """
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/api/search", response_model=List[ProductSearchResult], tags=["Products"])
def search_products(q: str, db: RealDictCursor = Depends(get_db)):
    """
    Performs a text-based search on product names and brands.
    Returns products with full price comparison data from all retailers.
    """
    search_query = f"%{q}%"
    query = """
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
                        'price', p.price,
                        'last_updated', p.scraped_at,
                        'in_stock', true
                    ) ORDER BY p.price ASC
                )
                FROM retailer_products rp
                JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
                JOIN stores s ON p.store_id = s.storeid
                JOIN retailers r ON s.retailerid = r.retailerid
                WHERE rp.barcode = cp.barcode
                  AND p.price > 0
                  AND p.price_timestamp = (
                      SELECT MAX(p2.price_timestamp)
                      FROM prices p2
                      WHERE p2.retailer_product_id = p.retailer_product_id
                        AND p2.store_id = p.store_id
                  )
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
        WHERE cp.is_active = true
          AND (cp.name ILIKE %s OR cp.brand ILIKE %s)
          AND EXISTS (
              SELECT 1
              FROM retailer_products rp2
              JOIN prices p2 ON rp2.retailer_product_id = p2.retailer_product_id
              WHERE rp2.barcode = cp.barcode
                AND p2.price > 0
          )
        LIMIT 50;
    """
    db.execute(query, (search_query, search_query))
    results = db.fetchall()

    # Convert None prices and promotions to empty lists
    for result in results:
        if result['prices'] is None:
            result['prices'] = []
        if result['promotions'] is None:
            result['promotions'] = []

    return results


@app.get("/api/products/by-barcode/{barcode}", response_model=ProductSearchResult, tags=["Products"])
def get_product_by_barcode(barcode: str, db: RealDictCursor = Depends(get_db)):
    """
    Used by the barcode scanner for an exact product match.
    Returns a single product with full price comparison data.
    """
    query = """
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
                        'price', p.price,
                        'last_updated', p.scraped_at,
                        'in_stock', true
                    ) ORDER BY p.price ASC
                )
                FROM retailer_products rp
                JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
                JOIN stores s ON p.store_id = s.storeid
                JOIN retailers r ON s.retailerid = r.retailerid
                WHERE rp.barcode = cp.barcode
                  AND p.price > 0
                  AND p.price_timestamp = (
                      SELECT MAX(p2.price_timestamp)
                      FROM prices p2
                      WHERE p2.retailer_product_id = p.retailer_product_id
                        AND p2.store_id = p.store_id
                  )
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
    db.execute(query, (barcode,))
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
    """
    query = """
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
                        'price', p.price,
                        'last_updated', p.scraped_at,
                        'in_stock', true
                    ) ORDER BY p.price ASC
                )
                FROM retailer_products rp
                JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
                JOIN stores s ON p.store_id = s.storeid
                JOIN retailers r ON s.retailerid = r.retailerid
                WHERE rp.barcode = cp.barcode
                  AND p.price > 0
                  AND p.price_timestamp = (
                      SELECT MAX(p2.price_timestamp)
                      FROM prices p2
                      WHERE p2.retailer_product_id = p.retailer_product_id
                        AND p2.store_id = p.store_id
                  )
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
    db.execute(query, (product_id,))
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
    """Fetches a list of all currently active promotions with product information."""
    # Simplified query - just get basic deal info without price calculations
    # Price calculations on 8.6M rows are too slow - skip them for now
    query = """
        SELECT DISTINCT
            p.promotion_id AS deal_id,
            r.retailername AS retailer_name,
            p.description AS title,
            p.remarks AS description,
            cp.barcode AS product_id,
            cp.name AS product_name,
            cp.brand AS product_brand,
            cp.image_url AS product_image_url,
            NULL::float AS original_price,
            NULL::float AS discounted_price,
            NULL AS image_url
        FROM promotions p
        JOIN retailers r ON p.retailer_id = r.retailerid
        LEFT JOIN promotion_product_links ppl ON p.promotion_id = ppl.promotion_id
        LEFT JOIN retailer_products rp ON ppl.retailer_product_id = rp.retailer_product_id
        LEFT JOIN canonical_products cp ON rp.barcode = cp.barcode
        WHERE (p.end_date IS NULL OR p.end_date >= NOW())
          AND cp.is_active = true
          AND cp.barcode IS NOT NULL
    """
    params = []
    if retailer_id:
        query += " AND p.retailer_id = %s"
        params.append(retailer_id)

    query += " ORDER BY p.promotion_id DESC"
    query += " LIMIT %s"
    params.append(limit)

    db.execute(query, tuple(params))
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

if __name__ == "__main__":
    print("🚀 Starting PharmMate Backend Server...")
    print("API documentation available at http://127.0.0.1:8000/docs")
    uvicorn.run("backend:app", host="0.0.0.0", port=8000, reload=True)