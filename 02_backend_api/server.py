import os
import psycopg2
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import json
import time
from functools import lru_cache
from typing import Optional

# --- Configuration ---
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "price_comparison_app_v2"
PG_USER = "postgres"
PG_PASSWORD = "***REMOVED***"

# --- FastAPI App Setup ---
app = FastAPI()

# Cache for search results (5-10 minute TTL)
SEARCH_CACHE = {}
CACHE_TTL = 300  # 5 minutes in seconds

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@lru_cache(maxsize=128)
def get_cached_search_key(query: str, limit: int, sort: str) -> str:
    """Generate cache key for search results"""
    return f"search:{query.lower()}:{limit}:{sort}"

def is_cache_valid(timestamp: float) -> bool:
    """Check if cache entry is still valid"""
    return time.time() - timestamp < CACHE_TTL

def get_db_connection():
    try:
        return psycopg2.connect(
            host=PG_HOST, port=PG_PORT, database=PG_DATABASE,
            user=PG_USER, password=PG_PASSWORD
        )
    except psycopg2.Error as e:
        print(f"[DB CRITICAL] Unable to connect: {e}")
        raise

# --- API Endpoints ---
@app.get("/search")
def search_products(
    q: str, 
    limit: int = Query(default=10, ge=1, le=50, description="Number of results to return"),
    sort: str = Query(default="relevance", regex="^(relevance|price_asc|price_desc|name)$", description="Sort order")
):
    """
    Searches for products and returns intelligently grouped comparison results.
    """
    if not q:
        return {"results": []}
    
    # Check cache first
    cache_key = get_cached_search_key(q, limit, sort)
    if cache_key in SEARCH_CACHE:
        cached_result, timestamp = SEARCH_CACHE[cache_key]
        if is_cache_valid(timestamp):
            return cached_result

    # Enhanced query with relevance scoring and sorting
    sort_clause = {
        "relevance": "ORDER BY relevance_score DESC, min_price ASC",
        "price_asc": "ORDER BY min_price ASC",
        "price_desc": "ORDER BY min_price DESC", 
        "name": "ORDER BY display_name ASC"
    }[sort]
    
    query = f"""
    WITH SearchedProducts AS (
        -- Optimized search with better indexing utilization
        SELECT 
            product_id,
            CASE 
                WHEN LOWER(canonical_name) = LOWER(%s) THEN 100
                WHEN canonical_name ILIKE %s THEN 90
                WHEN LOWER(brand) = LOWER(%s) THEN 80
                WHEN brand ILIKE %s THEN 70
                WHEN to_tsvector('simple', canonical_name || ' ' || brand) @@ to_tsquery('simple', %s) THEN 60
                ELSE 50
            END as relevance_score
        FROM products
        WHERE (canonical_name ILIKE %s OR brand ILIKE %s OR to_tsvector('simple', canonical_name || ' ' || brand) @@ to_tsquery('simple', %s))
        ORDER BY 
            CASE 
                WHEN LOWER(canonical_name) = LOWER(%s) THEN 0
                WHEN canonical_name ILIKE %s THEN 1
                WHEN LOWER(brand) = LOWER(%s) THEN 2
                WHEN brand ILIKE %s THEN 3
                ELSE 4
            END
        LIMIT 100  -- Reduced from 200 for better performance
    ),
    GroupedProducts AS (
        SELECT DISTINCT pgl.group_id, MAX(sp.relevance_score) as max_relevance
        FROM product_group_links pgl
        JOIN SearchedProducts sp ON pgl.product_id = sp.product_id
        GROUP BY pgl.group_id
    ),
    RankedPrices AS (
        SELECT
            pgl.group_id,
            p.canonical_name,
            p.brand,
            p.attributes,
            r.retailername AS retailer_name,
            pr.price,
            ROW_NUMBER() OVER(PARTITION BY pr.retailer_product_id ORDER BY pr.price_timestamp DESC) as rn
        FROM product_group_links pgl
        JOIN products p ON pgl.product_id = p.product_id
        JOIN retailer_products rp ON p.product_id = rp.product_id
        JOIN prices pr ON rp.retailer_product_id = pr.retailer_product_id
        JOIN retailers r ON rp.retailer_id = r.retailerid
        WHERE pgl.group_id IN (SELECT group_id FROM GroupedProducts)
    )
    SELECT
        rp.group_id,
        MIN(rp.canonical_name) as display_name,
        MIN(rp.brand) as display_brand,
        (array_agg(rp.attributes))[1] as display_attributes,
        json_agg(json_build_object(
            'retailer', rp.retailer_name,
            'price', rp.price
        )) as prices,
        gp.max_relevance as relevance_score,
        MIN(rp.price) as min_price
    FROM RankedPrices rp
    JOIN GroupedProducts gp ON rp.group_id = gp.group_id
    WHERE rp.rn = 1
    GROUP BY rp.group_id, gp.max_relevance
    {sort_clause}
    LIMIT %s;
    """
    search_term = ' & '.join(q.strip().split())
    query_like = f"%{q.strip()}%"

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, (
            q.strip(),  # exact name match
            query_like,  # name ILIKE
            q.strip(),  # exact brand match
            query_like,  # brand ILIKE
            search_term,  # text search
            query_like,  # WHERE name ILIKE
            query_like,  # WHERE brand ILIKE
            search_term,  # WHERE text search
            q.strip(),  # ORDER BY exact name
            query_like,  # ORDER BY name ILIKE
            q.strip(),  # ORDER BY exact brand
            query_like,  # ORDER BY brand ILIKE
            limit
        ))
            results = cursor.fetchall()

            product_groups = []
            for row in results:
                product_groups.append({
                    "groupId": row[0],
                    "name": row[1],
                    "brand": row[2],
                    "attributes": row[3],
                    "prices": row[4]
                })
            
            result = {"results": product_groups}
            # Cache the result
            SEARCH_CACHE[cache_key] = (result, time.time())
            return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()

@app.get("/group/{group_id}")
def get_product_group_details(group_id: int):
    """
    Fetches the details for a single, specific product group. This is highly optimized for performance.
    """
    # This new query is highly optimized. It finds the latest prices first
    # before joining, which is much faster.
    query = """
    WITH LatestPrices AS (
        -- Use DISTINCT ON, a fast PostgreSQL feature, to get only the latest price
        -- for each product listing. This avoids scanning the entire price history.
        SELECT DISTINCT ON (retailer_product_id)
            retailer_product_id,
            price
        FROM prices
        ORDER BY retailer_product_id, price_timestamp DESC
    )
    -- The main query now joins against the small, pre-filtered set of latest prices.
    SELECT
        MIN(p.canonical_name) as display_name,
        MIN(p.brand) as display_brand,
        (array_agg(p.attributes))[1] as display_attributes,
        json_agg(json_build_object(
            'retailer', r.retailername,
            'price', lp.price
        )) as prices
    FROM product_group_links pgl
    JOIN products p ON pgl.product_id = p.product_id
    JOIN retailer_products rp ON p.product_id = rp.product_id
    JOIN retailers r ON rp.retailer_id = r.retailerid
    JOIN LatestPrices lp ON rp.retailer_product_id = lp.retailer_product_id
    WHERE pgl.group_id = %s;
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, (group_id,))
            result = cursor.fetchone()

            if not result or not result[0]:
                raise HTTPException(status_code=404, detail="Product group not found")

            product_group = {
                "groupId": group_id,
                "name": result[0],
                "brand": result[1],
                "attributes": result[2],
                "prices": result[3]
            }
            return product_group

    except Exception as e:
        # Re-raise FastAPI's HTTPException, otherwise raise a generic 500
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()
