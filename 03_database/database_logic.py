import re
import psycopg2
from typing import Union, Dict, Set, List, Tuple

# --- CONFIGURATION FOR LOGIC ---
# Using the same thresholds from our final analysis script for consistency
JACCARD_SEARCH_THRESHOLD = 0.75

# Using the same brand keywords to help identify generic vs. branded names
BRAND_KEYWORDS = {
    'שופרסל', 'רמי לוי', 'ויקטורי', 'סוגת', 'אסם', 'תנובה', 'שטראוס', 'עלית',
    'קוקה קולה', 'פריגת', 'יכין', 'פרימור', 'טרה', 'יטבתה', 'מחלבות גד', 'זוגלובק', 'יחיעם',
    'טירת צבי', 'עוף טוב', 'מאמא עוף', 'סנו', 'ניקול', 'קלין', 'פיניש', 'פיירי',
    'קנור', 'היינץ', 'תלמה', 'בייגל בייגל', 'לייף', 'קרליין', 'ניוואה', 'דאב',
    'הד אנד שולדרס', 'קולגייט', 'אורביט', 'האגיס', 'פמפרס', 'טיטולים', 'יש', 'שווה'
}


def extract_product_features(name: str) -> Dict[str, Union[Set[str], str]]:
    """
    Extracts structured features (tokens) from a product name for matching.
    This function should be IDENTICAL to the one in the analysis script for consistency.
    """
    if not isinstance(name, str):
        return {"tokens": set(), "original_name": ""}

    text_part = name.lower()
    
    # Simple normalization for tokenization
    text_part = re.sub(r'[\d%]+', ' ', text_part)
    text_part = re.sub(r'[\\/!"#$%&\'()*+,-./:;<=>?@\[\]^_`{|}~]', ' ', text_part)
    stop_words = {'בטעם', 'אריזת', 'מארז', 'מבצע', 'ביחידה', 'יחידות', 'של'}
    tokens = {word for word in text_part.split() if word not in stop_words and len(word) > 1}
    
    return {"tokens": tokens, "original_name": name}

def calculate_jaccard_similarity(set1: Set[str], set2: Set[str]) -> float:
    """Calculates the Jaccard similarity between two sets of tokens."""
    if not set1 and not set2:
        return 1.0
    if not set1 or not set2:
        return 0.0
    return len(set1.intersection(set2)) / len(set1.union(set2))

def find_or_create_product(scraper_item_name: str, cur) -> Union[int, None]: # <<< FIXED THIS LINE
    """
    The "Search Before You Create" logic.
    Checks if a product already exists. If so, returns its canonical ID.
    If not, creates it and returns the new ID.
    
    Args:
        scraper_item_name (str): The product name from the scraping source.
        cur: An active psycopg2 database cursor.
        
    Returns:
        The canonical_masterproductid for this product, or None if it cannot be processed.
    """
    new_features = extract_product_features(scraper_item_name)
    new_tokens = new_features['tokens']
    
    if not new_tokens:
        print(f"  - Could not process product with no valid tokens: '{scraper_item_name}'")
        return None

    # This search should be optimized in a production system using tsvector indexes.
    # For now, fetching all canonical products demonstrates the core logic.
    cur.execute(
        "SELECT masterproductid, productname FROM products WHERE canonical_masterproductid = masterproductid;"
    )
    candidates = cur.fetchall()
    
    best_match_id = None
    highest_score = JACCARD_SEARCH_THRESHOLD

    # Score candidates against the new product
    for pid, pname in candidates:
        existing_features = extract_product_features(pname)
        score = calculate_jaccard_similarity(new_tokens, existing_features['tokens'])
        
        if score > highest_score:
            # Added check for generic names to avoid bad matches like 'מלח' vs 'סוכר'
            p_generic_tokens = existing_features['tokens'] - BRAND_KEYWORDS
            new_generic_tokens = new_tokens - BRAND_KEYWORDS
            if len(p_generic_tokens) > 1 or len(new_generic_tokens) > 1:
                highest_score = score
                best_match_id = pid

    if best_match_id:
        # A strong match was found! Return its canonical ID.
        print(f"  - Match found for '{scraper_item_name}'. Linking to existing canonical ID: {best_match_id}")
        return best_match_id
    else:
        # No confident match found. Create a new master product.
        print(f"  - No existing match for '{scraper_item_name}'. Creating new canonical product.")
        cur.execute(
            "INSERT INTO products (productname) VALUES (%s) RETURNING masterproductid;",
            (scraper_item_name,)
        )
        new_id = cur.fetchone()[0]
        
        # Make the new product its own canonical master
        cur.execute(
            "UPDATE products SET canonical_masterproductid = %s WHERE masterproductid = %s;",
            (new_id, new_id)
        )
        print(f"    > Created new canonical product with ID: {new_id}")
        return new_id

def insert_listing_and_price(canonical_id: int, store_id: int, item_code: str, price: float, cur):
    """
    Inserts or updates the retailer-specific product listing and adds a new price entry.
    This function uses ON CONFLICT to avoid creating duplicate listings.
    """
    try:
        # Step 1: Find or create the listing in `retailerproductlistings`
        # Using ON CONFLICT is a robust way to handle this "upsert" logic.
        # This assumes you have a unique constraint on (storeid, retaileritemcode).
        listing_query = """
        INSERT INTO retailerproductlistings (masterproductid, storeid, retaileritemcode, retailerid)
        SELECT %s, %s, %s, s.retailerid FROM stores s WHERE s.storeid = %s
        ON CONFLICT (storeid, retaileritemcode) DO UPDATE
        SET masterproductid = EXCLUDED.masterproductid, lastseenat = CURRENT_TIMESTAMP
        RETURNING listingid;
        """
        cur.execute(listing_query, (canonical_id, store_id, item_code, store_id))
        listing_id = cur.fetchone()[0]

        # Step 2: Always insert a new price entry into the `prices` table to track history
        price_query = """
        INSERT INTO prices (listingid, price, priceupdatetimestamp)
        VALUES (%s, %s, CURRENT_TIMESTAMP);
        """
        cur.execute(price_query, (listing_id, price))
        
        print(f"  - Updated listing for canonical_id {canonical_id} at store {store_id} with price {price}.")

    except psycopg2.Error as e:
        print(f"  ! DATABASE ERROR during insert for item {item_code} at store {store_id}: {e}")
        # The transaction in manager.py will handle the rollback.
        raise e