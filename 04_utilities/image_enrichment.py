import os
import psycopg2
import json
import csv
import re
from thefuzz import process
from openai import OpenAI
from dotenv import load_dotenv
import sys

# Load environment variables from .env file if it exists
load_dotenv()

# --- Configuration ---
# OpenAI Configuration
api_key = os.environ.get("OPENAI_API_KEY")
if not api_key:
    print("[ERROR] OPENAI_API_KEY environment variable not set.")
    print("Please set it using: export OPENAI_API_KEY='your-api-key'")
    print("Or create a .env file with: OPENAI_API_KEY=your-api-key")
    sys.exit(1)

# Initialize OpenAI client with the new API
client = OpenAI(api_key=api_key)

# Database Configuration
PG_HOST = os.environ.get("DB_HOST", "localhost")
PG_PORT = os.environ.get("DB_PORT", "5432")
PG_DATABASE = os.environ.get("DB_NAME", "price_comparison_app_v2")
PG_USER = os.environ.get("DB_USER", "postgres")
PG_PASSWORD = os.environ.get("DB_PASSWORD", "025655358")

# --- File Paths ---
# Update this path to your actual JSONL file location
SUPERPHARM_DATA_PATH = os.environ.get(
    "SUPERPHARM_DATA_PATH", 
    "/Users/noa/Desktop/PriceComparisonApp/superpharm_products_enriched.jsonl"
)
OUTPUT_CSV_PATH = "enrichment_review.csv"

# --- Functions ---
def get_db_connection():
    """Establishes and returns a database connection."""
    try:
        conn = psycopg2.connect(
            host=PG_HOST, 
            port=PG_PORT, 
            database=PG_DATABASE, 
            user=PG_USER, 
            password=PG_PASSWORD
        )
        print(f"✓ Connected to database: {PG_DATABASE}")
        return conn
    except psycopg2.Error as e:
        print(f"[ERROR] Failed to connect to database: {e}")
        raise

def get_products_to_enrich(cursor):
    """Fetches products from the database that are missing an image_url."""
    query = """
        SELECT product_id, canonical_name, brand 
        FROM products 
        WHERE image_url IS NULL OR image_url = ''
        ORDER BY product_id;
    """
    cursor.execute(query)
    return cursor.fetchall()

def load_superpharm_data(file_path):
    """
    Loads the Super-Pharm JSONL data and creates lookup maps for efficient matching.
    """
    print(f"-> Loading Super-Pharm data from '{file_path}'...")
    products_map = {}
    product_names = []
    # A map for fast, case-insensitive direct matching
    lower_to_original_name_map = {}
    
    if not os.path.exists(file_path):
        print(f"[ERROR] Super-Pharm data file not found at '{file_path}'")
        print(f"Current working directory: {os.getcwd()}")
        return None, None, None
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            line_count = 0
            for line in f:
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    product = json.loads(line)
                    product_name = product.get('name', '')
                    
                    if product_name:
                        products_map[product_name] = product
                        product_names.append(product_name)
                        lower_to_original_name_map[product_name.strip().lower()] = product_name
                        line_count += 1
                except json.JSONDecodeError as e:
                    print(f"  -> Warning: Skipping invalid JSON on line {line_count + 1}: {e}")
                    
        print(f"✓ Successfully loaded {len(products_map)} products from Super-Pharm data.")
        return products_map, product_names, lower_to_original_name_map
    except Exception as e:
        print(f"[ERROR] Failed to load Super-Pharm data: {e}")
        return None, None, None

def clean_product_name(name):
    """Remove size/quantity information from product name for better matching."""
    # Enhanced patterns to match various size formats
    patterns = [
        # Basic units with optional quotes
        r'\s+\d+(\.\d+)?\s?(מ"ל|מל|גרם|ג"ר|גר|ליטר|ל|ק"ג|קג|יח|יחידות|מ"ג|מג)\'?\'?$',
        # English units
        r'\s+\d+(\.\d+)?\s?(ml|ML|g|G|kg|KG|l|L|units?|pcs?)$',
        # Multi-pack formats (e.g., "3x100ml", "2X50g")
        r'\s+\d+\s*[xX×]\s*\d+(\.\d+)?\s*(מ"ל|מל|גרם|ג|ml|ML|g|G)$',
        # Percentage at the end (e.g., "קרם לחות 50%")
        r'\s+\d+(\.\d+)?%$',
    ]
    
    cleaned = name
    for pattern in patterns:
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
    
    return cleaned.strip()

def get_best_match_from_llm(db_product_name, candidates):
    """
    Asks the LLM to choose the best match from a list of candidates.
    Using the new OpenAI client API.
    """
    candidate_list_str = "\n".join([f"{i+1}. {name}" for i, name in enumerate(candidates)])
    
    prompt = f"""You are a product matching expert. Your task is to identify if any of the candidates match the given product.

Product from database: "{db_product_name}"

Candidate products:
{candidate_list_str}

Which candidate number (1-{len(candidates)}) is the exact same product? 
Consider variations in naming, abbreviations, and formatting.
Respond with ONLY the number (1-{len(candidates)}) or "none" if no match exists."""

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a product matching assistant. Be precise and consider product names may have slight variations."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.0,
            max_tokens=10
        )
        
        answer = response.choices[0].message.content.strip().lower()
        
        if answer == "none":
            return None
            
        # Try to extract number from response
        try:
            idx = int(answer) - 1
            if 0 <= idx < len(candidates):
                return candidates[idx]
        except ValueError:
            # If response contains a number, try to extract it
            import re
            numbers = re.findall(r'\d+', answer)
            if numbers:
                idx = int(numbers[0]) - 1
                if 0 <= idx < len(candidates):
                    return candidates[idx]
                    
        return None
        
    except Exception as e:
        print(f"  -> ERROR: OpenAI API call failed: {e}")
        return None

def print_configuration():
    """Print current configuration for verification."""
    print("\n--- Configuration ---")
    print(f"Database: {PG_USER}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}")
    print(f"Super-Pharm Data: {SUPERPHARM_DATA_PATH}")
    print(f"Output CSV: {OUTPUT_CSV_PATH}")
    print(f"OpenAI API Key: {'✓ Set' if api_key else '✗ Not Set'}")
    print("-------------------\n")

# --- Main Execution ---
def main():
    print("--- 🖼️ Starting Product Name and Image Enrichment Process ---")
    
    # Print configuration for verification
    print_configuration()
    
    # Step 1: Load external data
    sp_products, sp_product_names, sp_lower_map = load_superpharm_data(SUPERPHARM_DATA_PATH)
    if not sp_products:
        print("[ERROR] Failed to load Super-Pharm data. Exiting.")
        return

    # Statistics tracking
    stats = {
        'total': 0,
        'direct_matches': 0,
        'llm_matches': 0,
        'no_matches': 0,
        'missing_images': 0
    }

    conn = None
    try:
        # Step 2: Connect to database
        conn = get_db_connection()
        with conn.cursor() as cursor:
            products_to_process = get_products_to_enrich(cursor)
            
            if not products_to_process:
                print("✅ All products already have an image URL. Nothing to do.")
                return

            stats['total'] = len(products_to_process)
            print(f"-> Found {stats['total']} products to enrich.\n")

            # Step 3: Prepare the output CSV file
            with open(OUTPUT_CSV_PATH, 'w', newline='', encoding='utf-8-sig') as csvfile:
                csv_writer = csv.writer(csvfile)
                csv_writer.writerow([
                    'product_id', 
                    'current_name', 
                    'brand',
                    'proposed_name', 
                    'proposed_image_url', 
                    'match_source',
                    'confidence_score'
                ])

                # Step 4: The Matching Loop
                for i, (product_id, name, brand) in enumerate(products_to_process):
                    print(f"[{i+1}/{stats['total']}] Processing: '{name}'")
                    
                    best_match_name = None
                    match_source = ""
                    confidence_score = 0

                    # Layer 1: Direct Match with cleaned name
                    cleaned_name = clean_product_name(name)
                    db_name_lower = cleaned_name.lower()

                    if db_name_lower in sp_lower_map:
                        best_match_name = sp_lower_map[db_name_lower]
                        match_source = "Direct Match"
                        confidence_score = 100
                        stats['direct_matches'] += 1
                        print(f"  ✓ Direct Match: '{best_match_name}'")
                    else:
                        # Layer 2: Fuzzy Match + LLM
                        # Get fuzzy match candidates
                        fuzzy_candidates = process.extract(name, sp_product_names, limit=5)
                        candidate_names = [c[0] for c in fuzzy_candidates]
                        # Store the fuzzy scores for later use
                        fuzzy_scores = {c[0]: c[1] for c in fuzzy_candidates}

                        # Ask LLM to verify the best match
                        best_match_name = get_best_match_from_llm(name, candidate_names)
                        
                        if best_match_name:
                            match_source = "LLM Match"
                            confidence_score = fuzzy_scores.get(best_match_name, 0)
                            stats['llm_matches'] += 1
                            print(f"  ✓ LLM Match: '{best_match_name}' (confidence: {confidence_score})")
                        else:
                            match_source = "No Match Found"
                            stats['no_matches'] += 1
                            print(f"  ✗ No match found")

                    # Process the result
                    proposed_name = ""
                    proposed_image_url = ""

                    if best_match_name and best_match_name in sp_products:
                        matched_product = sp_products[best_match_name]
                        proposed_name = matched_product.get('name', '')
                        proposed_image_url = matched_product.get('imageUrl', '')
                        
                        # Check if image is missing
                        if not proposed_image_url:
                            stats['missing_images'] += 1
                            match_source += " (Missing Image)"
                            print(f"  ⚠ Warning: Matched product has no image URL")
                    
                    # Write to CSV
                    csv_writer.writerow([
                        product_id, 
                        name, 
                        brand or '',
                        proposed_name, 
                        proposed_image_url, 
                        match_source,
                        confidence_score
                    ])

            # Print summary statistics
            print(f"\n--- ✅ Enrichment Complete ---")
            print(f"Total products processed: {stats['total']}")
            print(f"Direct matches: {stats['direct_matches']} ({stats['direct_matches']/stats['total']*100:.1f}%)")
            print(f"LLM matches: {stats['llm_matches']} ({stats['llm_matches']/stats['total']*100:.1f}%)")
            print(f"No matches: {stats['no_matches']} ({stats['no_matches']/stats['total']*100:.1f}%)")
            print(f"Missing images: {stats['missing_images']}")
            print(f"\nEstimated API cost: ${stats['llm_matches'] * 0.001:.2f}")
            print(f"\nResults saved to: '{OUTPUT_CSV_PATH}'")

    except KeyboardInterrupt:
        print("\n\n[INFO] Process interrupted by user. Partial results may have been saved.")
    except Exception as e:
        print(f"\n[ERROR] An error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
            print("\n✓ Database connection closed.")

if __name__ == "__main__":
    main()