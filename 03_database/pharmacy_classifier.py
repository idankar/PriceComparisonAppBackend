import os
import re
import csv
import time
import json
import itertools
import collections
import psycopg2
from openai import OpenAI
from collections import deque

# --- CONFIGURATION ---

# 1. Database Connection Details
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DATABASE = os.environ.get("PG_DATABASE", "price_comparison_app")
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "025655358")

# 2. OpenAI API Key
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "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")
LLM_MODEL = "gpt-4o-mini" # Recommended for cost efficiency for this classification task

# 3. Logic Thresholds
# Jaccard threshold for initial grouping of very obvious duplicates *before* LLM classification
JACCARD_THRESHOLD_FOR_CLASSIFICATION_GROUPING = 0.90 # High threshold for very similar names

# --- INITIAL FILTER KEYWORDS (NEW) ---
# Keywords to filter for pharmacy products at the very beginning (case-insensitive)
# This list will reduce the total number of products fetched for classification.
PHARMACY_KEYWORDS_FOR_INITIAL_FILTER = {
    'שמפו', 'מרכך', 'קרם', 'משחה', 'סבון', 'דאודורנט', 'לק', 'בושם', 'ויטמין', 'תוסף',
    'חיתולים', 'מגבונים', 'תינוק', 'היגיינה', 'קוסמטיקה', 'טיפוח', 'שיניים', 'שיער',
    'פנים', 'גוף', 'SPF', 'תרופה', 'כדורים', 'קפסולות', 'טבליות', 'סירופ', 'טיפות',
    'עור', 'ציפורניים', 'אלרגיה', 'כאב', 'חום', 'צינון', 'שיעול', 'נזלת', 'דלקת',
    'פצעים', 'כוויות', 'עקיצות', 'אקנה', 'יובש', 'רגיש', 'שומני', 'אנטי אייג\'ינג',
    'הגנה מהשמש', 'סבון אינטימי', 'מדחום', 'פלסטר', 'תחבושת', 'גזה', 'כפפות', 'מזרק',
    'אלכוג\'ל', 'מסכה', 'אמפולה', 'צבע לשיער', 'קונסילר', 'רימל', 'שפתון', 'עיפרון',
    'צללית', 'מסקרה', 'אופטלגין', 'נורופן', 'אקמול', 'אדויל', 'דקסמול', 'סינופד',
    'ויקס', 'קלגרון', 'קרסטור', 'אגיסטן', 'ביו-אויל', 'דרמפון', 'הידרופיל',
    'טבע דרם', 'לחותית', 'מייק אפ', 'מסיר איפור', 'פילינג', 'צבע לשיער', 'קונסילר',
    'רימל', 'שפתון', 'עיפרון', 'צללית', 'מסקרה', 'לק', 'ציפורניים', 'מגבונים',
    'חיתולים', 'תחבושות', 'פדים', 'טמפונים', 'סבון אינטימי', 'כפפות', 'מסכות',
    'אלכוג\'ל', 'מגבונים אנטיבקטריאליים', 'מדחום', 'פלסטר', 'תחבושת', 'גזה', 'כפפות',
    'מזרק', 'תרופות', 'מרשם', 'ללא מרשם', 'קפסולות', 'טבליות', 'סירופ', 'טיפות',
    'משחה', "ג'ל", 'תרסיס', 'שמן', 'אבקה', 'נוזל', 'תמיסה', 'אמפולה', 'לק', 'ציפורניים', 'תרחיץ', 'דאודורנט', 'בושם', 'ויטמין', 'תוסף', 'כדורים', 'קפסולות', 'טבליות', 'סירופ', 'טיפות', 'כמוסות', 'כפפות', 'מסכות', 'אלכוג\'ל', 'מגבונים אנטיבקטריאליים', 'מדחום', 'פלסטר', 'תחבושת', 'גזה', 'כפפות', 

    # Common brand names that are almost exclusively pharmacy/H&B
    'לייף', 'סופר-פארם', 'בי', 'גוד פארם', 'וישי', 'לה רוש פוזה', 'אווה מילר', 'סרווה',
    'אוטריוין', 'סטרימר', 'ביודרמה', 'א-דרמה', 'בייבי פסטה', 'בלנאום', 'כצט', 'פרידום',
    'פמינה', 'קרסטס', 'לנקום', 'קליניק', 'אסתי לאודר', 'מאק', 'בובי בראון', 'קיקו',
    'ללין', 'אוליי', 'אלפא', 'בננה בוט', 'גרנייה', 'הימלאיה', 'וולדה', 'טבע נאות',
    'כמיקלים', 'לבידו', 'מדאמול', 'נובימול', 'פרופיל', 'קמיל בלו', 'רמדי', 'שמן טוב',
    'תפוח', 'ארומה', 'בייבי אויל', 'גליצרין', 'חומצה היאלורונית', 'קולגן', 'רטינול',
    'אומגה 3', 'פרוביוטיקה', 'כורכום', 'מגנזיום', 'ברזל', 'סידן', 'אבץ', 'מולטי ויטמין'
}


# 4. Progress and Output Files
PROGRESS_FILE = "pharmacy_classification_verdicts.json"
OUTPUT_CSV_FILE = "pharmacy_only_masterproductids.csv"

# 5. Execution Mode
SAMPLE_MODE = False # Set to True for initial testing
SAMPLE_SIZE = 500 # Number of products to classify in sample mode

# --- HELPER FUNCTIONS ---

def extract_product_features(name: str) -> dict:
    """
    Extracts tokens and original name from a product name for Jaccard comparison.
    Simplified as full feature extraction isn't needed for this initial grouping.
    """
    if not isinstance(name, str):
        return {"tokens": set(), "original_name": ""}
    
    text_part = name.lower()
    # Remove numbers and special characters to get core textual tokens
    text_part = re.sub(r'[\d%]+', ' ', text_part)
    text_part = re.sub(r'[\\/!"#$%&\'()*+,-./:;<=>?@\[\]^_`{|}~]', ' ', text_part)
    
    # Define common stop words (Hebrew and English)
    stop_words = {
        'בטעם', 'אריזת', 'מארז', 'מבצע', 'ביחידה', 'יחידות', 'של', 'עם', 'על', 'ל', 'ב', 'ו', 'או', 'את',
        'the', 'a', 'an', 'for', 'with', 'in', 'on', 'of', 'and', 'or', 'is', 'are', 'to', 'from', 'by'
    }
    
    # Tokenize and filter out stop words and single-character tokens
    tokens = {word for word in text_part.split() if word not in stop_words and len(word) > 1}
    
    return {"tokens": tokens, "original_name": name}

def calculate_jaccard_similarity(set1: set, set2: set) -> float:
    """Calculates the Jaccard similarity between two sets."""
    if not set1 and not set2: return 1.0
    if not set1 or not set2: return 0.0
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return intersection / union

def fetch_all_products_unfiltered(conn):
    """
    Fetches masterproductid and productname from the database, but only for products
    listed by specific retailer IDs (150, 97, 52).
    """
    print("Fetching products from specific retailers (IDs: 150, 97, 52)...")
    
    query = """
        SELECT DISTINCT
            p.masterproductid,
            p.productname
        FROM
            products p
        JOIN
            retailerproductlistings rpl ON p.masterproductid = rpl.masterproductid
        WHERE
            rpl.retailerid IN (150, 97, 52);
    """
    
    with conn.cursor() as cur:
        cur.execute(query)
        products = cur.fetchall()
        print(f"  > Found {len(products)} unique products across the specified retailers.")
        return products

def create_token_index(all_features: dict) -> dict:
    """Creates an inverted index mapping tokens to product IDs."""
    token_index = collections.defaultdict(list)
    for pid, features in all_features.items():
        for token in features['tokens']:
            token_index[token].append(pid)
    return token_index

def generate_candidate_pairs_for_grouping(token_index: dict) -> set:
    """
    Generates candidate pairs for initial Jaccard grouping.
    Uses a very wide blocking heuristic as we want to capture all obvious duplicates.
    """
    candidate_pairs = set()
    for token in token_index:
        pids = token_index[token]
        # Use a larger upper limit for blocking to find more initial groups
        if len(pids) > 1 and len(pids) < 300: # Increased upper limit
            for pair in itertools.combinations(pids, 2):
                candidate_pairs.add(tuple(sorted(pair)))
    return candidate_pairs

def build_initial_jaccard_groups(all_products_raw: list) -> dict:
    """
    Performs an initial Jaccard-based grouping to reduce the number of unique product names
    sent to the LLM for classification. Returns a mapping of representative PID to a list of PIDs in its group.
    """
    print("Performing initial Jaccard-based grouping to reduce LLM calls...")
    
    # Map PID to product name and features
    product_name_map = {pid: pname for pid, pname in all_products_raw}
    all_features = {pid: extract_product_features(pname) for pid, pname in all_products_raw}
    
    token_index = create_token_index(all_features)
    candidate_pairs = generate_candidate_pairs_for_grouping(token_index)

    # Build a graph of highly similar products
    adj = collections.defaultdict(list)
    for p1_id, p2_id in candidate_pairs:
        score = calculate_jaccard_similarity(all_features[p1_id]['tokens'], all_features[p2_id]['tokens'])
        if score >= JACCARD_THRESHOLD_FOR_CLASSIFICATION_GROUPING:
            adj[p1_id].append(p2_id)
            adj[p2_id].append(p1_id)

    # Find connected components (groups)
    visited = set()
    grouped_pids = {} # Maps each PID to its representative PID
    
    for pid in all_features: # Iterate through all products, even those without direct matches
        if pid not in visited:
            current_group = []
            q = deque([pid])
            visited.add(pid)
            
            # Find all connected products
            while q:
                current = q.popleft()
                current_group.append(current)
                for neighbor in adj[current]:
                    if neighbor not in visited:
                        visited.add(neighbor)
                        q.append(neighbor)
            
            # Choose a representative for the group (e.g., the smallest PID)
            representative_pid = min(current_group)
            for member_pid in current_group:
                grouped_pids[member_pid] = representative_pid
    
    # Create a list of unique representatives to send to LLM
    unique_representatives = sorted(list(set(grouped_pids.values())))
    print(f"  > Initial grouping reduced {len(all_products_raw)} products to {len(unique_representatives)} unique entities for LLM classification.")
    
    # Return a map of representative PID to its name, and the full grouped_pids map
    return {rep_pid: product_name_map[rep_pid] for rep_pid in unique_representatives}, grouped_pids

def get_llm_classification_verdict(product_name: str, client: OpenAI) -> dict:
    """
    Calls the LLM to classify if a product is a pharmacy-related item.
    Returns a dictionary with 'is_pharmacy_product' (boolean) and 'reason' (string).
    Includes retry logic.
    """
    prompt = f"""
You are an expert in Israeli pharmacy product classification. Your task is to determine if a given product is typically sold in a pharmacy (like Super-Pharm, Be, or GoodPharm) as a health, beauty, baby, hygiene, or over-the-counter medication item.

Examples of Pharmacy Products: Shampoo, toothpaste, vitamins, diapers, sunscreen, pain relievers, makeup, skin creams, baby formula, disinfectants, contact lens solution.
Examples of Non-Pharmacy Products: Fresh salmon, bread, milk, soft drinks, general groceries, electronics, clothing, stationery, hardware.

Respond ONLY with a valid JSON object with the following structure:
{{
  "is_pharmacy_product": boolean,
  "reason": "string"
}}
- is_pharmacy_product: true if it's a pharmacy-related item, false otherwise.
- reason: Briefly explain your reasoning.

Product Name: "{product_name}"
"""
    max_retries = 3
    delay = 2 # seconds
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=LLM_MODEL,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                temperature=0.0,
                max_tokens=100
            )
            verdict_data = json.loads(response.choices[0].message.content)
            if "is_pharmacy_product" in verdict_data and isinstance(verdict_data["is_pharmacy_product"], bool):
                return verdict_data
            else:
                print(f"  ! LLM returned invalid JSON structure (Attempt {attempt + 1}/{max_retries}): {verdict_data}")
                raise ValueError("Invalid LLM response structure")
        except (json.JSONDecodeError, ValueError) as e:
            print(f"  ! LLM response parsing failed (Attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay *= 2
            else:
                return {"is_pharmacy_product": False, "reason": f"LLM_RESPONSE_PARSE_ERROR: {e}"}
        except Exception as e:
            print(f"  ! LLM API call failed (Attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay *= 2
            else:
                return {"is_pharmacy_product": False, "reason": f"LLM_API_ERROR: {e}"}

def load_progress(filename: str) -> dict:
    """Loads previously saved LLM classification verdicts from a JSON file."""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            print(f"Loading existing progress from '{filename}'...")
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        print("No valid progress file found. Starting fresh.")
        return {}

def save_progress(filename: str, verdicts: dict):
    """Saves the LLM classification verdicts to a JSON file."""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(verdicts, f, indent=2, ensure_ascii=False)

# --- MAIN EXECUTION ---

def main():
    print("--- Starting Pharmacy Product Classification Script ---")
    
    conn = None
    try:
        conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD)
        all_products_raw = fetch_all_products_unfiltered(conn) # Now fetches filtered products
    except psycopg2.Error as e:
        print(f"FATAL: Database connection failed: {e}"); return
    finally:
        if conn: conn.close()
    
    # Step 1: Perform initial Jaccard grouping to reduce unique product names for LLM
    representative_names_map, grouped_pids_map = build_initial_jaccard_groups(all_products_raw)
    
    # Step 2: Classify unique product representatives using LLM
    print("Classifying unique product representatives using LLM...")
    
    verdicts = load_progress(PROGRESS_FILE)
    
    # Prepare list of representatives to process (original PID, original name)
    representatives_to_process = []
    for rep_pid, rep_name in representative_names_map.items():
        # Use a unique key for the verdict based on the representative PID
        if str(rep_pid) not in verdicts: # Check if this representative has already been classified
            representatives_to_process.append((rep_pid, rep_name))

    if SAMPLE_MODE:
        print(f"--- RUNNING IN SAMPLE MODE: PROCESSING {SAMPLE_SIZE} REPRESENTATIVES ---")
        representatives_to_process = representatives_to_process[:SAMPLE_SIZE]
        
    if not representatives_to_process:
        print("All relevant product representatives already classified. Script finished.")
        # If all classified, proceed to write the output file from existing verdicts
        write_pharmacy_products_csv(all_products_raw, grouped_pids_map, verdicts, OUTPUT_CSV_FILE)
        return

    if OPENAI_API_KEY == "YOUR_OPENAI_API_KEY_HERE":
        print("WARNING: OpenAI API key is not set. LLM classification will be skipped.")
        llm_client = None
    else:
        llm_client = OpenAI(api_key=OPENAI_API_KEY)
        
    if llm_client:
        try:
            for i, (rep_pid, rep_name) in enumerate(representatives_to_process):
                # The key for the verdict is the representative PID
                verdict_key = str(rep_pid) 
                
                print(f"  - LLM Classify [{i+1}/{len(representatives_to_process)}]: '{rep_name}' (Rep ID: {rep_pid})")
                verdict_data = get_llm_classification_verdict(rep_name, llm_client)
                verdicts[verdict_key] = verdict_data
                print(f"    > AI Verdict: Is Pharmacy: {verdict_data.get('is_pharmacy_product')}, Reason: {verdict_data.get('reason')}")
                
                # Save progress periodically
                if (i + 1) % 20 == 0: # Save every 20 LLM calls
                    print("  ... saving classification progress ...")
                    save_progress(PROGRESS_FILE, verdicts)
        except KeyboardInterrupt:
            print("\n! KeyboardInterrupt detected. Saving classification progress before exiting.")
        finally:
            print("... saving final classification progress ...")
            save_progress(PROGRESS_FILE, verdicts)
    else:
        print("LLM client not initialized. Skipping LLM classification.")
        # If LLM is skipped, no classification verdicts are generated, so the output CSV will be empty or incomplete.
        print("Script finished without full classification. No output CSV generated.")
        return # Exit if LLM is skipped, as output won't be meaningful.

    # Step 3: Write the final CSV with only pharmacy products
    write_pharmacy_products_csv(all_products_raw, grouped_pids_map, verdicts, OUTPUT_CSV_FILE)

    print("--- Script Finished ---")
    print(f"Review the classified pharmacy products in '{OUTPUT_CSV_FILE}'.")

def write_pharmacy_products_csv(all_products_raw: list, grouped_pids_map: dict, verdicts: dict, output_filename: str):
    """
    Writes a CSV file containing only masterproductids and names
    that were classified as pharmacy products.
    """
    print(f"Writing classified pharmacy products to '{output_filename}'...")
    
    pharmacy_products_for_output = []
    product_name_lookup = {pid: pname for pid, pname in all_products_raw}

    for original_pid, representative_pid in grouped_pids_map.items():
        verdict_data = verdicts.get(str(representative_pid)) # Get verdict for the representative
        
        if verdict_data and verdict_data.get("is_pharmacy_product"):
            pharmacy_products_for_output.append({
                'masterproductid': original_pid,
                'productname': product_name_lookup.get(original_pid, "Unknown Product")
            })
    
    # Sort by masterproductid for consistent output
    pharmacy_products_for_output.sort(key=lambda x: x['masterproductid'])

    with open(output_filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['masterproductid', 'productname'])
        for item in pharmacy_products_for_output:
            writer.writerow([item['masterproductid'], item['productname']])
            
    print(f"  > Wrote {len(pharmacy_products_for_output)} pharmacy products to '{output_filename}'.")

if __name__ == "__main__":
    main()
