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
PG_PASSWORD = os.environ.get("PG_PASSWORD", "***REMOVED***") # <-- IMPORTANT: Set your password

# 2. OpenAI API Key
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "***REMOVED***-TSybPJY6Yt9JIJ4k066J06XvV_Vz1E0QasT8jEEx6tZw70bg9RRMZQ-3oBBSjT3BlbkFJ2sUCNgLmgep2y2wrGb39IeJsJiVeEyLqiI_ufaK30DByYW6hkcyDdCx-Gsa0W63EmLZmy-bI4A") # <-- IMPORTANT: Set your API key
LLM_MODEL = "gpt-4o"

# 3. Logic Thresholds & Keywords
JACCARD_THRESHOLD_FOR_LLM = 0.75 # We can be slightly more lenient as auto-yes is gone
JACCARD_FOR_MISSING_SIZE_CASE = 0.90

# --- EXPANDED KEYWORD LISTS ---
DEAL_BREAKER_KEYWORDS = {
    'אורגני', 'דיאט', 'לייט', 'ללא סוכר', 'טבעוני', 'צמחוני', 'ללא גלוטן', 'לל"ס',
    'קפוא', 'טרי', 'מעושן', 'מיובש', 'מצונן', 'מבושל', 'חי',
    'חריף', 'עדין', 'פיקנטי', 'מתוק', 'מלוח', 'מתוקה', 'מלוחה', 'חמוץ', 'מריר',
    'גברים', 'נשים', 'ילדים', 'תינוקות', 'בנים', 'בנות',
    'יום', 'לילה', 'בוקר', 'ערב',
    'שמפו', 'מרכך', 'מסיכה', 'סרום', 'קרם', "ג'ל", 'תרסיס', 'שמן', 'אבקה', 'נוזל',
    'כדורים', 'קפסולות', 'טבליות', 'סירופ', 'טיפות', 'משחה',
    'רגיל', 'פורטה', 'אקסטרה', 'קלאסי', 'מקצועי', 'לחות', 'הגנה', 'טיפוח', 'ניקוי',
    'קשיח', 'רך', 'מתנפח', 'חשמלי', 'ידני', 'חד פעמי', 'רב פעמי',
    'פרוס', 'שלם', 'טחון', 'גוש', 'קצוץ', 'חתוך', 'מגורד',
    'אנטי אייג\'ינג', 'הבהרה', 'הגנה מהשמש', 'אקנה', 'יובש', 'רגיש', 'שומני', 'מעורב', 'רגיל',
}

CONFLICT_PAIRS = [
    {'שמפו', 'מרכך'}, {'שמפו', 'מסיכה'}, {'שמפו', 'סבון'}, {'שמפו', "ג'ל רחצה"},
    {'קרם יום', 'קרם לילה'}, {'סרום', 'קרם'}, {'קרם', "ג'ל"}, {'תרסיס', 'קרם'},
    {'אדפ', 'אדט'}, {'בושם', 'דאודורנט'},
    {'נוזל', 'אבקה'}, {'טבליות', 'אבקה'}, {'כדורים', 'קפסולות'},
    {'עשיר', 'קליל'}, {'עשיר', 'רגיל'}, {'קליל', 'רגיל'},
    {'לילה', 'יום'},
    {'מברשת שיניים', 'משחת שיניים'}, {'מגבונים', 'חיתולים'},
    {'גברים', 'נשים'}, {'בנים', 'בנות'}, {'תינוקות', 'ילדים'},
    {'עור יבש', 'עור שומני'}, {'עור שומני', 'עור רגיש'},
    {'ויטמין C', 'ויטמין D'}, {'ברזל', 'סידן'},
    {'ק', 'מ'}, {'י', 'ש'}, {'ח', 'ל'},
]

# 4. Progress and Output Files
PROGRESS_FILE = "llm_verdicts_pharmacy_v5.json"
OUTPUT_CSV_FILE = "duplicate_groups_pharmacy_v5.csv"
PHARMACY_PRODUCTS_CSV = "pharmacy_only_masterproductids.csv"

# 5. Execution Mode
SAMPLE_MODE = False
SAMPLE_SIZE = 5000

# --- HELPER FUNCTIONS ---

# --- MODIFIED: V5 - "Attribute-First" feature extraction ---
def extract_product_features(name: str) -> dict:
    if not isinstance(name, str):
        return {"tokens": set(), "amount": None, "unit": None, "spf": None, "variant_codes": set(), "original_name": ""}
    
    text_part = name.lower()
    features = {"amount": None, "unit": None, "spf": None, "variant_codes": set()}

    # 1. Extract SPF
    spf_match = re.search(r'spf\s*(\d+)', text_part)
    if spf_match:
        features["spf"] = int(spf_match.group(1))
        text_part = text_part.replace(spf_match.group(0), '')

    # 2. Extract multi-packs
    multipack_match = re.search(r'(\d+)\s*\*\s*(\d+)', text_part)
    if multipack_match:
        features["amount"] = int(multipack_match.group(1)) * int(multipack_match.group(2))
        features["unit"] = 'units'
        text_part = text_part.replace(multipack_match.group(0), '')
    
    # 3. Extract standard amounts and units
    if features["amount"] is None:
        unit_pattern = r'(\d+(?:\.\d+)?)\s*(g|gr|gm|kg|ml|l|ליטר|גרם|גר|קג|מל|מ"ל|מ|יח|יחידות|קפסולות|טבליות|מגבונים)\b'
        unit_match = re.search(unit_pattern, text_part, re.IGNORECASE)
        if unit_match:
            features["amount"] = float(unit_match.group(1))
            text_part = text_part.replace(unit_match.group(0), '') # Remove the size from text

    # 4. NEW: Extract all remaining numbers as potential variant/shade codes
    variant_codes = re.findall(r'\d+', text_part)
    features["variant_codes"] = {int(code) for code in variant_codes}
    
    # 5. Clean and tokenize the remaining text
    text_part = re.sub(r'[\d%]+', ' ', text_part) # Now we can strip all numbers
    text_part = re.sub(r'[\\/!"#$%&\'()*+,-./:;<=>?@\[\]^_`{|}~]', ' ', text_part)
    stop_words = {'בטעם', 'אריזת', 'מארז', 'מבצע', 'של', 'עם', 'ל', 'ב', 'ו', 'the', 'a', 'an', 'for', 'with'}
    tokens = {word for word in text_part.split() if word not in stop_words and len(word) >= 1}
    
    features["tokens"] = tokens
    features["original_name"] = name
    return features

def calculate_jaccard_similarity(set1: set, set2: set) -> float:
    if not set1 and not set2: return 1.0
    if not set1 or not set2: return 0.0
    return len(set1.intersection(set2)) / len(set1.union(set2))

# --- MODIFIED: V5 - "Attribute-First" filtering logic ---
def apply_automatic_filters(p1_features: dict, p2_features: dict) -> str or None:
    # PRIORITY 0: SPF MISMATCH
    p1_spf, p2_spf = p1_features.get('spf'), p2_features.get('spf')
    if (p1_spf is not None or p2_spf is not None) and p1_spf != p2_spf:
        return "NO"

    # PRIORITY 1: VARIANT CODE MISMATCH - new and critical
    p1_codes = p1_features.get('variant_codes', set())
    p2_codes = p2_features.get('variant_codes', set())
    if (p1_codes or p2_codes) and p1_codes != p2_codes:
        return "NO"

    # PRIORITY 2: STRICT AMOUNT/UNIT MISMATCH
    p1_amount, p2_amount = p1_features.get('amount'), p2_features.get('amount')
    p1_unit, p2_unit = p1_features.get('unit'), p2_features.get('unit')
    if (p1_amount is not None or p2_amount is not None) and (p1_amount != p2_amount or p1_unit != p2_unit):
        # A bit lenient if one is None, will be caught later
        if p1_amount is not None and p2_amount is not None:
             return "NO"

    p1_tokens, p2_tokens = p1_features['tokens'], p2_features['tokens']
    
    # PRIORITY 3: CONFLICT & DEAL_BREAKER KEYWORDS
    for conflict_set in CONFLICT_PAIRS:
        if p1_tokens.intersection(conflict_set) and p2_tokens.intersection(conflict_set) and p1_tokens.intersection(conflict_set) != p2_tokens.intersection(conflict_set):
            return "NO"
    if p1_tokens.intersection(DEAL_BREAKER_KEYWORDS) != p2_tokens.intersection(DEAL_BREAKER_KEYWORDS):
        return "NO"

    # PRIORITY 4: JACCARD SCORE - Only used for rejection or passing to LLM
    score = calculate_jaccard_similarity(p1_tokens, p2_tokens)
    if score < JACCARD_THRESHOLD_FOR_LLM:
        return "NO"

    # --- MAJOR CHANGE: NO MORE AUTOMATIC "YES" ---
    # If a pair survives all the "NO" filters, it MUST be sent to the LLM for a verdict.
    return None

def fetch_all_products(conn):
    print(f"Loading pharmacy products from '{os.path.basename(PHARMACY_PRODUCTS_CSV)}'...")
    products = []
    try:
        with open(PHARMACY_PRODUCTS_CSV, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                if len(row) >= 2:
                    products.append((int(row[0]), row[1]))
        print(f"  > Loaded {len(products)} pharmacy products from CSV.")
    except (FileNotFoundError, ValueError) as e:
        print(f"[CRITICAL] Error reading '{PHARMACY_PRODUCTS_CSV}': {e}")
        raise
    return products

def create_token_index(all_features: dict) -> dict:
    print("Creating token index for blocking...")
    token_index = collections.defaultdict(list)
    for pid, features in all_features.items():
        for token in features['tokens']:
            token_index[token].append(pid)
    print(f"  > Index created with {len(token_index)} unique tokens.")
    return token_index

def generate_candidate_pairs(token_index: dict) -> set:
    print("Generating candidate pairs for comparison...")
    candidate_pairs = set()
    for token, pids in token_index.items():
        if 1 < len(pids) < 150:
            for pair in itertools.combinations(pids, 2):
                candidate_pairs.add(tuple(sorted(pair)))
    print(f"  > Generated {len(candidate_pairs)} unique candidate pairs.")
    return candidate_pairs

def get_llm_verdict(p1_name: str, p2_name: str, client: OpenAI) -> dict:
    prompt = f"""
You are a data analyst expert for an Israeli supermarket database. Your task is to compare two product names and determine if they refer to the **EXACT SAME product**.
Pay close attention to all details, including:
- **Brand and Product Line:** Must be identical.
- **Product Type:** (e.g., "shampoo" vs "conditioner" are different).
- **Size/Weight/Quantity:** Must be identical.
- **SPF**: Must be identical if present.
- **Color/Shade/Variant Codes**: Must be identical.
- **Specific Formulation/Variant:** (e.g., "for dry hair" vs "for oily hair", "for men" vs "for women").

**Crucial Rules:**
1. If you find ANY specific attribute (size, SPF, color, shade, model number, formulation, scent, target audience) that differs, you must rule it as a non-match.
2. If one name specifies a concrete attribute (like size, shade, or quantity) and the other does not, they are NOT a match.

Respond ONLY with a valid JSON object:
{{
  "is_core_product_match": boolean,
  "match_reason": "string"
}}
- is_core_product_match: true ONLY if they are the exact same product.
- match_reason: Briefly explain your reasoning. If not a match, state the key difference.

Product A: "{p1_name}"
Product B: "{p2_name}"
"""
    max_retries = 3
    delay = 2
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=LLM_MODEL,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                temperature=0.0,
                max_tokens=150
            )
            verdict_data = json.loads(response.choices[0].message.content)
            if "is_core_product_match" in verdict_data and isinstance(verdict_data["is_core_product_match"], bool):
                return verdict_data
            else:
                raise ValueError("Invalid LLM response structure")
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay *= 2
            else:
                return {"is_core_product_match": False, "match_reason": f"LLM_ERROR: {e}"}

def load_progress(filename: str) -> dict:
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            print(f"Loading existing progress from '{filename}'...")
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        print(f"No valid progress file found ('{filename}'). Starting fresh.")
        return {}

def save_progress(filename: str, verdicts: dict):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(verdicts, f, indent=2, ensure_ascii=False)

def build_graph_and_find_clusters(all_loaded_pids: set, llm_verdicts: dict) -> list:
    print("Building graph and finding duplicate clusters...")
    adj = collections.defaultdict(list)
    
    # The graph is now built ONLY from LLM "YES" verdicts
    for pair_key, verdict_data in llm_verdicts.items():
        if verdict_data.get("is_core_product_match"):
            p1_id, p2_id = map(int, pair_key.split('-'))
            if p1_id in all_loaded_pids and p2_id in all_loaded_pids:
                adj[p1_id].append(p2_id)
                adj[p2_id].append(p1_id)

    visited = set()
    clusters = []
    pids_to_cluster = [pid for pid in adj.keys() if pid in all_loaded_pids]
    for pid in pids_to_cluster:
        if pid not in visited:
            cluster = []
            q = deque([pid])
            visited.add(pid)
            while q:
                current = q.popleft()
                cluster.append(current)
                # Any product that is identical to 'current' should also be in the cluster
                # Also add single-node "clusters" for products that had no matches
                for neighbor in adj[current]:
                    if neighbor not in visited and neighbor in all_loaded_pids:
                        visited.add(neighbor)
                        q.append(neighbor)
            if cluster:
                clusters.append(sorted(cluster))

    # Add all remaining un-clustered products as their own single-item groups
    all_clustered_pids = set(p for c in clusters for p in c)
    for pid in all_loaded_pids:
        if pid not in all_clustered_pids:
            clusters.append([pid])


    print(f"  > Found {len(clusters)} duplicate groups (including single-item groups).")
    return clusters

def main():
    print("--- Starting Pharmacy Product Consolidation Script (V5 - Attribute-First) ---")
    try:
        all_products_raw = fetch_all_products(None)
    except (FileNotFoundError, ValueError):
        return
    
    print("Extracting advanced features from all products...")
    all_features = {pid: extract_product_features(pname) for pid, pname in all_products_raw}
    product_name_map = {pid: pname for pid, pname in all_products_raw}
    all_loaded_pids_set = set(product_name_map.keys())
    
    token_index = create_token_index(all_features)
    candidate_pairs = generate_candidate_pairs(token_index)
    
    print("Classifying pairs with attribute-aware filters...")
    llm_candidates = []
    for p1_id, p2_id in candidate_pairs:
        if p1_id in all_loaded_pids_set and p2_id in all_loaded_pids_set:
            verdict = apply_automatic_filters(all_features[p1_id], all_features[p2_id])
            if verdict is None: # Only 'None' gets sent to LLM
                llm_candidates.append((p1_id, p2_id))
    print(f"  > Found {len(llm_candidates)} 'grey area' pairs to be verified by LLM.")
    
    verdicts = load_progress(PROGRESS_FILE)
    pairs_to_process = llm_candidates
    if SAMPLE_MODE:
        print(f"--- RUNNING IN SAMPLE MODE: PROCESSING UP TO {SAMPLE_SIZE} PAIRS ---")
        pairs_to_process = pairs_to_process[:SAMPLE_SIZE]
        
    if not OPENAI_API_KEY or OPENAI_API_KEY == "YOUR_OPENAI_API_KEY_HERE":
        print("WARNING: OpenAI API key is not set. LLM validation will be skipped.")
        llm_client = None
    else:
        llm_client = OpenAI(api_key=OPENAI_API_KEY)
        
    if llm_client:
        unprocessed_pairs = [p for p in pairs_to_process if f"{p[0]}-{p[1]}" not in verdicts]
        print(f"  > Total pairs for LLM: {len(pairs_to_process)}. Already processed: {len(pairs_to_process) - len(unprocessed_pairs)}. New pairs to process: {len(unprocessed_pairs)}")
        if not unprocessed_pairs:
            print("  > No new pairs to process with the LLM.")
        try:
            for i, (p1_id, p2_id) in enumerate(unprocessed_pairs):
                pair_key = f"{p1_id}-{p2_id}"
                print(f"  - LLM Check [{i+1}/{len(unprocessed_pairs)}]: {product_name_map[p1_id]} vs {product_name_map[p2_id]}")
                verdict_data = get_llm_verdict(product_name_map[p1_id], product_name_map[p2_id], llm_client)
                verdicts[pair_key] = verdict_data
                print(f"    > AI Analysis: Match: {verdict_data.get('is_core_product_match')}, Reason: {verdict_data.get('match_reason')}")
                if (i + 1) % 50 == 0:
                    print("  ... saving progress ...")
                    save_progress(PROGRESS_FILE, verdicts)
        except KeyboardInterrupt:
            print("\n! KeyboardInterrupt detected. Saving progress before exiting.")
        finally:
            print("... saving final progress ...")
            save_progress(PROGRESS_FILE, verdicts)
            
    # Note: build_graph_and_find_clusters now only takes llm_verdicts
    clusters = build_graph_and_find_clusters(all_loaded_pids_set, verdicts)
    print(f"Writing {len(clusters)} groups to {OUTPUT_CSV_FILE}...")
    with open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['group_id', 'masterproductid', 'productname'])
        group_id_counter = 1
        for cluster in clusters:
            # Only write groups with more than one item, as single items are not duplicates
            if len(cluster) > 1:
                for pid in cluster:
                    writer.writerow([group_id_counter, pid, product_name_map[pid]])
                group_id_counter += 1

    print(f"--- Script Finished ---")
    print(f"Wrote {group_id_counter - 1} duplicate groups to '{OUTPUT_CSV_FILE}'.")

if __name__ == "__main__":
    main()