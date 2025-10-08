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
LLM_MODEL = "gpt-4o"

# 3. Logic Thresholds & Keywords (MORE AGGRESSIVE)
JACCARD_THRESHOLD_FOR_SUBSET = 0.80 # Increased
JACCARD_THRESHOLD_FOR_LLM = 0.72  # *** SIGNIFICANTLY INCREASED THRESHOLD ***

# --- EXPANDED KEYWORD LISTS ---

# Keywords that, if present in one but not the other, might indicate a different product
DEAL_BREAKER_KEYWORDS = {
    'אורגני', 'דיאט', 'לייט', 'ללא סוכר', 'טבעוני', 'צמחוני', 'ללא גלוטן', 'לל"ס',
    'קפוא', 'טרי', 'מעושן', 'מיובש', 'מצונן', 'מבושל', 'חי',
    'חריף', 'עדין', 'פיקנטי', 'מתוק'
}

# Mutually exclusive keyword pairs
CONFLICT_PAIRS = [
    # Types
    {'שמפו', 'מרכך'}, {'שמפו', 'מסיכה'}, {'קרם יום', 'קרם לילה'}, {'סרום', 'קרם'},
    {'אדפ', 'אדט'}, {'בושם', 'דאודורנט'},
    {'נוזל', 'אבקה'}, {'נוזל', 'ג''ל'}, {'ג''ל', 'קרם'}, {'תרסיס', 'קרם'},
    # Flavors / Scents
    {'לימון', 'תפוז'}, {'לימון', 'לבנדר'}, {'תות', 'בננה'}, {'וניל', 'שוקולד'},
    {'עוף', 'בקר'}, {'פיצה', 'ברביקיו'}, {'אפרסק', 'מנגו'},
    # Food Types
    {'מרלו', 'קברנה'}, {'מרלו', 'שיראז'}, {'קברנה', 'שיראז'},
    {'אדום', 'לבן'}, {'יבש', 'חצי יבש'},
    {'קמח לבן', 'קמח מלא'}, {'חיטה', 'כוסמין'}, {'רגיל', 'מלא'},
    # Meats / Cuts
    {'כרעיים', 'כנפיים'}, {'חזה', 'שוק'}, {'בקר', 'עוף'}, {'בקר', 'טלה'},
    {'פילה', 'סטייק'}, {'טחון', 'קוביות'}, {'אסאדו', 'אנטריקוט'},
    # Tools
    {'כף', 'מזלג'}, {'כף', 'סכין'}, {'מזלג', 'סכין'},
    # Gender / Age
    {'בנים', 'בנות'}, {'גבר', 'אישה'}, {'גברים', 'נשים'}, {'תינוק', 'מבוגר'},
    # Materials / Forms
    {'טחון', 'שלם'}, {'פרוס', 'גוש'}, {'זכוכית', 'פלסטיק'}, {'קרטון', 'בקבוק'},
    {'כותנה', 'סינטטי'},
    # Colors
    {'שחור', 'לבן'}, {'אדום', 'כחול'}, {'ירוק', 'צהוב'}
]

# List of known brand names to help identify generic products
BRAND_KEYWORDS = {
    'שופרסל', 'רמי לוי', 'ויקטורי', 'סוגת', 'אסם', 'תנובה', 'שטראוס', 'עלית',
    'קוקה קולה', 'פריגת', 'יכין', 'פרימור', 'טרה', 'יטבתה', 'מחלבות גד',
    'זוגלובק', 'יחיעם', 'טירת צבי', 'עוף טוב', 'מאמא עוף',
    'סנו', 'ניקול', 'קלין', 'פיניש', 'פיירי',
    'קנור', 'היינץ', 'תלמה', 'בייגל בייגל',
    'לייף', 'קרליין', 'ניוואה', 'דאב', 'הד אנד שולדרס', 'קולגייט', 'אורביט',
    'האגיס', 'פמפרס', 'טיטולים',
    'יש', 'שווה' # Private labels
}

# 4. Progress and Output Files
PROGRESS_FILE = "llm_verdicts_final.json"
OUTPUT_CSV_FILE = "duplicate_groups_final.csv"

# 5. Execution Mode
SAMPLE_MODE = False
SAMPLE_SIZE = 200

# --- HELPER FUNCTIONS (No changes needed below this line) ---

def extract_product_features(name: str) -> dict:
    if not isinstance(name, str): return {"tokens": set(), "amount": None, "unit": None, "original_name": ""}
    text_part = name.lower()
    amount, unit = None, None
    pattern = re.compile(r'(\d+(?:\.\d+)?)\s*(g|gr|gm|kg|ml|l|ליטר|גרם|גר|קג|מל|יח|מ"ל)\b', re.IGNORECASE)
    match = pattern.search(text_part)
    if match:
        amount = float(match.group(1))
        unit_raw = match.group(2).lower()
        unit_map = {'g': ['g', 'gr', 'gm', 'גרם', 'גר'], 'kg': ['kg', 'קג'], 'ml': ['ml', 'מל', 'מ"ל'], 'l': ['l', 'ליטר'], 'units': ['יח']}
        for standard_unit, variations in unit_map.items():
            if unit_raw in variations: unit = standard_unit; break
        text_part = pattern.sub(' ', text_part)
    text_part = re.sub(r'[\d%]+', ' ', text_part)
    text_part = re.sub(r'[\\/!"#$%&\'()*+,-./:;<=>?@\[\]^_`{|}~]', ' ', text_part)
    stop_words = {'בטעם', 'אריזת', 'מארז', 'מבצע', 'ביחידה', 'יחידות', 'של'}
    tokens = {word for word in text_part.split() if word not in stop_words and len(word) > 1}
    return {"tokens": tokens, "amount": amount, "unit": unit, "original_name": name}

def calculate_jaccard_similarity(set1: set, set2: set) -> float:
    if not set1 and not set2: return 1.0
    if not set1 or not set2: return 0.0
    return len(set1.intersection(set2)) / len(set1.union(set2))

def apply_automatic_filters(p1_features: dict, p2_features: dict) -> str or None:
    p1_tokens, p2_tokens = p1_features['tokens'], p2_features['tokens']
    
    p1_generic_tokens = p1_tokens - BRAND_KEYWORDS
    p2_generic_tokens = p2_tokens - BRAND_KEYWORDS
    if len(p1_generic_tokens) <= 2 and len(p2_generic_tokens) <= 2:
        if calculate_jaccard_similarity(p1_tokens, p2_tokens) < 0.85:
            return "NO"

    score = calculate_jaccard_similarity(p1_tokens, p2_tokens)
    if score > JACCARD_THRESHOLD_FOR_SUBSET and (p1_tokens.issubset(p2_tokens) or p2_tokens.issubset(p1_tokens)):
        return "YES"

    p1_amount, p2_amount = p1_features.get('amount'), p2_features.get('amount')
    if p1_amount is not None and p2_amount is not None and p1_amount > 0 and p2_amount > 0:
        if max(p1_amount, p2_amount) / min(p1_amount, p2_amount) > 3.0:
            return "NO"

    for conflict_set in CONFLICT_PAIRS:
        p1_hits = p1_tokens.intersection(conflict_set)
        p2_hits = p2_tokens.intersection(conflict_set)
        if p1_hits and p2_hits and p1_hits != p2_hits:
            return "NO"
            
    p1_deal_breakers, p2_deal_breakers = p1_tokens.intersection(DEAL_BREAKER_KEYWORDS), p2_tokens.intersection(DEAL_BREAKER_KEYWORDS)
    if p1_deal_breakers != p2_deal_breakers:
        return "NO"
        
    if score < JACCARD_THRESHOLD_FOR_LLM:
        return "NO"

    return None

def fetch_all_products(conn):
    print("Fetching all products from the database...")
    with conn.cursor() as cur:
        cur.execute("SELECT masterproductid, productname FROM products;")
        products = cur.fetchall()
        print(f"  > Found {len(products)} products.")
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
    for token in token_index:
        pids = token_index[token]
        if len(pids) > 1 and len(pids) < 150:
            for pair in itertools.combinations(pids, 2):
                candidate_pairs.add(tuple(sorted(pair)))
    print(f"  > Generated {len(candidate_pairs)} unique candidate pairs.")
    return candidate_pairs

def get_llm_verdict(p1_name: str, p2_name: str, client: OpenAI) -> dict:
    prompt = f"""
You are a data analyst expert for an Israeli supermarket database. Compare Product A and Product B. Return ONLY a valid JSON object with the following structure:
{{
  "is_core_product_match": boolean,
  "match_reason": "string"
}}
- is_core_product_match: true if they are the same fundamental product (e.g., "Fanta Orange" vs "Fanta Orange 6-pack").
- match_reason: Briefly explain your reasoning. If not a match, state the key difference.

Product A: "{p1_name}"
Product B: "{p2_name}"
"""
    try:
        response = client.chat.completions.create(model=LLM_MODEL, messages=[{"role": "user", "content": prompt}], response_format={"type": "json_object"}, temperature=0.0)
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"  ! LLM Error: {e}")
        return {"is_core_product_match": False, "match_reason": "LLM_ERROR"}

def load_progress(filename: str) -> dict:
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            print(f"Loading existing progress from '{filename}'...")
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        print("No valid progress file found. Starting fresh.")
        return {}

def save_progress(filename: str, verdicts: dict):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(verdicts, f, indent=2, ensure_ascii=False)

def build_graph_and_find_clusters(automatic_matches: list, llm_verdicts: dict) -> list:
    print("Building graph and finding duplicate clusters...")
    adj = collections.defaultdict(list)
    
    for p1, p2 in automatic_matches:
        adj[p1].append(p2)
        adj[p2].append(p1)

    for pair_key, verdict_data in llm_verdicts.items():
        if verdict_data.get("is_core_product_match"):
            p1_id, p2_id = map(int, pair_key.split('-'))
            adj[p1_id].append(p2_id)
            adj[p2_id].append(p1_id)

    visited, clusters = set(), []
    all_pids_in_graph = list(adj.keys())
    for pid in all_pids_in_graph:
        if pid not in visited:
            cluster = []
            q = deque([pid])
            visited.add(pid)
            while q:
                current = q.popleft()
                cluster.append(current)
                for neighbor in adj[current]:
                    if neighbor not in visited:
                        visited.add(neighbor)
                        q.append(neighbor)
            clusters.append(sorted(cluster))
    print(f"  > Found {len(clusters)} duplicate groups.")
    return clusters

def main():
    print("--- Starting Final Product Consolidation Script ---")
    
    conn = None
    try:
        conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD)
        all_products_raw = fetch_all_products(conn)
    except psycopg2.Error as e:
        print(f"FATAL: Database connection failed: {e}"); return
    finally:
        if conn: conn.close()
    
    all_features = {pid: extract_product_features(pname) for pid, pname in all_products_raw}
    token_index = create_token_index(all_features)
    candidate_pairs = generate_candidate_pairs(token_index)

    print("Classifying pairs with advanced filters...")
    automatic_yes, llm_candidates = [], []
    for p1_id, p2_id in candidate_pairs:
        verdict = apply_automatic_filters(all_features[p1_id], all_features[p2_id])
        if verdict == "YES":
            automatic_yes.append((p1_id, p2_id))
        elif verdict is None:
            llm_candidates.append((p1_id, p2_id))
    
    print(f"  > Found {len(automatic_yes)} automatic 'YES' matches.")
    print(f"  > Found {len(llm_candidates)} 'grey area' pairs to be verified by LLM.")
    
    verdicts = load_progress(PROGRESS_FILE)
    pairs_to_process = llm_candidates
    if SAMPLE_MODE:
        print(f"--- RUNNING IN SAMPLE MODE: PROCESSING {SAMPLE_SIZE} PAIRS ---")
        pairs_to_process = llm_candidates[:SAMPLE_SIZE]
        
    if OPENAI_API_KEY == "YOUR_OPENAI_API_KEY_HERE":
        print("WARNING: OpenAI API key is not set. LLM validation will be skipped.")
        llm_client = None
    else:
        llm_client = OpenAI(api_key=OPENAI_API_KEY)
        
    if llm_client:
        try:
            for i, (p1_id, p2_id) in enumerate(pairs_to_process):
                pair_key = f"{p1_id}-{p2_id}"
                if pair_key not in verdicts:
                    print(f"  - LLM Check [{i+1}/{len(pairs_to_process)}]: {all_features[p1_id]['original_name']} vs {all_features[p2_id]['original_name']}")
                    verdict_data = get_llm_verdict(all_features[p1_id]['original_name'], all_features[p2_id]['original_name'], llm_client)
                    verdicts[pair_key] = verdict_data
                    print(f"    > AI Analysis: Match: {verdict_data.get('is_core_product_match')}, Reason: {verdict_data.get('match_reason')}")
                if (i + 1) % 20 == 0:
                    print(f"  ... saving progress ...")
                    save_progress(PROGRESS_FILE, verdicts)
        except KeyboardInterrupt:
            print("\n! KeyboardInterrupt detected. Saving progress before exiting.")
        finally:
            print("... saving final progress ...")
            save_progress(PROGRESS_FILE, verdicts)
            
    clusters = build_graph_and_find_clusters(automatic_yes, verdicts)

    print(f"Writing {len(clusters)} groups to {OUTPUT_CSV_FILE}...")
    with open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['group_id', 'masterproductid', 'productname'])
        for i, cluster in enumerate(clusters):
            group_id = i + 1
            for pid in cluster:
                writer.writerow([group_id, pid, all_features[pid]['original_name']])

    print("--- Script Finished ---")
    print(f"Review the results in '{OUTPUT_CSV_FILE}'.")

if __name__ == "__main__":
    main()