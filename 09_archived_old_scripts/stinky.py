import os
import psycopg2
import json
from psycopg2.extras import Json
import openai
import re
import argparse
import csv
import time

# --- Configuration ---
api_key = os.environ.get("OPENAI_API_KEY")
if not api_key:
    api_key = "YOUR_OPENAI_API_KEY_HERE"
if not api_key:
    raise Exception("CRITICAL: OPENAI_API_KEY is not set.")
openai.api_key = api_key

PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "price_comparison_app_v2"
PG_USER = "postgres"
PG_PASSWORD = "025655358"

BATCH_SIZE = 500
CACHE_FILE = "enrichment_cache.json"

PRODUCT_TYPE_MAP = {
    "Shampoo": ["שמפו", "שמפו נגד קשקשים", "שמפו יבש", "שמפו לשיער צבוע", "שמפו לתינוקות"],
    "Conditioner": ["מרכך", "קונדישינר", "מרכך שיער", "מסכה לשיער"],
    "Body Wash": ["ג'ל רחצה", "סבון גוף", "אל רחצה", "סבון נוזלי לגוף", "תחליב רחצה"],
    "Deodorant": ["דאודורנט", "דאו", "אנטיפרספירנט", "אנטי פרספירנט", "סטיק דאודורנט", "רול און"],
    "Toothpaste": ["משחת שיניים"], "Toothbrush": ["מברשת שיניים", "מברשת חשמלית"],
    "Mouthwash": ["שטיפת פה", "מי פה"], "Soap": ["סבון", "סבון ידיים", "סבון נוזלי", "סבון מוצק", "סבון רחצה", "סבון אנטיבקטריאלי"],
    "Face Wash": ["סבון פנים", "ג'ל ניקוי", "תחליב ניקוי", "ניקוי פנים", "מוס ניקוי"],
    "Moisturizer": ["קרם לחות", "תחליב לחות", "קרם פנים", "קרם גוף"], "Sunscreen": ["קרם הגנה", "מקדם הגנה", "הגנה מהשמש"],
    "Vitamins": ["ויטמינים", "ויטמין", "מולטי ויטמין"], "Supplements": ["תוספי תזונה", "תוסף", "פרוביוטיקה", "קולגן"],
    "Diapers": ["חיתולים", "חיתול", "טיטולים"], "Baby Wipes": ["מגבונים", "מגבונים לחים", "מגבוני תינוק"],
    "Baby Formula": ["תמ״ל", "תרכובת מזון", "תחליף חלב", "פורמולה"], "Feminine Hygiene": ["תחבושות", "טמפונים", "פדים יומיים", "מוצרי היגיינה נשית"],
    "Contraceptives": ["אמצעי מניעה", "קונדומים", "גלולות"], "Pregnancy Test": ["בדיקת הריון"],
    "Thermometer": ["מדחום", "מד חום"], "Bandages": ["פלסטרים", "פלסטר", "תחבושות", "אגד"],
    "Antiseptic": ["חומר חיטוי", "פולידין", "סבידין"], "Hand Sanitizer": ["ג'ל אלכוהול", "חיטוי ידיים", "אלכוג'ל"],
    "Allergy Medicine": ["תרופה לאלרגיה", "אנטיהיסטמין"], "Cold Medicine": ["תרופה להצטננות"],
    "Cough Syrup": ["סירופ שיעול"], "Nasal Spray": ["ספריי לאף", "טיפות אף"],
    "Eye Drops": ["טיפות עיניים", "דמעות מלאכותיות"], "Contact Lens Solution": ["תמיסה לעדשות", "נוזל עדשות"],
    "Razor": ["סכין גילוח", "סכיני גילוח"], "Shaving Cream": ["קצף גילוח", "ג'ל גילוח", "קרם גילוח"],
    "After Shave": ["אפטר שייב"], "Hair Dye": ["צבע שיער"], "Nail Polish": ["לק", "לק ציפורניים"],
    "Nail Polish Remover": ["מסיר לק"], "Makeup Remover": ["מסיר איפור", "מים מיסלריים"],
    "Foundation": ["מייק אפ", "פאונדיישן", "מייקאפ"], "Mascara": ["מסקרה"],
    "Lipstick": ["שפתון", "ליפסטיק", "אודם"], "Eyeliner": ["אייליינר", "עיפרון עיניים"],
    "Acne Treatment": ["טיפול באקנה"], "Anti-Aging Cream": ["קרם אנטי אייג'ינג", "נגד קמטים"],
    "Serum": ["סרום", "סרום פנים"], "Face Mask Treatment": ["מסכת פנים", "מסכת בד", "מסכת חימר"],
    "Pain Reliever": ["משכך כאבים", "אקמול", "נורופן", "אופטלגין"], "Reading Glasses": ["משקפי קריאה"]
}

# --- Caching Functions ---
def load_cache():
    """Loads the enrichment cache from a JSON file."""
    try:
        with open(CACHE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_to_cache(cache, key, data):
    """Saves a single entry to the cache and writes the whole cache to disk."""
    cache[key] = data
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

# --- Other Functions (DB, Rules, etc.) ---
def get_db_connection():
    try:
        return psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD)
    except psycopg2.Error as e:
        raise

def get_products_to_enrich(cursor):
    cursor.execute("SELECT product_id, canonical_name, brand FROM products WHERE attributes IS NULL OR attributes->>'product_type' IS NULL ORDER BY product_id;")
    return cursor.fetchall()

def _parse_reading_glasses(product_name: str):
    attributes = {}
    strength_match = re.search(r'\+(\d\.\d{2})', product_name)
    if strength_match:
        attributes['strength'] = f"+{strength_match.group(1)}"
    colors = ['שחור', 'כחול', 'זהב', 'אדום', 'ורוד', 'מנומר', 'שקוף']
    found_colors = [color for color in colors if color in product_name]
    if found_colors:
        attributes['variant'] = '/'.join(found_colors)
    return attributes

def extract_attributes_with_rules(product_name: str):
    attributes = {}
    best_match_type = None
    longest_match_len = 0
    for p_type, keywords in PRODUCT_TYPE_MAP.items():
        for keyword in keywords:
            pattern = r'(^|\s)' + re.escape(keyword) + r'($|\s)'
            if re.search(pattern, product_name):
                if len(keyword) > longest_match_len:
                    longest_match_len = len(keyword)
                    best_match_type = p_type
    if best_match_type:
        attributes['product_type'] = best_match_type
    SPECIALIZED_PARSERS = {"Reading Glasses": _parse_reading_glasses}
    if best_match_type and best_match_type in SPECIALIZED_PARSERS:
        attributes.update(SPECIALIZED_PARSERS[best_match_type](product_name))
    size_pattern = r'(\d+(?:\.\d+)?)\s?(מ"ל|מל|גרם|ג"ר|גר|ליטר|ל|ק"ג|קג|יח|יחידות|מ"ג|מג)'
    size_match = re.search(size_pattern, product_name, re.IGNORECASE)
    if size_match:
        attributes['size_value'] = float(size_match.group(1))
        unit = size_match.group(2).lower().replace('"',"").replace("'", "")
        unit_map = {'מל': 'ml', 'מ"ל': 'ml', 'גר': 'g', 'גרם': 'g', 'ג"ר': 'g', 'מג': 'mg', 'מ"ג': 'mg', 'ליטר': 'l', 'ל': 'l', 'קג': 'kg', 'ק"ג': 'kg', 'יחידות': 'units', 'יח': 'units'}
        attributes['size_unit'] = unit_map.get(unit, unit)
    return attributes if attributes else None

def get_extracted_attributes(product_name: str, brand: str):
    prompt = f"""
    From the Israeli product title below, extract the specified attributes into a valid JSON object.
    - product_type: The generic category of the item (e.g., "Shampoo", "Conditioner", "Body Lotion").
    - variant: The specific version, scent, or key ingredient (e.g., "Argan Oil", "Coconut Milk", "For Colored Hair").
    - size_value: The numerical size or amount. If not present, use null.
    - size_unit: The unit of measurement (e.g., "ml", "g", "l", "units"). If not present, use null.
    Product Title: "{product_name}"
    Brand: "{brand}"
    JSON:
    """
    try:
        response = openai.chat.completions.create(model="gpt-4o", messages=[{"role": "system", "content": "You are a highly accurate data extraction assistant. Your only output is a single, valid JSON object."}, {"role": "user", "content": prompt}], response_format={"type": "json_object"}, temperature=0.0)
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"  -> ERROR: OpenAI API call failed for '{product_name}': {e}")
        return None

def update_product_attributes(cursor, product_id, new_attributes):
    # THE CRITICAL SQL FIX IS HERE: COALESCE ensures we don't merge with NULL.
    cursor.execute("UPDATE products SET attributes = COALESCE(attributes, '{}'::jsonb) || %s WHERE product_id = %s;", (Json(new_attributes), product_id))

# --- Main Execution ---
def main():
    parser = argparse.ArgumentParser(description="Enrich product data using rules and an LLM.")
    parser.add_argument('--dry-run', action='store_true', help="Run without saving changes to the database.")
    parser.add_argument('--review-file', type=str, help="Save all extraction results to a CSV file for review.")
    args = parser.parse_args()

    if args.dry_run:
        print("--- 🔬 Starting Product Enrichment DRY RUN ---")
    else:
        print("--- 🧠 Starting Product Enrichment Process ---")

    # Load the cache at the beginning
    enrichment_cache = load_cache()
    print(f"-> Loaded {len(enrichment_cache)} items from local cache.")

    review_file, csv_writer = None, None
    if args.review_file:
        try:
            review_file = open(args.review_file, 'w', newline='', encoding='utf-8-sig')
            csv_writer = csv.writer(review_file)
            csv_writer.writerow(['product_id', 'original_name', 'source', 'product_type', 'variant', 'size_value', 'size_unit', 'strength'])
        except IOError as e:
            print(f"[ERROR] Could not open review file: {e}"); return

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            products = get_products_to_enrich(cursor)
            if not products:
                print("✅ All products are already enriched. Nothing to do."); return

            print(f"Found {len(products)} products to process...")
            for i, (product_id, name, brand) in enumerate(products):
                print(f"[{i+1}/{len(products)}] Processing: '{name}'")
                
                source = "Rules"
                attributes = extract_attributes_with_rules(name)

                if not attributes:
                    source = "LLM"
                    # Check cache before calling API
                    if name in enrichment_cache:
                        attributes = enrichment_cache[name]
                        print("  -> SUCCESS (Cache): Found in local cache.")
                    else:
                        print("  -> Rules failed, falling back to LLM...")
                        attributes = get_extracted_attributes(name, brand)
                        if attributes:
                            # Save to cache immediately after successful API call
                            save_to_cache(enrichment_cache, name, attributes)
                
                if attributes:
                    print(f"  -> SUCCESS ({source}): Extracted {attributes}")
                    if not args.dry_run:
                        update_product_attributes(cursor, product_id, attributes)
                    if csv_writer:
                        row = [product_id, name, source, attributes.get('product_type', ''), attributes.get('variant', ''), attributes.get('size_value', ''), attributes.get('size_unit', ''), attributes.get('strength', '')]
                        csv_writer.writerow(row)
                else:
                    print("  -> SKIPPED: All extraction methods failed.")

                if not args.dry_run and (i + 1) % BATCH_SIZE == 0:
                    conn.commit()
                    print(f"--- 💾 Committed batch of {BATCH_SIZE} products. Progress saved. ---")

            if not args.dry_run:
                conn.commit()
                print("\n--- ✅ Enrichment Complete. Database updated. ---")
            else:
                print("\n--- 🔬 Dry run complete. No changes were saved. ---")

    except (Exception, KeyboardInterrupt) as e:
        if conn and not args.dry_run:
            print("\n--- ⚠️ Process interrupted. Rolling back current batch. ---")
            conn.rollback()
        print(f"\n[FATAL ERROR] An error occurred: {e}")
    finally:
        if conn: conn.close()
        if review_file: review_file.close()

if __name__ == "__main__":
    main()
