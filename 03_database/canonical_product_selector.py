import os
import csv
import json
import collections
import time
from openai import OpenAI
from collections import deque

# --- CONFIGURATION ---

# 1. Database Connection Details (Needed to fetch product names if not in CSV, but not for this specific script)
#    These are included for completeness if you decide to fetch product names directly from DB
#    if your input CSV doesn't contain them (though it should).
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DATABASE = os.environ.get("PG_DATABASE", "price_comparison_app")
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "025655358")

# 2. OpenAI API Key
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "YOUR_OPENAI_API_KEY_HERE")
LLM_MODEL = "gpt-4o-mini" # Cost-effective for this classification task

# 3. Input/Output Files
INPUT_CSV_FILE = "duplicate_groups_pharmacy_v5.csv" # Output from consolidation script
PROGRESS_FILE = "canonical_selection_verdicts.json"
OUTPUT_CSV_FILE = "duplicate_groups_pharmacy_canonical.csv" # New CSV with is_canonical column

# 4. Execution Mode
SAMPLE_MODE = False  # Set to True for initial testing
SAMPLE_SIZE = 50   # Number of groups to process in sample mode

# --- HELPER FUNCTIONS ---

def load_duplicate_groups_from_csv(filename: str) -> dict:
    """
    Loads duplicate groups from the CSV, grouping products by group_id.
    Returns a dictionary: {group_id: [(masterproductid, productname), ...]}
    """
    print(f"Loading duplicate groups from '{filename}'...")
    groups = collections.defaultdict(list)
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader) # Skip header
            for row in reader:
                if len(row) >= 3: # Ensure row has group_id, masterproductid, productname
                    try:
                        group_id = int(row[0])
                        masterproductid = int(row[1])
                        productname = row[2]
                        groups[group_id].append({'masterproductid': masterproductid, 'productname': productname})
                    except ValueError:
                        print(f"  [WARNING] Skipping malformed row: {row}")
                else:
                    print(f"  [WARNING] Skipping incomplete row: {row}")
        print(f"  > Loaded {len(groups)} distinct groups.")
    except FileNotFoundError:
        print(f"[CRITICAL] Error: Input CSV '{filename}' not found. Please ensure the consolidation script ran successfully.")
        raise
    except Exception as e:
        print(f"[CRITICAL] Error reading input CSV '{filename}': {e}")
        raise
    return groups

def get_llm_canonical_verdict(product_names: list, client: OpenAI) -> dict:
    """
    Calls the LLM to select the most canonical product name from a list.
    Returns a dictionary: {'canonical_name': '...', 'reason': '...'}
    """
    names_list_str = "\n".join([f"- {name}" for name in product_names])
    prompt = f"""
You are an expert in product data normalization. Given a list of product names that refer to the same fundamental item but may have slight variations (e.g., due to different retailers, minor packaging changes, or typos), your task is to select the single most canonical (representative, complete, and clear) name from the provided list.

Prioritize names that:
- Are most descriptive and complete.
- Are least ambiguous.
- Avoid retailer-specific prefixes/suffixes unless essential for identity.
- Are grammatically correct and well-formatted.

Respond ONLY with a valid JSON object with the following structure:
{{
  "canonical_name": "string",
  "reason": "string"
}}
- canonical_name: The EXACT product name chosen from the provided list.
- reason: Briefly explain why this name was chosen as the most canonical.

Product Names:
{names_list_str}
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
                max_tokens=200 # Allow enough tokens for the JSON response
            )
            verdict_data = json.loads(response.choices[0].message.content)
            # Validate LLM's response structure and ensure the chosen name is in the original list
            if "canonical_name" in verdict_data and verdict_data["canonical_name"] in product_names:
                return verdict_data
            else:
                print(f"  ! LLM returned invalid or out-of-list canonical name (Attempt {attempt + 1}/{max_retries}): {verdict_data}")
                # Fallback: if LLM fails to pick a valid name, pick the first one from the list as a fallback
                return {"canonical_name": product_names[0], "reason": f"LLM_INVALID_CHOICE_FALLBACK: {e if 'e' in locals() else 'Invalid choice or structure'}"}
        except (json.JSONDecodeError, ValueError) as e:
            print(f"  ! LLM response parsing failed (Attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay *= 2
            else:
                # Fallback: if LLM fails, pick the first one from the list as a fallback
                return {"canonical_name": product_names[0], "reason": f"LLM_RESPONSE_PARSE_ERROR_FALLBACK: {e}"}
        except Exception as e:
            print(f"  ! LLM API call failed (Attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay *= 2
            else:
                # Fallback: if LLM fails, pick the first one from the list as a fallback
                return {"canonical_name": product_names[0], "reason": f"LLM_API_ERROR_FALLBACK: {e}"}
    return {"canonical_name": product_names[0], "reason": "UNKNOWN_ERROR_FALLBACK"} # Final fallback

def load_progress(filename: str) -> dict:
    """Loads previously saved LLM canonical selection verdicts from a JSON file."""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            print(f"Loading existing progress from '{filename}'...")
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        print("No valid progress file found. Starting fresh.")
        return {}

def save_progress(filename: str, verdicts: dict):
    """Saves the LLM canonical selection verdicts to a JSON file."""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(verdicts, f, indent=2, ensure_ascii=False)

# --- MAIN EXECUTION ---

def main():
    print("--- Starting Automated Canonical Product Selector Script ---")
    
    if OPENAI_API_KEY == "YOUR_OPENAI_API_KEY_HERE":
        print("[CRITICAL] OpenAI API key is not set. LLM selection will be skipped.")
        print("Exiting.")
        return

    llm_client = OpenAI(api_key=OPENAI_API_KEY)

    try:
        grouped_products = load_duplicate_groups_from_csv(INPUT_CSV_FILE)
        
        canonical_verdicts = load_progress(PROGRESS_FILE)
        
        groups_to_process = []
        for group_id, products_in_group in grouped_products.items():
            if len(products_in_group) > 1: # Only send groups with multiple products to LLM
                if str(group_id) not in canonical_verdicts: # Check if group already processed
                    groups_to_process.append(group_id)
            else: # Single product groups are automatically canonical
                canonical_verdicts[str(group_id)] = {
                    "canonical_name": products_in_group[0]['productname'],
                    "canonical_pid": products_in_group[0]['masterproductid'], # Store PID for single-item groups
                    "reason": "Single item in group, automatically canonical."
                }
        
        if SAMPLE_MODE:
            print(f"--- RUNNING IN SAMPLE MODE: PROCESSING {SAMPLE_SIZE} GROUPS ---")
            groups_to_process = groups_to_process[:SAMPLE_SIZE]
        
        if not groups_to_process and len(grouped_products) > 0:
            print("All relevant groups already have canonical selections. Proceeding to write output.")
            # If all groups processed, directly write the output CSV
            write_output_csv(grouped_products, canonical_verdicts, OUTPUT_CSV_FILE)
            return
        elif not groups_to_process and len(grouped_products) == 0:
            print("No groups found in input CSV. Script finished.")
            return

        print(f"Selecting canonical names for {len(groups_to_process)} groups using LLM...")
        
        for i, group_id in enumerate(groups_to_process):
            products_in_group = grouped_products[group_id]
            product_names = [p['productname'] for p in products_in_group]
            
            print(f"  - LLM Select Canonical [{i+1}/{len(groups_to_process)}]: Group {group_id} with {len(products_in_group)} products.")
            
            verdict_data = get_llm_canonical_verdict(product_names, llm_client)
            
            # Store the chosen canonical name and its corresponding PID
            chosen_name = verdict_data['canonical_name']
            chosen_pid = next((p['masterproductid'] for p in products_in_group if p['productname'] == chosen_name), None)
            
            if chosen_pid is None:
                # Fallback if LLM chose a name not exactly matching a PID (should be rare with exact match instruction)
                print(f"    [WARNING] LLM chose name '{chosen_name}' not found for PID in group {group_id}. Falling back to first PID.")
                chosen_pid = products_in_group[0]['masterproductid']
                chosen_name = products_in_group[0]['productname']
                verdict_data['reason'] += " (Fallback due to name mismatch)"


            canonical_verdicts[str(group_id)] = {
                "canonical_name": chosen_name,
                "canonical_pid": chosen_pid,
                "reason": verdict_data['reason']
            }
            print(f"    > Chosen Canonical: '{chosen_name}' (PID: {chosen_pid})")
            
            # Save progress periodically
            if (i + 1) % 10 == 0: # Save every 10 groups processed by LLM
                print("  ... saving canonical selection progress ...")
                save_progress(PROGRESS_FILE, canonical_verdicts)
            
            time.sleep(0.5) # Be respectful of API rate limits

    except Exception as e:
        print(f"FATAL: An unexpected error occurred: {e}")
    finally:
        print("... saving final canonical selection progress ...")
        save_progress(PROGRESS_FILE, canonical_verdicts)
        # Always attempt to write the output CSV even if interrupted or errors occur
        if 'grouped_products' in locals() and 'canonical_verdicts' in locals():
            write_output_csv(grouped_products, canonical_verdicts, OUTPUT_CSV_FILE)
        print("--- Script Finished ---")
        print(f"Review the results in '{OUTPUT_CSV_FILE}'.")

def write_output_csv(grouped_products: dict, canonical_verdicts: dict, output_filename: str):
    """
    Writes the final CSV with the 'is_canonical' column.
    """
    print(f"Writing final canonical CSV to '{output_filename}'...")
    
    output_rows = []
    for group_id, products_in_group in grouped_products.items():
        verdict = canonical_verdicts.get(str(group_id))
        if not verdict:
            print(f"  [WARNING] No canonical verdict found for group {group_id}, skipping this group in output.")
            continue

        canonical_pid_for_group = verdict['canonical_pid']
        
        for p in products_in_group:
            is_canonical = (p['masterproductid'] == canonical_pid_for_group)
            output_rows.append({
                'group_id': group_id,
                'masterproductid': p['masterproductid'],
                'productname': p['productname'],
                'is_canonical': is_canonical
            })
            
    # Sort by group_id and then by is_canonical (TRUE first) for readability
    output_rows.sort(key=lambda x: (x['group_id'], not x['is_canonical']))

    with open(output_filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['group_id', 'masterproductid', 'productname', 'is_canonical'])
        writer.writeheader()
        writer.writerows(output_rows)
            
    print(f"  > Wrote {len(output_rows)} rows to '{output_filename}'.")

if __name__ == "__main__":
    main()
