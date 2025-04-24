#!/usr/bin/env python3
# src/dataset.py - Dataset preparation for Donut

import os
import sys
import json
import csv
import shutil
import re
import logging
from typing import List, Dict, Any
from rapidfuzz import process, fuzz

# Add the project root to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(config.LOGS_DIR, "dataset.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Known product names for fuzzy matching
KNOWN_NAMES = [
    "ממרח נוטלה",
    "נוטלה",
    "ביסקוויט נוטלה",
    "ביסקוויס נוטלה",
    "נוטלה בי רדי",
    "ממרח אגוזי לוז נוטלה",
    "ממרח אגוזי לוז וקקאו נוטלה",
    "נוטלה ביסקוויט",
    "נוטלה ביסקוויס",
    "nutella",
    "nutella biscuit",
    "נוטלה ממרח"
]

def slugify(text):
    """Convert text to a file-system friendly slug"""
    return re.sub(r"[^\wא-ת]+", "_", text).strip("_")

def extract_quantity(raw_text: str) -> str:
    """Extract quantity information from text"""
    match = re.search(r'\d{2,4}\s*(גרם|יחידות|מ"ל|ליטר|מ״ל)', raw_text)
    return match.group(0).strip() if match else ""

def smart_dedup(text: str) -> str:
    """Remove duplicate words"""
    words = text.split()
    seen = []
    for w in words:
        if w not in seen:
            seen.append(w)
    return " ".join(seen)

def clean_name(name: str, full_ocr_text: str) -> str:
    # Define Nutella keywords for more specific filtering
    nutella_keywords = ["נוטלה", "nutella", "ממרח נוטלה", "nute11a"]
    
    # Check if product is actually a Nutella-related product
    is_nutella = False
    for keyword in nutella_keywords:
        if keyword in name.lower() or keyword in full_ocr_text.lower():
            is_nutella = True
            break
    
    if not is_nutella:
        return "UNKNOWN"  # This will be filtered out later
    
    # Extract quantity from full OCR block
    quantity = extract_quantity(full_ocr_text)

    # Remove digits not attached to quantity
    name = re.sub(r'\b[01]\b', '', name)

    # Deduplicate and normalize
    base = smart_dedup(name.strip())

    # Clean up long strings with too many random words
    words = base.split()
    if len(words) > 8:  # If name has too many words
        # Try to find the closest segment containing 'Nutella'
        nutella_index = -1
        for i, word in enumerate(words):
            if "נוטלה" in word or "nutella" in word.lower():
                nutella_index = i
                break
        
        if nutella_index >= 0:
            # Take a window of words around the Nutella mention
            start = max(0, nutella_index - 2)
            end = min(len(words), nutella_index + 4)
            base = " ".join(words[start:end])
        else:
            # Just take the first few words if no Nutella found
            base = " ".join(words[:6])

    # Attach quantity if not already included
    if quantity and quantity not in base:
        base = f"{base} {quantity}"

    return base.strip()

def fuzzy_match(name: str) -> str:
    """Find closest match in known product names"""
    best, score, _ = process.extractOne(name, KNOWN_NAMES, scorer=fuzz.token_sort_ratio)
    return best if score > 85 else name

def convert_csv_to_donut_format(master_csv=None, max_samples_per_product=100):
    """
    Create a balanced dataset with examples from different products
    
    Args:
        master_csv (str): Path to the master CSV file
        max_samples_per_product (int): Maximum samples to use per product type
    """
    import os
    import csv
    import json
    import random
    import logging
    import config

    logger = logging.getLogger(__name__)

    if master_csv is None:
        master_csv = config.MASTER_OCR_CSV

    if not os.path.exists(master_csv):
        logger.error(f"Master CSV file not found: {master_csv}")
        return

    logger.info(f"Converting {master_csv} to Donut format")

    # Create output directories
    os.makedirs(config.DONUT_TRAIN_DIR, exist_ok=True)
    os.makedirs(config.DONUT_VAL_DIR, exist_ok=True)

    # Read the master CSV
    with open(master_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    logger.info(f"Read {len(rows)} rows from master CSV")

    # Group products by query for balanced representation
    product_groups = {}
    for row in rows:
        query = row.get("query", "").strip()
        product_groups.setdefault(query, []).append(row)

    logger.info(f"Found {len(product_groups)} unique product types")

    # Sample from each group to create a balanced dataset
    balanced_rows = []
    for query, query_rows in product_groups.items():
        if len(query_rows) > max_samples_per_product:
            sampled = random.sample(query_rows, max_samples_per_product)
            logger.info(f"Sampling {len(sampled)} out of {len(query_rows)} for '{query}'")
            balanced_rows.extend(sampled)
        else:
            logger.info(f"Using all {len(query_rows)} samples for '{query}'")
            balanced_rows.extend(query_rows)

    logger.info(f"Created balanced dataset with {len(balanced_rows)} samples")

    # Shuffle and split into train/val
    random.shuffle(balanced_rows)
    split_idx = int(len(balanced_rows) * 0.8)
    train_rows, val_rows = balanced_rows[:split_idx], balanced_rows[split_idx:]
    logger.info(f"Split into {len(train_rows)} training and {len(val_rows)} validation samples")

    train_examples, val_examples = [], []
    missing = 0

    def _process(rows_list, examples_list):
        nonlocal missing
        for row in rows_list:
            img = row.get("image_path")
            if not img or not os.path.exists(img):
                missing += 1
                continue
            label = {
                "product_name": row.get("product_name", ""),
                "price": row.get("price", ""),
                "brand": row.get("brand", ""),
                "unit_description": row.get("unit_description", "")
            }
            examples_list.append({"image_path": img, "ground_truth": label})

    _process(train_rows, train_examples)
    _process(val_rows, val_examples)

    if missing > 0:
        logger.warning(f"Skipped {missing} entries due to missing images")

    # Save to JSON
    train_path = os.path.join(config.DONUT_TRAIN_DIR, "train.json")
    val_path = os.path.join(config.DONUT_VAL_DIR, "val.json")
    with open(train_path, 'w', encoding='utf-8') as f:
        json.dump(train_examples, f, ensure_ascii=False, indent=2)
    with open(val_path, 'w', encoding='utf-8') as f:
        json.dump(val_examples, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {len(train_examples)} training examples to {train_path}")
    logger.info(f"Saved {len(val_examples)} validation examples to {val_path}")

    return {"train_examples": len(train_examples), "val_examples": len(val_examples)}

def clean_donut_labels():
    """
    Clean and standardize Donut labels
    
    Returns:
        list: Cleaned Donut format data
    """
    if not os.path.exists(config.TRAIN_JSON):
        logger.error(f"Donut training data not found: {config.TRAIN_JSON}")
        return []
    
    # Load raw Donut data
    with open(config.TRAIN_JSON, encoding="utf-8") as f:
        data = json.load(f)
    
    cleaned = []
    used_filenames = set()
    
    for entry in data:
        raw_name = entry["label"]["name"]
        full_ocr_text = entry.get("full_ocr_text", "")
        price = entry["label"]["price"]

        # Skip entries with UNKNOWN names or non-Nutella products
        if raw_name == "UNKNOWN" or (
            "נוטלה" not in raw_name.lower() and 
            "nutella" not in raw_name.lower() and
            "ממרח" not in raw_name.lower()):
            continue
            
        # Skip entries with zero price
        try:
            if float(price) <= 0:
                continue
        except (ValueError, TypeError):
            continue

        # Clean and normalize
        name = clean_name(raw_name, full_ocr_text)
        name = fuzzy_match(name)

        # Create unique filename
        base_name = slugify(name)
        if not base_name:
            continue  # Skip if the name becomes empty after slugification
            
        filename = f"{base_name}.png"
        suffix = 1
        while filename in used_filenames:
            filename = f"{base_name}_{suffix}.png"
            suffix += 1
        used_filenames.add(filename)

        # Rename image file if needed
        old_path = os.path.join(config.DONUT_IMAGES_DIR, entry["image"])
        new_path = os.path.join(config.DONUT_IMAGES_DIR, filename)
        if os.path.exists(old_path) and old_path != new_path:
            shutil.move(old_path, new_path)

        # Update entry
        entry["image"] = filename
        entry["label"]["name"] = name
        cleaned.append(entry)
    
    # Save cleaned Donut data
    with open(config.TRAIN_CLEANED_JSON, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)
    
    logger.info(f"✅ Cleaned and renamed {len(cleaned)} labels: {config.TRAIN_CLEANED_JSON}")
    
    return cleaned