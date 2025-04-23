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
from . import config

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
    "שוקולד פרה",
    "חלב תנובה",
    "קוטג'",
    "יוגורט",
    "קפה נמס",
    "ביסלי",
    "תפוצ׳יפס",
    "עוגיות",
    "מיץ תפוזים",
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
    """Clean and normalize product name"""
    # Extract quantity from full OCR block
    quantity = extract_quantity(full_ocr_text)

    # Remove digits not attached to quantity
    name = re.sub(r'\b[01]\b', '', name)

    # Deduplicate and normalize
    base = smart_dedup(name.strip())

    # Attach quantity if not already included
    if quantity and quantity not in base:
        base = f"{base} {quantity}"

    return base.strip()

def fuzzy_match(name: str) -> str:
    """Find closest match in known product names"""
    best, score, _ = process.extractOne(name, KNOWN_NAMES, scorer=fuzz.token_sort_ratio)
    return best if score > 85 else name

def convert_csv_to_donut_format(master_csv=None):
    """
    Convert OCR CSV results to Donut training JSON format
    
    Args:
        master_csv (str, optional): Path to master CSV file. If None, use default.
        
    Returns:
        list: Donut format data
    """
    if master_csv is None:
        master_csv = config.MASTER_OCR_CSV
    
    if not os.path.exists(master_csv):
        logger.error(f"Master CSV file not found: {master_csv}")
        return []
    
    donut_data = []
    
    # Ensure output directories exist
    config.ensure_dir(config.DONUT_DATA_DIR)
    # Read all entries from the master CSV file
    with open(master_csv, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            crop_path = row.get("crop", "").strip()
            product_name = row.get("product_name", "").strip()
            price = row.get("price", "").strip()
            full_ocr_text = row.get("full_ocr_text", "").strip()
            
            if not crop_path or not product_name or not price:
                logger.warning(f"Skipping incomplete row: {row}")
                continue
            
            # Find the original crop image
            query = row.get("query", "unknown")
            query_dir = os.path.join(config.CROPPED_DIR, "by_query", query)
            original_path = None
            
            # First try exact path
            if os.path.exists(os.path.join(query_dir, crop_path)):
                original_path = os.path.join(query_dir, crop_path)
            else:
                # Try to find it by searching all query directories
                for root, _, files in os.walk(config.CROPPED_DIR):
                    if crop_path in files:
                        original_path = os.path.join(root, crop_path)
                        break
            
            if not original_path:
                logger.warning(f"Original image not found for: {crop_path}")
                continue
            
            # Clean and normalize product name
            clean_product_name = clean_name(product_name, full_ocr_text)
            normalized_name = fuzzy_match(clean_product_name)
            
            # Generate unique image filename
            base_name = slugify(normalized_name)
            filename = f"{base_name}.png"
            counter = 1
            
            while os.path.exists(os.path.join(config.DONUT_IMAGES_DIR, filename)):
                filename = f"{base_name}_{counter}.png"
                counter += 1
            
            # Copy image to Donut images directory
            dest_path = os.path.join(config.DONUT_IMAGES_DIR, filename)
            shutil.copy2(original_path, dest_path)
            
            # Add entry to Donut data
            donut_data.append({
                "image": filename,
                "label": {
                    "name": normalized_name,
                    "price": price
                },
                "full_ocr_text": full_ocr_text
            })
    
    # Save raw Donut data
    with open(config.TRAIN_JSON, "w", encoding="utf-8") as f:
        json.dump(donut_data, f, ensure_ascii=False, indent=2)
    
    logger.info(f"✅ Created Donut training data with {len(donut_data)} entries: {config.TRAIN_JSON}")
    
    return donut_data

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

        # Clean and normalize
        name = clean_name(raw_name, full_ocr_text)
        name = fuzzy_match(name)

        # Create unique filename
        base_name = slugify(name)
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