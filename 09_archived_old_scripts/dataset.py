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
    #!/usr/bin/env python3
# batch_generate_dataset.py - Main pipeline for generating training data

import os
import sys
import time
import logging
import argparse
from typing import List

from src.scraper import scrape_shufersal
from src.detector import process_query_screenshots
from src.ocr import process_query
from src.dataset import convert_csv_to_donut_format, clean_donut_labels
import config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(config.LOGS_DIR, "batch.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def process_single_query(query):
    """
    Process a single query through the entire pipeline
    
    Args:
        query (str): Search query for products
        
    Returns:
        dict: Results from each pipeline stage
    """
    logger.info(f"\n{'='*60}\n🔄 Processing query: {query}\n{'='*60}")
    
    # Stage 1: Scrape screenshots
    logger.info(f"Stage 1: Scraping Shufersal for '{query}'")
    scrape_result = scrape_shufersal(query)
    screenshots = scrape_result["screenshots"]
    
    if not screenshots:
        logger.error(f"❌ No screenshots captured for query: {query}")
        return {"success": False, "stage": "scrape", "query": query}
    
    # Stage 2: Detect and crop products
    logger.info(f"Stage 2: Detecting and cropping products for '{query}'")
    cropped_images = process_query_screenshots(query)
    
    if not cropped_images:
        logger.warning(f"⚠️ No products detected in screenshots for query: {query}")
        return {"success": False, "stage": "detect", "query": query}
    
    # Stage 3: OCR processing
    logger.info(f"Stage 3: Running OCR on cropped products for '{query}'")
    ocr_results = process_query(query)
    
    if not ocr_results:
        logger.warning(f"⚠️ No valid OCR results for query: {query}")
        return {"success": False, "stage": "ocr", "query": query}
    
    return {
        "success": True,
        "query": query,
        "screenshots": screenshots,
        "cropped_images": cropped_images,
        "ocr_results": ocr_results
    }

def run_batch_queries(queries, pause_seconds=5):
    """
    Run multiple queries through the pipeline
    
    Args:
        queries (list): List of search queries
        pause_seconds (int): Pause between queries in seconds
        
    Returns:
        dict: Results summary
    """
    results = {
        "total_queries": len(queries),
        "successful_queries": 0,
        "failed_queries": 0,
        "total_products": 0,
        "query_results": []
    }
    
    for i, query in enumerate(queries, 1):
        logger.info(f"\n🔄 Processing query {i}/{len(queries)}: '{query}'")
        
        query_result = process_single_query(query)
        results["query_results"].append(query_result)
        
        if query_result["success"]:
            results["successful_queries"] += 1
            results["total_products"] += len(query_result.get("ocr_results", []))
        else:
            results["failed_queries"] += 1
            logger.warning(f"⚠️ Query '{query}' failed at stage: {query_result['stage']}")
        
        # Pause between queries (except after the last one)
        if i < len(queries):
            logger.info(f"Pausing for {pause_seconds} seconds before next query...")
            time.sleep(pause_seconds)
    
    # After all queries, convert results to Donut format
    logger.info("\n🔄 Converting all results to Donut format")
    donut_data = convert_csv_to_donut_format()
    
    if donut_data:
        logger.info("🔄 Cleaning and standardizing Donut labels")
        cleaned_data = clean_donut_labels()
        results["donut_entries"] = len(cleaned_data)
    else:
        results["donut_entries"] = 0
    
    return results

def main():
    parser = argparse.ArgumentParser(description="Generate training data for price comparison")
    parser.add_argument("--queries_file", help="Path to file containing queries, one per line")
    parser.add_argument("--queries", nargs="+", help="List of search queries")
    parser.add_argument("--pause", type=int, default=5, help="Pause between queries in seconds")
    args = parser.parse_args()
    
    # Initialize directories
    config.init_directories()
    
    # Get queries from file or command line
    queries = []
    if args.queries_file and os.path.exists(args.queries_file):
        with open(args.queries_file, 'r', encoding='utf-8') as f:
            queries = [line.strip() for line in f if line.strip()]
    elif args.queries:
        queries = args.queries
    else:
        # Default queries
        queries = [
            "נוטלה",
            "מילקי",
            "קוקה קולה",
            "קוטג'",
            "גבינה",
            "לחם",
        ]
    
    logger.info(f"Starting batch processing with {len(queries)} queries")
    
    # Run the batch process
    result = run_batch_queries(queries, args.pause)
    
    # Log summary
    logger.info("\n" + "="*60)
    logger.info(f"✅ Batch processing complete!")
    logger.info(f"Total queries: {result['total_queries']}")
    logger.info(f"Successful queries: {result['successful_queries']}")
    logger.info(f"Failed queries: {result['failed_queries']}")
    logger.info(f"Total products detected: {result['total_products']}")
    logger.info(f"Final Donut dataset entries: {result['donut_entries']}")
    logger.info("="*60)
    
    return result

if __name__ == "__main__":
    main()