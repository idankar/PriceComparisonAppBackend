#!/usr/bin/env python3
# src/ocr.py - OCR processing for product images

import os
import sys
import glob
import csv
import logging
import cv2
import pytesseract
from typing import List, Dict, Any
import re

# Add the project root to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config
from .postprocess import clean_ocr_text, extract_product_and_price

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(config.LOGS_DIR, "ocr.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_ocr_on_image(image_path: str, lang: str = "heb+eng") -> List[str]:
    """
    Run OCR on a single image
    
    Args:
        image_path (str): Path to the image
        lang (str): Language for OCR
        
    Returns:
        list: Extracted text lines
    """
    image = cv2.imread(image_path)
    if image is None:
        logger.warning(f"Could not read image: {image_path}")
        return []

    # Grayscale
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # Increase resolution for better text detection
    height, width = gray.shape
    gray = cv2.resize(gray, (width * 3, height * 3), interpolation=cv2.INTER_LINEAR)

    # Apply different thresholding methods and combine results
    _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    adaptive = cv2.adaptiveThreshold(
        gray,
        255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY,
        blockSize=25,  # Larger block size
        C=10  # Higher constant for more contrast
    )

    # Combine binary and adaptive to get best of both
    combined = cv2.bitwise_or(binary, adaptive)

    # OCR with multiple page segmentation modes and combine results
    config1 = '--psm 6'  # Assume a single uniform block of text
    config2 = '--psm 11' # Sparse text with no orientation or layout
    config3 = '--psm 3'  # Fully automatic page segmentation

    text1 = pytesseract.image_to_string(combined, config=config1, lang=lang)
    text2 = pytesseract.image_to_string(combined, config=config2, lang=lang)
    text3 = pytesseract.image_to_string(combined, config=config3, lang=lang)

    # Combine results
    all_lines = []
    for text in [text1, text2, text3]:
        all_lines.extend([line.strip() for line in text.splitlines() if line.strip()])

    # Remove duplicates while preserving order
    seen = set()
    lines = []
    for line in all_lines:
        if line not in seen:
            seen.add(line)
            lines.append(line)

    return lines

# In src/ocr.py, add a function to detect if we're looking at Nutella search results
def is_nutella_search_page(ocr_lines):
    """Check if this appears to be a Nutella search results page"""
    search_indicators = [
        'תוצאות חיפוש עבור:"נוטלה"',
        'נוטלה',
        'ביסקוויט נוטלה',
        'ממרח נוטלה'
    ]
    
    page_score = 0
    for line in ocr_lines:
        for indicator in search_indicators:
            if indicator in line:
                page_score += 1
    
    # If we have multiple indicators, it's likely a search results page
    # Check if the score suggests it's a search page vs just a single product mention
    is_search_page = page_score >= 2
    if is_search_page:
        logger.info("Detected potential Nutella search results page based on keyword score.")
    return is_search_page

def process_images(images_dir: str, output_csv: str, query: str = None) -> List[Dict[str, Any]]:
    """
    Process all product images in a directory with OCR
    
    Args:
        images_dir (str): Directory containing product images
        output_csv (str): Path to save OCR results
        query (str, optional): Search query that was used
        
    Returns:
        list: Extracted products information
    """
    # Find all PNG files in the directory
    product_crops = glob.glob(os.path.join(images_dir, "product_*.png"))
    if not product_crops:
        logger.warning(f"No product images found in {images_dir}")
        return []
    
    logger.info(f"Found {len(product_crops)} product images to process")
    
    all_results = []

    for crop_path in product_crops:
        crop_filename = os.path.basename(crop_path)
        logger.info(f"\nProcessing {crop_filename}...")

        # Run OCR
        ocr_lines = run_ocr_on_image(crop_path, lang=config.OCR_LANG)
        logger.debug(f"OCR raw lines:")
        for line in ocr_lines:
            logger.debug(f"  RAW: {line}")

        # Clean OCR output
        cleaned_lines = clean_ocr_text(ocr_lines)
        logger.debug(f"Cleaned lines:")
        for line in cleaned_lines:
            logger.debug(f"  CLEANED: {line}")

        # Combine all text for reference
        full_text = " ".join(cleaned_lines)
        
        # Add special processing for search results pages
        if is_nutella_search_page(cleaned_lines):
            logger.info(f"Applying special search page processing for: {crop_filename}")
            # Find specific patterns in the search results format
            # Use re directly here
            product_pattern = r'(ביסקוויט נוטלה|ממרח נוטלה|נוטלה בי רדי|נוטלה)\s+(\d+)\s+גרם'
            price_pattern = r'₪\s*(\d+\.\d+)'
            
            products = []
            prices = []
            
            for line in cleaned_lines: # Use cleaned_lines here
                product_match = re.search(product_pattern, line)
                if product_match:
                    # Construct a meaningful name like "Nutella 750 גרם"
                    name = f"{product_match.group(1)} {product_match.group(2)} גרם"
                    products.append(name)
                
                price_match = re.search(price_pattern, line)
                if price_match:
                    try:
                        prices.append(float(price_match.group(1)))
                    except ValueError:
                        logger.warning(f"Could not parse price {price_match.group(1)} on search page")
            
            logger.info(f"Found {len(products)} products and {len(prices)} prices via search page patterns.")
            # Match products with prices if counts are similar
            if len(products) > 0 and abs(len(products) - len(prices)) <= 3:
                # Use shorter list length
                count = min(len(products), len(prices))
                logger.info(f"Matching {count} products/prices from search page.")
                for i in range(count):
                    result = {
                        "query": query,
                        "crop": crop_filename,
                        "product_name": products[i],
                        "price": prices[i],
                        "full_ocr_text": full_text
                    }
                    all_results.append(result)
                    logger.info(f"Found product (search page): {products[i]}, price: {prices[i]}")
            else:
                logger.warning("Product/price counts differ too much on search page, skipping specific extraction.")
                # Fallback or just skip?
                # Maybe try standard extraction as fallback?
                # For now, just logging and skipping this special path if counts mismatch badly.

        else: # Not a search page, use standard extraction
            # Extract product name and price using the standard method
            product_info = extract_product_and_price(cleaned_lines)

            if not product_info:
                logger.warning(f"No product/price pairs found in image (standard): {crop_filename}")
                # No continue here, let the outer loop proceed
            else:
                 # Add each product to results
                for name, price in product_info:
                    result = {
                        "query": query,
                        "crop": crop_filename,
                        "product_name": name,
                        "price": price,
                        "full_ocr_text": full_text
                    }
                    all_results.append(result)
                    logger.info(f"Found product (standard): {name}, price: {price}")

    # Save results to CSV
    if all_results:
        # Ensure directory exists
        os.makedirs(os.path.dirname(output_csv), exist_ok=True)
        
        # Write to CSV
        with open(output_csv, mode="w", newline='', encoding='utf-8') as f:
            fieldnames = ["query", "crop", "product_name", "price", "full_ocr_text"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_results)

        logger.info(f"✅ OCR results saved: {output_csv} ({len(all_results)} entries)")
        
        # Also append to master CSV
        append_to_master_csv(all_results)
    else:
        logger.warning(f"No valid product results found. Nothing saved to {output_csv}")

    return all_results

def append_to_master_csv(results: List[Dict[str, Any]]):
    """
    Append results to the master CSV file
    
    Args:
        results (list): Product information to append
    """
    # Check if master CSV exists
    master_csv_exists = os.path.exists(config.MASTER_OCR_CSV)
    
    # Open in append mode
    with open(config.MASTER_OCR_CSV, mode="a" if master_csv_exists else "w", newline='', encoding='utf-8') as f:
        fieldnames = ["query", "crop", "product_name", "price", "full_ocr_text"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        # Write header only if the file is new
        if not master_csv_exists:
            writer.writeheader()
        
        # Write all results
        writer.writerows(results)
    
    logger.info(f"✅ Appended {len(results)} entries to master CSV: {config.MASTER_OCR_CSV}")

def process_query(query: str) -> List[Dict[str, Any]]:
    """
    Process all product images for a specific query
    
    Args:
        query (str): Search query that was used
        
    Returns:
        list: Extracted products information
    """
    # Get query-specific paths
    paths = config.get_query_paths(query)
    
    # Run OCR processing
    return process_images(
        images_dir=paths["cropped_dir"],
        output_csv=paths["ocr_csv"],
        query=query
    )

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("⚠️ Missing query. Usage: python ocr.py 'מעדן'")
        sys.exit(1)
    
    query = sys.argv[1]
    results = process_query(query)
    logger.info(f"✅ Extracted {len(results)} products for query: {query}")