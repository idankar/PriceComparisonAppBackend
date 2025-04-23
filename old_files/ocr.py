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

# Add the project root to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config
from src.postprocess import clean_ocr_text, extract_product_and_price

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

    # Upscale (2x)
    height, width = gray.shape
    gray = cv2.resize(gray, (width * 2, height * 2), interpolation=cv2.INTER_LINEAR)

    # Adaptive Thresholding
    thresh = cv2.adaptiveThreshold(
        gray,
        255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY,
        blockSize=15,
        C=8
    )

    # Dilation (enhance contours)
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 2))
    processed = cv2.dilate(thresh, kernel, iterations=1)

    # OCR
    config_params = '--psm 6'
    text = pytesseract.image_to_string(processed, config=config_params, lang=lang)
    lines = text.splitlines()
    return [line.strip() for line in lines if line.strip()]

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
        
        # Extract product name and price
        product_info = extract_product_and_price(cleaned_lines)

        if not product_info:
            logger.warning(f"No product/price pairs found in image: {crop_filename}")
            continue

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
            logger.info(f"Found product: {name}, price: {price}")

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