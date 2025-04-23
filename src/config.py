#!/usr/bin/env python3
# config.py - Centralized configuration for PriceComparisonApp

import os
from datetime import datetime

# Root directory
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Data directories
DATA_DIR = os.path.join(ROOT_DIR, "data")
SCREENSHOTS_DIR = os.path.join(DATA_DIR, "screenshots")
CROPPED_DIR = os.path.join(DATA_DIR, "cropped")
OCR_RESULTS_DIR = os.path.join(DATA_DIR, "ocr_results")
DONUT_DATA_DIR = os.path.join(DATA_DIR, "donut_data")
DONUT_IMAGES_DIR = os.path.join(DONUT_DATA_DIR, "images")

# Model directories
MODELS_DIR = os.path.join(ROOT_DIR, "models")

# Log directories
LOGS_DIR = os.path.join(ROOT_DIR, "logs")

# File paths
TRAIN_JSON = os.path.join(DONUT_DATA_DIR, "train.json")
TRAIN_CLEANED_JSON = os.path.join(DONUT_DATA_DIR, "train.cleaned.json")
MASTER_OCR_CSV = os.path.join(OCR_RESULTS_DIR, "master_ocr_results.csv")

# OCR configuration
OCR_LANG = "heb+eng"
OCR_CONFIG = "--psm 6"

# YOLO configuration
YOLO_MODEL = "yolov8n.pt"

# Donut configuration
DONUT_MODEL = "naver-clova-ix/donut-base"

# Function to create directory if it doesn't exist
def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

# Function to get query-specific paths
def get_query_paths(query):
    # Sanitize query for file path use
    safe_query = "".join(c if c.isalnum() else "_" for c in query)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = f"{safe_query}_{timestamp}"
    
    # Create query-specific directories
    query_screenshots_dir = os.path.join(SCREENSHOTS_DIR, "by_query", safe_query)
    query_cropped_dir = os.path.join(CROPPED_DIR, "by_query", safe_query)
    query_ocr_dir = os.path.join(OCR_RESULTS_DIR, "by_query", safe_query)
    
    # Create query-specific file paths
    query_ocr_csv = os.path.join(query_ocr_dir, f"ocr_results_{run_id}.csv")
    
    # Ensure directories exist
    for dir_path in [query_screenshots_dir, query_cropped_dir, query_ocr_dir]:
        ensure_dir(dir_path)
    
    return {
        "run_id": run_id,
        "screenshots_dir": query_screenshots_dir,
        "cropped_dir": query_cropped_dir,
        "ocr_dir": query_ocr_dir,
        "ocr_csv": query_ocr_csv,
    }

# Create all necessary directories
def init_directories():
    dirs = [
        DATA_DIR, 
        SCREENSHOTS_DIR, 
        os.path.join(SCREENSHOTS_DIR, "by_query"),
        CROPPED_DIR, 
        os.path.join(CROPPED_DIR, "by_query"),
        OCR_RESULTS_DIR, 
        os.path.join(OCR_RESULTS_DIR, "by_query"),
        DONUT_DATA_DIR, 
        DONUT_IMAGES_DIR,
        MODELS_DIR, 
        LOGS_DIR
    ]
    
    for directory in dirs:
        ensure_dir(directory)

# Initialize directories when importing this module
init_directories()