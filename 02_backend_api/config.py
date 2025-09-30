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

# OCR configuration - Optimized for Hebrew product labels
OCR_LANG = "heb+eng"
OCR_CONFIG = "--oem 3 --psm 6"  # LSTM neural network with uniform block text

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
# config.py
import os
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Base directories
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Create main data directory if it doesn't exist
os.makedirs(DATA_DIR, exist_ok=True)

# Screenshots directories
SCREENSHOTS_DIR = os.path.join(DATA_DIR, "screenshots")
SCREENSHOTS_BY_QUERY_DIR = os.path.join(SCREENSHOTS_DIR, "by_query")

# Cropped images directories
CROPPED_DIR = os.path.join(DATA_DIR, "cropped")
CROPPED_BY_QUERY_DIR = os.path.join(CROPPED_DIR, "by_query")

# OCR results directories
OCR_RESULTS_DIR = os.path.join(DATA_DIR, "ocr_results")
OCR_RESULTS_BY_QUERY_DIR = os.path.join(OCR_RESULTS_DIR, "by_query")
MASTER_OCR_CSV = os.path.join(OCR_RESULTS_DIR, "master_ocr_results.csv")

# Logs directory
LOGS_DIR = os.path.join(BASE_DIR, "logs")

# Temp directory
TEMP_DIR = os.path.join(DATA_DIR, "temp")

# Model directories
MODELS_DIR = os.path.join(BASE_DIR, "models")
YOLO_MODEL_PATH = os.path.join(MODELS_DIR, "yolo", "yolov5s.pt")

# Donut model directories
DONUT_DIR = os.path.join(BASE_DIR, "donut")
DONUT_TRAIN_DIR = os.path.join(DONUT_DIR, "train")
DONUT_VAL_DIR = os.path.join(DONUT_DIR, "val")
DONUT_MODEL_DIR = os.path.join(DONUT_DIR, "model")

# Create directories
for directory in [
    SCREENSHOTS_DIR, SCREENSHOTS_BY_QUERY_DIR,
    CROPPED_DIR, CROPPED_BY_QUERY_DIR,
    OCR_RESULTS_DIR, OCR_RESULTS_BY_QUERY_DIR,
    LOGS_DIR, TEMP_DIR, MODELS_DIR,
    DONUT_DIR, DONUT_TRAIN_DIR, DONUT_VAL_DIR, DONUT_MODEL_DIR
]:
    os.makedirs(directory, exist_ok=True)

# Browser settings
HEADLESS = True  # Run browser in headless mode
SCREENSHOT_WIDTH = 1920
SCREENSHOT_HEIGHT = 1080
DEFAULT_BROWSER_TIMEOUT = 30  # seconds

# OCR settings - Optimized for Hebrew product text recognition
TESSERACT_CMD = r'tesseract'  # Update if tesseract is in a different location
# Best configuration based on testing: LSTM neural network + uniform block text
TESSERACT_CONFIG = r'--oem 3 --psm 6 -l heb+eng'
# Alternative PSM modes for different text layouts
TESSERACT_PSM_MODES = [3, 6, 8, 11]  # auto, uniform block, single word, sparse text
# High contrast threshold for confidence filtering
TESSERACT_MIN_CONFIDENCE = 70

# API extraction settings
API_MAX_RETRIES = 3
API_RETRY_DELAY = 2  # seconds

def get_query_paths(query):
    """
    Get all paths for a specific query
    
    Args:
        query (str): Search query
        
    Returns:
        dict: Dictionary of paths
    """
    # Handle spaces and special characters in query for directory naming
    query_dir = query.replace(' ', '_')
    
    # Screenshots paths
    screenshots_dir = os.path.join(SCREENSHOTS_BY_QUERY_DIR, query_dir)
    
    # Cropped paths
    cropped_dir = os.path.join(CROPPED_BY_QUERY_DIR, query_dir)
    
    # OCR paths
    ocr_dir = os.path.join(OCR_RESULTS_BY_QUERY_DIR, query_dir)
    timestamp = logger.handlers[0].formatter.converter().strftime("%Y%m%d_%H%M%S")
    ocr_csv = os.path.join(ocr_dir, f"ocr_results_{query_dir}_{timestamp}.csv")
    
    return {
        "query": query,
        "screenshots_dir": screenshots_dir,
        "cropped_dir": cropped_dir,
        "ocr_dir": ocr_dir,
        "ocr_csv": ocr_csv
    }

def ensure_dir(directory):
    """
    Ensure a directory exists
    
    Args:
        directory (str): Directory path
    """
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        logger.info(f"Created directory: {directory}")
