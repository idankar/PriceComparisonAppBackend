#!/usr/bin/env python3
"""
Cleanup script for PriceComparisonApp

This script cleans up the data directory and MongoDB database
before starting a fresh data extraction.
"""

import os
import shutil
from pathlib import Path
import time
import logging
from pymongo import MongoClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("cleanup.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
project_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(project_dir, "data")
backup_dir = os.path.join(project_dir, "data_backup_" + time.strftime("%Y%m%d_%H%M%S"))

def cleanup_filesystem():
    """Clean up the data directory while preserving structure"""
    # Create a backup (optional)
    logger.info(f"Creating backup in {backup_dir}...")
    if os.path.exists(data_dir):
        shutil.copytree(data_dir, backup_dir)
        logger.info("Backup completed.")

    # Directories to create/preserve
    directories = [
        "products",
        "raw/shufersal",
        "train_manifests",
        "images"
    ]

    # Clean up existing data
    logger.info("Cleaning data directories...")
    for directory in directories:
        dir_path = os.path.join(data_dir, directory)
        if os.path.exists(dir_path):
            # Remove contents but keep directory
            for item in os.listdir(dir_path):
                item_path = os.path.join(dir_path, item)
                if os.path.isfile(item_path):
                    os.unlink(item_path)
                    logger.debug(f"Removed file: {item_path}")
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                    logger.debug(f"Removed directory: {item_path}")
            logger.info(f"Cleaned {dir_path}")
        else:
            # Create directory
            os.makedirs(dir_path, exist_ok=True)
            logger.info(f"Created {dir_path}")

    # Remove summary files
    summary_files = [f for f in os.listdir(data_dir) if f.endswith('.json')]
    for file in summary_files:
        file_path = os.path.join(data_dir, file)
        if os.path.isfile(file_path):
            os.unlink(file_path)
            logger.info(f"Removed {file_path}")

    logger.info("Data directory cleaned successfully!")

def cleanup_mongodb():
    """Clean up the MongoDB database"""
    logger.info("Cleaning MongoDB collections...")
    
    try:
        # Connect to MongoDB
        client = MongoClient("mongodb://localhost:27017/")
        db = client["price_comparison"]
        
        # Get counts before deletion
        products_count = db.products.count_documents({})
        files_count = db.product_images.files.count_documents({})
        chunks_count = db.product_images.chunks.count_documents({})
        
        logger.info(f"Before cleanup: {products_count} products, {files_count} files, {chunks_count} chunks")
        
        # Clear collections
        db.products.delete_many({})
        db.product_images.files.delete_many({})
        db.product_images.chunks.delete_many({})
        
        # Verify deletion
        products_count_after = db.products.count_documents({})
        files_count_after = db.product_images.files.count_documents({})
        chunks_count_after = db.product_images.chunks.count_documents({})
        
        logger.info(f"After cleanup: {products_count_after} products, {files_count_after} files, {chunks_count_after} chunks")
        logger.info("MongoDB database cleared successfully!")
        
        client.close()
    except Exception as e:
        logger.error(f"Error cleaning MongoDB: {e}")
        raise

def main():
    """Main function to run cleanup operations"""
    try:
        logger.info("Starting cleanup process...")
        
        # Clean up filesystem
        cleanup_filesystem()
        
        # Clean up MongoDB
        cleanup_mongodb()
        
        logger.info("Cleanup completed successfully!")
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")

if __name__ == "__main__":
    main()