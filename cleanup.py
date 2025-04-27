#!/usr/bin/env python3
"""
cleanup.py - Script to clear previously collected product data
"""

import os
import shutil
import argparse

def cleanup_data(data_dir='data/products', keep_logs=False, keep_manifests=False):
    """
    Clean up the data directory to start a fresh data extraction.
    
    Args:
        data_dir (str): Path to the products data directory
        keep_logs (bool): If True, preserve log files
        keep_manifests (bool): If True, preserve manifest files
    """
    if not os.path.exists(data_dir):
        print(f"No data directory found at {data_dir}. Nothing to clean.")
        return
    
    # Remove product directories
    count = 0
    for item in os.listdir(data_dir):
        item_path = os.path.join(data_dir, item)
        
        # Skip logs if requested
        if keep_logs and item.endswith('.log'):
            continue
            
        # Skip manifests if requested
        if keep_manifests and item.endswith('.json') and 'manifest' in item.lower():
            continue
            
        # Remove directories and other files
        if os.path.isdir(item_path):
            shutil.rmtree(item_path)
            count += 1
        else:
            # For non-directory items that aren't being kept
            os.remove(item_path)
            
    print(f"Removed {count} product directories and cleaned up data files.")
    
    # Ensure the base directory structure exists
    os.makedirs(data_dir, exist_ok=True)
    print(f"Ready for fresh data extraction in {data_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Clean up product data for fresh extraction')
    parser.add_argument('--data-dir', default='data/products', help='Path to the products data directory')
    parser.add_argument('--keep-logs', action='store_true', help='Keep log files')
    parser.add_argument('--keep-manifests', action='store_true', help='Keep manifest files')
    
    args = parser.parse_args()
    
    # Confirm before deleting
    print(f"This will remove all product data from {args.data_dir}")
    confirm = input("Are you sure you want to continue? (y/n): ")
    
    if confirm.lower() == 'y':
        cleanup_data(args.data_dir, args.keep_logs, args.keep_manifests)
    else:
        print("Operation cancelled.")