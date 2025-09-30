#!/usr/bin/env python3
# price_comparison.py - Unified CLI for price comparison app

import os
import sys
import argparse
import logging
from datetime import datetime

# Add parent directory to path to allow imports from src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import local modules
from src.api.shufersal_api import extract_products, process_multiple_queries
from src.models.sqlite_database import SQLiteProductDatabase
from src.utils.image_utils import download_image, resize_image, augment_image

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/price_comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def collect_data(args):
    """Collect product data from online sources
    
    Args:
        args: Command-line arguments
    """
    logger.info("Collecting product data...")
    
    # Process product queries
    if args.query_file:
        # Load queries from file
        with open(args.query_file, 'r', encoding='utf-8') as f:
            queries = [line.strip() for line in f if line.strip()]
        
        # Process queries
        process_multiple_queries(
            queries=queries,
            max_pages=args.max_pages,
            delay_between_queries=args.delay
        )
        
    elif args.query:
        # Process single query
        extract_products(
            query=args.query,
            max_pages=args.max_pages,
            delay=args.delay
        )
    
    logger.info("Data collection complete")

def build_database(args):
    """Build the product database from collected data
    
    Args:
        args: Command-line arguments
    """
    logger.info("Building product database...")
    
    # Initialize database
    db = SQLiteProductDatabase(
        model_name=args.model,
        db_path=args.db_path
    )
    
    # Find product data files
    data_files = []
    
    if args.data_dir:
        # Walk through the data directory looking for JSON files
        for root, _, files in os.walk(args.data_dir):
            for file in files:
                if file.endswith('.json') and 'products_' in file:
                    data_files.append(os.path.join(root, file))
    
    if not data_files:
        logger.warning("No product data files found")
        return
    
    logger.info(f"Found {len(data_files)} product data files")
    
    # Process each data file
    import json
    total_products = 0
    
    for data_file in data_files:
        try:
            logger.info(f"Processing {data_file}")
            
            # Load the product data
            with open(data_file, 'r', encoding='utf-8') as f:
                products = json.load(f)
            
            # Add each product to the database
            for product in products:
                if product.get('image_path') and os.path.exists(product['image_path']):
                    # Try to extract English and Hebrew names
                    product_name = product.get('product_name', '')
                    if args.auto_translate and not product.get('product_name_en'):
                        # Simple heuristic for language detection
                        from src.utils.text_utils import is_hebrew
                        
                        if is_hebrew(product_name):
                            name_he = product_name
                            name_en = f"Product {product.get('code', 'unknown')}"  # Placeholder
                        else:
                            name_en = product_name
                            name_he = f"מוצר {product.get('code', 'unknown')}"  # Placeholder
                    else:
                        name_en = product.get('product_name_en', product_name)
                        name_he = product.get('product_name_he', product_name)
                    
                    # Add product to database
                    success = db.add_product(
                        product_id=product.get('code', f"product_{total_products}"),
                        image_path=product['image_path'],
                        name_en=name_en,
                        name_he=name_he,
                        brand=product.get('brand', None),
                        prices={
                            'Shufersal': float(product.get('price', 0)),
                        }
                    )
                    
                    if success:
                        total_products += 1
            
        except Exception as e:
            logger.error(f"Error processing {data_file}: {e}")
    
    logger.info(f"Database built with {total_products} products")

def search_products(args):
    """Search for products in the database
    
    Args:
        args: Command-line arguments
    """
    logger.info("Searching for products...")
    
    # Initialize database
    db = SQLiteProductDatabase(
        model_name=args.model,
        db_path=args.db_path
    )
    
    # Check if database is empty
    if db.get_product_count() == 0:
        logger.warning("Database is empty. Please build it first.")
        return
    
    # Perform search
    results = []
    
    if args.image:
        # Search by image
        if os.path.exists(args.image):
            logger.info(f"Searching by image: {args.image}")
            results = db.search_by_image(args.image, top_k=args.limit)
        else:
            logger.error(f"Image not found: {args.image}")
            return
    elif args.text:
        # Search by text
        logger.info(f"Searching by text: {args.text}")
        results = db.search_by_text(args.text, top_k=args.limit)
    else:
        logger.error("No search criteria provided")
        return
    
    # Display results
    if not results:
        logger.info("No results found")
        return
    
    logger.info(f"Found {len(results)} results:")
    
    for i, (product_id, similarity, product_info) in enumerate(results):
        print(f"\nMatch {i+1}:")
        print(f"  Product: {product_info['name_en']} / {product_info['name_he']}")
        if product_info.get('brand'):
            print(f"  Brand: {product_info['brand']}")
        print(f"  Similarity: {similarity:.4f}")
        
        # Print prices
        if product_info.get('prices'):
            print("  Prices:")
            for store, price in product_info['prices'].items():
                print(f"    {store}: ₪{price:.2f}")
        
        print(f"  Image: {product_info['image_path']}")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="PriceComparisonApp - Compare prices across Israeli supermarkets"
    )
    
    # Create subparsers for each command
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Collect command
    collect_parser = subparsers.add_parser('collect', help='Collect product data')
    collect_parser.add_argument('--query', help='Product query to search for')
    collect_parser.add_argument('--query-file', help='File containing product queries (one per line)')
    collect_parser.add_argument('--max-pages', type=int, default=5, help='Maximum number of pages to fetch')
    collect_parser.add_argument('--delay', type=int, default=1, help='Delay between requests')
    
    # Build command
    build_parser = subparsers.add_parser('build', help='Build product database')
    build_parser.add_argument('--data-dir', default='data/results', help='Directory containing product data')
    build_parser.add_argument('--db-path', default='data/database/products.db', help='Path to database file')
    build_parser.add_argument('--model', default='clip-ViT-B-16', help='Model to use for embeddings')
    build_parser.add_argument('--auto-translate', action='store_true', help='Automatically handle English/Hebrew')
    
    # Search command
    search_parser = subparsers.add_parser('search', help='Search for products')
    search_parser.add_argument('--image', help='Image to search with')
    search_parser.add_argument('--text', help='Text to search with')
    search_parser.add_argument('--db-path', default='data/database/products.db', help='Path to database file')
    search_parser.add_argument('--model', default='clip-ViT-B-16', help='Model to use for embeddings')
    search_parser.add_argument('--limit', type=int, default=5, help='Maximum number of results')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Run the appropriate command
    if args.command == 'collect':
        collect_data(args)
    elif args.command == 'build':
        build_database(args)
    elif args.command == 'search':
        search_products(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main() 