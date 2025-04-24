# In batch_generate_dataset.py

import argparse
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("batch_processing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load the product queries
def load_queries(queries_file='product_queries.json'):
    with open(queries_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def process_query_batch(queries, max_workers=3, delay_between_queries=5):
    """
    Process a batch of queries with parallel execution
    
    Args:
        queries (list): List of product queries
        max_workers (int): Maximum number of parallel workers
        delay_between_queries (int): Delay in seconds between queries to avoid rate limiting
        
    Returns:
        dict: Summary of results
    """
    logger.info(f"Starting batch processing with {len(queries)} queries")
    
    results = {
        "total": len(queries),
        "successful": 0,
        "failed": 0,
        "products_found": 0
    }
    
    # Import the API extractor
    from src.api_extractor import process_query
    
    def process_single_query(query):
        logger.info(f"Processing query: {query}")
        try:
            # Use the API extractor
            products = process_query(query)
            
            if products:
                logger.info(f"✅ Successfully extracted {len(products)} products for '{query}'")
                return {"success": True, "query": query, "products": products}
            else:
                logger.error(f"❌ No products extracted for query: {query}")
                return {"success": False, "query": query}
                
        except Exception as e:
            logger.error(f"❌ Error processing query '{query}': {str(e)}")
            return {"success": False, "query": query, "error": str(e)}
    
    # Process queries with parallel execution
    successful_queries = 0
    failed_queries = 0
    total_products = 0
    
    # Use tqdm for a progress bar
    with tqdm(total=len(queries)) as pbar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            
            # Submit all tasks
            for query in queries:
                futures.append(executor.submit(process_single_query, query))
                # Add slight delay to avoid overwhelming the API
                time.sleep(delay_between_queries)
            
            # Process results as they complete
            for future in futures:
                result = future.result()
                if result["success"]:
                    successful_queries += 1
                    if "products" in result:
                        total_products += len(result["products"])
                else:
                    failed_queries += 1
                
                pbar.update(1)
    
    # Update results summary
    results["successful"] = successful_queries
    results["failed"] = failed_queries
    results["products_found"] = total_products
    
    logger.info("=" * 60)
    logger.info(f"✅ Batch processing complete!")
    logger.info(f"Total queries: {results['total']}")
    logger.info(f"Successful queries: {results['successful']}")
    logger.info(f"Failed queries: {results['failed']}")
    logger.info(f"Total products detected: {results['products_found']}")
    logger.info("=" * 60)
    
    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch process product queries")
    parser.add_argument("--queries", nargs="+", help="List of product queries to process", default=[])
    parser.add_argument("--queries-file", help="JSON file containing product queries", default="product_queries.json")
    parser.add_argument("--max-workers", type=int, help="Maximum number of parallel workers", default=3)
    parser.add_argument("--delay", type=int, help="Delay between queries in seconds", default=5)
    parser.add_argument("--start-idx", type=int, help="Start index in the queries file", default=0)
    parser.add_argument("--end-idx", type=int, help="End index in the queries file", default=None)
    
    args = parser.parse_args()
    
    # Load queries based on arguments
    if args.queries:
        queries = args.queries
    elif hasattr(args, 'queries_file') and os.path.exists(args.queries_file):
        # Load queries from the JSON file
        try:
            with open(args.queries_file, 'r', encoding='utf-8') as f:
                all_queries = json.load(f)
        except Exception as e:
            logger.error(f"Error loading JSON file {args.queries_file}: {e}")
            sys.exit(1)
            
        # Apply start and end indices
        start_idx = args.start_idx
        end_idx = args.end_idx
        
        queries = all_queries[start_idx:end_idx]
        logger.info(f"Loaded {len(queries)} queries from {args.queries_file} (indices {start_idx}:{end_idx if end_idx is not None else 'end'})")
    else:
        # Default query behavior
        queries = ["נוטלה"]  # Default to Nutella
        logger.info("No queries provided via arguments or file, defaulting to Nutella.")
    
    # Process the queries
    process_query_batch(queries, max_workers=args.max_workers, delay_between_queries=args.delay)
    
    # Convert results to Donut format
    from src.dataset import convert_csv_to_donut_format, clean_donut_labels
    logger.info("🔄 Converting all results to Donut format")
    try:
        convert_csv_to_donut_format()
        clean_donut_labels()
        logger.info("✅ Successfully created Donut dataset")
    except Exception as e:
        logger.error(f"❌ Error creating Donut dataset: {str(e)}")