# batch_processor.py
import os
import sys
import json
import subprocess
import logging
import argparse
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("batch_processor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def process_batch(queries, batch_size=5, delay=60):
    """
    Process a batch of queries using the existing batch_generate_dataset.py script
    
    Args:
        queries (list): List of product queries
        batch_size (int): Number of queries to process in each batch
        delay (int): Delay in seconds between batches
    """
    logger.info(f"Processing {len(queries)} queries in batches of {batch_size}")
    
    # Process in batches
    for i in range(0, len(queries), batch_size):
        batch = queries[i:i+batch_size]
        batch_str = ' '.join([f'"{q}"' for q in batch])
        
        logger.info(f"Processing batch {i//batch_size + 1}/{(len(queries) + batch_size - 1)//batch_size}: {batch}")
        
        # Build the command
        cmd = f'python batch_generate_dataset.py --queries {batch_str}'
        
        # Run the command
        try:
            logger.info(f"Running command: {cmd}")
            subprocess.run(cmd, shell=True, check=True)
            logger.info(f"Batch {i//batch_size + 1} completed successfully")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error processing batch {i//batch_size + 1}: {str(e)}")
        
        # Add delay between batches to avoid overwhelming the API
        if i + batch_size < len(queries):
            logger.info(f"Waiting {delay} seconds before next batch...")
            time.sleep(delay)
    
    logger.info("All batches completed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process product queries in batches")
    parser.add_argument("--queries-file", help="JSON file containing product queries", default="product_queries.json")
    parser.add_argument("--batch-size", type=int, help="Number of queries to process in each batch", default=5)
    parser.add_argument("--delay", type=int, help="Delay in seconds between batches", default=60)
    parser.add_argument("--start-idx", type=int, help="Start index in the queries file", default=0)
    parser.add_argument("--end-idx", type=int, help="End index in the queries file", default=None)
    
    args = parser.parse_args()
    
    # Load queries from file
    try:
        with open(args.queries_file, 'r', encoding='utf-8') as f:
            all_queries = json.load(f)
        
        # Apply start and end indices
        queries = all_queries[args.start_idx:args.end_idx]
        logger.info(f"Loaded {len(queries)} queries from {args.queries_file} (indices {args.start_idx}:{args.end_idx or 'end'})")
        
        # Process the queries
        process_batch(queries, batch_size=args.batch_size, delay=args.delay)
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)