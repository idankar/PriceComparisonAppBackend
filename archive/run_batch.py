# run_batch.py
import argparse
import json
import logging
import time
import sys
from simple_api_extractor import process_multiple_queries

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("batch_run.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process product queries in batches")
    parser.add_argument("--queries-file", help="JSON file containing product queries", default="product_queries.json")
    parser.add_argument("--batch-size", type=int, help="Number of queries to process in each batch", default=5)
    parser.add_argument("--delay", type=int, help="Delay between queries in seconds", default=10)
    parser.add_argument("--start-idx", type=int, help="Start index in the queries file", default=0)
    parser.add_argument("--end-idx", type=int, help="End index in the queries file", default=None)
    parser.add_argument("--max-pages", type=int, help="Maximum pages to fetch per query", default=5)
    
    args = parser.parse_args()
    
    # Load queries from file
    try:
        with open(args.queries_file, 'r', encoding='utf-8') as f:
            all_queries = json.load(f)
        
        # Apply start and end indices
        queries = all_queries[args.start_idx:args.end_idx]
        logger.info(f"Loaded {len(queries)} queries from {args.queries_file} (indices {args.start_idx}:{args.end_idx or 'end'})")
        
        # Process the queries
        process_multiple_queries(
            queries, 
            max_pages=args.max_pages, 
            delay_between_queries=args.delay
        )
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)