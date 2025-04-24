# cleanup.py
import os
import logging
import shutil

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def cleanup_temp_files():
    """Clean up unwanted or temporary files from previous runs"""
    
    # Base directory
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Files to remove
    files_to_remove = [
        "process_many.log",
        "batch_processor.log"
    ]
    
    # Remove log files
    for file in files_to_remove:
        file_path = os.path.join(base_dir, file)
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.info(f"Removed file: {file_path}")
            except Exception as e:
                logger.error(f"Error removing file {file_path}: {str(e)}")
    
    # Stop the previous background process if it's still running
    try:
        with open('process_many.pid', 'r') as f:
            pid = f.read().strip()
            os.system(f"kill {pid} 2>/dev/null")
            logger.info(f"Terminated process with PID: {pid}")
        os.remove('process_many.pid')
    except:
        logger.info("No previous process PID file found")
    
    # Get user confirmation before removing data directories
    logger.info("Do you want to remove all previously collected data? (yes/no)")
    confirmation = input().strip().lower()
    
    if confirmation in ['yes', 'y']:
        # Directories to clean
        dirs_to_clean = [
            os.path.join(base_dir, "data", "ocr_results", "by_query"),
            os.path.join(base_dir, "data", "cropped", "by_query"),
        ]
        
        for dir_path in dirs_to_clean:
            if os.path.exists(dir_path):
                try:
                    # List subdirectories
                    query_dirs = [d for d in os.listdir(dir_path) if os.path.isdir(os.path.join(dir_path, d))]
                    
                    for query_dir in query_dirs:
                        query_path = os.path.join(dir_path, query_dir)
                        shutil.rmtree(query_path)
                        logger.info(f"Removed directory: {query_path}")
                except Exception as e:
                    logger.error(f"Error cleaning directory {dir_path}: {str(e)}")
        
        # Reset the master CSV file
        master_csv = os.path.join(base_dir, "data", "ocr_results", "master_ocr_results.csv")
        if os.path.exists(master_csv):
            try:
                os.remove(master_csv)
                logger.info(f"Removed master CSV: {master_csv}")
            except Exception as e:
                logger.error(f"Error removing master CSV {master_csv}: {str(e)}")
    else:
        logger.info("Skipping data removal")
    
    # Create directories for the new approach
    dirs_to_create = [
        os.path.join(base_dir, "data", "results"),
        os.path.join(base_dir, "data", "images")
    ]
    
    for dir_path in dirs_to_create:
        os.makedirs(dir_path, exist_ok=True)
        logger.info(f"Created directory: {dir_path}")
    
    logger.info("Cleanup complete!")

if __name__ == "__main__":
    cleanup_temp_files()