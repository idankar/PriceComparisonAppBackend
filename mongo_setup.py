import os
import argparse
import logging
from pymongo import MongoClient, ASCENDING, TEXT, DESCENDING

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("mongodb_setup.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("MongoDBSetup")

def setup_database(mongodb_uri="mongodb://localhost:27017/", db_name="price_comparison"):
    """
    Setup MongoDB database with required collections and indexes
    
    Args:
        mongodb_uri: MongoDB connection string
        db_name: Database name
    """
    try:
        # Connect to MongoDB
        client = MongoClient(mongodb_uri)
        db = client[db_name]
        logger.info(f"Connected to MongoDB at {mongodb_uri}")
        
        # Setup products collection
        products_collection = db["products"]
        products_collection.create_index([("product_id", ASCENDING), ("source", ASCENDING)], unique=True)
        products_collection.create_index([("name", TEXT)])
        products_collection.create_index([("brand", ASCENDING)])
        products_collection.create_index([("updated_at", DESCENDING)])
        logger.info("Products collection setup complete with indexes")
        
        # Setup categories collection
        categories_collection = db["categories"]
        categories_collection.create_index([("category_id", ASCENDING), ("source", ASCENDING)], unique=True)
        categories_collection.create_index([("name", TEXT)])
        logger.info("Categories collection setup complete with indexes")
        
        # Setup prices collection
        prices_collection = db["prices"]
        prices_collection.create_index([("product_id", ASCENDING), ("source", ASCENDING), ("timestamp", DESCENDING)])
        prices_collection.create_index([("timestamp", DESCENDING)])
        logger.info("Prices collection setup complete with indexes")
        
        # Return success
        logger.info(f"Database {db_name} setup complete")
        return True
        
    except Exception as e:
        logger.error(f"Error setting up database: {e}")
        return False
    finally:
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")

def clean_database(mongodb_uri="mongodb://localhost:27017/", db_name="price_comparison"):
    """
    Clean/reset MongoDB database (for testing purposes)
    
    Args:
        mongodb_uri: MongoDB connection string
        db_name: Database name
    """
    try:
        # Connect to MongoDB
        client = MongoClient(mongodb_uri)
        db = client[db_name]
        logger.info(f"Connected to MongoDB at {mongodb_uri}")
        
        # Drop collections
        db["products"].drop()
        db["categories"].drop()
        db["prices"].drop()
        
        logger.info(f"Database {db_name} has been reset")
        return True
        
    except Exception as e:
        logger.error(f"Error cleaning database: {e}")
        return False
    finally:
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")

def main():
    """Main function to set up or clean the database based on command line arguments"""
    parser = argparse.ArgumentParser(description="Setup or clean MongoDB database for Price Comparison App")
    parser.add_argument("--clean", action="store_true", help="Clean/reset the database (for testing)")
    parser.add_argument("--uri", default=os.getenv("MONGODB_URI", "mongodb://localhost:27017/"), help="MongoDB connection URI")
    parser.add_argument("--db", default=os.getenv("MONGODB_DB", "price_comparison"), help="Database name")
    
    args = parser.parse_args()
    
    if args.clean:
        logger.warning("Cleaning database - this will remove all data!")
        clean_database(args.uri, args.db)
    else:
        logger.info("Setting up database")
        setup_database(args.uri, args.db)


if __name__ == "__main__":
    main()