#!/usr/bin/env python3
"""
db_config.py - MongoDB connection configuration for the Price Comparison App
"""

from pymongo import MongoClient
import os

# MongoDB connection settings
MONGODB_URI = os.environ.get("MONGODB_URI", "mongodb://localhost:27017/")
DATABASE_NAME = os.environ.get("MONGODB_DB", "price_comparison")

# Connection client (shared across functions)
_client = None

def get_database():
    """Get MongoDB database connection"""
    global _client
    if _client is None:
        _client = MongoClient(MONGODB_URI)
    return _client[DATABASE_NAME]

def get_products_collection():
    """Get products collection"""
    return get_database()["products"]

def get_retailers_collection():
    """Get retailers collection"""
    return get_database()["retailers"]

def get_categories_collection():
    """Get categories collection"""
    return get_database()["categories"]

def get_prices_collection():
    """Get prices collection"""
    return get_database()["prices"]

def close_connection():
    """Close MongoDB connection"""
    global _client
    if _client is not None:
        _client.close()
        _client = None