#!/usr/bin/env python3
"""
db_query.py - Utility for querying the price comparison database
"""

import argparse
import json
from db_config import get_products_collection, get_retailers_collection
from pymongo import ASCENDING, DESCENDING
from tabulate import tabulate
from datetime import datetime, timedelta

def get_product_count():
    """Get total product count"""
    products = get_products_collection()
    return products.count_documents({})

def get_products_by_retailer():
    """Get product count by retailer"""
    products = get_products_collection()
    pipeline = [
        {"$group": {"_id": "$retailer", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    results = list(products.aggregate(pipeline))
    return results

def search_products(query, limit=10):
    """Search products by name or brand"""
    products = get_products_collection()
    
    # Use text search if available, otherwise use regex
    results = list(products.find(
        {"$or": [
            {"name": {"$regex": query, "$options": "i"}},
            {"brand": {"$regex": query, "$options": "i"}},
            {"name_he": {"$regex": query, "$options": "i"}} if "name_he" in products.find_one() else {}
        ]}
    ).limit(limit))
    
    return results

def compare_prices(product_name, limit=10):
    """Compare prices across retailers for similar products"""
    products = get_products_collection()
    
    # Search for products with similar name
    results = list(products.find(
        {"name": {"$regex": product_name, "$options": "i"}}
    ).sort("price", ASCENDING).limit(limit))
    
    return results

def get_recent_products(days=1, limit=20):
    """Get recently added products"""
    products = get_products_collection()
    
    # Calculate cutoff date
    cutoff = datetime.now() - timedelta(days=days)
    
    results = list(products.find(
        {"last_updated": {"$gte": cutoff}}
    ).sort("last_updated", DESCENDING).limit(limit))
    
    return results

def print_product_table(products):
    """Print products in a formatted table"""
    if not products:
        print("No products found")
        return
        
    # Prepare table data
    table_data = []
    for p in products:
        row = [
            p.get("product_id", ""),
            p.get("name", "")[:40] + "..." if len(p.get("name", "")) > 40 else p.get("name", ""),
            p.get("brand", ""),
            f"{p.get('price', 0)} {p.get('currency', '')}",
            p.get("retailer", ""),
            p.get("last_updated", "").strftime("%Y-%m-%d") if isinstance(p.get("last_updated"), datetime) else ""
        ]
        table_data.append(row)
    
    # Print table
    headers = ["ID", "Name", "Brand", "Price", "Retailer", "Updated"]
    print(tabulate(table_data, headers=headers, tablefmt="grid"))

def main():
    parser = argparse.ArgumentParser(description="Query the price comparison database")
    
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Show database statistics")
    
    # Search command
    search_parser = subparsers.add_parser("search", help="Search for products")
    search_parser.add_argument("query", help="Search query")
    search_parser.add_argument("--limit", type=int, default=10, help="Result limit")
    
    # Compare command
    compare_parser = subparsers.add_parser("compare", help="Compare prices")
    compare_parser.add_argument("product", help="Product name to compare")
    compare_parser.add_argument("--limit", type=int, default=10, help="Result limit")
    
    # Recent command
    recent_parser = subparsers.add_parser("recent", help="Show recent products")
    recent_parser.add_argument("--days", type=int, default=1, help="Days to look back")
    recent_parser.add_argument("--limit", type=int, default=20, help="Result limit")
    
    args = parser.parse_args()
    
    # Execute commands
    if args.command == "stats":
        total = get_product_count()
        by_retailer = get_products_by_retailer()
        
        print(f"Total products: {total}")
        print("\nProducts by retailer:")
        for item in by_retailer:
            print(f"  {item['_id']}: {item['count']}")
    
    elif args.command == "search":
        results = search_products(args.query, args.limit)
        print(f"Search results for '{args.query}':")
        print_product_table(results)
    
    elif args.command == "compare":
        results = compare_prices(args.product, args.limit)
        print(f"Price comparison for '{args.product}':")
        print_product_table(results)
    
    elif args.command == "recent":
        results = get_recent_products(args.days, args.limit)
        print(f"Products added/updated in the last {args.days} days:")
        print_product_table(results)
    
    else:
        parser.print_help()

if __name__ == "__main__":
    main()