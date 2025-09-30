#!/usr/bin/env python3
"""Quick test to scrape Super-Pharm portal"""

import requests
from bs4 import BeautifulSoup
import gzip
import xml.etree.ElementTree as ET
import psycopg2
from datetime import datetime
import re
import os

# Database
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'price_comparison_app_v2',
    'user': 'postgres',
    'password': '***REMOVED***'
}

def test_super_pharm():
    print("Testing Super-Pharm portal scraping...")

    # Get page
    response = requests.get("https://prices.super-pharm.co.il/")
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find price files
    price_files = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        if 'Price' in href and '.gz' in href:
            price_files.append(href)

    print(f"Found {len(price_files)} price files")

    if price_files:
        # Test downloading first file
        test_url = "https://prices.super-pharm.co.il" + price_files[0]
        print(f"Testing download: {test_url}")

        response = requests.get(test_url)
        print(f"Download status: {response.status_code}")

        if response.status_code == 200:
            # Decompress
            content = gzip.decompress(response.content)

            # Parse XML
            root = ET.fromstring(content)

            # Count products
            products = root.findall('.//Product') or root.findall('.//Item')
            print(f"Found {len(products)} products in file")

            # Show sample product
            if products:
                product = products[0]
                print("\nSample product:")
                print(f"  Barcode: {product.findtext('ItemCode', 'N/A')}")
                print(f"  Name: {product.findtext('ItemName', 'N/A')}")
                print(f"  Price: {product.findtext('ItemPrice', 'N/A')}")

            # Test database connection
            try:
                conn = psycopg2.connect(**DB_CONFIG)
                cur = conn.cursor()

                # Check retailer
                cur.execute("""
                    SELECT retailerid, retailername
                    FROM retailers
                    WHERE chainid = '7290172900007'
                       OR LOWER(retailername) LIKE '%super%pharm%'
                """)
                result = cur.fetchone()

                if result:
                    print(f"\nFound Super-Pharm in DB: ID={result[0]}, Name={result[1]}")
                else:
                    print("\nSuper-Pharm not found in database")

                conn.close()

            except Exception as e:
                print(f"\nDatabase error: {e}")

    return price_files

if __name__ == "__main__":
    test_super_pharm()