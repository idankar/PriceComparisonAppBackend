#!/usr/bin/env python3
"""
Inspect the structure of pharmacy data files to understand where store info is located
"""

import os
import gzip
import tempfile
import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from urllib3.exceptions import InsecureRequestWarning

# Suppress SSL warnings
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def inspect_super_pharm():
    """Download and inspect Super-Pharm file structure"""
    print("SUPER-PHARM FILE STRUCTURE ANALYSIS")
    print("=" * 80)

    session = requests.Session()
    session.verify = False

    base_url = 'https://prices.super-pharm.co.il'

    try:
        # Get file list
        response = session.get(base_url, timeout=30)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find PriceFull files
        links = soup.find_all('a', href=True)
        price_files = [link['href'] for link in links if 'PriceFull' in link['href']]

        if not price_files:
            print("No PriceFull files found")
            return

        # Download first PriceFull file
        first_file = price_files[0]
        filename = first_file.split('?')[0].split('/')[-1]
        file_url = base_url + '/' + first_file if not first_file.startswith('http') else first_file

        print(f"Downloading: {filename}")

        file_response = session.get(file_url, timeout=60)

        if file_response.status_code != 200 or len(file_response.content) == 0:
            print(f"Failed to download file (status: {file_response.status_code}, size: {len(file_response.content)})")
            return

        print(f"Downloaded {len(file_response.content):,} bytes")
        print()

        # Decompress
        try:
            content = gzip.decompress(file_response.content).decode('utf-8')
            print(f"Decompressed to {len(content):,} characters")
        except:
            content = file_response.content.decode('utf-8')
            print(f"File was not compressed, size: {len(content):,} characters")

        print()

        # Parse XML
        root = ET.fromstring(content)

        print(f"Root Tag: <{root.tag}>")
        print(f"Root Attributes: {root.attrib}")
        print()

        # Show structure
        print("XML Structure (first 3 levels):")
        print("-" * 80)

        for i, child in enumerate(list(root)[:3]):
            print(f"\\n[{i+1}] <{child.tag}>")
            if child.attrib:
                print(f"    Attributes: {child.attrib}")

            for j, subchild in enumerate(list(child)[:10]):
                value = (subchild.text[:50] + '...') if subchild.text and len(subchild.text) > 50 else subchild.text
                print(f"    <{subchild.tag}>: {value}")

        print()
        print("-" * 80)

        # Look specifically for Store elements
        print("\\nSearching for Store information...")
        stores = root.findall('.//Store')
        stores2 = root.findall('.//STORE')
        stores3 = root.findall('.//store')

        print(f"  .//Store: {len(stores)} elements")
        print(f"  .//STORE: {len(stores2)} elements")
        print(f"  .//store: {len(stores3)} elements")

        if stores or stores2 or stores3:
            store_elem = (stores + stores2 + stores3)[0]
            print("\\nFirst Store element structure:")
            for child in store_elem:
                print(f"  <{child.tag}>: {child.text}")

        # Check for StoreId at root level
        print("\\nChecking for StoreId/ChainId in root attributes...")
        for key, value in root.attrib.items():
            print(f"  {key}: {value}")

        # Check first Product/Item for store reference
        print("\\nChecking first item for store reference...")
        items = root.findall('.//Item') or root.findall('.//Product') or list(root)[0:1]
        if items:
            first_item = items[0]
            print(f"First item tag: <{first_item.tag}>")
            for child in first_item:
                if 'store' in child.tag.lower():
                    print(f"  *** {child.tag}: {child.text}")

        print()
        print("=" * 80)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

def inspect_good_pharm():
    """Download and inspect Good Pharm file structure"""
    print("\\n\\nGOOD PHARM FILE STRUCTURE ANALYSIS")
    print("=" * 80)

    # Similar structure to Super-Pharm
    print("(Good Pharm uses similar XML structure to Super-Pharm)")

if __name__ == "__main__":
    inspect_super_pharm()
    # inspect_good_pharm()  # Can add if needed
