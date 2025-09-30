#!/usr/bin/env python3
"""
Debug script to check what's in Be Pharm files from Shufersal portal
"""

import requests
import gzip
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup

# Get the file list from Shufersal
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
})

BE_PHARM_CHAIN_ID = "7290027600007"

print(f"Fetching file list from Shufersal for Be Pharm (chain {BE_PHARM_CHAIN_ID})...")
response = session.get("https://prices.shufersal.co.il/")

if response.status_code == 200:
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find Be Pharm files
    links = soup.find_all('a')
    be_pharm_files = []

    for link in links:
        href = link.get('href', '')
        if BE_PHARM_CHAIN_ID in href and 'PriceFull' in href and href.endswith('.gz'):
            be_pharm_files.append(href)
            if len(be_pharm_files) >= 3:
                break

    print(f"Found {len(be_pharm_files)} Be Pharm price files")

    if be_pharm_files:
        # Test first file
        file_url = be_pharm_files[0]
        print(f"\nDownloading: {file_url}")

        full_url = f"https://prices.shufersal.co.il/{file_url}"
        response = session.get(full_url)

        if response.status_code == 200:
            # Decompress
            content = gzip.decompress(response.content)

            # Parse XML
            root = ET.fromstring(content)

            # Find items
            items = root.findall('.//Item')
            products = root.findall('.//Product')

            print(f"  Found {len(items)} Item elements")
            print(f"  Found {len(products)} Product elements")

            # Try different paths
            if len(items) == 0 and len(products) > 0:
                items = products

            # Show first few items
            for i, item in enumerate(items[:5]):
                item_code = item.find('.//ItemCode') or item.find('.//ProductCode')
                item_name = item.find('.//ItemName') or item.find('.//ProductName')
                item_price = item.find('.//ItemPrice') or item.find('.//ProductPrice')

                print(f"\n  Item {i+1}:")
                if item_code is not None:
                    print(f"    Code: {item_code.text}")
                if item_name is not None:
                    print(f"    Name: {item_name.text}")
                if item_price is not None:
                    print(f"    Price: {item_price.text}")

            # Check structure
            print("\n  XML structure:")
            for child in list(root)[:3]:
                print(f"    {child.tag}")
                for subchild in list(child)[:5]:
                    text = subchild.text[:30] if subchild.text else 'None'
                    print(f"      {subchild.tag}: {text}")
        else:
            print(f"  Failed to download: {response.status_code}")
    else:
        print("No Be Pharm files found on Shufersal")
else:
    print(f"Failed to fetch file list: {response.status_code}")