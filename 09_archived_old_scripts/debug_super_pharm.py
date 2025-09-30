#!/usr/bin/env python3
"""
Debug script to check what's in Super-Pharm files
"""

import requests
import gzip
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup

# Get the file list
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
})

print("Fetching file list from Super-Pharm...")
response = session.get("https://prices.super-pharm.co.il/")
soup = BeautifulSoup(response.text, 'html.parser')

# Find price files
links = soup.find_all('a')
price_files = []
for link in links:
    href = link.get('href', '')
    if 'PriceFull' in href and href.endswith('.gz'):
        price_files.append(href)
        if len(price_files) >= 3:  # Get first 3 files
            break

print(f"Found {len(price_files)} price files to test")

for file_url in price_files[:1]:  # Test first file
    print(f"\nDownloading: {file_url}")
    full_url = f"https://prices.super-pharm.co.il/{file_url}"

    response = session.get(full_url)
    if response.status_code == 200:
        # Decompress
        content = gzip.decompress(response.content)

        # Parse XML
        root = ET.fromstring(content)

        # Find items
        items = root.findall('.//Item')
        print(f"  Found {len(items)} items in file")

        # Show first few items
        for i, item in enumerate(items[:5]):
            item_code = item.find('.//ItemCode')
            item_name = item.find('.//ItemName')
            item_price = item.find('.//ItemPrice')

            if item_code is not None and item_name is not None and item_price is not None:
                print(f"  Item {i+1}:")
                print(f"    Code: {item_code.text}")
                print(f"    Name: {item_name.text}")
                print(f"    Price: {item_price.text}")
            else:
                print(f"  Item {i+1}: Missing data")
                print(f"    Code element: {item_code}")
                print(f"    Name element: {item_name}")
                print(f"    Price element: {item_price}")

        # Check structure
        print("\n  XML structure sample:")
        for child in root[:2]:
            print(f"    Root child: {child.tag}")
            for subchild in child[:3]:
                print(f"      {subchild.tag}: {subchild.text[:50] if subchild.text else 'None'}")
    else:
        print(f"  Failed to download: {response.status_code}")