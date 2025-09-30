#!/usr/bin/env python3
"""
Debug script to check Super-Pharm file structure in detail
"""

import requests
import gzip
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
})

print("Fetching Super-Pharm file...")
response = session.get("https://prices.super-pharm.co.il/")
soup = BeautifulSoup(response.text, 'html.parser')

links = soup.find_all('a')
for link in links:
    href = link.get('href', '')
    if 'PriceFull' in href and href.endswith('.gz'):
        file_url = href
        break

print(f"Downloading: {file_url}")
full_url = f"https://prices.super-pharm.co.il/{file_url}"

response = session.get(full_url)
if response.status_code == 200:
    content = gzip.decompress(response.content)

    # Parse XML
    root = ET.fromstring(content)

    print(f"\nRoot tag: {root.tag}")
    print(f"Root attributes: {root.attrib}")

    # Check all possible paths
    items_paths = [
        './/Item',
        './/Items/Item',
        './/Products/Product',
        './/Product',
        './Items/Item',
        'Items/Item',
        'Item'
    ]

    for path in items_paths:
        items = root.findall(path)
        if items:
            print(f"\nFound {len(items)} items at path: {path}")
            break

    # Show structure
    print("\nXML Structure (first 3 levels):")
    def show_structure(element, indent=0):
        if indent > 2:
            return
        print("  " * indent + f"{element.tag}: {element.text[:30] if element.text and element.text.strip() else ''}")
        for child in list(element)[:5]:  # First 5 children
            show_structure(child, indent + 1)

    show_structure(root)

    # Try to find Items container
    items_container = root.find('.//Items')
    if items_container is not None:
        print(f"\nItems container attributes: {items_container.attrib}")
        items_in_container = items_container.findall('Item')
        print(f"Items in container: {len(items_in_container)}")

        if items_in_container:
            print("\nFirst item structure:")
            first_item = items_in_container[0]
            for child in first_item:
                print(f"  {child.tag}: {child.text}")
    else:
        print("\nNo Items container found")

    # Raw XML sample
    print("\nRaw XML (first 1000 chars):")
    print(content.decode('utf-8')[:1000])