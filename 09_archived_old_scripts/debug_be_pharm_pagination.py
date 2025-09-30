#!/usr/bin/env python3
"""
Debug script to find Be Pharm files by paginating through Shufersal
"""

import requests
from bs4 import BeautifulSoup
import time

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
})

BE_PHARM_CHAIN_ID = "7290027600007"

print(f"Searching for Be Pharm files (chain {BE_PHARM_CHAIN_ID}) across all pages...")

be_pharm_files = []
page = 1
max_pages = 200  # Search more pages

while page <= max_pages and len(be_pharm_files) == 0:
    print(f"\rChecking page {page}/{max_pages}...", end='', flush=True)

    url = f"https://prices.shufersal.co.il/?page={page}"
    try:
        response = session.get(url, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            links = soup.find_all('a')

            for link in links:
                href = link.get('href', '')
                if BE_PHARM_CHAIN_ID in href and 'PriceFull' in href and href.endswith('.gz'):
                    be_pharm_files.append(href)
                    print(f"\n  Found Be Pharm file on page {page}: {href}")
                    if len(be_pharm_files) >= 5:  # Stop after finding 5 files
                        break
    except:
        pass

    if len(be_pharm_files) > 0:
        break

    page += 1
    time.sleep(0.5)  # Be polite

print(f"\n\nTotal Be Pharm files found: {len(be_pharm_files)}")
if be_pharm_files:
    print("First few files:")
    for f in be_pharm_files[:5]:
        print(f"  - {f}")
else:
    print("No Be Pharm files found in first 200 pages")