#!/usr/bin/env python3
"""
Download StoresFull files from pharmacy government portals
"""

import os
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import gzip

# Suppress SSL warnings
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

DOWNLOAD_DIR = '/Users/idankarbat/Documents/noa_recovery/Desktop/PriceComparisonApp/sample_files'

def download_super_pharm_stores():
    """Download Super-Pharm StoresFull file"""
    print("Downloading Super-Pharm StoresFull file...")

    base_url = 'https://prices.super-pharm.co.il'

    try:
        # Fetch the file list page
        response = requests.get(base_url, verify=False, timeout=30)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all file links
        links = soup.find_all('a', href=True)
        stores_files = []

        for link in links:
            href = link['href']
            if 'StoresFull' in href or 'Stores' in href:
                full_url = urljoin(base_url, href)
                stores_files.append({
                    'url': full_url,
                    'filename': href.split('/')[-1]
                })

        if stores_files:
            # Download the first/latest stores file
            file_info = stores_files[0]
            print(f"  Found: {file_info['filename']}")

            file_response = requests.get(file_info['url'], verify=False, timeout=60)

            output_path = os.path.join(DOWNLOAD_DIR, f"SuperPharm_{file_info['filename']}")
            with open(output_path, 'wb') as f:
                f.write(file_response.content)

            print(f"  ✓ Downloaded to: {output_path}")
            return output_path
        else:
            print("  ✗ No StoresFull file found")
            return None

    except Exception as e:
        print(f"  ✗ Error: {e}")
        return None

def download_good_pharm_stores():
    """Download Good Pharm StoresFull file"""
    print("Downloading Good Pharm StoresFull file...")

    base_url = 'https://goodpharm.binaprojects.com'

    try:
        # Fetch the file list page
        response = requests.get(base_url, verify=False, timeout=30)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all file links
        links = soup.find_all('a', href=True)
        stores_files = []

        for link in links:
            href = link['href']
            if 'StoresFull' in href or 'Stores' in href:
                full_url = urljoin(base_url, href)
                stores_files.append({
                    'url': full_url,
                    'filename': href.split('/')[-1]
                })

        if stores_files:
            # Download the first/latest stores file
            file_info = stores_files[0]
            print(f"  Found: {file_info['filename']}")

            file_response = requests.get(file_info['url'], verify=False, timeout=60)

            output_path = os.path.join(DOWNLOAD_DIR, f"GoodPharm_{file_info['filename']}")
            with open(output_path, 'wb') as f:
                f.write(file_response.content)

            print(f"  ✓ Downloaded to: {output_path}")
            return output_path
        else:
            print("  ✗ No StoresFull file found")
            return None

    except Exception as e:
        print(f"  ✗ Error: {e}")
        return None

def download_be_pharm_stores():
    """Download Be Pharm StoresFull file from Shufersal portal"""
    print("Downloading Be Pharm (Shufersal) StoresFull file...")

    base_url = 'https://prices.shufersal.co.il'

    try:
        # Fetch the file list page
        response = requests.get(base_url, verify=False, timeout=30)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all file links
        links = soup.find_all('a', href=True)
        stores_files = []

        for link in links:
            href = link['href']
            # Look for Shufersal StoresFull files (ChainId 7290027600007)
            if 'StoresFull' in href and '7290027600007' in href:
                full_url = urljoin(base_url, href)
                stores_files.append({
                    'url': full_url,
                    'filename': href.split('/')[-1]
                })

        if stores_files:
            # Download the first/latest stores file
            file_info = stores_files[0]
            print(f"  Found: {file_info['filename']}")

            file_response = requests.get(file_info['url'], verify=False, timeout=60)

            output_path = os.path.join(DOWNLOAD_DIR, f"BePharm_Shufersal_{file_info['filename']}")
            with open(output_path, 'wb') as f:
                f.write(file_response.content)

            print(f"  ✓ Downloaded to: {output_path}")
            return output_path
        else:
            print("  ✗ No StoresFull file found")
            return None

    except Exception as e:
        print(f"  ✗ Error: {e}")
        return None

def main():
    """Download all StoresFull files"""
    print("DOWNLOADING STORESFULL FILES")
    print("=" * 70)
    print()

    # Create download directory
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    print(f"Download directory: {DOWNLOAD_DIR}")
    print()

    # Download from each portal
    results = {
        'Super-Pharm': download_super_pharm_stores(),
        'Good Pharm': download_good_pharm_stores(),
        'Be Pharm': download_be_pharm_stores()
    }

    print()
    print("=" * 70)
    print("DOWNLOAD SUMMARY")
    print("=" * 70)
    for retailer, path in results.items():
        status = "✓ Success" if path else "✗ Failed"
        print(f"{retailer:<20} | {status}")
        if path:
            print(f"  → {path}")

    return results

if __name__ == "__main__":
    main()
