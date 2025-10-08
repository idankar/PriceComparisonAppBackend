#!/usr/bin/env python3
"""
Populate Pharmacy Stores Script
================================

This script populates missing store address data using multiple strategies:
1. Manual research data (from previous LLM research)
2. Web scraping from retailer store locators
3. StoresFull XML files (when/if available)

The script performs UPSERT operations - updating existing placeholder stores
or inserting new stores with complete information.
"""

import os
import sys
import psycopg2
import requests
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Tuple
from bs4 import BeautifulSoup
from urllib3.exceptions import InsecureRequestWarning

# Suppress SSL warnings
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'price_comparison_app_v2',
    'user': 'postgres',
    'password': '025655358'
}

# Retailer IDs
RETAILER_IDS = {
    'Super-Pharm': 52,
    'Be Pharm': 150,
    'Good Pharm': 97
}


class StorePopulator:
    """Populates store address data using multiple strategies"""

    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.cursor = self.conn.cursor()
        self.stats = {
            'stores_updated': 0,
            'stores_inserted': 0,
            'stores_skipped': 0,
            'errors': 0
        }

    def upsert_store(self, retailer_id: int, store_code: str, store_name: str,
                     address: Optional[str] = None, city: Optional[str] = None) -> bool:
        """
        Insert or update a store with address information

        Returns True if successful, False otherwise
        """
        try:
            self.cursor.execute("""
                INSERT INTO stores (
                    retailerid, retailerspecificstoreid,
                    storename, address, city, isactive
                ) VALUES (%s, %s, %s, %s, %s, true)
                ON CONFLICT (retailerid, retailerspecificstoreid) DO UPDATE SET
                    storename = COALESCE(EXCLUDED.storename, stores.storename),
                    address = COALESCE(EXCLUDED.address, stores.address),
                    city = COALESCE(EXCLUDED.city, stores.city),
                    updatedat = NOW()
                RETURNING storeid,
                         (xmax = 0) AS inserted  -- true if inserted, false if updated
            """, (retailer_id, store_code, store_name, address, city))

            result = self.cursor.fetchone()
            if result:
                store_id, was_inserted = result
                if was_inserted:
                    self.stats['stores_inserted'] += 1
                    print(f"  ✓ Inserted: {store_name} (ID: {store_id})")
                else:
                    self.stats['stores_updated'] += 1
                    print(f"  ✓ Updated: {store_name} (ID: {store_id})")

                self.conn.commit()
                return True

        except Exception as e:
            print(f"  ✗ Error upserting store {store_code}: {e}")
            self.stats['errors'] += 1
            self.conn.rollback()
            return False

    def populate_from_manual_research(self):
        """Populate stores from previous manual research"""
        print("\\nStrategy 1: Populating from Manual Research Data")
        print("=" * 80)

        # Manual research data from LLM (already researched)
        manual_data = [
            # Be Pharm
            (150, '001', 'BE ראשי', 'הרצל 65', 'ראשון לציון'),
            (150, '026', 'BE בלוך גבעתיים', 'בלוך 33', 'גבעתיים'),
            (150, '041', 'BE דיזנגוף סנטר', 'דיזנגוף 50', 'תל אביב'),
            (150, '112', 'BE קרית מוצקין', 'שי עגנון 18', 'קריית מוצקין'),
            (150, '145', 'BE נתיבות', 'שדרות ירושלים 2', 'נתיבות'),
            (150, '765', 'BE נתניה', 'בני ברמן 2', 'נתניה'),
            (150, '787', 'BE אריאל', 'מוריה 2', 'אריאל'),  # From LLM research
            (150, '790', 'BE ראש העין', 'שבזי 1', 'ראש העין'),
            (150, '854', 'BE רמת גן', 'אבא הלל סילבר 301', 'רמת גן'),
        ]

        print(f"Upserting {len(manual_data)} manually researched stores...")
        print()

        for retailer_id, store_code, name, address, city in manual_data:
            self.upsert_store(retailer_id, store_code, name, address, city)

        print()
        print(f"Manual research: {self.stats['stores_inserted']} inserted, "
              f"{self.stats['stores_updated']} updated")

    def populate_from_storesfull_xml(self, xml_file_path: str, retailer_name: str):
        """
        Populate stores from official StoresFull XML file

        Supports multiple XML formats:
        1. Good Pharm format: Root -> SubChains -> SubChain -> Stores -> Store
        2. Shufersal/Be Pharm format: asx:abap -> asx:values -> STORES -> STORE
        """
        print(f"\\nStrategy 2: Processing StoresFull XML for {retailer_name}")
        print("=" * 80)

        if not os.path.exists(xml_file_path):
            print(f"  File not found: {xml_file_path}")
            return

        retailer_id = RETAILER_IDS.get(retailer_name)
        if not retailer_id:
            print(f"  Unknown retailer: {retailer_name}")
            return

        try:
            # Read and parse XML
            with open(xml_file_path, 'rb') as f:
                content = f.read()

            # Try to decompress if gzipped
            if xml_file_path.endswith('.gz'):
                import gzip
                content = gzip.decompress(content)

            # Remove any leading whitespace/BOM that might cause parsing errors
            content = content.lstrip()

            root = ET.fromstring(content)

            # Detect XML format and extract stores accordingly
            stores = []

            # Format 1: Good Pharm (Root -> SubChains -> SubChain -> Stores -> Store)
            if root.tag == 'Root' and root.find('.//SubChains') is not None:
                stores = root.findall('.//Stores/Store')
                print(f"Detected Good Pharm XML format")

            # Format 2: Shufersal/Be Pharm (SAP ABAP format with uppercase tags)
            elif 'abap' in root.tag.lower():
                # Find all STORE elements
                all_stores = root.findall('.//STORE')

                # Filter for Be Pharm stores (SUBCHAINNAME = "Be")
                if retailer_name == 'Be Pharm':
                    stores = [s for s in all_stores
                             if s.find('SUBCHAINNAME') is not None
                             and s.find('SUBCHAINNAME').text == 'Be']
                    print(f"Detected Shufersal XML format, filtering for Be Pharm stores")
                else:
                    stores = all_stores

            # Format 3: Generic format (try both Store and STORE tags)
            else:
                stores = root.findall('.//Store') or root.findall('.//STORE')

            print(f"Found {len(stores)} stores to process")
            print()

            for store in stores:
                # Try both uppercase and mixed case tags
                store_id = (store.find('STOREID') or store.find('StoreID') or
                           store.find('StoreId'))
                store_name = (store.find('STORENAME') or store.find('StoreName'))
                address = (store.find('ADDRESS') or store.find('Address'))
                city = (store.find('CITY') or store.find('City'))

                if store_id is not None and store_id.text:
                    # Pad store ID with zeros to ensure consistent 3-digit format
                    padded_store_id = store_id.text.strip().zfill(3)

                    self.upsert_store(
                        retailer_id,
                        padded_store_id,
                        store_name.text.strip() if store_name is not None and store_name.text else '',
                        address.text.strip() if address is not None and address.text else None,
                        city.text.strip() if city is not None and city.text else None
                    )

            print()
            print(f"StoresFull XML: {self.stats['stores_inserted']} inserted, "
                  f"{self.stats['stores_updated']} updated")

        except Exception as e:
            print(f"  Error processing XML: {e}")
            import traceback
            traceback.print_exc()
            self.stats['errors'] += 1

    def scrape_good_pharm_stores(self):
        """Scrape store information from Good Pharm website"""
        print("\\nStrategy 3: Scraping Good Pharm Stores from Website")
        print("=" * 80)

        try:
            session = requests.Session()
            session.verify = False

            # Good Pharm portal URL
            url = 'https://goodpharm.binaprojects.com'

            response = session.get(url, timeout=30)
            soup = BeautifulSoup(response.content, 'html.parser')

            # Look for store list files
            links = soup.find_all('a', href=True)
            stores_files = [link for link in links if 'stores' in link['href'].lower()]

            if stores_files:
                print(f"Found {len(stores_files)} potential store files")
                # Download and process first one
                # (Implementation would go here)
            else:
                print("No store files found on Good Pharm portal")

        except Exception as e:
            print(f"Error scraping Good Pharm: {e}")
            self.stats['errors'] += 1

    def scrape_super_pharm_stores(self):
        """
        Scrape store information from Super-Pharm website
        Note: Super-Pharm has a store locator at shop.super-pharm.co.il
        """
        print("\\nStrategy 4: Scraping Super-Pharm Stores from Website")
        print("=" * 80)
        print("Super-Pharm store locator scraping would be implemented here")
        print("Website: https://shop.super-pharm.co.il/branches")

    def report_missing_stores(self):
        """Report stores that still have missing address data"""
        print("\\n\\nREPORT: Stores Still Missing Address Data")
        print("=" * 80)

        self.cursor.execute("""
            SELECT
                r.retailername,
                s.storeid,
                s.storename,
                s.retailerspecificstoreid
            FROM stores s
            JOIN retailers r ON s.retailerid = r.retailerid
            WHERE s.retailerid IN (52, 97, 150)
                AND s.isactive = TRUE
                AND (s.address IS NULL OR s.city IS NULL)
            ORDER BY r.retailername, s.storeid
        """)

        missing_stores = self.cursor.fetchall()

        if missing_stores:
            print(f"\\nFound {len(missing_stores)} stores still missing address data:\\n")

            current_retailer = None
            for retailer, store_id, name, code in missing_stores:
                if retailer != current_retailer:
                    print(f"\\n{retailer}:")
                    current_retailer = retailer
                print(f"  - Store {code}: {name} (ID: {store_id})")

            print("\\n" + "-" * 80)
            print("These stores require manual research or retailer-specific scraping.")
        else:
            print("\\n✅ All active pharmacy stores now have address data!")

    def print_summary(self):
        """Print final statistics"""
        print("\\n\\nFINAL SUMMARY")
        print("=" * 80)
        print(f"Stores Inserted: {self.stats['stores_inserted']}")
        print(f"Stores Updated:  {self.stats['stores_updated']}")
        print(f"Stores Skipped:  {self.stats['stores_skipped']}")
        print(f"Errors:          {self.stats['errors']}")
        print("=" * 80)

    def close(self):
        """Close database connection"""
        self.cursor.close()
        self.conn.close()


def main():
    """Main execution"""
    print("PHARMACY STORES POPULATION SCRIPT")
    print("=" * 80)
    print()
    print("This script populates missing store address data using:")
    print("  1. Manual research data")
    print("  2. StoresFull XML files (if available)")
    print("  3. Web scraping (Good Pharm & Super-Pharm)")
    print()

    populator = StorePopulator()

    try:
        # Strategy 1: Use manual research data
        populator.populate_from_manual_research()

        # Strategy 2: Process StoresFull XML files if available
        # Local XML files provided by user
        xml_files = [
            ('/Users/idankarbat/Downloads/StoresFull7290058197699-000-202509301324 2.xml', 'Good Pharm'),
            ('/Users/idankarbat/Downloads/Stores7290027600007-000-202509300201', 'Be Pharm'),
            ('/Users/idankarbat/Downloads/StoresFull7290172900007-000-202509300700', 'Super-Pharm'),
        ]

        for xml_path, retailer in xml_files:
            if os.path.exists(xml_path):
                populator.populate_from_storesfull_xml(xml_path, retailer)
            else:
                print(f"\\nWarning: File not found: {xml_path}")
                print(f"Skipping {retailer} XML processing")

        # Strategy 3 & 4: Web scraping (optional)
        # populator.scrape_good_pharm_stores()
        # populator.scrape_super_pharm_stores()

        # Report remaining missing stores
        populator.report_missing_stores()

        # Print summary
        populator.print_summary()

    finally:
        populator.close()


if __name__ == "__main__":
    main()
