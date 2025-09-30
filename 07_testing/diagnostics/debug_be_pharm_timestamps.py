#!/usr/bin/env python3
"""
Debug script to test Be Pharm timestamp processing
"""

import os
import gzip
import xml.etree.ElementTree as ET
from datetime import datetime

def test_be_pharm_timestamps(filepath):
    """Test Be Pharm timestamp extraction and processing"""

    print(f"Testing file: {filepath}")

    # Parse XML file
    if filepath.endswith('.gz'):
        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            tree = ET.parse(f)
    else:
        tree = ET.parse(filepath)

    root = tree.getroot()

    # Get store ID
    store_elem = root.find('.//StoreId')
    store_id = store_elem.text if store_elem is not None else None
    print(f"Store ID: {store_id}")

    # Parse items
    items = root.findall('.//Item')
    print(f"Total items: {len(items)}")

    products = []
    timestamp_stats = {
        'with_date': 0,
        'without_date': 0,
        'parse_success': 0,
        'parse_failure': 0,
        'unique_timestamps': set()
    }

    for item in items[:20]:  # Test first 20 items
        try:
            product = {'store_id': store_id}

            # Extract item code
            item_code = item.find('ItemCode')
            if item_code is not None and item_code.text:
                product['item_code'] = item_code.text.strip()
            else:
                continue

            # Extract price update date (THE CRITICAL PART)
            price_date = item.find('PriceUpdateDate')
            if price_date is not None and price_date.text:
                product['price_date'] = price_date.text.strip()
                timestamp_stats['with_date'] += 1

                # Test the exact parsing logic from ETL
                try:
                    parsed_timestamp = datetime.strptime(
                        product['price_date'],
                        '%Y-%m-%d %H:%M'
                    )
                    product['parsed_timestamp'] = parsed_timestamp
                    timestamp_stats['parse_success'] += 1
                    timestamp_stats['unique_timestamps'].add(parsed_timestamp)
                except Exception as e:
                    print(f"  Parse error for '{product['price_date']}': {e}")
                    product['parsed_timestamp'] = datetime.now()
                    timestamp_stats['parse_failure'] += 1
            else:
                timestamp_stats['without_date'] += 1
                product['parsed_timestamp'] = datetime.now()

            products.append(product)

        except Exception as e:
            print(f"Error processing item: {e}")
            continue

    print(f"\nTimestamp Statistics:")
    print(f"  Items with PriceUpdateDate: {timestamp_stats['with_date']}")
    print(f"  Items without PriceUpdateDate: {timestamp_stats['without_date']}")
    print(f"  Successful timestamp parses: {timestamp_stats['parse_success']}")
    print(f"  Failed timestamp parses: {timestamp_stats['parse_failure']}")
    print(f"  Unique timestamps found: {len(timestamp_stats['unique_timestamps'])}")

    print(f"\nSample timestamps:")
    for i, product in enumerate(products[:10]):
        print(f"  Item {i+1} ({product.get('item_code', 'NO_CODE')}): {product.get('price_date', 'NO_DATE')} -> {product.get('parsed_timestamp', 'NO_TIMESTAMP')}")

    return products

if __name__ == "__main__":
    # Test with the file we've been analyzing
    test_file = "/Users/noa/Downloads/PriceFull7290027600007-748-202509170300.gz"
    products = test_be_pharm_timestamps(test_file)