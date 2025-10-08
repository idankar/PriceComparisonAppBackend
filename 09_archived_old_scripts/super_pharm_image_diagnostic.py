#!/usr/bin/env python3
"""
Super-Pharm Image Availability Diagnostic Script

This script checks whether products missing images in our database actually have
images available on Super-Pharm's Azure Blob Storage.

Strategy:
1. Query products with missing images from database
2. Construct expected Azure URLs using pattern:
   https://superpharmstorage.blob.core.windows.net/hybris/products/desktop/small/{barcode}.jpg
3. HTTP HEAD request to check if image exists (200 OK vs 404 Not Found)
4. Generate comprehensive diagnostic report

This will definitively answer: Is this a scraper bug or a data source issue?
"""

import psycopg2
import requests
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
from datetime import datetime
import json
from collections import defaultdict

# Load environment variables
load_dotenv()

DB_NAME = os.getenv("DB_NAME", "price_comparison_app_v2")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "025655358")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

SUPER_PHARM_RETAILER_ID = 52
AZURE_IMAGE_URL_TEMPLATE = "https://superpharmstorage.blob.core.windows.net/hybris/products/desktop/small/{barcode}.jpg"

# Sample size - increase if needed
SAMPLE_SIZE = 100

def connect_db():
    """Connect to the database"""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
            host=DB_HOST, port=DB_PORT
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"❌ Could not connect to the database: {e}")
        return None

def get_sample_products_missing_images(conn, sample_size=SAMPLE_SIZE):
    """Get a stratified sample of products missing images"""
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    # Get sample from top categories
    query = """
        WITH category_samples AS (
            SELECT barcode, name, brand, category,
                   ROW_NUMBER() OVER (PARTITION BY category ORDER BY RANDOM()) as rn
            FROM canonical_products
            WHERE source_retailer_id = %s
            AND image_url IS NULL
            AND is_active = TRUE
        )
        SELECT barcode, name, brand, category
        FROM category_samples
        WHERE rn <= 5  -- 5 products per category for diversity
        LIMIT %s;
    """

    cursor.execute(query, (SUPER_PHARM_RETAILER_ID, sample_size))
    products = cursor.fetchall()
    cursor.close()

    return products

def check_image_exists(barcode, timeout=5):
    """
    Check if an image exists on Azure for the given barcode.
    Returns: (exists: bool, status_code: int, url: str)
    """
    url = AZURE_IMAGE_URL_TEMPLATE.format(barcode=barcode)

    try:
        # Use HEAD request for efficiency (doesn't download the image)
        response = requests.head(url, timeout=timeout, allow_redirects=True)
        exists = response.status_code == 200
        return (exists, response.status_code, url)
    except requests.exceptions.RequestException as e:
        # Network error
        return (False, None, url)

def run_diagnostic():
    """Main diagnostic function"""
    print("="*80)
    print("🔍 SUPER-PHARM IMAGE AVAILABILITY DIAGNOSTIC")
    print("="*80)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print()

    # Connect to database
    print("📊 Connecting to database...")
    conn = connect_db()
    if not conn:
        return

    # Get sample products
    print(f"📦 Fetching {SAMPLE_SIZE} sample products missing images...")
    products = get_sample_products_missing_images(conn, SAMPLE_SIZE)
    print(f"✅ Retrieved {len(products)} products for testing")
    print()

    # Check each product
    print("🔎 Checking image availability on Azure Blob Storage...")
    print("-"*80)

    results = []
    category_stats = defaultdict(lambda: {"total": 0, "exists": 0, "missing": 0})
    brand_stats = defaultdict(lambda: {"total": 0, "exists": 0, "missing": 0})

    for i, product in enumerate(products, 1):
        barcode = product['barcode']
        name = product['name']
        category = product['category'] or 'Unknown'
        brand = product['brand'] or 'Unknown'

        exists, status_code, url = check_image_exists(barcode)

        result = {
            "barcode": barcode,
            "name": name,
            "category": category,
            "brand": brand,
            "image_exists": exists,
            "status_code": status_code,
            "azure_url": url
        }
        results.append(result)

        # Update statistics
        category_stats[category]["total"] += 1
        brand_stats[brand]["total"] += 1

        if exists:
            category_stats[category]["exists"] += 1
            brand_stats[brand]["exists"] += 1
            status_icon = "✅"
        else:
            category_stats[category]["missing"] += 1
            brand_stats[brand]["missing"] += 1
            status_icon = "❌"

        # Print progress
        if i % 10 == 0 or i <= 5:
            print(f"{status_icon} [{i:3d}/{len(products)}] {barcode:15s} | {name[:40]:40s} | Status: {status_code}")

    print("-"*80)
    print()

    # Calculate overall statistics
    total_tested = len(results)
    total_exists = sum(1 for r in results if r['image_exists'])
    total_missing = total_tested - total_exists
    exists_percentage = (total_exists / total_tested * 100) if total_tested > 0 else 0

    # Print summary
    print("="*80)
    print("📊 DIAGNOSTIC RESULTS")
    print("="*80)
    print()
    print(f"Total products tested: {total_tested}")
    print(f"Images EXIST on Azure: {total_exists} ({exists_percentage:.1f}%)")
    print(f"Images MISSING on Azure: {total_missing} ({100-exists_percentage:.1f}%)")
    print()

    # Print category breakdown
    print("📋 BREAKDOWN BY CATEGORY:")
    print("-"*80)
    print(f"{'Category':<40} | {'Total':>6} | {'Exists':>6} | {'Missing':>7} | {'% Exists':>9}")
    print("-"*80)
    for category, stats in sorted(category_stats.items(), key=lambda x: x[1]['total'], reverse=True)[:10]:
        total = stats['total']
        exists = stats['exists']
        missing = stats['missing']
        pct = (exists / total * 100) if total > 0 else 0
        print(f"{category[:40]:<40} | {total:6d} | {exists:6d} | {missing:7d} | {pct:8.1f}%")
    print()

    # Print brand breakdown
    print("🏷️  BREAKDOWN BY BRAND:")
    print("-"*80)
    print(f"{'Brand':<40} | {'Total':>6} | {'Exists':>6} | {'Missing':>7} | {'% Exists':>9}")
    print("-"*80)
    for brand, stats in sorted(brand_stats.items(), key=lambda x: x[1]['total'], reverse=True)[:10]:
        total = stats['total']
        exists = stats['exists']
        missing = stats['missing']
        pct = (exists / total * 100) if total > 0 else 0
        print(f"{brand[:40]:<40} | {total:6d} | {exists:6d} | {missing:7d} | {pct:8.1f}%")
    print()

    # Diagnosis
    print("="*80)
    print("🩺 DIAGNOSIS")
    print("="*80)

    if exists_percentage > 70:
        print("🔴 SCRAPER BUG DETECTED!")
        print(f"   {exists_percentage:.1f}% of 'missing' images actually EXIST on Azure.")
        print("   The scraper is failing to capture these images during scraping.")
        print()
        print("💡 RECOMMENDED FIX:")
        print("   Update super_pharm_scraper.py to check additional lazy-load attributes:")
        print("   - data-src")
        print("   - data-lazy-src")
        print("   - data-original")
        print("   - srcset")
    elif exists_percentage > 30:
        print("🟡 MIXED ISSUE DETECTED!")
        print(f"   {exists_percentage:.1f}% of images exist on Azure, {100-exists_percentage:.1f}% are genuinely missing.")
        print("   This is partially a scraper bug and partially a data source issue.")
        print()
        print("💡 RECOMMENDED ACTIONS:")
        print("   1. Update scraper to capture more lazy-load attributes (will fix ~{:.0f} products)".format(total_exists/total_tested*7276))
        print("   2. Accept that ~{:.0f} products have no images on source".format(total_missing/total_tested*7276))
    else:
        print("🟢 DATA SOURCE ISSUE!")
        print(f"   Only {exists_percentage:.1f}% of images exist on Azure.")
        print("   Most products genuinely don't have images on Super-Pharm's website.")
        print()
        print("💡 RECOMMENDED ACTIONS:")
        print("   1. Accept this limitation")
        print("   2. Consider alternative image sources (Google Images API, manufacturer websites)")
        print("   3. Mark these products as 'no-image-available' in the database")

    print()

    # Save detailed results to JSON
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"super_pharm_image_diagnostic_{timestamp}.json"

    output_data = {
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "sample_size": total_tested,
            "total_missing_in_db": 7276,  # From earlier query
            "images_exist_on_azure": total_exists,
            "images_missing_on_azure": total_missing,
            "exists_percentage": exists_percentage
        },
        "category_breakdown": dict(category_stats),
        "brand_breakdown": dict(brand_stats),
        "detailed_results": results
    }

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=4, ensure_ascii=False)

    print(f"💾 Detailed results saved to: {output_file}")
    print()

    conn.close()

if __name__ == "__main__":
    run_diagnostic()
