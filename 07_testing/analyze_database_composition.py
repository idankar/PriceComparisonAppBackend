#!/usr/bin/env python3
"""
Database Composition Analysis
Classify all products by brand type and calculate retention rates
"""

import csv
from brand_classification import classify_brand

# Read brand data
brands_data = []
with open('/tmp/all_brands.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        brand = row['brand']
        count = int(row['product_count'])
        classification = classify_brand(brand)
        brands_data.append({
            'brand': brand,
            'product_count': count,
            'classification': classification
        })

# Calculate totals
total_products = sum(b['product_count'] for b in brands_data)
international_products = sum(b['product_count'] for b in brands_data if b['classification'] == 'international')
local_products = sum(b['product_count'] for b in brands_data if b['classification'] == 'local')
generic_products = sum(b['product_count'] for b in brands_data if b['classification'] == 'generic')
unknown_products = sum(b['product_count'] for b in brands_data if b['classification'] == 'unknown')

# Count brands
international_brands = len([b for b in brands_data if b['classification'] == 'international'])
local_brands = len([b for b in brands_data if b['classification'] == 'local'])
generic_brands = len([b for b in brands_data if b['classification'] == 'generic'])
unknown_brands = len([b for b in brands_data if b['classification'] == 'unknown'])

print("=" * 80)
print("DATABASE COMPOSITION ANALYSIS")
print("=" * 80)
print()
print(f"TOTAL PRODUCTS: {total_products:,}")
print()
print("BREAKDOWN BY BRAND TYPE:")
print("-" * 80)
print(f"{'Category':<20} {'Products':<15} {'%':<10} {'Brands':<15}")
print("-" * 80)
print(f"{'International':<20} {international_products:>10,}   {international_products/total_products*100:>6.2f}%   {international_brands:>10,}")
print(f"{'Local Israeli':<20} {local_products:>10,}   {local_products/total_products*100:>6.2f}%   {local_brands:>10,}")
print(f"{'Generic/Unknown Brand':<20} {generic_products:>10,}   {generic_products/total_products*100:>6.2f}%   {generic_brands:>10,}")
print(f"{'Unclassified':<20} {unknown_products:>10,}   {unknown_products/total_products*100:>6.2f}%   {unknown_brands:>10,}")
print("-" * 80)
print(f"{'TOTAL':<20} {total_products:>10,}   {100.00:>6.2f}%   {len(brands_data):>10,}")
print()

# Show top international brands
print("=" * 80)
print("TOP 20 INTERNATIONAL BRANDS (Available Online Globally)")
print("=" * 80)
intl_brands = sorted([b for b in brands_data if b['classification'] == 'international'],
                     key=lambda x: x['product_count'], reverse=True)[:20]
for i, brand in enumerate(intl_brands, 1):
    print(f"{i:2}. {brand['brand']:<30} {brand['product_count']:>6,} products")

print()
print("=" * 80)
print("TOP 20 LOCAL ISRAELI BRANDS (Difficult to Get Internationally)")
print("=" * 80)
local_brands_sorted = sorted([b for b in brands_data if b['classification'] == 'local'],
                             key=lambda x: x['product_count'], reverse=True)[:20]
for i, brand in enumerate(local_brands_sorted, 1):
    print(f"{i:2}. {brand['brand']:<30} {brand['product_count']:>6,} products")

print()
print("=" * 80)
print("STRATEGIC RECOMMENDATION")
print("=" * 80)
print()
print(f"✅ KEEP: {international_products:,} products ({international_products/total_products*100:.1f}%)")
print(f"   - International brands available online")
print(f"   - Can be reliably ordered to Israel")
print(f"   - Good for price comparison with global retailers")
print()
print(f"❌ REMOVE: {local_products + generic_products:,} products ({(local_products+generic_products)/total_products*100:.1f}%)")
print(f"   - {local_products:,} local Israeli brands (not available internationally)")
print(f"   - {generic_products:,} generic/unknown brand products")
print()
print(f"🤔 REVIEW: {unknown_products:,} products ({unknown_products/total_products*100:.1f}%)")
print(f"   - Unclassified brands - need manual review")
print(f"   - May contain some international brands")
print()

retention_rate = international_products / total_products * 100
print(f"📊 ESTIMATED RETENTION RATE: {retention_rate:.1f}%")
print(f"📊 DATABASE SIZE AFTER CLEANUP: {international_products:,} products")
print()

# Calculate potential with unknown brands
potential_max = international_products + (unknown_products * 0.3)  # Assume 30% of unknown are international
print(f"📈 POTENTIAL MAXIMUM (if 30% of unknown are international): {int(potential_max):,} products ({potential_max/total_products*100:.1f}%)")
print()
