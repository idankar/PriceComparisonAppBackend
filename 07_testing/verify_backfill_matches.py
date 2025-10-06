#!/usr/bin/env python3
"""
Verify that backfilled products match the original product names
"""
import json
from difflib import SequenceMatcher

def similarity(a, b):
    """Calculate string similarity ratio"""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

# Load original failures
with open('kolboyehuda_scraper_output/scraping_report.json', 'r', encoding='utf-8') as f:
    report = json.load(f)
    
original_products = {
    p['barcode']: p['name'] 
    for p in report['failures'] 
    if p.get('barcode') and 'barcode_invalid_format' not in p.get('missing_fields', [])
}

# Load enriched products
with open('kolboyehuda_scraper_output/backfill_ready_with_categories.json', 'r', encoding='utf-8') as f:
    enriched = json.load(f)

print("Verifying product name matches for backfilled products...")
print("="*80)

mismatches = []
perfect_matches = 0
good_matches = 0  # >0.8 similarity

for product in enriched:
    barcode = product['barcode']
    enriched_name = product['name']
    
    if barcode in original_products:
        original_name = original_products[barcode]
        sim = similarity(original_name, enriched_name)
        
        if sim == 1.0:
            perfect_matches += 1
        elif sim > 0.8:
            good_matches += 1
            print(f"\n⚠️  High similarity but not exact ({sim:.2%}):")
            print(f"  Barcode: {barcode}")
            print(f"  Original: {original_name[:80]}")
            print(f"  Enriched: {enriched_name[:80]}")
        else:
            mismatches.append({
                'barcode': barcode,
                'original': original_name,
                'enriched': enriched_name,
                'similarity': sim
            })

print("\n" + "="*80)
print("VERIFICATION RESULTS:")
print("="*80)
print(f"Total enriched products: {len(enriched)}")
print(f"Perfect matches: {perfect_matches}")
print(f"Good matches (>80% similar): {good_matches}")
print(f"Potential mismatches: {len(mismatches)}")

if mismatches:
    print("\n⛔ POTENTIAL MISMATCHES FOUND:")
    for m in mismatches[:10]:  # Show first 10
        print(f"\n  Barcode: {m['barcode']} (similarity: {m['similarity']:.2%})")
        print(f"  Original: {m['original'][:80]}")
        print(f"  Enriched: {m['enriched'][:80]}")
else:
    print("\n✅ All products matched correctly!")

print("="*80)
