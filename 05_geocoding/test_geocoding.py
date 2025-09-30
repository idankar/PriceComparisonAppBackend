#!/usr/bin/env python3
"""
Test geocoding on a small sample
"""

import sys
from pharmacy_geocoder import PharmacyGeocoder

def main():
    print("Testing pharmacy store geocoding with Nominatim (free)...")
    print("Processing 10 stores as a test...")

    # Create geocoder without Google API key (Nominatim only)
    geocoder = PharmacyGeocoder(google_api_key=None)

    # Run geocoding on 10 stores in test mode
    geocoder.run_geocoding(limit=10, test_mode=True)

    print("\n" + "="*80)
    print("TEST COMPLETED - No database changes were made")
    print("Review the results above to see if Nominatim works well for your addresses")
    print("To geocode all stores, run: python3 run_geocoding.py")
    print("="*80)

if __name__ == "__main__":
    main()