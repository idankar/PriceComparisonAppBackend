#!/usr/bin/env python3
"""
Simple geocoding runner that bypasses the confirmation prompt
"""

import sys
from pharmacy_geocoder import PharmacyGeocoder

def main():
    print("Starting pharmacy store geocoding with Nominatim...")
    print("This will use the free OpenStreetMap geocoding service.")
    print("Estimated time: ~10 minutes for all 552 stores")
    
    # Create geocoder without Google API key (Nominatim only)
    geocoder = PharmacyGeocoder(google_api_key=None)
    
    # Run geocoding on all stores
    geocoder.run_geocoding(limit=None, test_mode=False)

if __name__ == "__main__":
    main()