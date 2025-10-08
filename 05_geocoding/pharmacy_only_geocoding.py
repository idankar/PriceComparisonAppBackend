#!/usr/bin/env python3
"""
Geocode ONLY pharmacy stores (Super-Pharm, Be Pharm, Good Pharm)
"""

import sys
from pharmacy_geocoder import PharmacyGeocoder

# Pharmacy retailer IDs
PHARMACY_RETAILERS = [52, 150, 97]  # Super-Pharm, Be Pharm, Good Pharm

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Geocode pharmacy store addresses')
    parser.add_argument('--google-key', required=True, help='Google Maps API key')
    parser.add_argument('--test', action='store_true', help='Test mode - no database updates')

    args = parser.parse_args()

    print("Starting pharmacy-only geocoding...")
    print("Retailers: Super-Pharm, Be Pharm, Good Pharm")

    # Create custom geocoder
    geocoder = PharmacyGeocoder(google_api_key=args.google_key)

    # Override the get_stores_to_geocode method to filter by pharmacy retailers
    original_method = geocoder.get_stores_to_geocode

    def filtered_get_stores(limit=None):
        stores = original_method(limit)
        # Filter to only pharmacy retailers
        import psycopg2
        conn = psycopg2.connect(
            host='localhost',
            port='5432',
            database='price_comparison_app_v2',
            user='postgres',
            password='025655358'
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute(f"""
            SELECT s.storeid, s.storename, s.address, s.city, s.latitude, s.longitude, r.retailername
            FROM stores s
            JOIN retailers r ON s.retailerid = r.retailerid
            WHERE s.isactive = true
                AND (s.latitude IS NULL OR s.longitude IS NULL)
                AND s.retailerid IN ({','.join(map(str, PHARMACY_RETAILERS))})
                AND s.address IS NOT NULL
                AND s.address != ''
                AND s.city IS NOT NULL
                AND s.city != ''
            ORDER BY r.retailername, s.storename
            {f'LIMIT {limit}' if limit else ''}
        """)

        filtered_stores = cur.fetchall()
        cur.close()
        conn.close()

        return filtered_stores

    geocoder.get_stores_to_geocode = filtered_get_stores

    # Run geocoding
    geocoder.run_geocoding(limit=None, test_mode=args.test)

if __name__ == "__main__":
    main()