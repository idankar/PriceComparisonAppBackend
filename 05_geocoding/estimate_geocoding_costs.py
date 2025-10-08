#!/usr/bin/env python3
"""
Geocoding Cost Estimator
Estimates Google Maps API costs for geocoding all pharmacy stores
"""

import psycopg2
import psycopg2.extras

# Configuration
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "price_comparison_app_v2"
PG_USER = "postgres"
PG_PASSWORD = "025655358"

def estimate_costs():
    """Estimate geocoding costs"""
    
    # Connect to database
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, database=PG_DATABASE,
            user=PG_USER, password=PG_PASSWORD
        )
    except Exception as e:
        print(f"Database connection failed: {e}")
        return
    
    cursor = conn.cursor()
    
    # Count stores needing geocoding
    cursor.execute("""
        SELECT 
            retailerid,
            COUNT(*) as total_stores,
            COUNT(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 END) as need_geocoding,
            COUNT(CASE WHEN address IS NULL OR city IS NULL THEN 1 END) as missing_address
        FROM stores 
        WHERE retailerid IN (52, 150, 97)
        GROUP BY retailerid
        ORDER BY retailerid
    """)
    
    results = cursor.fetchall()
    total_need_geocoding = 0
    
    print("="*60)
    print("GEOCODING COST ESTIMATION")
    print("="*60)
    print(f"{'Retailer':<15} {'Total':<8} {'Need Geo':<10} {'Missing Addr':<12}")
    print("-" * 60)
    
    for retailer_id, total, need_geo, missing_addr in results:
        retailer_names = {52: 'Super-Pharm', 150: 'Be Pharm', 97: 'Good Pharm'}
        name = retailer_names.get(retailer_id, f'Retailer {retailer_id}')
        print(f"{name:<15} {total:<8} {need_geo:<10} {missing_addr:<12}")
        total_need_geocoding += need_geo
    
    print("-" * 60)
    print(f"{'TOTAL':<15} {'':<8} {total_need_geocoding:<10}")
    print("="*60)
    
    # Google Maps Pricing (as of 2024)
    print("\nGOOGLE MAPS API PRICING:")
    print("- First $200/month: FREE")
    print("- After $200: $5.00 per 1,000 requests")
    print("- Monthly free tier: 40,000 requests")
    
    print(f"\nCOST BREAKDOWN:")
    print(f"Total requests needed: {total_need_geocoding:,}")
    
    if total_need_geocoding <= 40000:
        cost = 0
        print(f"Estimated cost: $0.00 (within free tier)")
    else:
        paid_requests = total_need_geocoding - 40000
        cost = (paid_requests / 1000) * 5.00
        print(f"Free requests: 40,000")
        print(f"Paid requests: {paid_requests:,}")
        print(f"Estimated cost: ${cost:.2f}")
    
    # Time estimates
    print(f"\nTIME ESTIMATES:")
    # Google: 10 requests/sec, Nominatim fallback: 1 request/sec
    # Assume 80% success with Google, 20% fallback to Nominatim
    google_requests = int(total_need_geocoding * 0.8)
    nominatim_requests = total_need_geocoding - google_requests
    
    google_time = google_requests / 10  # 10 requests per second
    nominatim_time = nominatim_requests / 1  # 1 request per second
    total_time = google_time + nominatim_time
    
    print(f"With Google API (80% success): {google_time/60:.1f} minutes")
    print(f"Nominatim fallback (20%): {nominatim_time/60:.1f} minutes")
    print(f"Total estimated time: {total_time/60:.1f} minutes")
    
    # Nominatim only
    nominatim_only_time = total_need_geocoding / 1  # 1 request per second
    print(f"Nominatim only (no Google): {nominatim_only_time/60:.1f} minutes")
    
    print(f"\nRECOMMENDATION:")
    if cost <= 10:
        print("✓ Recommended to use Google API - low cost, high accuracy")
    elif cost <= 50:
        print("⚠ Consider Google API - moderate cost, much better accuracy")
    else:
        print("⚠ High cost - consider Nominatim only or batch processing")
    
    print("\nNEXT STEPS:")
    print("1. Get Google Maps API key from: https://console.cloud.google.com/")
    print("2. Enable Geocoding API")
    print("3. Run: python pharmacy_geocoder.py --google-key YOUR_KEY --limit 10 --test")
    print("4. Review results and run full geocoding")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    estimate_costs()