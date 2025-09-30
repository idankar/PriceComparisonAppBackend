#!/usr/bin/env python3
"""
Pharmacy Store Geocoding Script
Geocodes Hebrew addresses in Israel for pharmacy stores using Google Maps API with Nominatim fallback
"""

import os
import sys
import time
import json
import csv
import requests
import psycopg2
import psycopg2.extras
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime
import logging
from urllib.parse import quote

# Configuration
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "price_comparison_app_v2"
PG_USER = "postgres"
PG_PASSWORD = "***REMOVED***"

# Israel boundary validation
ISRAEL_BOUNDS = {
    'lat_min': 29.5,
    'lat_max': 33.5,
    'lng_min': 34.0,
    'lng_max': 36.0
}

# Rate limiting (requests per second)
GOOGLE_RATE_LIMIT = 10  # Google allows 10/sec with standard API
NOMINATIM_RATE_LIMIT = 1  # Nominatim requires 1/sec max

@dataclass
class GeocodingResult:
    """Geocoding result data structure"""
    latitude: float
    longitude: float
    accuracy: str  # 'high', 'medium', 'low'
    source: str    # 'google', 'nominatim'
    address_components: Dict
    success: bool = True
    error_message: str = ""

class PharmacyGeocoder:
    def __init__(self, google_api_key: Optional[str] = None):
        self.google_api_key = google_api_key
        self.db_connection = None
        self.stats = {
            'total_stores': 0,
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'google_success': 0,
            'nominatim_success': 0,
            'out_of_bounds': 0
        }
        self.failed_addresses = []
        self.progress_file = 'geocoding_progress.json'
        self.failed_file = 'failed_geocoding.csv'
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('geocoding.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def connect_to_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_connection = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                database=PG_DATABASE,
                user=PG_USER,
                password=PG_PASSWORD
            )
            self.logger.info("Connected to database successfully")
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            sys.exit(1)
    
    def load_progress(self) -> List[int]:
        """Load list of already processed store IDs"""
        if os.path.exists(self.progress_file):
            with open(self.progress_file, 'r') as f:
                data = json.load(f)
                return data.get('processed_store_ids', [])
        return []
    
    def save_progress(self, processed_store_ids: List[int]):
        """Save progress to file"""
        data = {
            'processed_store_ids': processed_store_ids,
            'last_updated': datetime.now().isoformat(),
            'stats': self.stats
        }
        with open(self.progress_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def is_within_israel_bounds(self, lat: float, lng: float) -> bool:
        """Check if coordinates are within Israel boundaries"""
        return (ISRAEL_BOUNDS['lat_min'] <= lat <= ISRAEL_BOUNDS['lat_max'] and
                ISRAEL_BOUNDS['lng_min'] <= lng <= ISRAEL_BOUNDS['lng_max'])
    
    def geocode_with_google(self, address: str, city: str) -> Optional[GeocodingResult]:
        """Geocode address using Google Maps API"""
        if not self.google_api_key:
            return None
            
        # Construct full address for Google
        full_address = f"{address}, {city}, Israel"
        
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            'address': full_address,
            'key': self.google_api_key,
            'region': 'il',  # Bias results to Israel
            'language': 'he'  # Request Hebrew responses when available
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data['status'] == 'OK' and data['results']:
                result = data['results'][0]
                location = result['geometry']['location']
                lat, lng = location['lat'], location['lng']
                
                # Validate coordinates are in Israel
                if not self.is_within_israel_bounds(lat, lng):
                    self.stats['out_of_bounds'] += 1
                    return GeocodingResult(
                        latitude=lat, longitude=lng, accuracy='low', 
                        source='google', address_components=result.get('address_components', {}),
                        success=False, error_message="Coordinates outside Israel bounds"
                    )
                
                # Determine accuracy based on location_type
                location_type = result['geometry'].get('location_type', 'APPROXIMATE')
                if location_type in ['ROOFTOP', 'RANGE_INTERPOLATED']:
                    accuracy = 'high'
                elif location_type == 'GEOMETRIC_CENTER':
                    accuracy = 'medium'
                else:
                    accuracy = 'low'
                
                return GeocodingResult(
                    latitude=lat, longitude=lng, accuracy=accuracy, 
                    source='google', address_components=result.get('address_components', {})
                )
            else:
                return GeocodingResult(
                    latitude=0, longitude=0, accuracy='low', source='google',
                    address_components={}, success=False,
                    error_message=f"Google API: {data.get('status', 'Unknown error')}"
                )
                
        except Exception as e:
            self.logger.warning(f"Google geocoding failed for '{full_address}': {e}")
            return GeocodingResult(
                latitude=0, longitude=0, accuracy='low', source='google',
                address_components={}, success=False, error_message=str(e)
            )
    
    def geocode_with_nominatim(self, address: str, city: str) -> Optional[GeocodingResult]:
        """Geocode address using Nominatim (OpenStreetMap)"""
        full_address = f"{address}, {city}, Israel"
        
        url = "https://nominatim.openstreetmap.org/search"
        params = {
            'q': full_address,
            'format': 'json',
            'limit': 1,
            'countrycodes': 'il',
            'addressdetails': 1,
            'accept-language': 'he,en'
        }
        
        headers = {
            'User-Agent': 'PharmacyGeocodingScript/1.0 (pharmacy.geocoding@example.com)'
        }
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data:
                result = data[0]
                lat, lng = float(result['lat']), float(result['lon'])
                
                # Validate coordinates are in Israel
                if not self.is_within_israel_bounds(lat, lng):
                    self.stats['out_of_bounds'] += 1
                    return GeocodingResult(
                        latitude=lat, longitude=lng, accuracy='low',
                        source='nominatim', address_components=result.get('address', {}),
                        success=False, error_message="Coordinates outside Israel bounds"
                    )
                
                # Determine accuracy based on importance and class
                importance = float(result.get('importance', 0))
                if importance > 0.7:
                    accuracy = 'high'
                elif importance > 0.4:
                    accuracy = 'medium'
                else:
                    accuracy = 'low'
                
                return GeocodingResult(
                    latitude=lat, longitude=lng, accuracy=accuracy,
                    source='nominatim', address_components=result.get('address', {})
                )
            else:
                return GeocodingResult(
                    latitude=0, longitude=0, accuracy='low', source='nominatim',
                    address_components={}, success=False,
                    error_message="No results found"
                )
                
        except Exception as e:
            self.logger.warning(f"Nominatim geocoding failed for '{full_address}': {e}")
            return GeocodingResult(
                latitude=0, longitude=0, accuracy='low', source='nominatim',
                address_components={}, success=False, error_message=str(e)
            )
    
    def geocode_store(self, store_data: Dict) -> Optional[GeocodingResult]:
        """Geocode a single store using primary and fallback methods"""
        address = store_data.get('address', '').strip()
        city = store_data.get('city', '').strip()
        store_name = store_data.get('storename', 'Unknown Store')
        
        if not address or not city:
            return GeocodingResult(
                latitude=0, longitude=0, accuracy='low', source='none',
                address_components={}, success=False,
                error_message="Missing address or city"
            )
        
        self.logger.info(f"Geocoding: {store_name} - {address}, {city}")
        
        # Try Google Maps first
        if self.google_api_key:
            result = self.geocode_with_google(address, city)
            if result and result.success:
                self.stats['google_success'] += 1
                time.sleep(1.0 / GOOGLE_RATE_LIMIT)  # Rate limiting
                return result
            
            time.sleep(1.0 / GOOGLE_RATE_LIMIT)  # Rate limiting even on failure
        
        # Fallback to Nominatim
        self.logger.info(f"Falling back to Nominatim for: {store_name}")
        result = self.geocode_with_nominatim(address, city)
        if result and result.success:
            self.stats['nominatim_success'] += 1
        
        time.sleep(1.0 / NOMINATIM_RATE_LIMIT)  # Rate limiting
        return result
    
    def update_store_coordinates(self, store_id: int, result: GeocodingResult):
        """Update store coordinates in database"""
        cursor = self.db_connection.cursor()
        
        try:
            cursor.execute("""
                UPDATE stores 
                SET latitude = %s, longitude = %s, updatedat = CURRENT_TIMESTAMP
                WHERE storeid = %s
            """, (result.latitude, result.longitude, store_id))
            
            self.db_connection.commit()
            self.logger.info(f"Updated store {store_id} with coordinates: {result.latitude}, {result.longitude}")
            
        except Exception as e:
            self.db_connection.rollback()
            self.logger.error(f"Failed to update store {store_id}: {e}")
            raise
        finally:
            cursor.close()
    
    def get_stores_to_geocode(self, limit: Optional[int] = None) -> List[Dict]:
        """Get stores that need geocoding"""
        cursor = self.db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        sql = """
        SELECT storeid, retailerid, storename, address, city, postalcode
        FROM stores 
        WHERE retailerid IN (52, 150, 97) 
        AND (latitude IS NULL OR longitude IS NULL)
        AND address IS NOT NULL 
        AND city IS NOT NULL
        ORDER BY retailerid, city, storename
        """
        
        if limit:
            sql += f" LIMIT {limit}"
        
        cursor.execute(sql)
        stores = [dict(row) for row in cursor.fetchall()]
        cursor.close()
        
        return stores
    
    def save_failed_addresses(self):
        """Save failed addresses to CSV for manual review"""
        if not self.failed_addresses:
            return
            
        with open(self.failed_file, 'w', newline='', encoding='utf-8-sig') as csvfile:
            fieldnames = ['store_id', 'store_name', 'address', 'city', 'retailer_id', 
                         'error_message', 'google_maps_url', 'new_lat', 'new_lng']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for failed in self.failed_addresses:
                # Create Google Maps search URL
                search_query = quote(f"{failed['address']}, {failed['city']}, Israel")
                google_url = f"https://www.google.com/maps/search/{search_query}"
                
                writer.writerow({
                    'store_id': failed['store_id'],
                    'store_name': failed['store_name'],
                    'address': failed['address'],
                    'city': failed['city'],
                    'retailer_id': failed['retailer_id'],
                    'error_message': failed.get('error_message', ''),
                    'google_maps_url': google_url,
                    'new_lat': '',  # For manual entry
                    'new_lng': ''   # For manual entry
                })
        
        self.logger.info(f"Saved {len(self.failed_addresses)} failed addresses to {self.failed_file}")
    
    def print_summary(self):
        """Print geocoding summary"""
        print("\n" + "="*60)
        print("GEOCODING SUMMARY")
        print("="*60)
        print(f"Total stores processed: {self.stats['processed']:,}")
        print(f"Successfully geocoded: {self.stats['successful']:,} ({self.stats['successful']/max(self.stats['processed'],1)*100:.1f}%)")
        print(f"Failed geocoding: {self.stats['failed']:,}")
        print(f"Skipped (already have coords): {self.stats['skipped']:,}")
        print(f"\nBy source:")
        print(f"  Google Maps: {self.stats['google_success']:,}")
        print(f"  Nominatim: {self.stats['nominatim_success']:,}")
        print(f"  Out of bounds: {self.stats['out_of_bounds']:,}")
        
        if self.google_api_key:
            cost_estimate = (self.stats['google_success'] + self.stats['failed']) * 0.005  # $5 per 1000
            print(f"\nEstimated Google API cost: ${cost_estimate:.2f}")
        
        print("="*60)
    
    def run_geocoding(self, limit: Optional[int] = None, test_mode: bool = False):
        """Main geocoding workflow"""
        self.connect_to_db()
        
        # Load previous progress
        processed_ids = self.load_progress()
        self.logger.info(f"Loaded {len(processed_ids)} previously processed stores")
        
        # Get stores to geocode
        stores = self.get_stores_to_geocode(limit)
        self.stats['total_stores'] = len(stores)
        
        if not stores:
            self.logger.info("No stores need geocoding")
            return
        
        self.logger.info(f"Starting geocoding for {len(stores)} stores")
        if test_mode:
            self.logger.info("RUNNING IN TEST MODE - No database updates")
        
        batch_save_count = 0
        
        for i, store in enumerate(stores, 1):
            store_id = store['storeid']
            
            # Skip if already processed
            if store_id in processed_ids:
                self.stats['skipped'] += 1
                continue
            
            self.stats['processed'] += 1
            
            # Progress indicator
            print(f"\nProgress: {i}/{len(stores)} ({i/len(stores)*100:.1f}%) - Store ID: {store_id}")
            
            # Geocode the store
            result = self.geocode_store(store)
            
            if result and result.success:
                if not test_mode:
                    self.update_store_coordinates(store_id, result)
                self.stats['successful'] += 1
                self.logger.info(f"✓ Success: {result.source} - {result.accuracy} accuracy")
            else:
                self.stats['failed'] += 1
                self.failed_addresses.append({
                    'store_id': store_id,
                    'store_name': store.get('storename', ''),
                    'address': store.get('address', ''),
                    'city': store.get('city', ''),
                    'retailer_id': store.get('retailerid', ''),
                    'error_message': result.error_message if result else "Unknown error"
                })
                self.logger.warning(f"✗ Failed: {result.error_message if result else 'Unknown error'}")
            
            # Add to processed list
            processed_ids.append(store_id)
            batch_save_count += 1
            
            # Save progress every 10 successful geocodes
            if batch_save_count >= 10:
                self.save_progress(processed_ids)
                self.save_failed_addresses()
                batch_save_count = 0
                self.logger.info("Progress saved")
        
        # Final save
        self.save_progress(processed_ids)
        self.save_failed_addresses()
        
        # Print summary
        self.print_summary()
        
        if self.db_connection:
            self.db_connection.close()

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Geocode pharmacy store addresses')
    parser.add_argument('--google-key', help='Google Maps API key')
    parser.add_argument('--limit', type=int, help='Limit number of stores to process (for testing)')
    parser.add_argument('--test', action='store_true', help='Test mode - no database updates')
    
    args = parser.parse_args()
    
    if not args.google_key:
        print("Warning: No Google API key provided. Using Nominatim only.")
        print("This will be slower and potentially less accurate.")
        response = input("Continue? (y/N): ")
        if response.lower() != 'y':
            sys.exit(0)
    
    geocoder = PharmacyGeocoder(google_api_key=args.google_key)
    geocoder.run_geocoding(limit=args.limit, test_mode=args.test)

if __name__ == "__main__":
    main()