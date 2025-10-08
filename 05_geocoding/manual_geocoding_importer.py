#!/usr/bin/env python3
"""
Manual Geocoding Importer
Imports manually geocoded coordinates from CSV back to the database
"""

import csv
import psycopg2
import psycopg2.extras
import sys
import logging
from typing import List, Dict

# Configuration
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "price_comparison_app_v2"
PG_USER = "postgres"
PG_PASSWORD = "025655358"

# Israel boundary validation
ISRAEL_BOUNDS = {
    'lat_min': 29.5,
    'lat_max': 33.5,
    'lng_min': 34.0,
    'lng_max': 36.0
}

def is_within_israel_bounds(lat: float, lng: float) -> bool:
    """Check if coordinates are within Israel boundaries"""
    return (ISRAEL_BOUNDS['lat_min'] <= lat <= ISRAEL_BOUNDS['lat_max'] and
            ISRAEL_BOUNDS['lng_min'] <= lng <= ISRAEL_BOUNDS['lng_max'])

def validate_coordinates(lat_str: str, lng_str: str) -> tuple[bool, float, float]:
    """Validate and parse coordinates"""
    try:
        lat = float(lat_str.strip())
        lng = float(lng_str.strip())
        
        if not is_within_israel_bounds(lat, lng):
            print(f"Warning: Coordinates {lat}, {lng} are outside Israel bounds")
            return False, lat, lng
            
        return True, lat, lng
    except ValueError:
        return False, 0.0, 0.0

def import_manual_coordinates(csv_file: str, test_mode: bool = False):
    """Import manually geocoded coordinates from CSV"""
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    # Connect to database
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, database=PG_DATABASE,
            user=PG_USER, password=PG_PASSWORD
        )
        logger.info("Connected to database successfully")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        sys.exit(1)
    
    updates = []
    errors = []
    
    # Read CSV file
    try:
        with open(csv_file, 'r', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row_num, row in enumerate(reader, 2):  # Start at 2 (accounting for header)
                store_id = row.get('store_id', '').strip()
                new_lat = row.get('new_lat', '').strip()
                new_lng = row.get('new_lng', '').strip()
                store_name = row.get('store_name', '').strip()
                
                # Skip rows without coordinates
                if not new_lat or not new_lng:
                    continue
                
                # Validate store_id
                try:
                    store_id = int(store_id)
                except ValueError:
                    errors.append(f"Row {row_num}: Invalid store_id '{store_id}'")
                    continue
                
                # Validate coordinates
                valid, lat, lng = validate_coordinates(new_lat, new_lng)
                if not valid:
                    errors.append(f"Row {row_num}: Invalid coordinates for store {store_id}")
                    continue
                
                updates.append({
                    'store_id': store_id,
                    'latitude': lat,
                    'longitude': lng,
                    'store_name': store_name,
                    'row_num': row_num
                })
                
    except FileNotFoundError:
        logger.error(f"CSV file not found: {csv_file}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        sys.exit(1)
    
    # Print validation results
    print(f"\nValidation Results:")
    print(f"  Valid updates: {len(updates)}")
    print(f"  Errors: {len(errors)}")
    
    if errors:
        print("\nErrors found:")
        for error in errors:
            print(f"  {error}")
    
    if not updates:
        print("No valid updates to process")
        return
    
    if test_mode:
        print("\nTEST MODE - Would update the following stores:")
        for update in updates[:10]:  # Show first 10
            print(f"  Store {update['store_id']}: {update['latitude']}, {update['longitude']}")
        if len(updates) > 10:
            print(f"  ... and {len(updates) - 10} more")
        return
    
    # Confirm before proceeding
    response = input(f"\nProceed with updating {len(updates)} stores? (y/N): ")
    if response.lower() != 'y':
        print("Operation cancelled")
        return
    
    # Perform updates
    cursor = conn.cursor()
    successful_updates = 0
    failed_updates = 0
    
    for update in updates:
        try:
            cursor.execute("""
                UPDATE stores 
                SET latitude = %s, longitude = %s, updatedat = CURRENT_TIMESTAMP
                WHERE storeid = %s
            """, (update['latitude'], update['longitude'], update['store_id']))
            
            if cursor.rowcount == 0:
                errors.append(f"Store {update['store_id']} not found in database")
                failed_updates += 1
            else:
                successful_updates += 1
                logger.info(f"Updated store {update['store_id']}: {update['latitude']}, {update['longitude']}")
                
        except Exception as e:
            conn.rollback()
            errors.append(f"Failed to update store {update['store_id']}: {e}")
            failed_updates += 1
        else:
            conn.commit()
    
    cursor.close()
    conn.close()
    
    # Final summary
    print(f"\nImport Summary:")
    print(f"  Successfully updated: {successful_updates}")
    print(f"  Failed updates: {failed_updates}")
    
    if errors:
        print(f"\nFinal errors:")
        for error in errors:
            print(f"  {error}")
    
    print("Import completed")

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Import manually geocoded coordinates')
    parser.add_argument('csv_file', help='CSV file with manually geocoded coordinates')
    parser.add_argument('--test', action='store_true', help='Test mode - show what would be updated')
    
    args = parser.parse_args()
    
    import_manual_coordinates(args.csv_file, test_mode=args.test)

if __name__ == "__main__":
    main()