# Manual Geocoding Instructions

This guide explains how to manually geocode failed pharmacy store addresses.

## Overview

When the automated geocoding script fails to find coordinates for some stores, they are exported to `failed_geocoding.csv` for manual processing. You'll need to look up these addresses manually and enter the coordinates.

## Process

### 1. Open the Failed Addresses File

Open `failed_geocoding.csv` in Excel or Google Sheets. You'll see these columns:
- `store_id` - Database ID of the store
- `store_name` - Name of the pharmacy
- `address` - Street address 
- `city` - City name
- `retailer_id` - Pharmacy chain ID (52=Super-Pharm, 150=Be Pharm, 97=Good Pharm)
- `error_message` - Why automatic geocoding failed
- `google_maps_url` - Pre-built search URL for Google Maps
- `new_lat` - **Enter latitude here**
- `new_lng` - **Enter longitude here**

### 2. For Each Failed Address

1. **Click the Google Maps URL** - This will open Google Maps with a search for the address
2. **Find the correct location** on the map
3. **Right-click on the exact store location** 
4. **Select "What's here?"** or the coordinates option
5. **Copy the latitude and longitude** (format: 32.0853, 34.7818)
6. **Paste coordinates** into the `new_lat` and `new_lng` columns

### 3. Tips for Finding Stores

**Shopping Centers/Malls:**
- Look for the main entrance or central location
- Don't place at parking lot - place at building entrance

**Multiple Results:**
- Choose the most specific/accurate location
- Prefer addresses over general city results

**Hard to Find Addresses:**
- Try searching just the city name
- Look for landmarks mentioned in the store name
- Search for the pharmacy chain name + city

**Verification:**
- Check that coordinates are in Israel (latitude ~29-34, longitude ~34-36)
- Make sure it's not in the Mediterranean Sea or desert

### 4. Example

For "סופר-פארם ביג-עפולה, רח' השוק 13, עפולה":

1. Click the Google Maps URL
2. Maps shows the location on HaShuk Street in Afula
3. Right-click on the Super-Pharm store location
4. Copy coordinates: 32.6025, 35.2902
5. Enter:
   - `new_lat`: 32.6025
   - `new_lng`: 35.2902

### 5. Quality Guidelines

**High Quality** (preferred):
- Exact store location visible on map
- Building-level accuracy
- Street address matches

**Medium Quality** (acceptable):
- General building/block location
- Close to correct address
- Within 100 meters

**Low Quality** (avoid):
- City center only
- Wrong neighborhood
- Outside city bounds

### 6. Import Back to Database

When you're done:

1. **Save the CSV file** (keep UTF-8 encoding)
2. **Run the import script**:
   ```bash
   python manual_geocoding_importer.py failed_geocoding.csv --test
   ```
3. **Review the test output**
4. **Run without --test to actually import**:
   ```bash
   python manual_geocoding_importer.py failed_geocoding.csv
   ```

## Troubleshooting

**Can't find the address?**
- Try searching without house numbers
- Search for nearby landmarks
- Use the pharmacy chain name + city

**Coordinates look wrong?**
- Israel coordinates should be roughly:
  - Latitude: 29.5 to 33.5
  - Longitude: 34.0 to 36.0
- If outside these bounds, double-check

**Address in wrong city?**
- Some stores may have moved or closed
- Use your best judgment for current location
- Add notes in error_message if needed

## Contact

If you have questions about specific addresses or the process, check the geocoding logs or ask for assistance.