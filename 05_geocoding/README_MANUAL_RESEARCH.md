# Manual Store Research Workflow

## Overview
114 pharmacy stores are missing address/city data and cannot be geocoded automatically. This workflow guides you through researching and importing that data.

## Files Created

1. **`missing_store_data.csv`** - List of 114 stores needing research
2. **`LLM_STORE_RESEARCH_INSTRUCTIONS.md`** - Instructions for the LLM researcher
3. **`import_researched_stores.py`** - Script to import completed research into database

## Workflow

### Step 1: Provide Task to LLM

Give your LLM these two files:
- `missing_store_data.csv` (input data)
- `LLM_STORE_RESEARCH_INSTRUCTIONS.md` (instructions)

Ask the LLM to:
> "Please research the missing store addresses following the instructions in LLM_STORE_RESEARCH_INSTRUCTIONS.md and using the data in missing_store_data.csv. Return the completed CSV file."

### Step 2: Receive Results

The LLM should return a file named `completed_store_data.csv` with this format:
```csv
store_id,address,city,notes
7678835,אוסישקין 34,תל אביב,Found on BE Pharm website
7696681,רחוב זית שמן 3,אפרת,Verified location
```

### Step 3: Import Data

Place the `completed_store_data.csv` in this directory and run:
```bash
python3 import_researched_stores.py
```

This will:
- Update the database with new addresses and cities
- Show summary of updates
- Report any errors

### Step 4: Run Geocoding

After importing, geocode the newly updated stores:
```bash
rm -f geocoding_progress.json
python3 pharmacy_only_geocoding.py --google-key "***REMOVED***" > geocoding_round2.log 2>&1 &
```

Monitor progress:
```bash
tail -f geocoding_round2.log
```

### Step 5: Verify Results

Check final geocoding status:
```bash
python3 -c "
import psycopg2
conn = psycopg2.connect(host='localhost', port=5432, database='price_comparison_app_v2', user='postgres', password='***REMOVED***')
cur = conn.cursor()
cur.execute('SELECT r.retailername, COUNT(*) FROM stores s JOIN retailers r ON s.retailerid=r.retailerid WHERE s.retailerid IN (52,150,97) AND s.latitude IS NOT NULL GROUP BY r.retailername')
for row in cur.fetchall(): print(f'{row[0]}: {row[1]} stores geocoded')
"
```

## Summary

**Current Status:**
- ✅ Geocoded: 463 stores
- ⏳ Missing data: 114 stores
  - Be Pharm: 21 stores
  - Good Pharm: 14 stores
  - Super-Pharm: 79 stores

**Expected After Research:**
- 🎯 Target: ~550+ stores with coordinates
- Stores with truly missing data (online/closed stores) will remain without coordinates
