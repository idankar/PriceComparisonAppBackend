# Missing Store Addresses - Diagnostic Report

**Date:** September 30, 2025
**Mission:** Diagnose Missing Address Information for Pharmacy Stores

---

## Executive Summary

**Finding:** **Script Parsing/Processing Error** - ETL scripts are failing to download and process official `StoresFull` XML files from government portals, resulting in placeholder stores without address data.

**Impact:** 100 active pharmacy stores (17.3%) are missing address, city, and coordinate information.

---

## Step 1: Quantification of the Problem

### Stores Missing Address/City/Coordinates

| Retailer    | Stores Missing Data | % of Active Stores |
|-------------|--------------------:|-------------------:|
| Super-Pharm | 79                  | 20.6%              |
| Good Pharm  | 13                  | 15.7%              |
| Be Pharm    | 8                   | 7.3%               |
| **TOTAL**   | **100**             | **17.3%**          |

### Detailed Breakdown by Field

| Retailer    | Missing Address | Missing City | Missing Coordinates |
|-------------|----------------:|-------------:|--------------------:|
| Be Pharm    | 8               | 8            | 8                   |
| Good Pharm  | 13              | 13           | 13                  |
| Super-Pharm | 79              | 79           | 79                  |

**Note:** All stores missing one field are missing all three fields (address, city, coordinates).

---

## Step 2: Root Cause Diagnosis

### Sample Stores Investigated

**Be Pharm:**
- Store 703 (ID: 7661630)
- Store 779 (ID: 7687784)
- Store 787/אריאל (ID: 7695467)
- Store 819 (ID: 7700040)
- Store ONLINE (ID: 15001)

**Good Pharm:**
- Store 914 (ID: 11248417)
- Store 757 (ID: 11248418)

**Super-Pharm:**
- 79 stores with generic placeholder names

### ETL Script Analysis

#### 1. **Be Pharm ETL** (`be_pharm_etl_refactored.py`)

**Lines 126-156:** `ensure_be_pharm_stores()` function

```python
known_stores = [
    ('001', 'BE ראשי'),
    ('026', 'BE בלוך גבעתיים'),
    ...
]

for store_id, store_name in known_stores:
    self.cursor.execute("""
        INSERT INTO stores (retailerid, retailerspecificstoreid, storename, isactive)
        VALUES (%s, %s, %s, true)
        ...
    """)
```

**Problem:**
- ❌ No code to download `StoresFull` files
- ❌ Only inserts hardcoded store ID and name
- ❌ NO address, city, or coordinates inserted
- ❌ Hardcoded list only covers 17 stores

**Result:** Be Pharm stores created as placeholders without address data.

---

#### 2. **Good Pharm ETL** (`good_pharm_barcode_matching.py`)

**Lines 159-175:** Attempts to insert stores WITH address data (from web scraping)

```python
INSERT INTO stores (
    retailerid, retailerspecificstoreid,
    storename, address, city, isactive
) VALUES (%s, %s, %s, %s, %s, true)
```

**Lines 416-420:** Falls back to placeholder stores WITHOUT address data

```python
INSERT INTO stores (retailerid, retailerspecificstoreid, storename, isactive)
VALUES (%s, %s, %s, true)
```

**Problem:**
- ⚠️ Tries to get address data from web scraping
- ❌ Falls back to creating placeholder stores when scraping fails
- ❌ No fallback to official `StoresFull` XML files

**Result:** Good Pharm stores with failed scraping have no address data.

---

#### 3. **Super-Pharm ETL** (`super_pharm_barcode_matching.py`)

**Lines 127-141:** `ensure_super_pharm_stores()` function

```python
stores_file = "/Users/noa/Downloads/StoresFull7290172900007-000-202509180700"

if os.path.exists(stores_file):
    logger.info(f"Loading stores from official XML: {stores_file}")
    self.process_store_file(content, "StoresFull7290172900007-000-202509180700")
else:
    logger.info("Stores XML file not found, stores will be created as needed")
```

**Lines 143-193:** `process_store_file()` - DOES extract address data from XML

```python
store_id = store.find('StoreID')
store_name = store.find('StoreName')
address = store.find('Address')  # ✓ Extracts address
city = store.find('City')        # ✓ Extracts city

INSERT INTO stores (
    retailerid, retailerspecificstoreid,
    storename, address, city, isactive
) VALUES (%s, %s, %s, %s, %s, true)
```

**Lines 415-419:** Falls back to placeholder stores when processing prices

```python
INSERT INTO stores (retailerid, retailerspecificstoreid, storename, isactive)
VALUES (%s, %s, %s, true)
```

**Problems:**
- ❌ Hardcoded path to specific file that doesn't exist on this machine
- ❌ Path points to `/Users/noa/Downloads/` but script runs as `idankarbat`
- ❌ No code to download `StoresFull` files automatically
- ❌ Falls back to creating placeholder stores from price files
- ⚠️ Price files (PriceFull) do NOT contain store address information

**Result:** Super-Pharm stores created from price files have no address data because:
1. `StoresFull` file is not found at hardcoded path
2. Script falls back to creating stores from `PriceFull` files
3. `PriceFull` files only contain store IDs, not addresses

---

## Root Cause: Script Processing Error

### The Issue

The ETL scripts are designed to extract address data from official `StoresFull` XML files, but they are **NOT downloading or processing these files**. Instead, they:

1. Look for hardcoded file paths that don't exist
2. Fall back to creating placeholder stores from `PriceFull` files
3. `PriceFull` files only contain price data and store IDs, NOT store addresses

### Evidence

1. **Super-Pharm ETL:** Looks for `/Users/noa/Downloads/StoresFull...` (wrong user, file doesn't exist)
2. **Be Pharm ETL:** Doesn't even try to download/parse `StoresFull` files
3. **Good Pharm ETL:** Relies on web scraping, falls back to placeholders when scraping fails
4. **All ETLs:** Create placeholder stores with only ID and name when address data is unavailable

### What SHOULD Happen

According to the Israeli government price transparency law, retailers must publish:
- `StoresFull` XML files containing store information (ID, name, address, city)
- `PriceFull` XML files containing product prices per store

The ETL scripts SHOULD:
1. Download `StoresFull` XML files from government portals
2. Parse store address data from `<Address>` and `<City>` XML tags
3. Insert stores with complete information
4. THEN download `PriceFull` files and link prices to stores

### What IS Happening

The ETL scripts:
1. ❌ Skip downloading `StoresFull` files (hardcoded paths, no auto-download)
2. ✅ Download `PriceFull` files successfully
3. ❌ Create placeholder stores with only IDs when processing prices
4. ❌ Placeholder stores have NO address, city, or coordinates

---

## Diagnosis: Script Parsing/Processing Error

**Answer:** **Option B - Script Parsing Error**

The missing address data is caused by **ETL scripts failing to download and process `StoresFull` XML files** from the government portals.

### Specific Failures

1. **Super-Pharm:**
   - Hardcoded file path that doesn't exist
   - No automatic download of `StoresFull` files
   - 79 stores created as placeholders from price files

2. **Be Pharm:**
   - No code to process `StoresFull` files at all
   - Only hardcoded list of 17 stores
   - 8 stores missing addresses

3. **Good Pharm:**
   - Relies on web scraping for addresses
   - Falls back to placeholders when scraping fails
   - 13 stores missing addresses

---

## Recommendations

### Immediate Fixes

#### 1. **Super-Pharm** - Fix File Path and Add Auto-Download

```python
# Current (broken):
stores_file = "/Users/noa/Downloads/StoresFull7290172900007-000-202509180700"

# Should be:
def download_stores_file(self):
    \"\"\"Download latest StoresFull file from Super-Pharm portal\"\"\"
    # Fetch file list
    files = self.fetch_file_list()

    # Find latest StoresFull file
    stores_files = [f for f in files if 'StoresFull' in f['name']]
    if stores_files:
        latest = max(stores_files, key=lambda x: x['date'])
        return self.download_file(latest['url'])
    return None
```

#### 2. **Be Pharm** - Add StoresFull Processing

Be Pharm uses Shufersal's portal (ChainId 7290027600007). Add code to:
- Download `StoresFull` files from Shufersal portal
- Filter for Be Pharm stores (SubChainId 005)
- Parse address and city data
- Insert stores with complete information

#### 3. **Good Pharm** - Add StoresFull Fallback

When web scraping fails, try downloading `StoresFull` from Good Pharm transparency portal before falling back to placeholders.

### Long-Term Solutions

1. **Separate Stores ETL Step**
   - Create dedicated scripts to process `StoresFull` files FIRST
   - Ensure all stores have address data before processing prices
   - Run stores ETL daily, prices ETL hourly

2. **Address Data Validation**
   - Require address and city before marking stores active
   - Log warnings when stores are created without addresses
   - Add data quality checks

3. **Manual Address Research**
   - For the 100 existing stores without addresses
   - Use LLM to research and populate missing data
   - Already have workflow from geocoding mission

---

## Summary Statistics

### Current State
- **Total Active Pharmacy Stores:** 578
- **Stores WITH Address Data:** 478 (82.7%)
- **Stores WITHOUT Address Data:** 100 (17.3%)

### By Retailer
| Retailer    | Total Active | With Address | Missing | % Complete |
|-------------|-------------:|-------------:|--------:|-----------:|
| Super-Pharm | 385          | 306          | 79      | 79.5%      |
| Good Pharm  | 83           | 70           | 13      | 84.3%      |
| Be Pharm    | 110          | 102          | 8       | 92.7%      |

### Impact
- ✅ 82.7% of stores are fully functional with location data
- ⚠️ 17.3% of stores cannot be used for location-based features
- 🎯 All 100 stores CAN be fixed by downloading/parsing StoresFull files

---

## Next Steps

1. **Immediate:** Fix Super-Pharm ETL to auto-download StoresFull files
2. **Short-term:** Add StoresFull processing to Be Pharm and Good Pharm ETLs
3. **Manual:** Use LLM research workflow to fill in 100 missing addresses
4. **Long-term:** Implement dedicated stores ETL pipeline
5. **Validation:** Add data quality checks and monitoring

---

## Conclusion

The root cause is definitively a **Script Processing Error**. The ETL scripts have the ability to parse address data from XML files (code exists in Super-Pharm ETL), but they fail to:
1. Download the `StoresFull` XML files automatically
2. Use correct file paths
3. Process stores before processing prices

The source XML files from government portals DO contain address information - the scripts are simply not downloading or parsing them correctly.
