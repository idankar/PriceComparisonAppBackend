# Store Address Research Task

## Objective
Research and find complete address information (street address and city) for 114 pharmacy stores in Israel that are missing location data.

## Input File
`missing_store_data.csv` contains the following columns:
- `store_id`: Database ID (do not modify)
- `retailer`: Pharmacy chain name (Be Pharm, Good Pharm, or Super-Pharm)
- `store_name`: Store name (often contains location hints in Hebrew)
- `current_address`: Existing address (may be NULL or incomplete)
- `current_city`: Existing city (may be NULL or incomplete)

## Your Task
For each store in the CSV:

1. **Skip online/virtual stores** - If store name contains "Online", "אונליין", or only has generic IDs like "Store 017", skip it (leave address/city empty)

2. **Research the store location** using:
   - Web searches for the store name + retailer
   - The retailer's official website store locators:
     - BE Pharm: https://www.bestore.co.il/online/he/branchs
     - Super-Pharm: https://shop.super-pharm.co.il/branches
     - Good Pharm: Search online
   - Google Maps searches
   - Store name often contains city/location in Hebrew

3. **Extract information**:
   - **Street address**: Full street address in Hebrew (e.g., "דיזנגוף 50")
   - **City**: City name in Hebrew (e.g., "תל אביב")

4. **Validation**:
   - Verify the store actually exists and is active
   - Ensure address and city are in Hebrew (original language)
   - If you cannot find reliable information, leave fields empty

## Output Format
Create a new CSV file named `completed_store_data.csv` with these columns:
```
store_id,address,city,notes
```

### Example Output:
```csv
store_id,address,city,notes
7678835,אוסישקין 34,תל אביב,Found on BE Pharm website
7696681,רחוב זית שמן 3,אפרת,Verified location
7695467,,,Could not find - may be closed
16678063,,,Generic ID - no location info available
```

## Important Notes
- **Do NOT modify store_id** - this is the database key
- Use Hebrew characters for addresses and cities
- If uncertain, leave empty rather than guessing
- Add helpful notes about your research findings
- Focus on accuracy over speed

## Deliverable
Return the completed CSV file (`completed_store_data.csv`) with all stores researched.

Total stores to research: **114 stores**
- Be Pharm: 21 stores
- Good Pharm: 14 stores
- Super-Pharm: 79 stores
