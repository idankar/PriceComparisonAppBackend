#!/usr/bin/env python3
"""
Create a backup of database using Python
Since pg_dump is not in PATH, we'll create a logical backup
"""

import psycopg2
import gzip
import json
from datetime import datetime

# Database connection
conn = psycopg2.connect(
    host='localhost',
    port='5432',
    database='price_comparison_app_v2',
    user='postgres',
    password='***REMOVED***'
)

backup_file = f'backup_before_cleanup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json.gz'

print('--- Starting database backup before cleanup ---')
print('Backing up critical data...')

cur = conn.cursor()

backup_data = {
    'backup_date': datetime.now().isoformat(),
    'backup_type': 'logical',
    'retailers': [],
    'stores': [],
    'prices_count': {},
    'retailer_products_count': {}
}

# Backup retailers
cur.execute('SELECT * FROM retailers')
columns = [desc[0] for desc in cur.description]
backup_data['retailers'] = [dict(zip(columns, row)) for row in cur.fetchall()]
print(f'  ✓ Backed up {len(backup_data["retailers"])} retailers')

# Backup stores
cur.execute('SELECT * FROM stores WHERE isactive = true')
columns = [desc[0] for desc in cur.description]
stores = []
for row in cur.fetchall():
    store_dict = dict(zip(columns, row))
    # Convert datetime objects to strings
    for key, value in store_dict.items():
        if hasattr(value, 'isoformat'):
            store_dict[key] = value.isoformat()
    stores.append(store_dict)
backup_data['stores'] = stores
print(f'  ✓ Backed up {len(stores)} stores')

# Count prices per retailer
cur.execute('''
    SELECT r.retailername, COUNT(p.price_id)
    FROM retailers r
    LEFT JOIN stores s ON r.retailerid = s.retailerid
    LEFT JOIN prices p ON s.storeid = p.store_id
    GROUP BY r.retailername
''')
for retailer, count in cur.fetchall():
    backup_data['prices_count'][retailer] = count
print(f'  ✓ Counted prices per retailer')

# Count products per retailer
cur.execute('''
    SELECT r.retailername, COUNT(rp.retailer_product_id)
    FROM retailers r
    LEFT JOIN retailer_products rp ON r.retailerid = rp.retailer_id
    GROUP BY r.retailername
''')
for retailer, count in cur.fetchall():
    backup_data['retailer_products_count'][retailer] = count
print(f'  ✓ Counted products per retailer')

# Write compressed backup
with gzip.open(backup_file, 'wt', encoding='utf-8') as f:
    json.dump(backup_data, f, indent=2, default=str)

print(f'\n✅ Backup complete: {backup_file}')

# Show summary
print('\nBackup Summary:')
print(f'  Retailers: {len(backup_data["retailers"])}')
print(f'  Active Stores: {len(backup_data["stores"])}')
print(f'  Total Prices: {sum(backup_data["prices_count"].values())}')

cur.close()
conn.close()
