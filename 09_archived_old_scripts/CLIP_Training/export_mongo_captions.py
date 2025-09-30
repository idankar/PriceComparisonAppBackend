import json
from pymongo import MongoClient
from tqdm import tqdm

# --- CONFIGURATION ---
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "price_comparison"
COLLECTION_NAME = "products"
OUTPUT_PATH = "captions.txt"

# --- CONNECT TO MONGO ---
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# --- FETCH DOCUMENTS AND BUILD CAPTIONS ---
captions = []
count = 0
for doc in tqdm(collection.find({}, {"product_id": 1, "name_he": 1, "brand": 1, "_id": 0})):
    product_id = doc.get("product_id", "")
    name = doc.get("name_he", "")
    brand = doc.get("brand", "")

    if not (product_id and name and brand):
        continue

    # Flip field order for English brand names (heuristic: contains any Latin letter)
    if any(c.isascii() and c.isalpha() for c in brand):
        caption = f"{brand} :מותג\n{name} :שם מוצר"
    else:
        caption = f"שם מוצר: {name}\nמותג: {brand}"

    captions.append(f"{product_id}\n{caption}")
    count += 1

# --- SAVE TO FILE ---
with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    for cap in captions:
        f.write(cap + "\n")

print(f"✅ Exported {count} captions to {OUTPUT_PATH}")
