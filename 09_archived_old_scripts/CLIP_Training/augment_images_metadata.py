import os
import json
from pymongo import MongoClient
from PIL import Image
from io import BytesIO
from torchvision import transforms
from tqdm import tqdm

# --- CONFIGURATION ---
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "price_comparison"
FILES_COLLECTION = "product_images.files"
CHUNKS_COLLECTION = "product_images.chunks"
PRODUCTS_COLLECTION = "products"
OUTPUT_DIR = "data/augmented_dataset"
METADATA_PATH = os.path.join(OUTPUT_DIR, "metadata.json")
IMG_SIZE = 224
AUGS_PER_IMAGE = 5

# --- CREATE OUTPUT DIR ---
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- DB CONNECTION ---
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
files_col = db[FILES_COLLECTION]
chunks_col = db[CHUNKS_COLLECTION]
products_col = db[PRODUCTS_COLLECTION]

# --- TRANSFORM ---
augmentations = transforms.Compose([
    transforms.Resize((IMG_SIZE, IMG_SIZE)),
    transforms.RandomApply([transforms.ColorJitter(0.2, 0.2)], p=0.7),
    transforms.RandomRotation(5),
    transforms.RandomHorizontalFlip(),
    transforms.ToTensor()
])

# --- METADATA COLLECTION ---
metadata = {}

# --- IMAGE LOOP ---
print("Processing product images from MongoDB...")
for file_doc in tqdm(files_col.find()):
    filename = file_doc.get("filename", "")
    product_id = file_doc.get("metadata", {}).get("product_id")
    if not product_id:
        continue

    product = products_col.find_one({"product_id": product_id}, {"name_he": 1, "brand": 1, "_id": 0})
    if not product:
        continue

    file_id = file_doc["_id"]
    chunks = list(chunks_col.find({"files_id": file_id}).sort("n", 1))
    image_data = b"".join(chunk["data"] for chunk in chunks)

    try:
        image = Image.open(BytesIO(image_data)).convert("RGB")
    except Exception as e:
        print(f"❌ Could not decode image for product {product_id}: {e}")
        continue

    for i in range(AUGS_PER_IMAGE):
        aug_image = augmentations(image)
        aug_filename = f"augmented_{product_id}_{i}.jpg"
        aug_path = os.path.join(OUTPUT_DIR, aug_filename)
        transforms.ToPILImage()(aug_image).save(aug_path)

        metadata[aug_filename] = {
            "product_id": product_id,
            "name_he": product.get("name_he", ""),
            "brand": product.get("brand", "")
        }

# --- SAVE METADATA ---
with open(METADATA_PATH, "w", encoding="utf-8") as f:
    json.dump(metadata, f, ensure_ascii=False, indent=2)

print(f"\n✅ Finished. Augmented images saved to '{OUTPUT_DIR}', metadata saved to '{METADATA_PATH}'")
