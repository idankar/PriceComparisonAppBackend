import csv
import json
import os

CSV_PATH = "ocr_results.csv"
OUT_JSON = "donut_data/train.json"
IMAGE_FOLDER = IMAGE_FOLDER = "donut_data/images"


donut_data = []

with open(CSV_PATH, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        image = os.path.basename(row.get("crop", "").strip())
        name = row.get("product_name", "").strip()
        price = row.get("price", "").strip()

        if not image or not name or not price:
            continue

        image_path = os.path.join(IMAGE_FOLDER, image)
        if not os.path.isfile(image_path):
            print(f"[SKIP] Image not found: {image_path}")
            continue

        donut_data.append({
            "image": image,
            "label": {
                "name": name,
                "price": price
            }
        })

os.makedirs("donut_data", exist_ok=True)
with open(OUT_JSON, "w", encoding="utf-8") as f:
    json.dump(donut_data, f, ensure_ascii=False, indent=2)

print(f"✅ Converted {len(donut_data)} items into {OUT_JSON}")
