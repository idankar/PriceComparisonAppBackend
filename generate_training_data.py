import os
import shutil
import subprocess
import csv
import json
import re

SCREENSHOT_DIR = "screenshots"
CROP_DIR = "cropped_products"
DONUT_DIR = "donut_data"
DONUT_IMG_DIR = os.path.join(DONUT_DIR, "images")
CSV_PATH = "ocr_results.csv"
TRAIN_JSON = os.path.join(DONUT_DIR, "train.json")

def run_yolo():
    print("🚀 Running YOLO cropper...")
    subprocess.run(["python", "yolo_detect_crop.py"], check=True)

def run_ocr():
    print("🧠 Running OCR on crops...")
    subprocess.run(["python", "full_ocr_loop.py", CROP_DIR], check=True)

def slugify(text):
    return re.sub(r"[^\wא-ת]+", "_", text).strip("_")

def convert_csv_to_json():
    print("🔁 Converting OCR results to Donut JSON...")
    donut_data = []
    existing_names = set()

    with open(CSV_PATH, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            raw_image = os.path.basename(row.get("crop", "").strip())
            name = row.get("product_name", "").strip()
            price = row.get("price", "").strip()
            full_ocr_text = row.get("full_ocr_text", "").strip()

            if not raw_image or not name or not price:
                continue

            src_path = os.path.join(CROP_DIR, raw_image)
            if not os.path.isfile(src_path):
                continue

            base_name = slugify(name)
            filename = f"{base_name}.png"
            suffix = 1
            while filename in existing_names or os.path.exists(os.path.join(DONUT_IMG_DIR, filename)):
                filename = f"{base_name}_{suffix}.png"
                suffix += 1
            existing_names.add(filename)

            dest_path = os.path.join(DONUT_IMG_DIR, filename)
            os.makedirs(DONUT_IMG_DIR, exist_ok=True)
            shutil.copy2(src_path, dest_path)

            donut_data.append({
                "image": filename,
                "label": {
                    "name": name,
                    "price": price
                },
                "full_ocr_text": full_ocr_text
            })

    os.makedirs(DONUT_DIR, exist_ok=True)
    with open(TRAIN_JSON, "w", encoding="utf-8") as f:
        json.dump(donut_data, f, ensure_ascii=False, indent=2)

    print(f"✅ Generated {len(donut_data)} entries in {TRAIN_JSON}")
    print("📂 Images saved to:", DONUT_IMG_DIR)
    print("\n🧐 Please review the generated file before training:")
    print(f"  open {TRAIN_JSON}")
    print(f"  open {DONUT_IMG_DIR}")

if __name__ == "__main__":
    print("🔁 Starting full training data generation loop...")
    run_yolo()
    run_ocr()
    convert_csv_to_json()
    print("\n✅ Done. You're ready to review and train!")
