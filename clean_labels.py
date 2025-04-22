import json
import re
import os
from rapidfuzz import process, fuzz
import shutil

INPUT = "donut_data/train.json"
OUTPUT = "donut_data/train.cleaned.json"
IMAGE_FOLDER = "donut_data/images"

KNOWN_NAMES = [
    "ממרח נוטלה",
    "שוקולד פרה",
    "חלב תנובה",
    "קוטג'",
    "יוגורט",
    "קפה נמס",
    "ביסלי",
    "תפוצ׳יפס",
    "עוגיות",
    "מיץ תפוזים",
]

def slugify(text):
    return re.sub(r"[^\wא-ת]+", "_", text).strip("_")

def extract_quantity(raw_text: str) -> str:
    match = re.search(r'\d{2,4}\s*(גרם|יחידות|מ"ל|ליטר|מ״ל)', raw_text)
    return match.group(0).strip() if match else ""

def smart_dedup(text: str) -> str:
    words = text.split()
    seen = []
    for w in words:
        if w not in seen:
            seen.append(w)
    return " ".join(seen)

def clean_name(name: str, full_ocr_text: str) -> str:
    # Extract quantity from full OCR block
    quantity = extract_quantity(full_ocr_text)

    # Remove digits not attached to quantity
    name = re.sub(r'\b[01]\b', '', name)

    # Deduplicate and normalize
    base = smart_dedup(name.strip())

    # Attach quantity if not already included
    if quantity and quantity not in base:
        base = f"{base} {quantity}"

    return base.strip()

def fuzzy_match(name: str) -> str:
    best, score, _ = process.extractOne(name, KNOWN_NAMES, scorer=fuzz.token_sort_ratio)
    return best if score > 85 else name

def clean_labels(data):
    cleaned = []
    used_filenames = set()

    for entry in data:
        raw_name = entry["label"]["name"]
        full_ocr_text = entry.get("full_ocr_text", "")
        price = entry["label"]["price"]

        name = clean_name(raw_name, full_ocr_text)
        name = fuzzy_match(name)

        base_name = slugify(name)
        filename = f"{base_name}.png"
        suffix = 1
        while filename in used_filenames or os.path.exists(os.path.join(IMAGE_FOLDER, filename)):
            filename = f"{base_name}_{suffix}.png"
            suffix += 1
        used_filenames.add(filename)

        old_path = os.path.join(IMAGE_FOLDER, entry["image"])
        new_path = os.path.join(IMAGE_FOLDER, filename)
        if os.path.exists(old_path) and old_path != new_path:
            shutil.move(old_path, new_path)

        entry["image"] = filename
        entry["label"]["name"] = name
        cleaned.append(entry)

    return cleaned

if __name__ == "__main__":
    with open(INPUT, encoding="utf-8") as f:
        data = json.load(f)

    cleaned = clean_labels(data)

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)

    print(f"✅ Cleaned and renamed {len(cleaned)} labels → {OUTPUT}")
