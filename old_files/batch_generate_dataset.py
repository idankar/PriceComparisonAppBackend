import subprocess
import os
from datetime import datetime

# === Optional: reset old data ===
def reset_old_data():
    print("🧹 Clearing old cropped products...")
    if os.path.exists("cropped_products"):
        for f in os.listdir("cropped_products"):
            os.remove(os.path.join("cropped_products", f))
    print("🧹 Clearing previous OCR output...")
    if os.path.exists("ocr_results.csv"):
        os.remove("ocr_results.csv")
    if os.path.exists("donut_data/train.json"):
        os.remove("donut_data/train.json")
    if os.path.exists("donut_data/train.cleaned.json"):
        os.remove("donut_data/train.cleaned.json")
    print("✅ Old data cleared.\n")

# === Define your list of 300 common grocery search terms ===
POPULAR_QUERIES = [
    "חלב", "שוקולד", "גבינה", "מעדן", "יוגורט", "קוטג'", "שמנת",
    "קפה", "תה", "קולה", "פפסי", "מים מינרליים", "מיץ תפוזים", "מיץ תפוחים",
    "אורז", "פסטה", "קוסקוס", "רוטב עגבניות", "רסק עגבניות", "מלח", "סוכר", "שמן",
    "חומץ", "טחינה", "חטיפים", "במבה", "ביסלי", "עוגיות", "לחם", "לחמניה", "חלה",
    "חיתולים", "מגבונים", "שמפו", "סבון", "נייר טואלט", "נייר סופג", "אקונומיקה",
    "סבון כלים", "כלים חד פעמיים", "חומוס", "סלטים", "פירות", "ירקות", "קישוא",
    "תפוח", "בננה", "תפוז", "ענבים", "עגבניות", "מלפפון", "חציל", "אבטיח",
    "אוכל לחתולים", "אוכל לכלבים", "עצמות לעיסה", "חול לחתולים", "ביצים", "חמאה",
    "מרגרינה", "נקניקים", "שניצל", "פיצה", "פיתה", "קרואסון", "קורנפלקס",
    "חטיפי בריאות", "קינמון", "תבלינים", "קמח", "פתי בר", "שוקו", "משקה אנרגיה",
    # ... continue to expand to 300
]

# === Run the pipeline for each query ===
def run_pipeline():
    for i, query in enumerate(POPULAR_QUERIES, start=1):
        print(f"\n🔍 [{i}/{len(POPULAR_QUERIES)}] Searching for: {query}")

        subprocess.run(["python", "scraper.py", query], check=True)
        print("✅ Scraper done.")

        subprocess.run(["python", "yolo_detect_crop.py"], check=True)
        print("✅ YOLO cropping done.")

        subprocess.run(["python", "full_ocr_loop.py", "cropped_products"], check=True)
        print("✅ OCR extraction done.")



# === Convert OCR results to Donut format and clean ===
def finalize_labels():
    print("\n🔁 Converting OCR output to Donut JSON...")
    subprocess.run(["python", "generate_donut_json_from_csv.py"], check=True)
    print("🧽 Cleaning labels and renaming images...")
    subprocess.run(["python", "clean_labels.py"], check=True)

# === Entry point ===
if __name__ == "__main__":
    print("✅ Batch script started...")
    print(f"\n🚀 Starting batch generation: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    reset_old_data()
    run_pipeline()
    finalize_labels()
    print(f"\n✅ Done! You can now train with: python donut_finetune.py")
