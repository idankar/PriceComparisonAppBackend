from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from PIL import Image, ImageOps
from rapidfuzz import fuzz
import pytesseract
import time
import os
import re

# === Screenshot Grid ===
def capture_grid_screenshots(driver, cols=1, rows=3, base_folder="screenshots", base_filename="screenshot"):
    os.makedirs(base_folder, exist_ok=True)
    screenshots = []

    driver.execute_script("document.body.style.zoom='125%'")
    time.sleep(2)

    window_width = driver.execute_script("return document.body.scrollWidth")
    window_height = driver.execute_script("return document.body.scrollHeight")
    view_width = driver.get_window_size()["width"]
    view_height = driver.get_window_size()["height"]

    x_steps = min(cols, int(window_width / view_width) + 1)
    y_steps = rows

    for col in range(x_steps):
        driver.execute_script(f"window.scrollTo({col * view_width}, 0)")
        time.sleep(1)

        for row in range(y_steps):
            y_scroll = int((row / y_steps) * window_height)
            driver.execute_script(f"window.scrollTo({col * view_width}, {y_scroll})")
            time.sleep(1)

            filename = f"{base_filename}_{col}_{row}.png"
            full_path = os.path.join(base_folder, filename)
            driver.save_screenshot(full_path)
            screenshots.append(full_path)
            print(f"📸 Captured {full_path}")

    return screenshots

# === Image Enhancement ===
def enhance_image(image_path):
    img = Image.open(image_path)
    img = img.convert("L")
    img = ImageOps.autocontrast(img)
    width, height = img.size
    img = img.resize((int(width * 1.5), int(height * 1.5)))
    return img

# === OCR + Filtering ===
def extract_prices_from_image(img, image_path=None, debug=True):
    text = pytesseract.image_to_string(img, lang="heb+eng", config="--psm 6")

    if image_path:
        print(f"\n📝 OCR output from: {image_path}")
        print("-" * 50)
        print("\n".join(text.splitlines()[:20]))
        print("-" * 50)

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    results = []

    for i, line in enumerate(lines):
        if any(sym in line for sym in ["₪", "w", "m"]):
            price_match = re.search(r"[₪wm]\s*([\d\.,]+)", line)
            if not price_match:
                continue
            try:
                price = float(price_match.group(1).replace(",", "."))
            except:
                continue

            title = "Unknown"
            for j in range(i-1, i-6, -1):
                if j < 0:
                    break
                candidate = lines[j].strip()
                clean = re.sub(r"[^א-תa-zA-Z0-9\s]", "", candidate)

                if len(clean) < 6:
                    if debug: print(f"⛔ Rejected (too short): {clean}")
                    continue
                if not re.search(r"[א-תa-zA-Z]", clean):
                    if debug: print(f"⛔ Rejected (no letters): {clean}")
                    continue
                if re.match(r"^(INT|N70|N7[0-9]{2}|ATO|mw|O[0-9]{2})", clean):
                    if debug: print(f"⛔ Rejected (technical or label code): {clean}")
                    continue
                digit_ratio = sum(c.isdigit() for c in clean) / max(len(clean), 1)
                if digit_ratio > 0.5:
                    if debug: print(f"⛔ Rejected (too many digits): {clean}")
                    continue

                # Final fuzzy match
                hebrew_score = fuzz.partial_ratio("נוטלה", clean)
                english_score = fuzz.partial_ratio("nutella", clean.lower())
                max_score = max(hebrew_score, english_score)

                if max_score < 40:
                    if debug: print(f"⛔ Rejected (low similarity {max_score}%): {clean}")
                    continue
                else:
                    if debug: print(f"✅ Accepted (similarity {max_score}%): {clean}")

                title = clean
                break

            if title != "Unknown":
                results.append({"title": title, "price_ils": price})

    return results

# === Main Scraper ===
def scrape_shufersal_grid_ocr(query):
    print(f"\n🔍 OCR Grid Scraping for: {query}")

    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.binary_location = "/Applications/Chromium.app/Contents/MacOS/Chromium"

    driver = webdriver.Chrome(service=Service(), options=options)
    driver.set_window_size(1400, 1000)
    driver.get("https://www.shufersal.co.il/online/he/")

    try:
        search_box = driver.find_element(By.ID, "js-site-search-input")
        search_box.clear()
        search_box.send_keys(query)
        search_box.send_keys(Keys.RETURN)
        time.sleep(5)

        screenshots = capture_grid_screenshots(driver, cols=1, rows=3)
    finally:
        driver.quit()

    all_results = []
    for path in screenshots:
        img = enhance_image(path)
        results = extract_prices_from_image(img, image_path=path, debug=True)
        all_results.extend(results)

    return all_results

# === Run ===
if __name__ == "__main__":
    query = "נוטלה"
    results = scrape_shufersal_grid_ocr(query)

    if results:
        print("\n🛒 OCR Results:")
        for item in results:
            print(f"{item['title']}: ₪{item['price_ils']}")
    else:
        print("⚠️ No products found.")
