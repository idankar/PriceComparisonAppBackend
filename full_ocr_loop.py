import os
import cv2
import pytesseract
import csv
from glob import glob
from typing import List, Tuple, Optional
from ocr_postprocessing import clean_ocr_text, extract_product_and_price


def run_ocr_on_image(image_path: str, lang: str = "heb+eng") -> List[str]:
    """
    Run Tesseract OCR on an image with preprocessing.
    """
    image = cv2.imread(image_path)
    if image is None:
        print(f"[WARN] Could not read image: {image_path}")
        return []

    # Step 1: Convert to grayscale
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # Step 2: Resize (2x scale for better OCR on small fonts)
    height, width = gray.shape
    gray = cv2.resize(gray, (width * 2, height * 2), interpolation=cv2.INTER_LINEAR)

    # Step 3: Adaptive thresholding (binarize)
    thresh = cv2.adaptiveThreshold(
        gray, 255,
        cv2.ADAPTIVE_THRESH_MEAN_C,
        cv2.THRESH_BINARY_INV,
        blockSize=15,
        C=10
    )

    # Step 4: Optional dilation (strengthen digits if needed)
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 2))
    processed = cv2.dilate(thresh, kernel, iterations=1)

    # Step 5: Run OCR
    config = '--psm 6'
    text = pytesseract.image_to_string(processed, config=config, lang=lang)
    lines = text.splitlines()
    return [line.strip() for line in lines if line.strip()]



def process_folder(folder_path: str, output_csv: str = "ocr_results.csv"):
    """
    Process all cropped product images in a folder and save results to CSV.
    """
    product_crops = sorted(glob(os.path.join(folder_path, "product_*.png")))
    all_results = []

    for crop_path in product_crops:
        screenshot_id = os.path.basename(folder_path)
        crop_filename = os.path.basename(crop_path)

        print(f"\n[INFO] Processing {crop_filename}...")

        ocr_lines = run_ocr_on_image(crop_path)
        print(f"[DEBUG] OCR raw lines:")
        for line in ocr_lines:
            print(f"  RAW: {line}")

        cleaned_lines = clean_ocr_text(ocr_lines)
        print(f"[DEBUG] Cleaned lines:")
        for line in cleaned_lines:
            print(f"  CLEANED: {line}")

        product_info = extract_product_and_price(cleaned_lines)
        if not product_info:
            print("[DEBUG] No product/price pairs found in this image.")

        for name, price in product_info:
            all_results.append({
                "screenshot_id": screenshot_id,
                "crop": crop_filename,
                "product_name": name,
                "price": price
            })

    # Save to CSV
    if all_results:
        with open(output_csv, mode="w", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["screenshot_id", "crop", "product_name", "price"])
            writer.writeheader()
            writer.writerows(all_results)
        print(f"\n[INFO] Saved {len(all_results)} results to {output_csv}")
    else:
        print("\n[INFO] No valid product results found.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run OCR over product crops")
    parser.add_argument("folder", help="Folder containing product_*.png")
    args = parser.parse_args()

    process_folder(args.folder)
