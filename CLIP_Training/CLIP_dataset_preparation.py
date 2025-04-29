import os
import random
import json
from PIL import Image, ImageEnhance, ImageOps, ImageDraw
import torch
from torchvision import transforms
from torch.utils.data import Dataset, DataLoader
import matplotlib.pyplot as plt
import matplotlib as mpl

# Force matplotlib to use a consistent font that supports Hebrew
mpl.rcParams['font.family'] = 'DejaVu Sans'

# --- CONFIGURATION ---
PRODUCTS_DIR = '/Users/noa/Desktop/PriceComparisonApp/data/products'
OUTPUT_DIR = '/Users/noa/Desktop/PriceComparisonApp/data/augmented_dataset'
SELECT_N_PRODUCTS = 20  # Limiting for quick test
IMG_SIZE = 224

# Create output dir if needed
os.makedirs(OUTPUT_DIR, exist_ok=True)

# For saving metadata
metadata_dict = {}

def reverse_hebrew_text(text):
    return text[::-1]

def augment_image(img):
    augmentations = []

    # Original
    augmentations.append(img)

    # Lighten
    enhancer = ImageEnhance.Brightness(img)
    augmentations.append(enhancer.enhance(1.3))

    # Darken
    enhancer = ImageEnhance.Brightness(img)
    augmentations.append(enhancer.enhance(0.7))

    # Slight rotation
    augmentations.append(img.rotate(5))
    augmentations.append(img.rotate(-5))

    # Small occlusion (white square)
    occluded = img.copy()
    draw = ImageDraw.Draw(occluded)
    x = random.randint(20, 180)
    y = random.randint(20, 180)
    draw.rectangle([x, y, x+20, y+20], fill=(255,255,255))
    augmentations.append(occluded)

    return augmentations[:6]

# --- MAIN DATASET PREPARATION ---
if __name__ == '__main__':
    product_ids = random.sample(os.listdir(PRODUCTS_DIR), SELECT_N_PRODUCTS)
    print(f"Selected products: {product_ids}")

    transform = transforms.Compose([
        transforms.Resize((IMG_SIZE, IMG_SIZE)),
    ])

    for pid in product_ids:
        metadata_path = os.path.join(PRODUCTS_DIR, pid, 'metadata.json')
        try:
            with open(metadata_path, 'r') as f:
                meta = json.load(f)
            name_he = meta.get('name_he', '')
            brand = meta.get('brand', '')

            img1_path = os.path.join(PRODUCTS_DIR, pid, '001.jpg')
            img2_path = os.path.join(PRODUCTS_DIR, pid, '002.jpg')

            if os.path.exists(img2_path):
                img = Image.open(img2_path).convert('RGB')
            else:
                img = Image.open(img1_path).convert('RGB')

            augmented_imgs = augment_image(img)

            for idx, aug_img in enumerate(augmented_imgs):
                aug_img = transform(aug_img)
                out_filename = f"{pid}_aug{idx}.jpg"
                out_path = os.path.join(OUTPUT_DIR, out_filename)
                aug_img.save(out_path)

                metadata_dict[out_filename] = {
                    "name_he": name_he,
                    "brand": brand,
                    "product_id": pid
                }

        except Exception as e:
            print(f"Skipping {pid}: {e}")

    # Save metadata JSON if there is metadata collected
    if metadata_dict:
        metadata_json_path = os.path.join(OUTPUT_DIR, 'metadata.json')
        with open(metadata_json_path, 'w', encoding='utf-8') as f:
            json.dump(metadata_dict, f, ensure_ascii=False, indent=2)
        print(f"✅ Metadata saved to {metadata_json_path} with {len(metadata_dict)} entries.")
    else:
        print("⚠️ No metadata was collected. Please check the product folders or images.")

    print(f"✅ Dataset preparation complete.")
