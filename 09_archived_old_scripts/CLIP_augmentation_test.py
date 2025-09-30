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
SELECT_N_PRODUCTS = 5
IMG_SIZE = 224
BATCH_SIZE = 5

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

class ProductsDataset(Dataset):
    def __init__(self, products_dir, product_ids, transform=None):
        self.products_dir = products_dir
        self.product_ids = product_ids
        self.transform = transform
        self.data = []

        for pid in self.product_ids:
            metadata_path = os.path.join(products_dir, pid, 'metadata.json')
            try:
                with open(metadata_path, 'r') as f:
                    meta = json.load(f)
                name_he = meta.get('name_he', '')
                brand = meta.get('brand', '')
                text = f"מוצר: {name_he} | מותג: {brand}"

                img1_path = os.path.join(products_dir, pid, '001.jpg')
                img2_path = os.path.join(products_dir, pid, '002.jpg')

                if os.path.exists(img2_path):
                    img = Image.open(img2_path).convert('RGB')
                else:
                    img = Image.open(img1_path).convert('RGB')

                augmented = augment_image(img)

                for aug_img in augmented:
                    self.data.append((aug_img, text))

            except Exception as e:
                print(f"Skipping {pid}: {e}")

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        img, text = self.data[idx]
        if self.transform:
            img = self.transform(img)
        return img, text

# --- MAIN TEST ---
if __name__ == '__main__':
    product_ids = random.sample(os.listdir(PRODUCTS_DIR), SELECT_N_PRODUCTS)
    print(f"Selected products: {product_ids}")

    transform = transforms.Compose([
        transforms.Resize((IMG_SIZE, IMG_SIZE)),
        transforms.ToTensor(),
    ])

    dataset = ProductsDataset(PRODUCTS_DIR, product_ids, transform)
    loader = DataLoader(dataset, batch_size=BATCH_SIZE, shuffle=False)

    # Show first batch
    imgs, texts = next(iter(loader))

    # Visualization
    fig, axes = plt.subplots(1, BATCH_SIZE, figsize=(20, 5))
    for idx in range(BATCH_SIZE):
        img = imgs[idx].permute(1,2,0).numpy()
        axes[idx].imshow(img)
        axes[idx].axis('off')
        axes[idx].set_title(reverse_hebrew_text(texts[idx]), fontsize=8, fontname='DejaVu Sans')
    plt.show()

    print("Batch ready with shapes:")
    print("Images:", imgs.shape)
    print("Texts:", texts)
