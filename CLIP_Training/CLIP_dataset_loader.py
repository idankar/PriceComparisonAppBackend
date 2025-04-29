import os
import json
from PIL import Image
import torch
from torchvision import transforms
from torch.utils.data import Dataset, DataLoader
import matplotlib.pyplot as plt
import matplotlib
import arabic_reshaper
from bidi.algorithm import get_display
from matplotlib import font_manager

# Use fallback Hebrew-capable font
font_properties = font_manager.FontProperties(family='DejaVu Sans')

matplotlib.rcParams['axes.unicode_minus'] = False
matplotlib.rcParams['text.usetex'] = False
matplotlib.rcParams['font.family'] = 'DejaVu Sans'

# --- CONFIGURATION ---
AUGMENTED_DIR = '/Users/noa/Desktop/PriceComparisonApp/data/augmented_dataset'
UNIQUE_PRODUCTS = 5  # Show one image per product for test
IMG_SIZE = 224

# Helper to reshape and apply BiDi for RTL text
def prepare_rtl_text(text):
    reshaped = arabic_reshaper.reshape(text)
    return get_display(reshaped)

class ClipProductDataset(Dataset):
    def __init__(self, images_dir, metadata_path, max_products=None):
        self.images_dir = images_dir
        with open(metadata_path, 'r', encoding='utf-8') as f:
            full_metadata = json.load(f)

        product_to_img = {}
        for fname, meta in full_metadata.items():
            pid = meta['product_id']
            if pid not in product_to_img:
                product_to_img[pid] = (fname, meta)

        self.samples = list(product_to_img.values())
        if max_products is not None:
            self.samples = self.samples[:max_products]

        self.transform = transforms.Compose([
            transforms.Resize((IMG_SIZE, IMG_SIZE)),
            transforms.ToTensor(),
        ])

    def __len__(self):
        return len(self.samples)

    def __getitem__(self, idx):
        filename, info = self.samples[idx]
        img_path = os.path.join(self.images_dir, filename)
        image = Image.open(img_path).convert('RGB')
        image = self.transform(image)

        # Construct full logical string before reshaping for RTL
        raw_label = f"שם מוצר: {info['name_he']}\nמותג: {info['brand']}"
        label = prepare_rtl_text(raw_label)

        return image, label

if __name__ == '__main__':
    dataset = ClipProductDataset(
        images_dir=AUGMENTED_DIR,
        metadata_path=os.path.join(AUGMENTED_DIR, 'metadata.json'),
        max_products=UNIQUE_PRODUCTS
    )

    loader = DataLoader(dataset, batch_size=5, shuffle=True)
    imgs, texts = next(iter(loader))
    print(f"Batch ready with shapes:\nImages: {imgs.shape}\nTexts: {texts}")

    # --- Visualization ---
    plt.figure(figsize=(15, 5))
    for idx in range(imgs.size(0)):
        img = imgs[idx].permute(1, 2, 0).numpy()
        plt.subplot(1, imgs.size(0), idx + 1)
        plt.imshow(img)
        plt.axis('off')
        plt.text(
            0.95, 1.08, texts[idx],
            fontsize=8, ha='right', va='bottom', wrap=True,
            transform=plt.gca().transAxes, fontproperties=font_properties
        )

    plt.tight_layout()
    plt.show()