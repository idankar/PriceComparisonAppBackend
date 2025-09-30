import os
import matplotlib.pyplot as plt
from torch.utils.data import DataLoader
from clip_dataset import ClipProductDataset
from matplotlib import font_manager

# --- CONFIG ---
AUGMENTED_DIR = "/Users/noa/Desktop/PriceComparisonApp/data/augmented_dataset"
METADATA_PATH = os.path.join(AUGMENTED_DIR, "metadata.json")

# --- Font for Hebrew ---
font_properties = font_manager.FontProperties(family='DejaVu Sans')

# --- Load dataset ---
dataset = ClipProductDataset(images_dir=AUGMENTED_DIR, metadata_path=METADATA_PATH, max_products=5)
loader = DataLoader(dataset, batch_size=5, shuffle=True)

images, texts = next(iter(loader))

# --- Visualize ---
plt.figure(figsize=(15, 5))
for i in range(images.size(0)):
    img = images[i].permute(1, 2, 0).numpy()
    plt.subplot(1, images.size(0), i + 1)
    plt.imshow(img)
    plt.axis('off')
    plt.text(0.95, 1.08, texts[i], fontsize=8, ha='right', va='bottom',
             wrap=True, transform=plt.gca().transAxes, fontproperties=font_properties)

plt.tight_layout()
plt.show()
