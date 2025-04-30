import os
import torch
from torch.utils.data import DataLoader
import open_clip
from tqdm import tqdm
from clip_dataset import ClipProductDataset


# --- CONFIGURATION ---
AUGMENTED_DIR = "/Users/noa/Desktop/PriceComparisonApp/data/augmented_dataset"
METADATA_PATH = os.path.join(AUGMENTED_DIR, "metadata.json")
BATCH_SIZE = 3
EPOCHS = 3

# --- LOAD DATASET ---
dataset = ClipProductDataset(images_dir=AUGMENTED_DIR, metadata_path=METADATA_PATH, max_products=9)
loader = DataLoader(dataset, batch_size=BATCH_SIZE, shuffle=True)

# --- LOAD MODEL ---
model, _, preprocess = open_clip.create_model_and_transforms("ViT-B-32", pretrained="openai")
tokenizer = open_clip.get_tokenizer("ViT-B-32")
model.eval()

# --- OPTIMIZER & LOSS ---
model.train()
device = "cuda" if torch.cuda.is_available() else "cpu"
model = model.to(device)
optimizer = torch.optim.AdamW(model.parameters(), lr=1e-5)
loss_fn = torch.nn.CrossEntropyLoss()

# --- TRAINING LOOP ---
for epoch in range(EPOCHS):
    print(f"Epoch {epoch+1}/{EPOCHS}")
    for i, (images, texts) in enumerate(loader):
        images = images.to(device)
        texts_tokenized = tokenizer(list(texts)).to(device)

        image_features = model.encode_image(images)
        text_features = model.encode_text(texts_tokenized)

        image_features = image_features / image_features.norm(dim=-1, keepdim=True)
        text_features = text_features / text_features.norm(dim=-1, keepdim=True)


        logits_per_image = image_features @ text_features.t()
        logits_per_text = text_features @ image_features.t()

        labels = torch.arange(len(images), device=device)
        loss_i = loss_fn(logits_per_image, labels)
        loss_t = loss_fn(logits_per_text, labels)
        loss = (loss_i + loss_t) / 2

        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        print(f"Batch {i+1} | Loss: {loss.item():.4f}")

print("\n✅ Training loop completed (test version)")
