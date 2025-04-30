import os
import torch
import open_clip
from PIL import Image
from clip_dataset import ClipProductDataset
from open_clip import get_tokenizer
from torchvision import transforms

# --- CONFIGURATION ---
CAPTIONS_FILE = "captions.txt"
MODEL_NAME = "ViT-B-32"
PRETRAINED = "openai"
IMG_SIZE = 224

# --- FUNCTION TO LOAD IMAGE ---
def load_image(image_path):
    transform = transforms.Compose([
        transforms.Resize((IMG_SIZE, IMG_SIZE)),
        transforms.ToTensor(),
    ])
    image = Image.open(image_path).convert("RGB")
    return transform(image).unsqueeze(0)

# --- FUNCTION TO LOAD AND ENCODE CAPTIONS ---
def load_caption_embeddings(captions_file, model, tokenizer, device):
    with open(captions_file, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f.read().split("\n") if line.strip()]
    captions = ["\n".join(lines[i:i+2]) for i in range(0, len(lines), 2)]

    tokens = tokenizer(captions).to(device)
    with torch.no_grad():
        text_features = model.encode_text(tokens)
        text_features /= text_features.norm(dim=-1, keepdim=True)
    return captions, text_features

# --- RETRIEVAL FUNCTION ---
def retrieve_top_k_matches(image_path, captions_file, k=5):
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model, _, _ = open_clip.create_model_and_transforms(MODEL_NAME, pretrained=PRETRAINED)
    model = model.to(device).eval()
    tokenizer = get_tokenizer(MODEL_NAME)

    image_tensor = load_image(image_path).to(device)
    with torch.no_grad():
        image_features = model.encode_image(image_tensor)
        image_features /= image_features.norm(dim=-1, keepdim=True)

    captions, text_features = load_caption_embeddings(captions_file, model, tokenizer, device)
    with torch.no_grad():
        logits = (image_features @ text_features.T).squeeze(0)
        topk = torch.topk(logits, k=k)

    return [(captions[idx], topk.values[i].item()) for i, idx in enumerate(topk.indices)]

# --- MAIN TEST EXAMPLE ---
if __name__ == "__main__":
    test_image_path = "/Users/noa/Desktop/PriceComparisonApp/data/augmented_dataset/shufersal_P_1594155_aug4.jpg"  # replace as needed
    results = retrieve_top_k_matches(test_image_path, CAPTIONS_FILE, k=5)

    print("\n🔍 Top-k matches:")
    for i, (caption, score) in enumerate(results):
        print(f"{i+1}. {caption} (score: {score:.4f})")