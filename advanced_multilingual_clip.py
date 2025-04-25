# advanced_multilingual_clip.py
from sentence_transformers import SentenceTransformer, util
from PIL import Image
import torch
import time

# Configuration
image_path = "/Users/noa/Desktop/PriceComparisonApp/data/images/קולה זירו.jpg"

# Product descriptions in both languages
texts = [
    "Coca-Cola Zero 500ml",  # English
    "קוקה קולה זירו 500 מ\"ל"  # Hebrew
]

# Try different CLIP variants
models_to_try = [
    'clip-ViT-B-32',  # Base CLIP (for comparison)
    'clip-ViT-B-16',  # Higher resolution CLIP
    'laion/CLIP-ViT-H-14-laion2B-s32B-b79K'  # LAION CLIP - trained on more diverse data
]

for model_name in models_to_try:
    print(f"\n🔍 Testing model: {model_name}")
    try:
        start_time = time.time()
        
        # Load model
        print("Loading model...")
        model = SentenceTransformer(model_name)
        
        # Load the image
        image = Image.open(image_path).convert('RGB')
        
        # Encode image and text
        image_embedding = model.encode(image)
        text_embeddings = model.encode(texts)
        
        # Calculate similarities
        similarities = util.cos_sim(image_embedding, text_embeddings)[0]
        
        # Print results
        print("\n✅ Results:")
        for i, text in enumerate(texts):
            print(f"Similarity for '{text}': {similarities[i].item():.4f}")
        
        elapsed = time.time() - start_time
        print(f"Processing time: {elapsed:.2f} seconds")
        
    except Exception as e:
        print(f"Error with model {model_name}: {e}")