from sentence_transformers import SentenceTransformer, util
from PIL import Image

# Load the CLIP model
print("Loading model...")
model = SentenceTransformer('clip-ViT-B-32-multilingual-v1')

# Load the image
image_path = "/Users/noa/Desktop/PriceComparisonApp/data/images/קולה זירו.jpg"  # Update this to your image path
print(f"Processing image: {image_path}")
image = Image.open(image_path).convert('RGB')

# Encode the image
image_embedding = model.encode(image)

# Define text descriptions
texts = [
    "Coca-Cola Zero 500ml",       # English
    "קוקה קולה זירו 500 מ\"ל"     # Hebrew
]

# Encode texts
text_embeddings = model.encode(texts)

# Calculate similarities
similarities = util.cos_sim(image_embedding, text_embeddings)[0]

# Print results
print("\n📊 Results:")
for i, text in enumerate(texts):
    print(f"Similarity for '{text}': {similarities[i].item():.4f}")