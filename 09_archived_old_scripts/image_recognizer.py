import clip
import torch
from PIL import Image

# Set device to GPU if available, otherwise use CPU
device = "cuda" if torch.cuda.is_available() else "cpu"

# Load the CLIP model
model, preprocess = clip.load("ViT-B/32", device=device)

# Load and preprocess the image
image_path = "nutella.jpg"  # Make sure this file exists in your folder
image = preprocess(Image.open(image_path)).unsqueeze(0).to(device)

# Candidate product descriptions to compare against
product_descriptions = [
    "Nutella Hazelnut Spread 350g",
    "Nutella Hazelnut Spread 750g",
    "Generic Chocolate Spread 500g",
    "Peanut Butter Smooth 340g",
    "Peanut Butter Crunchy 500g",
    "Heinz Ketchup 500ml",
    "Generic Ketchup 500ml",
    "Hellmann's Mayonnaise 400g",
    "Generic Mayonnaise 400g"
]


# Convert text to model-friendly format
text_inputs = torch.cat([clip.tokenize(desc) for desc in product_descriptions]).to(device)

# Run the model
with torch.no_grad():
    image_features = model.encode_image(image)
    text_features = model.encode_text(text_inputs)

    # Compute similarity between image and each text
    similarities = (image_features @ text_features.T).squeeze(0)
    best_index = similarities.argmax().item()
    best_match = product_descriptions[best_index]

# Debug print
print("✅ Script ran, now computing similarity...")
print(f"🧠 Best match: {best_match}")
