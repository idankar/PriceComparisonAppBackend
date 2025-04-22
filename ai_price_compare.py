import clip
import torch
from PIL import Image
import csv

# --- Step 1: Recognize product from image ---
device = "cuda" if torch.cuda.is_available() else "cpu"
model, preprocess = clip.load("ViT-B/32", device=device)

image_path = "nutella.jpg"  # Your test image
image = preprocess(Image.open(image_path)).unsqueeze(0).to(device)

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

text_inputs = torch.cat([clip.tokenize(desc) for desc in product_descriptions]).to(device)

with torch.no_grad():
    image_features = model.encode_image(image)
    text_features = model.encode_text(text_inputs)
    similarities = (image_features @ text_features.T).squeeze(0)
    best_index = similarities.argmax().item()
    best_match = product_descriptions[best_index]

print(f"🧠 Best match: {best_match}\n")


# --- Step 2: Search product prices in CSV ---
def get_prices_for_product(product_name):
    prices = []
    with open("products.csv", newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['product_name'] == product_name:
                prices.append({
                    "country": row["country"],
                    "price_ils": float(row["price_ils"]),
                    "weight_g": int(row["weight_g"]),
                    "brand": row["brand"]
                })
    return prices


# --- Step 3: Show prices ranked by difference ---
results = get_prices_for_product(best_match)

if not results:
    print("⚠️ No prices found for that product.")
else:
    # Sort by price
    results = sorted(results, key=lambda r: r["price_ils"])
    israel_price = next((r["price_ils"] for r in results if r["country"] == "Israel"), None)

    print(f"💰 Price comparison for '{best_match}':\n")
    for r in results:
        diff = round(100 * (israel_price - r["price_ils"]) / israel_price) if israel_price else 0
        print(f"{r['country']}: ₪{r['price_ils']} ({diff}% cheaper)")
