import gradio as gr
import clip
import torch
from PIL import Image
import csv

# Load CLIP model
device = "cuda" if torch.cuda.is_available() else "cpu"
model, preprocess = clip.load("ViT-B/32", device=device)

# Load product list for matching
product_descriptions = [
    "Nutella Hazelnut Spread 350g",
    "Milky Chocolate Vanilla Dessert 130g",
    "Heinz Ketchup 500ml",
    "Barilla Pasta Penne 500g",
    "Colgate Whitening Toothpaste 75ml",
    "Dove Shampoo 400ml",
    "Coca-Cola Bottle 1.5L",
    "Pampers Diapers Size 3 (30pcs)",
    "Osem White Rice 1kg",
    "Similac Baby Formula 400g"
]

# Load CSV price data
def get_prices_for_product(product_name):
    prices = []
    with open("products.csv", newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['product_name'] == product_name:
                prices.append({
                    "country": row["country"],
                    "price_ils": float(row["price_ils"]),
                    "brand": row["brand"],
                    "weight_g": int(row["weight_g"])
                })
    return prices


# Main function for Gradio
def compare_prices(image):
    image_input = preprocess(image).unsqueeze(0).to(device)
    text_inputs = torch.cat([clip.tokenize(desc) for desc in product_descriptions]).to(device)

    with torch.no_grad():
        image_features = model.encode_image(image_input)
        text_features = model.encode_text(text_inputs)
        similarities = (image_features @ text_features.T).squeeze(0)
        best_index = similarities.argmax().item()
        best_match = product_descriptions[best_index]

    results = get_prices_for_product(best_match)
    if not results:
        return f"No prices found for {best_match}"

    israel_price = next((r["price_ils"] for r in results if r["country"] == "Israel"), None)
    results = sorted(results, key=lambda r: r["price_ils"])

    output = f"🧠 Best match: {best_match}\n\n💰 Price comparison:\n"
    for r in results:
        diff = round(100 * (israel_price - r["price_ils"]) / israel_price) if israel_price else 0
        tag = "🇮🇱" if r["country"] == "Israel" else ""
        output += f"{r['country']}: ₪{r['price_ils']} ({diff}% cheaper) {tag}\n"

    return output


# Launch Gradio app
gr.Interface(
    fn=compare_prices,
    inputs=gr.Image(type="pil"),
    outputs="text",
    title="📸 AI Price Comparison App",
    description="Upload a product image to see price differences between Israel and other countries."
).launch()
