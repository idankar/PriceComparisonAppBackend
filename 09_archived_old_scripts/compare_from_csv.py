import csv

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
                    "weight_g": int(row["weight_g"]),
                    
                })
    return prices


# === TEST ===
product = "Nutella Hazelnut Spread 350g"
results = get_prices_for_product(product)

if not results:
    print("⚠️ No prices found.")
else:
    israel_price = next((r["price_ils"] for r in results if r["country"] == "Israel"), None)
    results = sorted(results, key=lambda r: r["price_ils"])

    print(f"📦 {product}\n")
    for r in results:
        diff = round(100 * (israel_price - r["price_ils"]) / israel_price) if israel_price else 0
        tag = "🇮🇱" if r["country"] == "Israel" else ""
        print(f"{r['country']}: ₪{r['price_ils']} ({diff}% cheaper) {tag}")
