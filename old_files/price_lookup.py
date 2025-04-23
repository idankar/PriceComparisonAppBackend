import csv


with open("products.csv", newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        print(row)



def get_prices_for_product(product_name):
    prices = []

    try:
        with open("products.csv", newline='', encoding='utf-8') as csvfile:
            print("📄 CSV loaded successfully")
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Debug: show each row
                print(f"👀 Checking row: {row['product_name']}")
                if row['product_name'] == product_name:
                    prices.append({
                        "country": row["country"],
                        "price_ils": float(row["price_ils"]),
                        "weight_g": int(row["weight_g"]),
                        "brand": row["brand"]
                    })
    except FileNotFoundError:
        print("❌ Error: products.csv file not found.")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

    return prices


# === TESTING ===
product = "Nutella Hazelnut Spread 350g"
print(f"\n🔍 Looking up prices for: {product}\n")
results = get_prices_for_product(product)

if not results:
    print("⚠️ No matches found.")
else:
    for r in results:
        print(f"{r['country']}: ₪{r['price_ils']} ({r['brand']}, {r['weight_g']}g)")
