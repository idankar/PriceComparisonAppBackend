# demo_product_db.py
from product_database import ProductDatabase
import os

# Initialize database with the better-performing model
db = ProductDatabase(model_name='clip-ViT-B-16')

# Add sample products (if database is empty)
if not db.products:
    print("Adding sample products to database...")
    
    # Directory containing product images
    image_dir = "/Users/noa/Desktop/PriceComparisonApp/data/images"
    
    # Sample product data
    sample_products = [
        {
            'id': 'cola_zero_500ml',
            'image': 'קולה זירו.jpg',
            'name_en': 'Coca-Cola Zero 500ml',
            'name_he': 'קוקה קולה זירו 500 מ"ל',
            'brand': 'Coca-Cola',
            'prices': {'Shufersal': 5.90, 'Rami Levy': 5.50}
        },
        # Add more products here as you gather them
    ]
    
    # Add each product to the database
    for product in sample_products:
        image_path = os.path.join(image_dir, product['image'])
        if os.path.exists(image_path):
            db.add_product(
                product_id=product['id'],
                image_path=image_path,
                name_en=product['name_en'],
                name_he=product['name_he'],
                brand=product['brand'],
                prices=product['prices']
            )
        else:
            print(f"Warning: Image {image_path} not found, skipping")

# Test search by image
test_image = "/Users/noa/Desktop/PriceComparisonApp/data/images/קולה זירו.jpg"
print("\n🔍 Testing image search...")
results = db.search_by_image(test_image)

# Display results
print("\n✅ Search Results:")
for i, (product_id, similarity, product_info) in enumerate(results):
    print(f"Match {i+1}:")
    print(f"  Product: {product_info['name_en']} / {product_info['name_he']}")
    if product_info['brand']:
        print(f"  Brand: {product_info['brand']}")
    print(f"  Similarity: {similarity:.4f}")
    print(f"  Prices: {product_info['prices']}")
    print()

# Test search by text in both languages
queries = [
    "Coca-Cola Zero",  # English
    "קוקה קולה זירו"   # Hebrew
]

for query in queries:
    print(f"\n🔍 Testing text search: '{query}'")
    results = db.search_by_text(query)
    
    # Display results
    print("✅ Search Results:")
    for i, (product_id, similarity, product_info) in enumerate(results):
        print(f"Match {i+1}:")
        print(f"  Product: {product_info['name_en']} / {product_info['name_he']}")
        if product_info['brand']:
            print(f"  Brand: {product_info['brand']}")
        print(f"  Similarity: {similarity:.4f}")
        print()