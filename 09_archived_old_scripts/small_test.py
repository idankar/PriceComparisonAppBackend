import requests
import re
import json

# Product page for the milk product we've seen in the logs
url = "https://www.carrefour.fr/p/lait-facile-a-digerer-carrefour-3560070437405"

# Use browser-like headers
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7"
}

# Get the HTML
print(f"Requesting URL: {url}")
response = requests.get(url, headers=headers)
print(f"Response status code: {response.status_code}")

# If we got a 403, let's try a different Carrefour region
if response.status_code == 403:
    print("Got 403 Forbidden error, trying Carrefour UAE...")
    url = "https://www.carrefouruae.com/mafuae/en/c/F1"
    response = requests.get(url, headers=headers)
    print(f"UAE Response status code: {response.status_code}")

# Save the first 1000 characters of the response to see what we're getting
print("\nFirst 1000 characters of response:")
print(response.text[:1000])

# Extract LD+JSON blocks
json_data = re.findall(r'<script type="application\/ld\+json">(.*?)</script>', response.text, re.DOTALL)
print(f"\nFound {len(json_data)} LD+JSON blocks in the HTML")

# Parse the JSON
for i, json_str in enumerate(json_data):
    try:
        product_data = json.loads(json_str)
        print(f"\nLD+JSON Block #{i+1}:")
        print(json.dumps(product_data, indent=2)[:500] + "..." if len(json.dumps(product_data, indent=2)) > 500 else json.dumps(product_data, indent=2))
    except Exception as e:
        print(f"Error parsing JSON block #{i+1}: {e}")
        print(f"First 100 characters: {json_str[:100]}")