import requests
from bs4 import BeautifulSoup

# The URL for the makeup category
url = "https://www.bestore.co.il/online/he/makeup"

# Standard headers to mimic a browser
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9,he;q=0.8",
}

print(f"Attempting to fetch {url} with the requests library...")

try:
    # Make the request to get the page's HTML
    response = requests.get(url, headers=headers, timeout=15)
    response.raise_for_status() # Raise an error for bad status codes like 403 or 500

    # Parse the HTML with BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find all product containers using the selector we identified
    product_items = soup.select("li.item.product.product-item")
    
    print(f"Request successful. Status Code: {response.status_code}")
    print(f"Page Title: {soup.title.string.strip()}")
    print(f"Number of products found in the HTML: {len(product_items)}")

except requests.exceptions.RequestException as e:
    print(f"The request failed: {e}")