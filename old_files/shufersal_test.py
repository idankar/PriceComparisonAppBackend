import requests

url = "https://www.carrefour.fr/api/graphql"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.carrefour.fr/",
    "Origin": "https://www.carrefour.fr"
}

# Simple query to test the API
payload = {
    "query": """
    query {
      categories {
        id
        label
      }
    }
    """
}

response = requests.post(url, json=payload, headers=headers)
print(f"API response status: {response.status_code}")
print(f"Response content: {response.text[:200]}")

# Check if the website returns any useful info about the restriction
main_response = requests.get("https://www.carrefour.fr", headers=headers)
print(f"Main site status code: {main_response.status_code}")
print(f"Cookies received: {main_response.cookies}")