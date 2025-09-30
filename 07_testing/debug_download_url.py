import requests
from bs4 import BeautifulSoup

# Debug the download URL extraction
getlink_url = "https://prices.shufersal.co.il/FileObject/ViewFile?FileNm=PriceFull7290027600007-026-202507240300&code=Price"
headers = {'User-Agent': 'Mozilla/5.0'}

response = requests.get(getlink_url, headers=headers, timeout=30)
print(f"Status: {response.status_code}")
print(f"URL: {response.url}")
print(f"\nFirst 1000 chars of response:")
print(response.text[:1000])

soup = BeautifulSoup(response.text, 'lxml')

# Find all links
links = soup.find_all('a')
print(f"\nFound {len(links)} links:")
for link in links:
    print(f"  Text: '{link.get_text(strip=True)}' | href: {link.get('href', 'N/A')}")

# Look for Azure blob storage URL
import re
blob_url_match = re.search(r'https://[^"\']+blob\.core\.windows\.net[^"\']+', response.text)
if blob_url_match:
    print(f"\nFound blob URL: {blob_url_match.group(0)}")