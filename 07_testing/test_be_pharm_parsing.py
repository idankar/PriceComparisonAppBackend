import requests
import gzip
from xml.etree import ElementTree as ET

# Test downloading and parsing a Be Pharm file
url = "https://pricesprodpublic.blob.core.windows.net/pricefull/PriceFull7290027600007-026-202507240300.gz?sv=2014-02-14&sr=b&sig=vnBjJnzl4UCdAMb2Tv3JriuiWlFW9FF19%2FEnSU%2F546c%3D&se=2025-07-24T18%3A36%3A23Z&sp=r"

headers = {'User-Agent': 'Mozilla/5.0'}

try:
    print("Downloading file...")
    response = requests.get(url, headers=headers, timeout=30, verify=False)
    response.raise_for_status()
    
    print(f"Downloaded {len(response.content)} bytes")
    
    # Decompress if gzipped
    if response.content.startswith(b'\x1f\x8b'):
        print("File is gzipped, decompressing...")
        xml_content = gzip.decompress(response.content).decode('utf-8-sig')
    else:
        xml_content = response.content.decode('utf-8-sig')
    
    print(f"XML content length: {len(xml_content)} characters")
    print("\nFirst 500 characters:")
    print(xml_content[:500])
    
    # Try to parse
    print("\nParsing XML...")
    root = ET.fromstring(xml_content)
    print(f"Root tag: {root.tag}")
    
    # Find items
    items = root.findall('.//Item')
    if not items:
        items = root.findall('.//Items/Item')
    
    print(f"\nFound {len(items)} items")
    
    if items:
        # Show first item
        first_item = items[0]
        print("\nFirst item:")
        for child in first_item:
            if child.text:
                print(f"  {child.tag}: {child.text[:100]}")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()