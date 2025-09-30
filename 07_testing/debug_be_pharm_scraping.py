import requests
from bs4 import BeautifulSoup

# Debug Be Pharm scraping
url = "https://prices.shufersal.co.il/FileObject/UpdateCategory?catID=2&storeId=0&page=2"
headers = {'User-Agent': 'Mozilla/5.0'}

response = requests.get(url, headers=headers, timeout=30)
soup = BeautifulSoup(response.text, 'lxml')

table = soup.find('table', class_='webgrid')
if table:
    rows = table.find('tbody').find_all('tr')[:3]  # First 3 rows
    
    for i, row in enumerate(rows):
        cells = row.find_all('td')
        if len(cells) >= 7:
            store_name = cells[5].get_text(strip=True)
            file_name = cells[6].get_text(strip=True)
            
            print(f"\nRow {i+1}:")
            print(f"  Store: {store_name}")
            print(f"  File: {file_name}")
            
            if "BE" in store_name.upper():
                print("  ** BE PHARM STORE **")
                
                # Check first cell for link
                link_tag = cells[0].find('a')
                if link_tag:
                    print(f"  Link href: {link_tag.get('href', 'N/A')}")
                    print(f"  Link onclick: {link_tag.get('onclick', 'N/A')}")
                    print(f"  Link text: {link_tag.get_text(strip=True)}")
                    
                # Check all cells for links
                for j, cell in enumerate(cells):
                    links = cell.find_all('a')
                    if links:
                        print(f"  Cell {j} has {len(links)} link(s)")
                        for link in links:
                            print(f"    - href: {link.get('href', 'N/A')[:50]}...")
                            onclick = link.get('onclick', '')
                            if onclick:
                                print(f"    - onclick: {onclick[:100]}...")