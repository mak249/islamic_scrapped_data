import requests
from bs4 import BeautifulSoup

url = "https://darussalam.com/quran-mushaf/"
headers = {'User-Agent': 'Mozilla/5.0'}

try:
    print(f"Fetching {url}...")
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    print("\n--- Inspecting Headers ---")
    headers = soup.find_all(['h3', 'h4', 'h5', 'h2'])
    for h in headers[:20]:
        print(f"<{h.name} class='{h.get('class', [])}'> Text: {h.get_text(strip=True)[:50]}...")
        if h.find('a'):
            print(f"   -> Link: {h.find('a')['href']}")

    print("\n--- Inspecting Product Card Links (Generic) ---")
    # Look for likely product links
    links = soup.find_all('a', href=True)
    product_links = []
    for a in links:
        href = a['href']
        # typical product link pattern?
        if '/quran-mushaf/' in href or '/books/' in href: 
            pass 
        class_list = a.get('class', [])
        if class_list:
            print(f"Link Class: {class_list} | Href: {href}")

except Exception as e:
    print(e)
