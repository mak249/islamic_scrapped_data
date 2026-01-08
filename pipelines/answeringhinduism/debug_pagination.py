import requests
from bs4 import BeautifulSoup

url = "https://answeringhinduism.org/category/hinduism/"
headers = {'User-Agent': 'Mozilla/5.0'}

try:
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    print("--- Pagination Classes ---")
    for div in soup.find_all('div', class_=True):
        if 'page' in div['class'] or 'nav' in div['class']:
            print(f"DIV: {div['class']}")

    print("\n--- UL Classes ---")
    for ul in soup.find_all('ul', class_=True):
        if 'page' in ul['class'] or 'nav' in ul['class']:
            print(f"UL: {ul['class']}")

    print("\n--- Links containing 'page' ---")
    for a in soup.find_all('a', href=True):
        if '/page/' in a['href']:
            print(f"LINK: {a['href']} | Text: {a.get_text(strip=True)}")

except Exception as e:
    print(e)
