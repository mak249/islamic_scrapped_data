import sqlite3
from bs4 import BeautifulSoup

conn = sqlite3.connect('pipelines/salafipublications/data.db')
c = conn.cursor()
c.execute("SELECT url, html FROM products LIMIT 1")
row = c.fetchone()
if row:
    print(f"URL: {row[0]}")
    soup = BeautifulSoup(row[1], 'html.parser')
    
    print("--- TITLE ---")
    print(soup.select_one('.product_title').get_text() if soup.select_one('.product_title') else "Not Found")
    
    print("\n--- META ---")
    print(soup.select_one('.product_meta').get_text() if soup.select_one('.product_meta') else "Not Found")
    
    print("\n--- ATTRIBUTES ---")
    # WooCommerce attributes table
    for row in soup.select('.woocommerce-product-attributes tr'):
        print(row.get_text(strip=True))

    print("\n--- SHORT DESC ---")
    print(soup.select_one('.woocommerce-product-details__short-description').get_text()[:200] if soup.select_one('.woocommerce-product-details__short-description') else "Not Found")
else:
    print("No data")
conn.close()
