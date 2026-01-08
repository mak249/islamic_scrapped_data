import sqlite3
import json
import re
import os
from bs4 import BeautifulSoup
from typing import Dict, Optional

class SalafiProcessor:
    def __init__(self, db_path: str, output_path: str):
        self.db_path = db_path
        self.output_path = output_path
        
    def clean_text(self, text: str) -> str:
        if not text:
            return ""
        return re.sub(r'\s+', ' ', text).strip()

    def extract_metadata(self, html: str, url: str) -> Dict:
        soup = BeautifulSoup(html, 'html.parser')
        metadata = {
            "title": "",
            "author": "",
            "translator": "",
            "publisher": "Salafi Publications",
            "publication_year": "",
            "language": "",
            "category": "",
            "ISBN": "",
            "format": "",
            "page_count": "",
            "price": "",
            "product_url": url,
            "short_description": "" 
        }

        # 1. Title
        title_tag = soup.select_one('.product_title')
        if title_tag:
            metadata['title'] = self.clean_text(title_tag.get_text())

        # 2. Price
        price_tag = soup.select_one('p.price') or soup.select_one('span.price')
        if price_tag:
            metadata['price'] = self.clean_text(price_tag.get_text())

        # 3. Category
        posted_in = soup.select_one('.posted_in')
        if posted_in:
            # "Category: Books" -> "Books"
            cat_text = self.clean_text(posted_in.get_text())
            cat_text = cat_text.replace("Category:", "").replace("Categories:", "").strip()
            metadata['category'] = cat_text

        # 4. Attributes (Author, Pages, ISBN, etc.)
        # WooCommerce attributes table
        for row in soup.select('.woocommerce-product-attributes tr'):
            th = row.select_one('th')
            td = row.select_one('td')
            if not th or not td:
                continue
            
            label = self.clean_text(th.get_text()).lower()
            value = self.clean_text(td.get_text())
            
            if 'author' in label:
                metadata['author'] = value
            elif 'isbn' in label:
                metadata['ISBN'] = value
            elif 'pages' in label:
                metadata['page_count'] = value
            elif 'publisher' in label:
                metadata['publisher'] = value
            elif 'translator' in label:
                metadata['translator'] = value
            elif 'language' in label:
                metadata['language'] = value
            elif 'format' in label or 'binding' in label:
                metadata['format'] = value
            elif 'year' in label or 'publication' in label:
                metadata['publication_year'] = value

        # 5. Short Description (Marketing Only)
        # Verify it's not a religious text
        desc_div = soup.select_one('.woocommerce-product-details__short-description')
        if desc_div:
            text = self.clean_text(desc_div.get_text())
            # Truncate to avoid content leakage
            metadata['short_description'] = text[:300] + "..." if len(text) > 300 else text

        return metadata

    def run(self):
        print("⚙️ PROCESSING SALAFI METADATA...")
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        c.execute("SELECT url, html, category_url FROM products WHERE html IS NOT NULL")
        rows = c.fetchall()
        
        processed_data = []
        for row in rows:
            meta = self.extract_metadata(row['html'], row['url'])
            if meta['title']:
                processed_data.append(meta)
        
        conn.close()
        
        with open(self.output_path, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, indent=2, ensure_ascii=False)
            
        print(f"✅ Processed {len(processed_data)} items.")
        print(f"📁 Output saved to {self.output_path}")

if __name__ == "__main__":
    base_dir = os.path.dirname(__file__)
    db_path = os.path.join(base_dir, "data.db")
    output_path = os.path.join(base_dir, "catalog.json")
    
    processor = SalafiProcessor(db_path, output_path)
    processor.run()
