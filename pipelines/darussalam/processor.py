import sqlite3
import json
import re
import os
from bs4 import BeautifulSoup
from typing import Dict, Optional

class DarussalamProcessor:
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
            "editor": "", # Some books have editors
            "publisher": "Darussalam", # Default since it's the site, but check page
            "language": "",
            "ISBN": "",
            "edition": "",
            "publication_year": "",
            "format": "",
            "page_count": "",
            "category": "",
            "price": "",
            "product_url": url,
            "short_description": "" 
        }

        # 1. Title
        # Usually h1.productView-title
        h1 = soup.find('h1', class_='productView-title')
        if h1:
            metadata['title'] = self.clean_text(h1.get_text())

        # 2. Price
        # span.price.price--withTax or similar
        price_span = soup.find('span', class_='price--withTax') or soup.find('span', class_='price--withoutTax')
        if price_span:
            metadata['price'] = self.clean_text(price_span.get_text())

        # 3. Description (Short marketing only)
        # div.productView-description
        # STRICT RULE: Grab only first paragraph if it looks like marketing
        desc_div = soup.find('div', class_='productView-description')
        if desc_div:
            # Check for "Description" tab content
            text = self.clean_text(desc_div.get_text())
            # Limit to 300 chars for safety to avoid content extraction
            metadata['short_description'] = text[:300] + "..." if len(text) > 300 else text

        # 4. Custom Fields (Author, ISBN, Pages, etc.)
        # These are often in a DL list or table
        # Look for <dt>Label</dt><dd>Value</dd>
        for dt in soup.find_all('dt', class_='productView-info-name'):
            label = self.clean_text(dt.get_text()).lower()
            dd = dt.find_next_sibling('dd', class_='productView-info-value')
            if not dd:
                continue
            
            value = self.clean_text(dd.get_text())
            
            if 'author' in label:
                metadata['author'] = value
            elif 'isbn' in label:
                metadata['ISBN'] = value
            elif 'pages' in label or 'page count' in label:
                metadata['page_count'] = value
            elif 'publisher' in label:
                metadata['publisher'] = value
            elif 'translator' in label:
                metadata['translator'] = value
            elif 'binding' in label or 'format' in label:
                metadata['format'] = value
            elif 'publication' in label or 'year' in label:
                metadata['publication_year'] = value
            elif 'language' in label:
                metadata['language'] = value

        # 5. Fallback Category from Breadcrumbs
        breadcrumbs = soup.find('ul', class_='breadcrumbs')
        if breadcrumbs:
            crumbs = [self.clean_text(li.get_text()) for li in breadcrumbs.find_all('li')]
            if len(crumbs) > 1:
                # Exclude Home and Current Title
                valid_crumbs = [c for c in crumbs if c and c != 'Home' and c != metadata['title']]
                if valid_crumbs:
                    metadata['category'] = " > ".join(valid_crumbs)

        return metadata

    def run(self):
        print("⚙️ PROCESSING METADATA...")
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        c.execute("SELECT url, html, category_url FROM products WHERE html IS NOT NULL")
        rows = c.fetchall()
        
        processed_data = []
        for row in rows:
            meta = self.extract_metadata(row['html'], row['url'])
            # Only keeping valid items (must have title)
            if meta['title']:
                processed_data.append(meta)
        
        conn.close()
        
        # Save to JSON
        with open(self.output_path, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, indent=2, ensure_ascii=False)
            
        print(f"✅ Processed {len(processed_data)} items.")
        print(f"📁 Output saved to {self.output_path}")

if __name__ == "__main__":
    base_dir = os.path.dirname(__file__)
    db_path = os.path.join(base_dir, "data.db")
    output_path = os.path.join(base_dir, "catalog.json")
    
    processor = DarussalamProcessor(db_path, output_path)
    processor.run()
