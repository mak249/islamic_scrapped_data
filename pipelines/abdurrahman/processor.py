import sqlite3
import json
import re
import os
from bs4 import BeautifulSoup
from typing import Dict, List, Optional

class AbdurRahmanProcessor:
    def __init__(self, db_path: str, output_path: str):
        self.db_path = db_path
        self.output_path = output_path
        
    def clean_text(self, text: str) -> str:
        if not text:
            return ""
        return re.sub(r'\s+', ' ', text).strip()

    def extract_article(self, html: str, url: str) -> Optional[Dict]:
        soup = BeautifulSoup(html, 'html.parser')
        
        # 1. Title
        title_tag = soup.find(class_='entry-title')
        if not title_tag:
            return None
        title = self.clean_text(title_tag.get_text())
        
        # 2. Author (Heuristic: Look for "Shaykh" in title or tags)
        # AbdurRahman often puts author in title or first line
        author = ""
        if "Shaykh" in title:
            # Extract author from title if possible, e.g. "Title – Shaykh Author"
            parts = title.split('–')
            for part in parts:
                if "Shaykh" in part:
                    author = self.clean_text(part).split('[')[0].strip() # remove [Video] tags
                    break
        
        # 3. Content Body
        content_div = soup.find(class_='entry-content')
        if not content_div:
            return None
            
        # Clean Noise
        for noise in content_div.select('.sharedaddy, .jp-relatedposts, .wpcnt, .yarpp-related, script, style, .post-navigation'):
            noise.decompose()
            
        # Remove "Share this:" text if lingering
        # Strategy: Get text, identify footer markers
        content_text = content_div.get_text("\n\n", strip=True)
        
        # Truncate at common footer markers if they survived decomposition
        footer_markers = ["Share this:", "Related Links:", "Post navigation"]
        for marker in footer_markers:
            if marker in content_text:
                content_text = content_text.split(marker)[0].strip()

        # 4. References Extraction (Strict Regex)
        references = []
        
        # Quran: (Surah Name: Verse) or (Surah Name 10:12)
        # Regex to capture Surah refs
        quran_matches = re.findall(r'((?:Surah|Suratul)\s+[A-Za-z\-\’\']+(?:\s+[A-Za-z]+)?(?:\s*:\s*|\s+)?(?:\d+:)?(\d+(?:-\d+)?))', content_text, re.IGNORECASE)
        for match in quran_matches:
            references.append({
                "type": "Quran",
                "citation": match[0] # capture full string for context, or strictly parsing numbers
            })
            
        # Hadith: Bukhari, Muslim, etc.
        hadith_books = ["Bukhari", "Muslim", "Tirmidhi", "Abu Dawood", "Ibn Majah", "Nasa'i", "Ahmad", "Muwatta"]
        for book in hadith_books:
            # Look for Book Name followed by number
            # e.g. "Sahih Muslim 123" or "Muslim, 123"
            h_matches = re.findall(rf'({book}.*?\d+)', content_text, re.IGNORECASE)
            for m in h_matches:
                # Basic filter to avoid long strings
                if len(m) < 30:
                    references.append({
                        "type": "Hadith",
                        "citation": self.clean_text(m)
                    })

        # Scholar citations (Harder, look for "Shaykh said" etc. - ignored for now as requested strict regex)
        # User rule: "Clearly separate: Qur’an verses, Hadith, Scholar commentary"
        # Since Scholar commentary is the *body* usually, we treat content_text as the scholar's words (or translation).
        
        # 5. Category
        # Standard WP categories
        categories = []
        cat_links = soup.select('.cat-links a')
        for cl in cat_links:
            categories.append(cl.get_text())
        category_str = ", ".join(categories)

        return {
            "title": title,
            "author": author,
            "translator": "", # Hard to extract reliably without specific pattern
            "category": category_str,
            "content": content_text,
            "references": references,
            "source_url": url,
            "language": "en" # Majority content
        }

    def run(self):
        print("⚙️ PROCESSING ABDURRAHMAN CONTENT...")
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        c.execute("SELECT url, html FROM articles WHERE html IS NOT NULL")
        rows = c.fetchall()
        
        processed_data = []
        for row in rows:
            article = self.extract_article(row['html'], row['url'])
            if article and article['content']:
                processed_data.append(article)
        
        conn.close()
        
        with open(self.output_path, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, indent=2, ensure_ascii=False)
            
        print(f"✅ Processed {len(processed_data)} articles.")
        print(f"📁 Output saved to {self.output_path}")

if __name__ == "__main__":
    base_dir = os.path.dirname(__file__)
    db_path = os.path.join(base_dir, "data.db")
    output_path = os.path.join(base_dir, "content.json")
    
    processor = AbdurRahmanProcessor(db_path, output_path)
    processor.run()
