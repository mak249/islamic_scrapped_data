import sqlite3
from bs4 import BeautifulSoup
import re

conn = sqlite3.connect('pipelines/abdurrahman/data.db')
c = conn.cursor()
c.execute("SELECT url, html FROM articles ORDER BY RANDOM() LIMIT 1")
row = c.fetchone()

if row:
    print(f"URL: {row[0]}")
    soup = BeautifulSoup(row[1], 'html.parser')
    
    # Title
    title = soup.find(class_='entry-title').get_text() if soup.find(class_='entry-title') else "No Title"
    print(f"TITLE: {title}")
    
    # Body
    content_div = soup.find(class_='entry-content')
    if content_div:
        text = content_div.get_text("\n", strip=True)
        print("\n--- CONTENT PREVIEW ---")
        print(text[:1000])
        
        print("\n--- REFERENCES CHECK ---")
        # Check for Surah
        surahs = re.findall(r'(Surah.*?\d+)', text, re.IGNORECASE)
        print(f"Surahs found: {surahs}")
        
        # Check for Hadith
        hadiths = re.findall(r'(Bukhaari|Muslim|Tirmidhi|Abu Dawood).*?(\d+)', text, re.IGNORECASE)
        print(f"Hadiths found: {hadiths}")
        
    else:
        print("No content div found")
else:
    print("No articles found")
conn.close()
