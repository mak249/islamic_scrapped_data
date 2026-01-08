import sqlite3
import json
import os
from datetime import datetime

def setup_db(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # Schema for Vedkabhed articles
    c.execute('''
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE,
            title TEXT,
            content TEXT,
            date_published TEXT,
            categories TEXT, 
            references_json TEXT,
            images_json TEXT,
            scraped_at TEXT,
            language TEXT DEFAULT 'en'
        )
    ''')
    conn.commit()
    return conn

def import_data(jsonl_path, db_path):
    if not os.path.exists(jsonl_path):
        print(f"❌ Input file not found: {jsonl_path}")
        return

    conn = setup_db(db_path)
    c = conn.cursor()
    
    count = 0
    skipped = 0
    
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                data = json.loads(line)
                
                # Format data for DB
                url = data.get('url')
                title = data.get('title')
                content = data.get('content')
                date = data.get('date')
                cats = json.dumps(data.get('categories', []), ensure_ascii=False)
                refs = json.dumps(data.get('references', []), ensure_ascii=False)
                imgs = json.dumps(data.get('images', []), ensure_ascii=False)
                scraped = data.get('scraped_at', datetime.now().isoformat())
                
                c.execute('''
                    INSERT OR IGNORE INTO articles 
                    (url, title, content, date_published, categories, references_json, images_json, scraped_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (url, title, content, date, cats, refs, imgs, scraped))
                
                if c.rowcount > 0:
                    count += 1
                else:
                    skipped += 1
                    
            except Exception as e:
                print(f"Error parsing line: {e}")

    conn.commit()
    conn.close()
    
    print(f"✅ Import Complete for {os.path.basename(db_path)}")
    print(f"   Imported: {count}")
    print(f"   Skipped (Duplicates): {skipped}")

if __name__ == "__main__":
    base_dir = os.path.dirname(__file__)
    
    # 1. Import English Data
    english_json = os.path.join(base_dir, 'output', 'vedkabhed_data_final_english.jsonl')
    db_path = os.path.join(base_dir, 'data.db')
    
    print(f"🚀 Importing English Data into {db_path}...")
    import_data(english_json, db_path)
    
    # 2. Import Hindi Data (Optional, but good to have in same DB with lang flag if needed, 
    #    but for now let's keep them in same table but maybe we should add lang column update?)
    #    Let's import Hindi data too but mark language.
    
    hindi_json = os.path.join(base_dir, 'output', 'vedkabhed_data_hindi_only.jsonl')
    if os.path.exists(hindi_json):
        print(f"🚀 Importing Hindi Data...")
        
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        
        with open(hindi_json, 'r', encoding='utf-8') as f:
            h_count = 0
            for line in f:
                data = json.loads(line)
                url = data.get('url')
                title = data.get('title')
                content = data.get('content')
                date = data.get('date')
                cats = json.dumps(data.get('categories', []), ensure_ascii=False)
                refs = json.dumps(data.get('references', []), ensure_ascii=False)
                imgs = json.dumps(data.get('images', []), ensure_ascii=False)
                scraped = data.get('scraped_at', datetime.now().isoformat())
                
                c.execute('''
                    INSERT OR IGNORE INTO articles 
                    (url, title, content, date_published, categories, references_json, images_json, scraped_at, language)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'hi')
                ''', (url, title, content, date, cats, refs, imgs, scraped))
                if c.rowcount > 0:
                    h_count += 1
        
        conn.commit()
        conn.close()
        print(f"   Imported Hindi: {h_count}")

