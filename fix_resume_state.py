#!/usr/bin/env python3
"""
One-time script to fix the resume_state table for IslamQA.
Supports both unified storage (content table) and fast_scraper format (qa_pairs table).
"""
import sqlite3
import re
import sys
import os

# Add parent dir to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def get_max_id_from_urls(db_path: str, table: str, url_column: str = 'url') -> int:
    """Extract max question ID from URLs in any table."""
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute(f'SELECT {url_column} FROM {table}')
        max_id = 0
        for (url,) in c.fetchall():
            match = re.search(r'/answers/(\d+)', url)
            if match:
                max_id = max(max_id, int(match.group(1)))
        conn.close()
        return max_id
    except Exception as e:
        print(f"   Error reading {table}: {e}")
        return 0


def fix_resume_state(db_path: str = "islamic_data.db"):
    """Fix the resume_state by calculating max ID from URLs."""
    
    print(f"\n🔧 Fixing resume_state in {db_path}")
    print("=" * 60)
    
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    # Check what tables exist
    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [t[0] for t in c.fetchall()]
    print(f"📋 Tables: {tables}")
    
    max_id = 0
    
    # Check for content table (unified storage)
    if 'content' in tables:
        c.execute('SELECT COUNT(*) FROM content WHERE source = "islamqa"')
        count = c.fetchone()[0]
        print(f"📊 content table: {count} IslamQA records")
        if count > 0:
            max_id = max(max_id, get_max_id_from_urls(db_path, 'content'))
    
    # Check for qa_pairs table (fast_scraper format)
    if 'qa_pairs' in tables:
        c.execute('SELECT COUNT(*) FROM qa_pairs')
        count = c.fetchone()[0]
        print(f"📊 qa_pairs table: {count} records")
        if count > 0:
            max_id = max(max_id, get_max_id_from_urls(db_path, 'qa_pairs'))
    
    print(f"📊 Max question ID found: {max_id}")
    
    # Check/create resume_state table and update
    if 'resume_state' in tables:
        c.execute('SELECT * FROM resume_state WHERE source = "islamqa"')
        current = c.fetchone()
        print(f"📊 Current resume_state: {current}")
    else:
        # Create resume_state table for fast_scraper DBs
        print("📊 Creating resume_state table...")
        c.execute('''
            CREATE TABLE IF NOT EXISTS resume_state (
                source TEXT PRIMARY KEY,
                last_url TEXT,
                last_id INTEGER,
                last_scraped_at TEXT,
                status TEXT
            )
        ''')
        conn.commit()
    
    if max_id > 0:
        # Update resume_state
        from datetime import datetime
        c.execute('''
            INSERT OR REPLACE INTO resume_state (source, last_url, last_id, last_scraped_at, status)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            'islamqa',
            f"https://islamqa.info/en/answers/{max_id}",
            max_id,
            datetime.now().isoformat(),
            'paused'
        ))
        conn.commit()
        
        # Verify
        c.execute('SELECT * FROM resume_state WHERE source = "islamqa"')
        new_state = c.fetchone()
        print(f"\n✅ Updated resume_state:")
        print(f"   last_id: {new_state[2]}")
        print(f"   last_url: {new_state[1]}")
        print(f"   status: {new_state[4]}")
        print(f"\n🚀 Next scrape will start from ID: {max_id + 1}")
    else:
        print("⚠️  No IslamQA records found. Nothing to fix.")
    
    conn.close()
    return max_id


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Fix resume_state for IslamQA scraping'
    )
    parser.add_argument('--db', default='islamic_data.db',
                       help='Database file to fix (default: islamic_data.db)')
    parser.add_argument('--all', action='store_true',
                       help='Fix all .db files in current directory')
    
    args = parser.parse_args()
    
    if args.all:
        # Fix all databases
        db_files = [f for f in os.listdir('.') if f.endswith('.db')]
        for db_file in sorted(db_files):
            try:
                fix_resume_state(db_file)
            except Exception as e:
                print(f"   ❌ Error: {e}")
    else:
        fix_resume_state(args.db)


if __name__ == "__main__":
    main()
