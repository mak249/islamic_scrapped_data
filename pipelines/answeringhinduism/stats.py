#!/usr/bin/env python3
"""
STATS MODULE
============
Display status of the Answering Hinduism scraping pipeline.
"""

import sqlite3
import os


def get_stats():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(base_dir, "data.db")
    
    if not os.path.exists(db_path):
        print("❌ Database not found. Run scraper.py first.")
        return
    
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    print("\n🕉️ ANSWERING HINDUISM PIPELINE STATUS")
    print("=" * 50)
    
    # Articles
    c.execute("SELECT COUNT(*) FROM articles")
    total_articles = c.fetchone()[0]
    print(f"📚 Total Articles Scraped: {total_articles}")
    
    # By category
    c.execute("SELECT category, COUNT(*) FROM articles GROUP BY category")
    categories = c.fetchall()
    for cat, count in categories:
        print(f"   - {cat}: {count}")
    
    # Criticisms
    c.execute("SELECT COUNT(*) FROM criticisms")
    total_crit = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM criticisms WHERE retain = 1")
    retained = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM criticisms WHERE retain = 0")
    discarded = c.fetchone()[0]
    
    print(f"\n🔬 Criticism Units Extracted: {total_crit}")
    print(f"   ✅ Retained (clean): {retained}")
    print(f"   ❌ Discarded (Christian dep.): {discarded}")
    
    # By reasoning type
    c.execute("SELECT reasoning_type, COUNT(*) FROM criticisms WHERE retain = 1 GROUP BY reasoning_type")
    rtypes = c.fetchall()
    if rtypes:
        print("\n📊 By Reasoning Type:")
        for rtype, count in rtypes:
            print(f"   - {rtype}: {count}")
    
    conn.close()
    print("=" * 50)


if __name__ == "__main__":
    get_stats()
