#!/usr/bin/env python3
"""
AbdurRahman.org SCRAPER (Controlled Content)
============================================
Crawls articles for educational content.
Stores raw HTML for careful processing.
"""

import asyncio
import aiohttp
import aiosqlite
import os
import re
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from typing import Set, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AbdurRahmanScraper")

BASE_URL = "https://abdurrahman.org"

class AbdurRahmanScraper:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.concurrency = 5
        self.scraped_urls: Set[str] = set()
        
    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                CREATE TABLE IF NOT EXISTS articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE,
                    html TEXT,
                    scraped_at TEXT
                )
            ''')
            await db.commit()
            
    async def get_existing_urls(self) -> Set[str]:
        if not os.path.exists(self.db_path):
            return set()
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute('SELECT url FROM articles') as cursor:
                rows = await cursor.fetchall()
                logger.info(f"Loaded {len(rows)} existing URLs.")
                return {row[0] for row in rows}

    async def fetch_page(self, session: aiohttp.ClientSession, url: str) -> Optional[str]:
        try:
            async with session.get(url, timeout=30) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    logger.warning(f"Failed to fetch {url}: Status {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    async def save_article(self, url: str, html: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                INSERT OR IGNORE INTO articles (url, html, scraped_at)
                VALUES (?, ?, ?)
            ''', (url, html, datetime.now().isoformat()))
            await db.commit()

    async def crawl_blog_pages(self, session: aiohttp.ClientSession, max_pages: int = 5):
        """Crawl the main blog pagination."""
        for page_num in range(1, max_pages + 1):
            if page_num == 1:
                url = BASE_URL
            else:
                url = f"{BASE_URL}/page/{page_num}/"
            
            logger.info(f"📄 Scanning Page {page_num}: {url}")
            html = await self.fetch_page(session, url)
            if not html:
                break
            
            soup = BeautifulSoup(html, 'html.parser')
            article_urls = set()
            
            # Standard WP: <article ...> <h2 class="entry-title"><a href="...">
            for article in soup.find_all('article'):
                h2 = article.find(['h1', 'h2'], class_='entry-title')
                if h2 and h2.find('a'):
                    href = h2.find('a')['href']
                    if href not in self.scraped_urls:
                        article_urls.add(href)
            
            if not article_urls:
                logger.info("❌ No new articles found on this page.")
                break # Should not break if just duplicates, but if EMPTY structure
                # Actually WP might return 404 if page out of range, handled by fetch_page returning None
            
            logger.info(f"   ✅ Found {len(article_urls)} new articles. Downloading...")
            
            for art_url in article_urls:
                if art_url in self.scraped_urls:
                    continue
                
                print(f"      📥 Downloading: {art_url.split('/')[-2]}")
                art_html = await self.fetch_page(session, art_url)
                if art_html:
                    await self.save_article(art_url, art_html)
                    self.scraped_urls.add(art_url)
                await asyncio.sleep(0.5)
            
            await asyncio.sleep(1)

    async def run(self):
        print("📚 ABDURRAHMAN.ORG CONTENT SCRAPER")
        print("==================================")
        
        await self.init_db()
        self.scraped_urls = await self.get_existing_urls()
        
        connector = aiohttp.TCPConnector(limit=self.concurrency)
        async with aiohttp.ClientSession(connector=connector) as session:
            await self.crawl_blog_pages(session, max_pages=3) # Limit for initial run

        print("\n🎉 SCRAPE COMPLETE!")

def main():
    import sys
    # Initialize DB path in current directory
    db_path = os.path.join(os.path.dirname(__file__), "data.db")
    
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    asyncio.run(AbdurRahmanScraper(db_path).run())

if __name__ == "__main__":
    main()
