#!/usr/bin/env python3
"""
DARUSSALAM SCRAPER (Metadata Only)
==================================
Crawls darussalam.com to extract book metadata.
Strictly ignores religious content extraction.
Stores raw HTML in SQLite for processing.
"""

import asyncio
import aiohttp
import aiosqlite
import os
import re
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from typing import List, Dict, Set, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DarussalamScraper")

BASE_URL = "https://darussalam.com"

# Main categories to crawl for books
CATEGORIES = [
    f"{BASE_URL}/quran-mushaf/",
    f"{BASE_URL}/books/hadith/",
    f"{BASE_URL}/fiqh-aqidah/",
    f"{BASE_URL}/books/",
    f"{BASE_URL}/books/history/",
    f"{BASE_URL}/books/children/",
    f"{BASE_URL}/books/education/",
    f"{BASE_URL}/books/other-languages/",
]

class DarussalamScraper:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.concurrency = 5
        self.scraped_urls: Set[str] = set()
        
    async def init_db(self):
        """Initialize SQLite database for raw storage."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE,
                    html TEXT,
                    scraped_at TEXT,
                    category_url TEXT
                )
            ''')
            await db.commit()
            
    async def get_existing_urls(self) -> Set[str]:
        """Load already scraped URLs to avoid duplication."""
        if not os.path.exists(self.db_path):
            return set()
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute('SELECT url FROM products') as cursor:
                rows = await cursor.fetchall()
                logger.info(f"Loaded {len(rows)} existing URLs.")
                return {row[0] for row in rows}

    async def fetch_page(self, session: aiohttp.ClientSession, url: str) -> Optional[str]:
        """Fetch page HTML with error handling."""
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

    async def save_product(self, url: str, html: str, category_url: str):
        """Save raw HTML to database."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                INSERT OR IGNORE INTO products (url, html, scraped_at, category_url)
                VALUES (?, ?, ?, ?)
            ''', (url, html, datetime.now().isoformat(), category_url))
            await db.commit()

    async def process_category(self, session: aiohttp.ClientSession, category_url: str):
        """Crawl a category and download products immediately."""
        logger.info(f"📂 Scanning Category: {category_url}")
        page_num = 1
        
        while True:
            if page_num == 1:
                url = category_url
            else:
                url = f"{category_url}?page={page_num}"
            
            logger.info(f"   📄 Page {page_num}...")
            html = await self.fetch_page(session, url)
            if not html:
                break
            
            soup = BeautifulSoup(html, 'html.parser')
            found_urls = set()
            
            # Selector strategy: Broaden search for product cards
            # Look for links inside elements with 'card' in their class name
            for container in soup.find_all(class_=lambda x: x and 'card' in x):
                a = container.find('a', href=True)
                if a:
                    href = a['href']
                    if href.startswith(BASE_URL) or href.startswith('/'):
                        if href.startswith('/'):
                            href = BASE_URL + href
                        
                        # Filter out non-product links if accidental
                        if any(x in href for x in ['/account.php', '/cart.php', '/login.php']):
                            continue

                        if href not in self.scraped_urls:
                            found_urls.add(href)
            
            # Fallback: Check all headers if no cards found
            if not found_urls:
                for h in soup.find_all(['h3', 'h4', 'h5']):
                    a = h.find('a', href=True)
                    if a:
                       href = a['href']
                       if href.startswith('/'): href = BASE_URL + href
                       if any(x in href for x in ['/account.php', '/cart.php', '/login.php']): continue
                       if href not in self.scraped_urls:
                           found_urls.add(href)

            if not found_urls:
                logger.info("   ❌ No new products found on page. Checking next or stopping.")
                # heuristic: if page is empty, usually end of category.
                # Double check if really empty or just all duplicates
                # If truly 0 items found on page structure, break.
                # Note: 'found_urls' only contains NEW urls. 
                # Let's check raw count to distinguish "end of list" vs "all duplicates"
                raw_count = 0
                for h in soup.find_all(['h3', 'h4', 'h5'], class_=lambda x: x and 'card' in str(x)):
                     if h.find('a'): raw_count += 1
                
                if raw_count == 0: 
                    break 
                
                if not found_urls: # Found items but all duplicates
                    logger.info("   ⚠️ All items on this page already scraped. Moving to next page.")
                    page_num += 1
                    await asyncio.sleep(1)
                    continue

            logger.info(f"   ✅ Found {len(found_urls)} new products on page {page_num}. Downloading...")
            
            # Download these products immediately
            for prod_url in found_urls:
                if prod_url in self.scraped_urls:
                    continue
                    
                print(f"      📦 Downloading: {prod_url.split('/')[-2]}")
                prod_html = await self.fetch_page(session, prod_url)
                if prod_html:
                    await self.save_product(prod_url, prod_html, category_url)
                    self.scraped_urls.add(prod_url)
                    # print(f"         Saved.")
                await asyncio.sleep(0.5)

            page_num += 1
            await asyncio.sleep(1)

    async def run(self):
        print("📚 DARUSSALAM METADATA SCRAPER")
        print("================================")
        
        await self.init_db()
        self.scraped_urls = await self.get_existing_urls()
        
        connector = aiohttp.TCPConnector(limit=self.concurrency)
        async with aiohttp.ClientSession(connector=connector) as session:
            for category in CATEGORIES:
                await self.process_category(session, category)

        print("\n🎉 SCRAPE COMPLETE!")

def main():
    import sys
    db_path = os.path.join(os.path.dirname(__file__), "data.db")
    
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    asyncio.run(DarussalamScraper(db_path).run())

if __name__ == "__main__":
    main()
