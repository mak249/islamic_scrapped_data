#!/usr/bin/env python3
"""
SALAFI PUBLICATIONS SCRAPER (Metadata Only)
===========================================
Source: salafihubbookstore.com (Verified Host)
Crawls book metadata. Strictly ignores content.
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
logger = logging.getLogger("SalafiScraper")

BASE_URL = "https://salafihubbookstore.com"

# Main categories
CATEGORIES = [
    f"{BASE_URL}/product-category/spubs/",
    f"{BASE_URL}/product-category/aqeedah/",
    f"{BASE_URL}/product-category/fiqh/",
    f"{BASE_URL}/product-category/hadeeth/",
    f"{BASE_URL}/product-category/tafseer/",
    f"{BASE_URL}/product-category/manhaj/",
    f"{BASE_URL}/product-category/biographies/",
    f"{BASE_URL}/product-category/women/",
    f"{BASE_URL}/product-category/family/",
    f"{BASE_URL}/product-category/children/",
]

class SalafiScraper:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.concurrency = 5
        self.scraped_urls: Set[str] = set()
        
    async def init_db(self):
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
        if not os.path.exists(self.db_path):
            return set()
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute('SELECT url FROM products') as cursor:
                rows = await cursor.fetchall()
                logger.info(f"Loaded {len(rows)} existing URLs.")
                return {row[0] for row in rows}

    async def fetch_page(self, session: aiohttp.ClientSession, url: str) -> Optional[str]:
        try:
            # Add User-Agent to avoid 403 blocks (just in case)
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
            async with session.get(url, headers=headers, timeout=30) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    logger.warning(f"Failed to fetch {url}: Status {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    async def save_product(self, url: str, html: str, category_url: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                INSERT OR IGNORE INTO products (url, html, scraped_at, category_url)
                VALUES (?, ?, ?, ?)
            ''', (url, html, datetime.now().isoformat(), category_url))
            await db.commit()

    async def process_category(self, session: aiohttp.ClientSession, category_url: str):
        logger.info(f"📂 Scanning Category: {category_url}")
        page_num = 1
        
        while True:
            # WordPress pagination: /page/2/
            if page_num == 1:
                url = category_url
            else:
                url = footer_url = f"{category_url.rstrip('/')}/page/{page_num}/"
            
            logger.info(f"   📄 Page {page_num}...")
            html = await self.fetch_page(session, url)
            if not html:
                break
            
            soup = BeautifulSoup(html, 'html.parser')
            found_urls = set()
            
            # Selector strategies
            # 1. Look for 'product' class in list
            # Usually <li class="product type-product ...">
            #   <a href="...">
            
            # Generic approach: Search for any link containing '/product/' (but not add-to-cart)
            for a in soup.find_all('a', href=True):
                href = a['href']
                if '/product/' in href:
                    if 'add-to-cart' in href: continue
                    if '/product-category/' in href: continue # Skip subcategories
                    
                    if href not in self.scraped_urls:
                         found_urls.add(href)
            
            if not found_urls:
                logger.info("   ❌ No new products found on page. Checking next or stopping.")
                # Verify if it's a 404 page content (WP usually 404s on invalid pagination but sometimes 200 with empty list)
                if "Nothing Found" in soup.get_text() or "404" in soup.title.string:
                    break
                
                # If truly empty
                break

            logger.info(f"   ✅ Found {len(found_urls)} new products on page {page_num}. Downloading...")
            
            for prod_url in found_urls:
                if prod_url in self.scraped_urls:
                    continue
                    
                print(f"      📦 Downloading: {prod_url.split('/')[-2]}")
                prod_html = await self.fetch_page(session, prod_url)
                if prod_html:
                    await self.save_product(prod_url, prod_html, category_url)
                    self.scraped_urls.add(prod_url)
                await asyncio.sleep(0.5)

            page_num += 1
            await asyncio.sleep(1)

    async def run(self):
        print("📚 SALAFI PUBLICATIONS METADATA SCRAPER")
        print("=======================================")
        
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
        
    asyncio.run(SalafiScraper(db_path).run())

if __name__ == "__main__":
    main()
