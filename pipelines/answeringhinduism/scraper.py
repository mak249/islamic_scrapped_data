#!/usr/bin/env python3
"""
ANSWERING HINDUISM SCRAPER
==========================
Extracts criticisms of Hinduism from answeringhinduism.org
Filters out Christian theology and apologetics.
"""

import asyncio
import aiohttp
import aiosqlite
import os
import re
import json
import time
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Set

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AnsweringHinduism")

BASE_URL = "https://answeringhinduism.org"

# URLs to EXCLUDE (Christian content)
EXCLUDE_PATTERNS = [
    r'/trinity',
    r'/genocide-in-the-bible',
    r'/cults-an-introduction',
    r'/hindus-embracing-christianity',
    r'/category/christianity',
]

# URLs to INCLUDE (Hindu criticism)
# URLs to INCLUDE (Hindu criticism)
# We now scrape everything found in the Hinduism category
INCLUDE_PATTERNS = [
    r'.*',  # Match everything (we rely on exclusions and source page context)
]


class AnsweringHinduismScraper:
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
                    title TEXT,
                    raw_content TEXT,
                    category TEXT,
                    scraped_at TEXT
                )
            ''')
            await db.execute('''
                CREATE TABLE IF NOT EXISTS criticisms (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    article_id INTEGER,
                    topic TEXT,
                    claim TEXT,
                    source_excerpt TEXT,
                    hindu_reference TEXT,
                    reasoning_type TEXT,
                    dependency_on_christianity INTEGER DEFAULT 0,
                    retain INTEGER DEFAULT 1,
                    FOREIGN KEY(article_id) REFERENCES articles(id)
                )
            ''')
            await db.commit()
    
    async def get_existing_urls(self) -> Set[str]:
        if not os.path.exists(self.db_path):
            return set()
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute('SELECT url FROM articles') as cursor:
                rows = await cursor.fetchall()
                return {row[0] for row in rows}
    
    def should_scrape(self, url: str) -> bool:
        """Check if URL matches include patterns and doesn't match exclude patterns."""
        # Check exclusions first
        for pattern in EXCLUDE_PATTERNS:
            if re.search(pattern, url, re.IGNORECASE):
                return False
        # Check inclusions
        for pattern in INCLUDE_PATTERNS:
            if re.search(pattern, url, re.IGNORECASE):
                return True
        return False
    
    async def discover_urls(self, session: aiohttp.ClientSession) -> List[str]:
        """Crawl all pages under the Hinduism category using brute-force pagination."""
        discovered = set()
        base_category = f"{BASE_URL}/category/hinduism/"
        
        print("🔍 Crawling Hinduism category (Pages 1-20)...")
        
        for page_num in range(1, 21):
            if page_num == 1:
                url = base_category
            else:
                url = f"{base_category}page/{page_num}/"
            
            print(f"   scanning: {url}...")
            
            try:
                async with session.get(url, timeout=30) as response:
                    # Treat 404 as end of list
                    if response.status != 200:
                        print(f"   ⚠️ Page {page_num} returned {response.status}. Stopping.")
                        break
                        
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    found_on_page = 0
                    for a in soup.find_all('a', href=True):
                        href = a['href']
                        
                        # Only look for article URLs within domain
                        if not href.startswith(BASE_URL):
                            continue
                            
                        # If it looks like an article URL (usually has date or typical slug structure)
                        # And fits our inclusive/exclusive patterns
                        if self.should_scrape(href):
                            clean_url = href.split('#')[0].split('?')[0]
                            if clean_url not in discovered:
                                discovered.add(clean_url)
                                found_on_page += 1
                    
                    print(f"   ✅ Found {found_on_page} articles on page {page_num}")
                    
                    # Stop heuristic: If we find 0 articles deep in pagination
                    if found_on_page == 0 and page_num > 6:
                        print("   ⚠️ No text articles found. Stopping.")
                        break
                        
            except Exception as e:
                logger.error(f"Error crawling {url}: {e}")
                
        print(f"📊 Crawl finished. Found {len(discovered)} unique articles.")
        return list(discovered)
    
    async def fetch_article(self, session: aiohttp.ClientSession, url: str) -> Optional[Dict]:
        """Fetch and parse a single article."""
        try:
            async with session.get(url, timeout=30) as response:
                if response.status != 200:
                    return None
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Extract title
                title_elem = soup.find('h1', class_='entry-title') or soup.find('h1')
                title = title_elem.get_text(strip=True) if title_elem else "Untitled"
                
                # Extract main content
                content_elem = soup.find('div', class_='entry-content') or soup.find('article')
                if not content_elem:
                    return None
                
                # Remove scripts and styles
                for tag in content_elem(['script', 'style', 'nav', 'footer']):
                    tag.decompose()
                
                raw_content = content_elem.get_text(separator='\n', strip=True)
                
                # Detect category
                category = "general"
                if 'manusmriti' in url.lower():
                    category = "manusmriti"
                elif 'varna' in url.lower():
                    category = "varna"
                elif 'intoxicant' in url.lower():
                    category = "intoxicants"
                elif 'yuga' in url.lower() or 'brahminical-hoax' in url.lower():
                    category = "yugas"
                elif 'panini' in url.lower():
                    category = "history"
                
                return {
                    'url': url,
                    'title': title,
                    'raw_content': raw_content,
                    'category': category,
                    'scraped_at': datetime.now().isoformat()
                }
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    async def save_article(self, article: Dict):
        """Save article to database."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                INSERT OR REPLACE INTO articles (url, title, raw_content, category, scraped_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (article['url'], article['title'], article['raw_content'], 
                  article['category'], article['scraped_at']))
            await db.commit()
    
    async def run(self):
        """Main scraping loop."""
        print("🕉️ ANSWERING HINDUISM SCRAPER")
        print("=" * 50)
        
        await self.init_db()
        existing = await self.get_existing_urls()
        
        connector = aiohttp.TCPConnector(limit=self.concurrency)
        async with aiohttp.ClientSession(connector=connector) as session:
            # Discover URLs
            print("🔍 Discovering article URLs...")
            urls = await self.discover_urls(session)
            new_urls = [u for u in urls if u not in existing]
            
            print(f"📊 Found {len(urls)} total, {len(new_urls)} new articles")
            
            if not new_urls:
                print("✨ All articles already scraped!")
                return
            
            # Fetch articles
            for i, url in enumerate(new_urls):
                print(f"📥 [{i+1}/{len(new_urls)}] Fetching: {url}")
                article = await self.fetch_article(session, url)
                if article:
                    await self.save_article(article)
                    print(f"   ✅ Saved: {article['title'][:50]}...")
                await asyncio.sleep(1)  # Be polite
        
        print("\n🎉 SCRAPING COMPLETE!")


def main():
    import sys
    db_path = os.path.join(os.path.dirname(__file__), "data.db")
    
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(AnsweringHinduismScraper(db_path).run())


if __name__ == "__main__":
    main()
