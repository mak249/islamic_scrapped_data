import asyncio
import os
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright
import time
from bs4 import BeautifulSoup

# Add shared modules to path
sys.path.append(os.path.join(os.getcwd(), 'shared'))
try:
    from cleaners.text_cleaner import clean_text
    from system_monitor.monitor import ResourceMonitor
except ImportError as e:
    print(f"Import Error: {e}")
    def clean_text(text): return text.strip()
    class ResourceMonitor:
        def check(self): pass
        def get_stats(self): return "CPU: ? | RAM: ?"

# Configuration
OUTPUT_DIR = Path("pipelines/vedkabhed/output")
STATE_FILE = Path("pipelines/vedkabhed/state.json")
BASE_URL = "https://vedkabhed.com"

# Exact Categories from Meta Prompt
TARGET_CATEGORIES = [
    "Rebuttals", "Rebuttal: Religion", "Rebuttal: Politics", "Rebuttal: History", "Rebuttals: General",
    "Hinduism", "Women", "Politics", "History", "Science", "Violence", "Immorality",
    "Meat Consumption", "Caste System", "Books", "General", "Responses", "Recent", 
    "Comments", "List of all Rebuttals"
]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("VedkaBhed")

class VedkaBhedScraper:
    def __init__(self):
        self.visited_urls = set()
        self.post_queue = [] # [(url, category), ...]
        self.stats = {
            "total_posts": 0,
            "processed": 0,
            "remaining": 0,
            "start_time": time.time()
        }
        self.monitor = ResourceMonitor()
        self.load_state()

    def load_state(self):
        if STATE_FILE.exists():
            try:
                with open(STATE_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.visited_urls = set(data.get("visited", []))
                    self.stats["processed"] = len(self.visited_urls)
            except Exception as e:
                logger.error(f"State load error: {e}")
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    def save_state(self):
        with open(STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump({
                "visited": list(self.visited_urls),
                "stats": self.stats
            }, f, indent=2)

    async def run(self):
        async with async_playwright() as p:
            # Create a persistent session folder to save cookies/solver state
            user_data_dir = os.path.join(os.getcwd(), "pipelines", "vedkabhed", "browser_session")
            os.makedirs(user_data_dir, exist_ok=True)
            
            logger.info(f"📂 Using persistent session: {user_data_dir}")
            
            # Launch persistent context (combines launch + context)
            # Use channel="chrome" if available for better stealth
            try:
                context = await p.chromium.launch_persistent_context(
                    user_data_dir=user_data_dir,
                    channel="chrome", # Use real Chrome if installed
                    headless=False,
                    args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
                    viewport={"width": 1280, "height": 720},
                    java_script_enabled=True,
                )
            except:
                context = await p.chromium.launch_persistent_context(
                    user_data_dir=user_data_dir,
                    headless=False,
                    args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
                    viewport={"width": 1280, "height": 720},
                    java_script_enabled=True,
                )
            
            # Inject stealth scripts
            await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            page = context.pages[0] if context.pages else await context.new_page()

            logger.info("🚀 Starting Vedka Bhed Scraper (Auto-Detection Mode)")
            logger.info("🌍 Opening Homepage... Please solve any Captcha in the browser window.")
            
            await page.goto(BASE_URL)
            
            # Wait for manual solve
            while True:
                title = await page.title()
                if "Just a moment" not in title and "Cloudflare" not in title:
                    logger.info("✅ Site content detected! Automation starting...")
                    break
                logger.info("⌛ Waiting for manual verification in browser...")
                await asyncio.sleep(5)
            
            # Small delay to let page settle
            await asyncio.sleep(2)
            
            # Phase 1: Category & Post Discovery
            await self.discover_content(page)
            
            # Phase 2: Processing
            self.stats["total_posts"] = len(self.post_queue) + len(self.visited_urls)
            self.stats["remaining"] = len(self.post_queue)
            
            logger.info(f"📊 Discovery Complete. Total: {self.stats['total_posts']} | Remaining: {self.stats['remaining']}")
            
            processed_in_session = 0
            
            while self.post_queue:
                self.monitor.check()
                
                url, category = self.post_queue.pop(0)

                # Normalize Category URL (Ensure index.php is present if it's missing)
                if ("/category/" in url or "/list-of-all" in url) and "index.php" not in url:
                    url = url.replace("vedkabhed.com/", "vedkabhed.com/index.php/")

                if url in self.visited_urls:
                    self.stats["remaining"] -= 1
                    continue

                try:

                    # Detect if Category URL
                    if "/category/" in url or "/list-of-all" in url:
                        logger.info(f"📂 Scanning Category: {url}")
                        new_links = await self.scan_category(page, url, category)
                        
                        count_added = 0
                        for link in new_links:
                            if link not in self.visited_urls:
                                self.post_queue.append((link, category))
                                count_added += 1
                        
                        # Only mark category as visited if it actually worked or we want to skip it
                        if new_links:
                            self.visited_urls.add(url)
                            self.stats["total_posts"] += count_added
                            self.stats["remaining"] += count_added
                        else:
                            logger.warning(f"⚠️ Category {url} returned 0 results. Marking as visited.")
                            self.visited_urls.add(url)
                        
                        self.stats["remaining"] -= 1
                        continue
                        
                    data = await self.scrape_post(page, url, category)
                    if data:
                        self.save_record(data)
                        self.visited_urls.add(url)
                        self.stats["processed"] += 1
                        self.stats["remaining"] -= 1
                        processed_in_session += 1
                        
                        if processed_in_session % 5 == 0:
                            self.print_progress(category)
                            self.save_state()
                            
                except Exception as e:
                    logger.error(f"❌ Error {url}: {e}")
                
            await context.close()
            logger.info("✅ All tasks complete.")
            self.stats["remaining"] = len(self.post_queue)
            logger.info(f"🏁 Final Stats: Total: {self.stats['total_posts']} | Processed: {self.stats['processed']} | Remaining: {self.stats['remaining']}")

    async def discover_content(self, page):
        """Discover posts from categories."""
        logger.info("🔍 Phase 1: Discovering Content...")
        
        try:
            # 1. Get Category Links
            # Strategy: Try common sitemaps but proceed to Menu Crawl if they fail/404
            sitemaps_to_try = [
                f"{BASE_URL}/sitemap_index.xml",
                f"{BASE_URL}/sitemap.xml",
                f"{BASE_URL}/wp-sitemap.xml"
            ]
            
            links_found = set()
            
            for sitemap_url in sitemaps_to_try:
                try:
                    logger.info(f"🔍 Checking Sitemap: {sitemap_url}")
                    response = await page.goto(sitemap_url, timeout=30000)
                    
                    if not response or response.status != 200:
                        logger.warning(f"   Sitemap {sitemap_url} returned status {response.status if response else 'None'}")
                        continue

                    # Parse Sitemap
                    content = await page.content()
                    if "loc" not in content.lower():
                        continue
                        
                    soup = BeautifulSoup(content, 'lxml')
                    post_sitemaps = [loc.text for loc in soup.find_all('loc') if 'post-sitemap' in loc.text or '-post-' in loc.text]
                    
                    if not post_sitemaps and "loc" in content.lower():
                        # Maybe it's a direct post sitemap
                        post_sitemaps = [sitemap_url]

                    for sm in post_sitemaps:
                        logger.info(f"📂 Scanning Sitemap: {sm}")
                        await page.goto(sm, timeout=30000)
                        content = await page.content()
                        soup = BeautifulSoup(content, 'lxml')
                        urls = [loc.text for loc in soup.find_all('loc')]
                        for u in urls:
                            if u not in self.visited_urls:
                                links_found.add((u, "Detected"))
                    
                    if links_found:
                        self.post_queue = list(links_found)
                        return # Success
                        
                except Exception as e:
                    logger.warning(f"   Failed to process sitemap {sitemap_url}: {e}")

            if not links_found:
                raise Exception("Empty Sitemap")
                
        except Exception as e:
            logger.warning(f"Sitemap discovery failed ({e}). Switching to Menu Crawl.")
            
        # Fallback: Homepage Menu
        try:
            logger.info("🌍 Crawling Homepage Menu for categories...")
            await page.goto(BASE_URL, timeout=60000)
            await page.wait_for_load_state("domcontentloaded")
            
            # Get all links
            links = await page.eval_on_selector_all('a', 'elements => elements.map(e => ({href: e.href, text: e.innerText}))')
            for l in links:
                href = l['href']
                # Prefer URLs with index.php if available, but keep as is if not
                
                # Loose matching for target categories
                if any(c.lower() in l['text'].lower() for c in TARGET_CATEGORIES) and 'vedkabhed.com' in href:
                     if href not in self.visited_urls:
                          self.post_queue.append((href, l['text'].strip()))
            
            # Also try to find "All Rebuttals" or similar
            if not self.post_queue:
                logger.info(f"Homepage crawl found {len(links)} links, but 0 matched targets.")
                logger.info("Trying Direct Category URLs...")
                
                for cat in TARGET_CATEGORIES:
                    slug = cat.lower().replace(':', '').replace(' ', '-')
                    cat_url = f"{BASE_URL}/index.php/category/{slug}/"
                    self.post_queue.append((cat_url, cat))
            
            # Also add /all-rebuttals/ if exists
            self.post_queue.append((f"{BASE_URL}/index.php/list-of-all-rebuttals/", "General"))
        except Exception as e:
            logger.error(f"❌ Menu Crawl failed: {e}")



    async def scan_category(self, page, url, category):
        links_found = set()
        current_url = url
        
        while True:
            logger.info(f"📂 Scanning Page: {current_url}")
            try:
                await page.goto(current_url, timeout=30000)
                await page.wait_for_load_state("domcontentloaded")
                
                # Extract article links (Standard WP Selectors + Loose Fallbacks)
                new_links = await page.eval_on_selector_all('article h2 a, .entry-title a, h2.title a, .post-title a, .entry-header a', 'elements => elements.map(e => e.href)')
                
                if not new_links:
                    # Try a very broad search for links that look like posts
                    new_links = await page.eval_on_selector_all('a[href*="/20"]', 'elements => elements.map(e => e.href)')

                # Further filtering to avoid pagination/category links
                new_links = [l for l in new_links if "/category/" not in l and "/tag/" not in l and "vedkabhed.com" in l and l != current_url]
                
                if not new_links:
                    logger.info("   No posts found on this page. Saving debug HTML.")
                    debug_path = Path("pipelines/vedkabhed/debug_page.html")
                    content = await page.content()
                    with open(debug_path, "w", encoding="utf-8") as f:
                        f.write(content)
                    break
                    
                count_new = 0
                for l in new_links:
                    if l not in links_found:
                        links_found.add(l)
                        count_new += 1
                
                logger.info(f"   + {count_new} new posts (Category Total: {len(links_found)})")
                
                # Find Next Page
                next_link = await page.eval_on_selector('.nav-links a.next, a.next, a.next-page', 'e => e ? e.href : null')
                if next_link and next_link != current_url:
                    current_url = next_link
                    await asyncio.sleep(1) # Polite delay
                else:
                    logger.info("   Reached last page.")
                    break
            except Exception as e:
                logger.error(f"Error scanning page {current_url}: {e}")
                break
        
        return list(links_found)

    async def scrape_post(self, page, url, category):
        await page.goto(url, timeout=30000)
        
        # Metadata Extraction
        title = await page.title()
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')
        
        article = soup.find('article') or soup.find('div', class_='entry-content')
        if not article: 
            return None

        # 3. Images
        images = []
        for img in article.find_all('img'):
            images.append({
                "url": img.get('src') or img.get('data-src'),
                "alt": img.get('alt', '')
            })
            
        # 4. Source Links
        refs = []
        for link in article.find_all('a'):
            href = link.get('href')
            if href and 'vedkabhed.com' not in href:
                refs.append({
                    "url": href,
                    "text": link.get_text(strip=True)
                })

        # Category extraction (if page has breadcrumbs/tags)
        page_cats = [c.get_text() for c in soup.select('.cat-links a, .post-categories a')]
        final_cats = page_cats if page_cats else [category]

        return {
            "title": title.replace(" - Vedkabhed", ""),
            "date": self.extract_date(soup),
            "content": clean_text(str(article)),
            "categories": final_cats,
            "images": images,
            "references": refs,
            "url": url,
            "scraped_at": datetime.now().isoformat()
        }

    def extract_date(self, soup):
        time_tag = soup.find('time', class_='entry-date') or soup.find('time', class_='published')
        return time_tag.get('datetime') if time_tag else None

    def save_record(self, record):
        with open(OUTPUT_DIR / "vedkabhed_data.jsonl", 'a', encoding='utf-8') as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    def print_progress(self, current_category):
        speed = self.stats['processed'] / (time.time() - self.stats['start_time']) * 60
        print(f"\nSection: Vedka Bhed")
        print(f"Category: {current_category}")
        print(f"Processed: {self.stats['processed']} / {self.stats['total_posts']}")
        print(f"Remaining: {self.stats['remaining']}")
        print(f"Speed: {speed:.1f} posts/min")
        print(f"{self.monitor.get_stats()}")

if __name__ == "__main__":
    scraper = VedkaBhedScraper()
    asyncio.run(scraper.run())
