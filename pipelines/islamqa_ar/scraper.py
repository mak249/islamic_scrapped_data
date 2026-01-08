#!/usr/bin/env python3
"""
ISLAMQA ARABIC HIGH-SPEED SCRAPER
=================================
Optimized for Arabic RTL content and direct database storage.
"""

import asyncio
import aiohttp
import aiosqlite
import psutil
import time
import sys
import os
import json
import re
import html
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Set

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("IslamQA_AR")

# =============================================================================
# 1. SYSTEM MONITORING & SAFETY
# =============================================================================

class SystemMonitor:
    def __init__(self, cpu_limit=90, ram_limit=85):
        self.cpu_limit = cpu_limit
        self.ram_limit = ram_limit
        self.paused = False
    
    def check_health(self) -> Dict[str, Any]:
        cpu = psutil.cpu_percent(interval=None)
        ram = psutil.virtual_memory().percent
        status = {"cpu": cpu, "ram": ram, "safe": True}
        if cpu > self.cpu_limit or ram > self.ram_limit:
            status["safe"] = False
        return status
    
    async def throttle_if_needed(self):
        while True:
            health = self.check_health()
            if health["safe"]:
                if self.paused:
                    logger.info("✅ System recovered. Resuming...")
                    self.paused = False
                return
            if not self.paused:
                logger.warning(f"⚠️ System Overload (CPU {health['cpu']}%). Throttling...")
            self.paused = True
            await asyncio.sleep(5) # Increase sleep to 5s to allow cooling

# =============================================================================
# 2. DATA HANDLER
# =============================================================================

class DataHandler:
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                CREATE TABLE IF NOT EXISTS qa_pairs (
                    id TEXT PRIMARY KEY,
                    url TEXT UNIQUE,
                    question TEXT,
                    answer TEXT,
                    language TEXT DEFAULT 'ar',
                    quality_score REAL,
                    scraped_at TEXT
                )
            ''')
            await db.commit()
    
    async def save_batch(self, batch: List[Dict[str, Any]]):
        async with aiosqlite.connect(self.db_path) as db:
            await db.executemany('''
                INSERT OR REPLACE INTO qa_pairs 
                (id, url, question, answer, language, quality_score, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', [
                (i['id'], i['url'], i['question'], i['answer'], i['language'], i['quality_score'], i['scraped_at'])
                for i in batch
            ])
            await db.commit()
            
    async def get_existing_urls(self) -> Set[str]:
        if not os.path.exists(self.db_path):
            return set()
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute('SELECT url FROM qa_pairs') as cursor:
                rows = await cursor.fetchall()
                return {row[0] for row in rows}

# =============================================================================
# 3. SCRAPER
# =============================================================================

class ArabicScraper:
    def __init__(self, start_id: int, end_id: int, db_path: str):
        self.start_id = start_id
        self.end_id = end_id
        self.db_path = db_path
        self.monitor = SystemMonitor(cpu_limit=95)
        self.handler = DataHandler(db_path)
        self.concurrency = 50
        self.batch_size = 100
        self.processed = 0
        self.success_count = 0
        self.start_time = time.time()
        self.total_range = end_id - start_id + 1

    async def fetch(self, session: aiohttp.ClientSession, url: str) -> Tuple[str, Optional[str]]:
        try:
            async with session.get(url, timeout=30) as response:
                if response.status == 200:
                    return url, await response.text()
                return url, None
        except Exception:
            return url, None

    def parse(self, html_content: str, url: str) -> Optional[Dict[str, Any]]:
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Use same selectors since IslamQA structure is consistent across translations
            q_elem = soup.select_one('.question-title, h1.title, article h1')
            a_elem = soup.select_one('.answer-content, .post-content, article')
            
            if not q_elem or not a_elem:
                return None
                
            for s in a_elem(["script", "style"]):
                s.decompose()
                
            question = html.unescape(q_elem.get_text(strip=True))
            answer = html.unescape(a_elem.get_text(separator=' ', strip=True))
            
            match = re.search(r'/answers/(\d+)', url)
            qid = match.group(1) if match else str(int(time.time()))
            
            return {
                'id': f"ar_{qid}",
                'url': url,
                'question': question,
                'answer': answer,
                'language': 'ar',
                'quality_score': min(1.0, len(answer.split()) / 50),
                'scraped_at': datetime.now().isoformat()
            }
        except Exception:
            return None

    async def worker(self, queue: asyncio.Queue, session: aiohttp.ClientSession, results: List):
        while True:
            url = await queue.get()
            try:
                await self.monitor.throttle_if_needed()
                _, content = await self.fetch(session, url)
                if content:
                    data = self.parse(content, url)
                    if data:
                        results.append(data)
                        self.success_count += 1
                        q_preview = data['question'][:60] + "..." if len(data['question']) > 60 else data['question']
                        sys.stdout.write(f"\r\033[K✅ [AR_{data['id'][3:]}] {q_preview}\n")
                        sys.stdout.flush()
                
                self.processed += 1
                if len(results) >= self.batch_size:
                    batch = results[:]
                    results.clear()
                    await self.handler.save_batch(batch)
                elif self.processed % 50 == 0:
                    self.print_progress()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker error: {e}")
            finally:
                queue.task_done()

    def print_progress(self):
        elapsed = time.time() - self.start_time
        speed = self.processed / (elapsed / 60) if elapsed > 0 else 0
        rem = self.total_range - self.processed
        eta = rem / speed if speed > 0 else 0
        sys.stdout.write(f"\r🚀 Arabic: {speed:.0f} p/m | Done: {self.success_count} | Rem: {rem} | ETA: {eta:.1f}m")
        sys.stdout.flush()

    async def run(self):
        print(f"🔥 ARABIC SCRAPER | Range: {self.start_id} to {self.end_id}")
        await self.handler.init_db()
        existing_urls = await self.handler.get_existing_urls()
        
        queue = asyncio.Queue()
        queued = 0
        for i in range(self.start_id, self.end_id + 1):
            url = f"https://islamqa.info/ar/answers/{i}"
            if url not in existing_urls:
                queue.put_nowait(url)
                queued += 1
            else:
                self.processed += 1
                self.success_count += 1
        
        if queued == 0:
            print("✨ Arabic database is already up to date for this range.")
            return

        connector = aiohttp.TCPConnector(limit=self.concurrency)
        async with aiohttp.ClientSession(connector=connector) as session:
            results = []
            workers = [asyncio.create_task(self.worker(queue, session, results)) for _ in range(self.concurrency)]
            try:
                await queue.join()
            except asyncio.CancelledError:
                logger.info("🛑 Cancellation received. Wrapping up...")
            finally:
                for w in workers: w.cancel()
                if results: 
                    print(f"\n💾 Saving final batch of {len(results)} records...")
                    await self.handler.save_batch(results)
        print("\n\n🎉 ARABIC EXTRACTION PHASE COMPLETE!")

def main():
    if len(sys.argv) < 3:
        print("Usage: python scraper.py <START_ID|auto> <END_ID|+COUNT>")
        return

    db_path = os.path.join(os.path.dirname(__file__), "data.db")
    
    if sys.argv[1].lower() == 'auto':
        if not os.path.exists(db_path):
            start_id = 1
        else:
            import sqlite3
            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            c.execute('SELECT url FROM qa_pairs')
            max_id_db = 0
            for (url,) in c.fetchall():
                match = re.search(r'/answers/(\d+)', url)
                if match: max_id_db = max(max_id_db, int(match.group(1)))
            start_id = max_id_db + 1
            conn.close()
    else:
        start_id = int(sys.argv[1])

    arg2 = sys.argv[2]
    end_id = start_id + int(arg2[1:]) - 1 if arg2.startswith('+') else int(arg2)

    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(ArabicScraper(start_id, end_id, db_path).run())

if __name__ == "__main__":
    main()
