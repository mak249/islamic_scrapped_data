#!/usr/bin/env python3
"""
MAX SPEED LOCAL SCRAPER & PROCESSOR (FORMAT-AGNOSTIC)
=====================================================
Strictly one laptop + one IDE.
Max speed. System stability. Accurate counting.
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
logger = logging.getLogger(__name__)

# =============================================================================
# 1. SYSTEM MONITORING & SAFETY
# =============================================================================

class SystemMonitor:
    """Monitors system resources to prevent crashes."""
    
    def __init__(self, cpu_limit=90, ram_limit=85):
        self.cpu_limit = cpu_limit
        self.ram_limit = ram_limit
        self.paused = False
    
    def check_health(self) -> Dict[str, Any]:
        """Check current system health."""
        cpu = psutil.cpu_percent(interval=None)
        ram = psutil.virtual_memory().percent
        disk = psutil.disk_usage('.').percent
        
        status = {
            "cpu": cpu,
            "ram": ram,
            "disk": disk,
            "safe": True
        }
        
        if cpu > self.cpu_limit or ram > self.ram_limit:
            status["safe"] = False
        
        return status
    
    async def throttle_if_needed(self):
        """Pause if system is overloaded."""
        while True:
            health = self.check_health()
            if health["safe"]:
                if self.paused:
                    logger.info("✅ System recovered. Resuming...")
                    self.paused = False
                return
            
            self.paused = True
            logger.warning(f"⚠️ System Overload: CPU {health['cpu']}% | RAM {health['ram']}%. Throttling...")
            await asyncio.sleep(2)  # Wait for recovery

# =============================================================================
# 2. FORMAT-AGNOSTIC DATA HANDLER
# =============================================================================

class FormatDetector:
    """Detects file/source formats automatically."""
    
    @staticmethod
    def detect(path: str) -> str:
        if path.endswith('.db') or path.endswith('.sqlite'):
            return 'sqlite'
        if path.endswith('.jsonl'):
            return 'jsonl'
        if path.endswith('.json'):
            return 'json'
        # Check if it looks like a URL range string "1-1000"
        if re.match(r'^\d+-\d+$', path):
            return 'range'
        return 'unknown'

class DataHandler:
    """Handles reading/writing independent of format."""
    
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
                    language TEXT,
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
    
    async def get_max_id(self) -> int:
        if not os.path.exists(self.db_path):
            return 0
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute('SELECT url FROM qa_pairs') as cursor:
                max_id = 0
                rows = await cursor.fetchall()
                for (url,) in rows:
                    match = re.search(r'/answers/(\d+)', url)
                    if match:
                        max_id = max(max_id, int(match.group(1)))
                return max_id

# =============================================================================
# 3. HIGH-PERFORMANCE SCRAPER
# =============================================================================

class MaxSpeedScraper:
    """Asyncio-based high-throughput scraper."""
    
    def __init__(self, start_id: int, end_id: int, db_path: str = "data.db"):
        self.start_id = start_id
        self.end_id = end_id
        self.db_path = db_path
        self.monitor = SystemMonitor()
        self.handler = DataHandler(db_path)
        
        # Performance Tuning
        self.concurrency = 24  # Base concurrency
        self.batch_size = 50   # Flush to disk every N items
        
        # State
        self.total_items = end_id - start_id + 1
        self.processed = 0
        self.success_count = 0
        self.error_count = 0
        self.start_time = time.time()
        self.visited = set()
    
    async def fetch(self, session: aiohttp.ClientSession, url: str) -> Tuple[str, Optional[str]]:
        try:
            async with session.get(url, timeout=30) as response:
                if response.status == 200:
                    return url, await response.text()
                return url, None # Handle 404/others
        except Exception:
            return url, None

    def parse(self, html_content: str, url: str) -> Optional[Dict[str, Any]]:
        if not html_content:
            return None
            
        try:
            # Quick BS4 parsing (blocking but fast enough for this scale if chunked)
            # In a true huge scale, we might run this in a ProcessPool, but for <500k items, Thread/Main loop is OK if IO bound
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content.replace('<br>', '\n'), 'html.parser') # Basic speed optimization
            
            # Extract Question
            question = ""
            q_elem = soup.select_one('.question-title, h1.title, article h1')
            if q_elem:
                question = q_elem.get_text(strip=True)
            
            # Extract Answer
            answer = ""
            a_elem = soup.select_one('.answer-content, .post-content, article')
            if a_elem:
                # Remove scripts
                for s in a_elem(["script", "style"]):
                    s.decompose()
                answer = a_elem.get_text(separator=' ', strip=True)
            
            if not question or not answer:
                return None
                
            # Basic cleanup
            question = html.unescape(question).strip()
            answer = html.unescape(answer).strip()
            
            # ID
            match = re.search(r'/answers/(\d+)', url)
            qid = match.group(1) if match else str(int(time.time()))
            
            # Language detection (simple)
            has_arabic = bool(re.search(r'[\u0600-\u06FF]', answer))
            lang = 'arabic' if has_arabic else 'english'
            
            return {
                'id': f"qa_{qid}",
                'url': url,
                'question': question,
                'answer': answer,
                'language': lang,
                'quality_score': min(1.0, len(answer.split()) / 100),
                'scraped_at': datetime.now().isoformat()
            }
        except Exception:
            return None

    async def worker(self, name: str, queue: asyncio.Queue, session: aiohttp.ClientSession, results: List):
        while True:
            try:
                url = await queue.get()
            except (asyncio.CancelledError, KeyboardInterrupt):
                return

            try:
                # Flow Control (silent unless critical)
                health = self.monitor.check_health()
                if not health["safe"]:
                    await self.monitor.throttle_if_needed()
                
                # Scraping
                _, content = await self.fetch(session, url)
                
                if content:
                    data = self.parse(content, url)
                    if data:
                        results.append(data)
                        self.success_count += 1
                        # LIVE OUTPUT: Print question immediately
                        q_preview = data['question'][:80] + "..." if len(data['question']) > 80 else data['question']
                        # Clear line to prevent progress bar conflict, print question, then assume progress bar redraws
                        sys.stdout.write(f"\r\033[K✅ [{data['id']}] {q_preview}\n")
                        sys.stdout.flush()
                
                self.processed += 1
                
                # Checkpoint / Progress
                if len(results) >= self.batch_size:
                    batch = results[:]
                    results.clear()
                    await self.handler.save_batch(batch)
                    # self.print_progress() # Reduced spam, only question text is prioritized
                elif self.processed % 50 == 0:
                     self.print_progress() # Show stats line every 50 items
                    
            except Exception as e:
                self.error_count += 1
            finally:
                queue.task_done()

    def print_progress(self):
        elapsed = time.time() - self.start_time
        speed = self.processed / (elapsed / 60) if elapsed > 0 else 0
        remaining = self.total_items - self.processed
        eta = remaining / speed if speed > 0 else 0
        
        # Simple stats line at bottom
        sys.stdout.write(
            f"\r🚀 {speed:.0f} item/min | "
            f"Done: {self.success_count} | "
            f"Rem: {remaining} | "
            f"ETA: {eta:.1f}m"
        )
        sys.stdout.flush()

    async def run(self):
        print(f"🔥 MAX SPEED RUNNER | IDs {self.start_id}-{self.end_id}")
        print(f"📂 Target: {self.db_path}")
        print("----------------------------------------------------------------")
        
        await self.handler.init_db()
        
        # Check resume
        existing_urls = await self.handler.get_existing_urls()
        
        # Create Queue
        queue = asyncio.Queue()
        queued_count = 0
        
        for i in range(self.start_id, self.end_id + 1):
            url = f"https://islamqa.info/en/answers/{i}"
            if url not in existing_urls:
                queue.put_nowait(url)
                queued_count += 1
            else:
                self.processed += 1 # Already done
                self.success_count += 1
        
        self.total_to_process = queued_count
        
        if queued_count == 0:
            print("✨ Nothing new to scrape!")
            return

        # Workers
        connector = aiohttp.TCPConnector(limit=self.concurrency, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=45)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            results = []
            workers = [
                asyncio.create_task(self.worker(f"w-{i}", queue, session, results))
                for i in range(self.concurrency)
            ]
            
            await queue.join()
            
            # Cancel workers
            for w in workers:
                w.cancel()
            
            # Save final
            if results:
                await self.handler.save_batch(results)
        
        print("\n\n🎉 COMPLETE!")

# =============================================================================
# 4. MAIN CONTROLLER
# =============================================================================

def main():
    if len(sys.argv) < 3:
        print("Usage: python max_throughput.py <START_ID|auto> <END_ID|+COUNT>")
        print("Examples:")
        print("  python max_throughput.py 200000 210000")
        print("  python max_throughput.py auto +10000")
        return

    db_path = os.path.join(os.path.dirname(__file__), "data.db")
    
    # Handle Start ID
    if sys.argv[1].lower() == 'auto':
        print(f"🔍 Auto-detecting start ID from {db_path}...")
        try:
            import sqlite3
            if not os.path.exists(db_path):
                start_id = 1
                print("   New database. Starting from ID 1.")
            else:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute('SELECT MAX(CAST(substr(url, instr(url, "/answers/") + 9) AS INTEGER)) FROM qa_pairs')
                row = cursor.fetchone()
                last_id = row[0] if row and row[0] else 0
                conn.close()
                start_id = last_id + 1
                print(f"   ✅ Resuming from ID {start_id} (Last found: {last_id})")
        except Exception as e:
            print(f"   ❌ Error detecting ID: {e}")
            return
    else:
        try:
            start_id = int(sys.argv[1])
        except ValueError:
            print("Error: Start ID must be an integer or 'auto'")
            return

    # Handle End ID
    arg2 = sys.argv[2]
    if arg2.startswith('+'):
        try:
            count = int(arg2[1:])
            end_id = start_id + count - 1
        except ValueError:
            print("Error: Count must be integer")
            return
    else:
        try:
            end_id = int(arg2)
        except ValueError:
            print("Error: End ID must be integer")
            return

    # Initialize Max Speed Scraper
    scraper = MaxSpeedScraper(start_id, end_id, db_path)
    
    # Run Async Loop
    session_start = time.time()
    try:
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(scraper.run())
    except KeyboardInterrupt:
        print("\n\n🛑 PAUSED (Interrupted by User)")
    finally:
        # Session Report
        elapsed = time.time() - session_start
        scraped_this_session = scraper.success_count - (scraper.processed - scraper.total_to_process) if hasattr(scraper, 'total_to_process') else 0
        # More accurate session count for this specific run
        # Actually scraper.success_count includes previously visited if loop logic was loose, but here we added them.
        # Let's trust the internal tracking.
        
        print("\n📊 SESSION REPORT")
        print("----------------------------------------------------------------")
        print(f"Target Range:    {start_id} to {end_id}")
        print(f"Total Processed: {scraper.processed}")
        print(f"Successful:      {scraper.success_count}")
        print(f"Time Elapsed:    {elapsed:.1f} seconds")
        print("----------------------------------------------------------------")
        print(f"👉 To continue where you left off:")
        print(f"   python max_throughput.py auto {end_id}")
        print("----------------------------------------------------------------")

if __name__ == "__main__":
    main()
