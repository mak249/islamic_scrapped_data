4#!/usr/bin/env python3
"""
FAST IslamQA Scraper using Scrapy + Playwright
"""

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import json
import sqlite3
import time
import re
import html
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup

class IslamQASpider(scrapy.Spider):
    name = 'islamqa_fast'
    handle_httpstatus_list = [200, 404]
    start_urls = []
    
    def __init__(self, start_id=1, end_id=10000, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_id = start_id
        self.end_id = end_id
        self.db_path = "islamqa_fast.db"
        self.scraped_count = 0
        self.visited_urls = set()
        
        # Initialize database
        self.init_database()
        self.load_visited_urls()
        
        # Generate start URLs
        self.start_urls = [f"https://islamqa.info/en/answers/{i}" for i in range(start_id, end_id + 1)]
        
        print(f"🚀 FAST Scraper initialized for IDs {start_id} to {end_id}")
        print(f"📋 Loaded {len(self.visited_urls)} previously visited URLs")
    
    def init_database(self):
        """Initialize SQLite database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS qa_pairs (
                id TEXT PRIMARY KEY,
                url TEXT UNIQUE,
                question TEXT NOT NULL,
                answer TEXT NOT NULL,
                fatwa_number TEXT,
                language TEXT,
                word_count INTEGER,
                quality_score REAL,
                scraped_at TEXT,
                cleaned_at TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def load_visited_urls(self):
        """Load previously visited URLs from database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT url FROM qa_pairs')
        self.visited_urls = {row[0] for row in cursor.fetchall()}
        
        conn.close()

    @staticmethod
    def get_last_scraped_id(db_path: str) -> int:
        """Return highest numeric ID seen in stored URLs, or 0 if none."""
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COALESCE(MAX(CAST(substr(url, instr(url, '/answers/') + 9) AS INTEGER)), 0) FROM qa_pairs")
            row = cursor.fetchone()
            conn.close()
            return int(row[0]) if row and row[0] else 0
        except Exception:
            return 0

    @staticmethod
    def estimate_remaining(db_path: str, max_id: int) -> int:
        last = IslamQASpider.get_last_scraped_id(db_path)
        remaining = max(0, max_id - last)
        return remaining
    
    def start_requests(self):
        """Generate requests for each URL"""
        for url in self.start_urls:
            if url not in self.visited_urls:
                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    # Use regular HTTP client; Playwright not needed for these pages
                    meta={'playwright': False},
                    dont_filter=True
                )
    
    @staticmethod
    def clean_text(text):
        """Clean HTML and extract plain text"""
        if not text:
            return ""
        # Decode HTML entities first
        text = html.unescape(text)
        # Remove HTML tags using BeautifulSoup
        soup = BeautifulSoup(text, 'html.parser')
        text = soup.get_text(separator=' ', strip=True)
        # Clean up whitespace and normalize
        text = re.sub(r'\s+', ' ', text).strip()
        # Decode again in case BeautifulSoup didn't catch everything
        text = html.unescape(text)
        return text
    
    @staticmethod
    def contains_html(text):
        """Check if text contains HTML tags"""
        if not text:
            return False
        return bool(re.search(r'<[^>]+>', text))
    
    def parse(self, response):
        """Parse the response using Scrapy selectors and BeautifulSoup for clean text extraction."""
        url = response.url

        # Skip previously visited and non-200 (404s are gaps)
        if url in self.visited_urls or getattr(response, 'status', 200) != 200:
            return

        # Extract question using CSS selectors
        question = "No question found"
        question_selectors = [
            'h1.title',
            'h1',
            'h2.title',
            'title',
            'article h1',
            '.question-title',
            '.post-title'
        ]
        
        for selector in question_selectors:
            q_elem = response.css(selector).get()
            if q_elem:
                question = self.clean_text(q_elem)
                if question and len(question) > 10 and not self.contains_html(question):
                    break
        
        # If CSS selectors didn't work, try XPath
        if question == "No question found" or self.contains_html(question):
            xpath_selectors = [
                '//h1[contains(@class, "title")]//text()',
                '//h1//text()',
                '//title//text()'
            ]
            for xpath in xpath_selectors:
                q_text = response.xpath(xpath).get()
                if q_text:
                    question = self.clean_text(q_text.strip())
                    if question and len(question) > 10 and not self.contains_html(question):
                        break

        # Extract answer using CSS selectors - target the main content area
        answer = "No answer found"
        answer_selectors = [
            'article#single-post-content',
            'article.single-post-content',
            '.post-content',
            '.entry-content',
            '.answer-content',
            'article',
            '.content',
            '.main-content'
        ]
        
        for selector in answer_selectors:
            answer_elem = response.css(selector).get()
            if answer_elem:
                # Use BeautifulSoup to extract clean text from the answer
                soup = BeautifulSoup(answer_elem, 'html.parser')
                # Remove script and style tags
                for script in soup(["script", "style", "nav", "aside", "footer", "header"]):
                    script.decompose()
                # Get text
                answer = soup.get_text(separator=' ', strip=True)
                # Clean up
                answer = re.sub(r'\s+', ' ', answer).strip()
                if answer and len(answer) > 20 and not self.contains_html(answer):
                    break
        
        # If CSS selectors didn't work, try XPath
        if answer == "No answer found" or self.contains_html(answer):
            xpath_selectors = [
                '//article[@id="single-post-content"]//text()',
                '//article[@class="single-post-content"]//text()',
                '//div[contains(@class, "post-content")]//text()',
                '//div[contains(@class, "entry-content")]//text()',
                '//article//text()'
            ]
            for xpath in xpath_selectors:
                answer_parts = response.xpath(xpath).getall()
                if answer_parts:
                    answer = ' '.join([part.strip() for part in answer_parts if part.strip()])
                    answer = self.clean_text(answer)
                    if answer and len(answer) > 20 and not self.contains_html(answer):
                        break

        # Final validation - reject if still contains HTML
        if self.contains_html(question) or self.contains_html(answer):
            print(f"⚠️ Skipping {url} - contains HTML in extracted content")
            return

        # Extract fatwa number from text
        fatwa_number = ""
        fatwa_patterns = [
            r'Fatwa\s*No\.?\s*(\d+)',
            r'Question\s*No\.?\s*(\d+)',
            r'Answer\s*No\.?\s*(\d+)',
            r'ID:\s*(\d+)'
        ]
        full_text = f"{question} {answer}"
        for pattern in fatwa_patterns:
            m = re.search(pattern, full_text, re.IGNORECASE)
            if m:
                fatwa_number = m.group(1)
                break

        language = "english"
        if re.search(r'[\u0600-\u06FF]', full_text):
            language = "arabic"

        word_count = len(answer.split())
        quality_score = min(1.0, word_count / 100.0)

        if question != "No question found" and answer != "No answer found":
            qa_data = {
                'id': f"qa_{int(time.time())}_{self.scraped_count}",
                'url': url,
                'question': question,
                'answer': answer,
                'fatwa_number': fatwa_number,
                'language': language,
                'word_count': word_count,
                'quality_score': quality_score,
                'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'cleaned_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            self.save_qa_to_database(qa_data)
            self.visited_urls.add(url)
            self.scraped_count += 1
            print(f"✅ [{self.scraped_count}] {question[:80]}...")
    
    def save_qa_to_database(self, qa_data):
        """Save Q&A data to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO qa_pairs 
                (id, url, question, answer, fatwa_number, language, word_count, quality_score, scraped_at, cleaned_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                qa_data['id'],
                qa_data['url'],
                qa_data['question'],
                qa_data['answer'],
                qa_data['fatwa_number'],
                qa_data['language'],
                qa_data['word_count'],
                qa_data['quality_score'],
                qa_data['scraped_at'],
                qa_data['cleaned_at']
            ))
            
            conn.commit()
        except Exception as e:
            print(f"❌ Database error: {e}")
        finally:
            conn.close()

def run_fast_scraper(start_id=1, end_id=1000):
    """Run the fast scraper"""
    print("🚀 FAST IslamQA Scraper (high-concurrency HTTP)")
    print("=" * 60)
    
    # Configure Scrapy settings
    settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 0.05,
        'RANDOMIZE_DOWNLOAD_DELAY': 0.05,
        'CONCURRENT_REQUESTS': 24,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 12,
        'DOWNLOAD_TIMEOUT': 45,
        'RETRY_TIMES': 2,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429],
        # Reduce noisy 404 logs (these are expected due to gaps in IDs)
        'HTTPERROR_ALLOWED_CODES': [404],
        # Accept partial responses to avoid data-loss slowdowns
        'DOWNLOAD_FAIL_ON_DATALOSS': False,
        'LOG_LEVEL': 'WARNING',
        # Optional throttling toggles
        'AUTOTHROTTLE_ENABLED': False,
        # Disable cookies to avoid overhead
        'COOKIES_ENABLED': False,
        # DNS cache for faster connections
        'DNSCACHE_ENABLED': True,
        # HTTP compression is already enabled by middleware
    }
    
    process = CrawlerProcess(settings)
    process.crawl(IslamQASpider, start_id=start_id, end_id=end_id)
    print(f"📈 Planning scrape: IDs {start_id}–{end_id}")
    try:
        remaining = IslamQASpider.estimate_remaining("islamqa_fast.db", end_id)
        last = IslamQASpider.get_last_scraped_id("islamqa_fast.db")
        print(f"   - Last scraped ID: {last}")
        print(f"   - Estimated remaining in this window: ~{remaining:,} (includes gaps/404s)")
    except Exception:
        pass
    process.start()

def export_training_data(db_path="islamqa_fast.db"):
    """Export data to training formats"""
    print("\n📤 Exporting training data...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM qa_pairs ORDER BY created_at DESC')
    rows = cursor.fetchall()
    
    if not rows:
        print("No data to export")
        conn.close()
        return
    
    # Convert to list of dictionaries
    qa_data = []
    for row in rows:
        qa_data.append({
            'id': row[0],
            'url': row[1],
            'question': row[2],
            'answer': row[3],
            'fatwa_number': row[4],
            'language': row[5],
            'word_count': row[6],
            'quality_score': row[7],
            'scraped_at': row[8],
            'cleaned_at': row[9]
        })
    
    conn.close()
    
    # Create training data directory
    Path("training_data").mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Export in multiple formats
    formats_exported = []
    
    # 1. JSON format
    json_file = f"training_data/fast_data_{timestamp}.json"
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(qa_data, f, ensure_ascii=False, indent=2)
    formats_exported.append(f"JSON: {json_file}")
    
    # 2. ChatGPT format
    chatgpt_file = f"training_data/fast_chatgpt_{timestamp}.jsonl"
    with open(chatgpt_file, 'w', encoding='utf-8') as f:
        for qa in qa_data:
            conversation = {
                "messages": [
                    {"role": "user", "content": qa['question']},
                    {"role": "assistant", "content": qa['answer']}
                ],
                "metadata": {
                    "id": qa['id'],
                    "url": qa['url'],
                    "language": qa['language'],
                    "quality_score": qa['quality_score']
                }
            }
            f.write(json.dumps(conversation, ensure_ascii=False) + '\n')
    formats_exported.append(f"ChatGPT: {chatgpt_file}")
    
    # 3. LLaMA format
    llama_file = f"training_data/fast_llama_{timestamp}.jsonl"
    with open(llama_file, 'w', encoding='utf-8') as f:
        for qa in qa_data:
            instruction_data = {
                "instruction": f"Answer this Islamic question: {qa['question']}",
                "input": "",
                "output": qa['answer'],
                "metadata": {
                    "id": qa['id'],
                    "language": qa['language'],
                    "quality_score": qa['quality_score']
                }
            }
            f.write(json.dumps(instruction_data, ensure_ascii=False) + '\n')
    formats_exported.append(f"LLaMA: {llama_file}")
    
    # 4. Alpaca format
    alpaca_file = f"training_data/fast_alpaca_{timestamp}.json"
    alpaca_data = []
    for qa in qa_data:
        alpaca_entry = {
            "instruction": f"Answer this Islamic question: {qa['question']}",
            "input": "",
            "output": qa['answer'],
            "language": qa['language'],
            "quality_score": qa['quality_score']
        }
        alpaca_data.append(alpaca_entry)
    
    with open(alpaca_file, 'w', encoding='utf-8') as f:
        json.dump(alpaca_data, f, ensure_ascii=False, indent=2)
    formats_exported.append(f"Alpaca: {alpaca_file}")
    
    # 5. RAG format
    rag_file = f"training_data/fast_rag_{timestamp}.jsonl"
    with open(rag_file, 'w', encoding='utf-8') as f:
        for qa in qa_data:
            document = {
                "id": qa['id'],
                "content": f"Question: {qa['question']}\n\nAnswer: {qa['answer']}",
                "metadata": {
                    "question": qa['question'],
                    "answer": qa['answer'],
                    "language": qa['language'],
                    "quality_score": qa['quality_score'],
                    "word_count": qa['word_count'],
                    "url": qa['url']
                }
            }
            f.write(json.dumps(document, ensure_ascii=False) + '\n')
    formats_exported.append(f"RAG: {rag_file}")
    
    # 6. Text format
    txt_file = f"training_data/fast_training_{timestamp}.txt"
    with open(txt_file, 'w', encoding='utf-8') as f:
        f.write(f"ISLAMQA FAST DATA EXPORT\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total Q&A pairs: {len(qa_data):,}\n")
        f.write("=" * 80 + "\n\n")
        
        for i, qa in enumerate(qa_data, 1):
            f.write(f"Q&A Pair #{i}\n")
            f.write("-" * 40 + "\n")
            f.write(f"Question: {qa['question']}\n\n")
            f.write(f"Answer: {qa['answer']}\n\n")
            f.write(f"Language: {qa['language']}\n")
            f.write(f"Quality Score: {qa['quality_score']:.2f}\n")
            f.write(f"URL: {qa['url']}\n")
            f.write(f"Scraped: {qa['scraped_at']}\n")
            f.write("\n" + "=" * 80 + "\n\n")
    formats_exported.append(f"Text: {txt_file}")
    
    print(f"✅ Exported {len(qa_data):,} Q&A pairs in 6 formats:")
    for fmt in formats_exported:
        print(f"   - {fmt}")

def clean_existing_data(db_path="islamqa_fast.db"):
    """Clean existing bad data in the database that contains HTML"""
    print("\n🧹 Cleaning existing data with HTML content...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Find records with HTML content
    cursor.execute('SELECT id, url, question, answer FROM qa_pairs')
    rows = cursor.fetchall()
    
    cleaned_count = 0
    deleted_count = 0
    
    for row_id, url, question, answer in rows:
        needs_update = False
        new_question = question
        new_answer = answer
        
        # Check and clean question
        if IslamQASpider.contains_html(question):
            new_question = IslamQASpider.clean_text(question)
            needs_update = True
        else:
            # Still check for HTML entities even if no HTML tags
            if '&amp;' in question or '&lt;' in question or '&gt;' in question:
                new_question = html.unescape(question)
                needs_update = True
        
        # Check and clean answer
        if IslamQASpider.contains_html(answer):
            new_answer = IslamQASpider.clean_text(answer)
            needs_update = True
        else:
            # Still check for HTML entities even if no HTML tags
            if '&amp;' in answer or '&lt;' in answer or '&gt;' in answer:
                new_answer = html.unescape(answer)
                needs_update = True
        
        # If still contains HTML after cleaning, delete the record
        if IslamQASpider.contains_html(new_question) or IslamQASpider.contains_html(new_answer):
            cursor.execute('DELETE FROM qa_pairs WHERE id = ?', (row_id,))
            deleted_count += 1
            print(f"❌ Deleted {url} - could not extract clean text")
        elif needs_update:
            # Update with cleaned text
            cursor.execute('''
                UPDATE qa_pairs 
                SET question = ?, answer = ?, cleaned_at = ?
                WHERE id = ?
            ''', (new_question, new_answer, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), row_id))
            cleaned_count += 1
    
    conn.commit()
    conn.close()
    
    print(f"✅ Cleaned {cleaned_count} records")
    print(f"❌ Deleted {deleted_count} records with unresolvable HTML")
    print(f"📊 Total processed: {len(rows)} records")

def main():
    """Main function"""
    print("🕌 FAST IslamQA Scraper")
    print("=" * 60)
    print("High-speed mode (no JS rendering):")
    print("✅ 24 concurrent requests")
    print("✅ 0.05s delay between requests")
    print("✅ Retries + DNS cache enabled")
    print("✅ Clean text extraction (no HTML)")
    print()
    
    # Clean existing bad data first
    clean_existing_data()
    
    print("\nChoose your scraping range:")
    print("1. Quick Test (1-100)")
    print("2. Medium Run (1-1000)")
    print("3. Large Run (1-5000)")
    print("4. Massive Run (1-10000)")
    print("5. Custom Range")
    
    choice = input("\nEnter your choice (1-5): ").strip()
    
    if choice == "1":
        start_id, end_id = 1, 100
    elif choice == "2":
        start_id, end_id = 1, 1000
    elif choice == "3":
        start_id, end_id = 1, 5000
    elif choice == "4":
        # Massive Run (auto-resume): continue from last scraped ID or start at 9000
        last = IslamQASpider.get_last_scraped_id("islamqa_fast.db")
        start_id = (last + 1) if last else 9000
        end_id = start_id + 9999
    elif choice == "5":
        try:
            start_id = int(input("Start ID: "))
            end_id = int(input("End ID: "))
        except ValueError:
            print("Invalid input, using default range 1-1000")
            start_id, end_id = 1, 1000
    else:
        print("Invalid choice, using quick test range 1-100")
        start_id, end_id = 1, 100
    
    print(f"\nConfiguration:")
    print(f"  - Range: {start_id} to {end_id}")
    # Show remaining estimate before starting
    try:
        last = IslamQASpider.get_last_scraped_id("islamqa_fast.db")
        remaining = IslamQASpider.estimate_remaining("islamqa_fast.db", end_id)
        print(f"  - Last scraped ID: {last}")
        print(f"  - Estimated remaining in window: ~{remaining:,}")
    except Exception:
        pass
    print(f"  - Speed: 24 concurrent requests")
    print(f"  - Delay: 0.05s between requests")
    print()
    
    # Run the scraper
    run_fast_scraper(start_id, end_id)
    
    # Export training data
    export_training_data()
    
    print("\n🎉 FAST SCRAPING COMPLETED!")
    print("Check the 'training_data' folder for your AI training files.")

if __name__ == "__main__":
    main()
