#!/usr/bin/env python3
"""
Islamic Dataset Re-Implementation & Separation Pipeline
========================================================
Separates IslamQA and Sunnah data, cleans for RAG-readiness.
NO scraping - operates on already-scraped data only.
"""

import sqlite3
import json
import hashlib
import re
import html
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Set, Optional, Generator
from bs4 import BeautifulSoup


class DataSeparator:
    """Separates and cleans Islamic data for RAG."""
    
    def __init__(self, output_dir: str = "clean_data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        self.stats = {
            "total_records": 0,
            "islamqa_records": 0,
            "sunnah_records": 0,
            "unclassified": 0,
            "duplicates_removed": 0,
            "html_cleaned": 0,
            "processing_time_seconds": 0
        }
        
        self.seen_hashes: Set[str] = set()
    
    # =========================================================================
    # CLEANING FUNCTIONS (RAG-SAFE)
    # =========================================================================
    
    @staticmethod
    def clean_text(text: str) -> str:
        """Clean text without altering meaning."""
        if not text:
            return ""
        
        # Decode HTML entities
        text = html.unescape(text)
        
        # Remove HTML tags if present
        if '<' in text and '>' in text:
            soup = BeautifulSoup(text, 'html.parser')
            # Remove script, style, nav, footer, etc.
            for tag in soup(["script", "style", "nav", "aside", "footer", "header", "dialog", "button", "form"]):
                tag.decompose()
            text = soup.get_text(separator=' ', strip=True)
        
        # Normalize whitespace (preserve single newlines for paragraph structure)
        text = re.sub(r'[ \t]+', ' ', text)  # Multiple spaces/tabs to single space
        text = re.sub(r'\n{3,}', '\n\n', text)  # Max 2 newlines
        text = text.strip()
        
        return text
    
    @staticmethod
    def contains_html(text: str) -> bool:
        """Check if text contains HTML tags."""
        if not text:
            return False
        return bool(re.search(r'<[a-zA-Z][^>]*>', text))
    
    @staticmethod
    def compute_hash(text: str) -> str:
        """Compute normalized text hash for deduplication."""
        if not text:
            return ""
        # Normalize for hashing
        normalized = re.sub(r'\s+', ' ', text.lower().strip())
        return hashlib.sha256(normalized.encode('utf-8')).hexdigest()[:16]
    
    # =========================================================================
    # SOURCE DETECTION (NO GUESSING)
    # =========================================================================
    
    @staticmethod
    def detect_source(record: Dict[str, Any]) -> str:
        """Detect source based on URL and structural patterns."""
        url = record.get('url', '').lower()
        source = record.get('source', '').lower()
        
        # IslamQA detection
        if 'islamqa.info' in url or source == 'islamqa':
            return 'islamqa'
        if '/answers/' in url and 'islamqa' in url:
            return 'islamqa'
        
        # Sunnah.com detection
        if 'sunnah.com' in url or source == 'sunnah':
            return 'sunnah'
        
        # Structural detection
        content = record.get('content', '') or ''
        
        # Q&A pattern suggests IslamQA
        if record.get('question') and record.get('answer'):
            if 'fatwa' in content.lower() or 'ruling' in content.lower():
                return 'islamqa'
        
        # Hadith pattern suggests Sunnah
        if record.get('arabic_text') and record.get('english_text'):
            return 'sunnah'
        if 'narrated' in content.lower() and ('bukhari' in content.lower() or 'muslim' in content.lower()):
            return 'sunnah'
        
        return 'unclassified'
    
    # =========================================================================
    # SCHEMA TRANSFORMATION
    # =========================================================================
    
    def transform_islamqa(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform to IslamQA schema."""
        # Extract question and answer
        question = record.get('question', '')
        answer = record.get('answer', '')
        
        # Try to extract from content if not present
        if not question or not answer:
            content = record.get('content', '')
            if 'Question:' in content and 'Answer:' in content:
                parts = content.split('Answer:', 1)
                question = parts[0].replace('Question:', '').strip()
                answer = parts[1].strip() if len(parts) > 1 else ''
        
        if not question or not answer:
            return None
        
        # Clean
        question = self.clean_text(question)
        answer = self.clean_text(answer)
        
        if self.contains_html(question) or self.contains_html(answer):
            self.stats["html_cleaned"] += 1
            question = self.clean_text(question)
            answer = self.clean_text(answer)
        
        # Quality check - reject very short answers
        if len(answer.split()) < 10:
            return None
        
        # Deduplication
        content_hash = self.compute_hash(f"{question}{answer}")
        if content_hash in self.seen_hashes:
            self.stats["duplicates_removed"] += 1
            return None
        self.seen_hashes.add(content_hash)
        
        # Detect language
        has_arabic = bool(re.search(r'[\u0600-\u06FF]', f"{question}{answer}"))
        language = 'arabic' if has_arabic and not re.search(r'[a-zA-Z]{3,}', answer) else 'english'
        
        # Extract ID from URL
        url = record.get('url', '')
        id_match = re.search(r'/answers/(\d+)', url)
        record_id = f"islamqa_{id_match.group(1)}" if id_match else f"islamqa_{record.get('id', 'unknown')}"
        
        return {
            "id": record_id,
            "source": "islamqa",
            "language": language,
            "question": question,
            "answer": answer,
            "url": url,
            "notes": "cleaned_only"
        }
    
    def transform_sunnah(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform to Sunnah schema."""
        arabic_text = self.clean_text(record.get('arabic_text', '') or record.get('arabic', ''))
        english_text = self.clean_text(record.get('english_text', '') or record.get('english', ''))
        
        # Try to extract from content
        if not arabic_text and not english_text:
            content = record.get('content', '')
            if 'Arabic:' in content:
                parts = content.split('Arabic:', 1)
                if len(parts) > 1:
                    arabic_part = parts[1].split('English:')[0] if 'English:' in parts[1] else parts[1]
                    arabic_text = self.clean_text(arabic_part)
            if 'English:' in content:
                english_text = self.clean_text(content.split('English:', 1)[1])
        
        if not arabic_text and not english_text:
            return None
        
        # Deduplication
        content_hash = self.compute_hash(f"{arabic_text}{english_text}")
        if content_hash in self.seen_hashes:
            self.stats["duplicates_removed"] += 1
            return None
        self.seen_hashes.add(content_hash)
        
        # Extract metadata
        metadata = record.get('metadata', {})
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except:
                metadata = {}
        
        book = metadata.get('collection', '') or metadata.get('book', '')
        hadith_number = metadata.get('hadith_number', '') or metadata.get('reference', '')
        url = record.get('url', '')
        
        # Build ID
        record_id = f"sunnah_{book}_{hadith_number}" if book and hadith_number else f"sunnah_{record.get('id', 'unknown')}"
        
        languages = []
        if arabic_text:
            languages.append('arabic')
        if english_text:
            languages.append('english')
        
        return {
            "id": record_id,
            "source": "sunnah",
            "language": languages,
            "arabic_text": arabic_text,
            "english_text": english_text,
            "book": book,
            "hadith_number": str(hadith_number),
            "url": url
        }
    
    # =========================================================================
    # BATCH PROCESSING (PERFORMANCE)
    # =========================================================================
    
    def stream_records_from_db(self, db_path: str) -> Generator[Dict[str, Any], None, None]:
        """Stream records from database for memory efficiency."""
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Check what tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [t[0] for t in cursor.fetchall()]
        
        # Stream from content table (unified storage)
        if 'content' in tables:
            cursor.execute('SELECT * FROM content')
            for row in cursor:
                yield dict(row)
        
        # Stream from qa_pairs table (fast_scraper format)
        if 'qa_pairs' in tables:
            cursor.execute('SELECT * FROM qa_pairs')
            for row in cursor:
                yield dict(row)
        
        conn.close()
    
    def process_database(self, db_path: str):
        """Process a single database file."""
        print(f"📂 Processing: {db_path}")
        
        for record in self.stream_records_from_db(db_path):
            self.stats["total_records"] += 1
            
            source = self.detect_source(record)
            
            if source == 'islamqa':
                transformed = self.transform_islamqa(record)
                if transformed:
                    yield ('islamqa', transformed)
                    self.stats["islamqa_records"] += 1
            
            elif source == 'sunnah':
                transformed = self.transform_sunnah(record)
                if transformed:
                    yield ('sunnah', transformed)
                    self.stats["sunnah_records"] += 1
            
            else:
                yield ('unclassified', record)
                self.stats["unclassified"] += 1
    
    # =========================================================================
    # OUTPUT GENERATION
    # =========================================================================
    
    def run(self, db_paths: List[str]):
        """Main processing pipeline."""
        start_time = datetime.now()
        
        # Open output files for streaming writes
        islamqa_file = open(self.output_dir / "islamqa_dataset.jsonl", 'w', encoding='utf-8')
        sunnah_file = open(self.output_dir / "sunnah_dataset.jsonl", 'w', encoding='utf-8')
        unclassified_file = open(self.output_dir / "unclassified.jsonl", 'w', encoding='utf-8')
        
        try:
            for db_path in db_paths:
                if not os.path.exists(db_path):
                    print(f"⚠️  Skipping (not found): {db_path}")
                    continue
                
                for source_type, record in self.process_database(db_path):
                    if source_type == 'islamqa':
                        islamqa_file.write(json.dumps(record, ensure_ascii=False) + '\n')
                    elif source_type == 'sunnah':
                        sunnah_file.write(json.dumps(record, ensure_ascii=False) + '\n')
                    else:
                        unclassified_file.write(json.dumps(record, ensure_ascii=False) + '\n')
        
        finally:
            islamqa_file.close()
            sunnah_file.close()
            unclassified_file.close()
        
        # Calculate processing time
        self.stats["processing_time_seconds"] = (datetime.now() - start_time).total_seconds()
        
        # Write processing report
        with open(self.output_dir / "processing_report.json", 'w', encoding='utf-8') as f:
            json.dump(self.stats, f, indent=2)
        
        # Generate additional formats (RAG-ready)
        self._generate_additional_formats()
        
        # Print summary
        self._print_summary()
    
    def _generate_additional_formats(self):
        """Generate additional RAG-ready formats."""
        
        # Generate IslamQA formats
        islamqa_path = self.output_dir / "islamqa_dataset.jsonl"
        if islamqa_path.exists():
            records = []
            with open(islamqa_path, 'r', encoding='utf-8') as f:
                for line in f:
                    records.append(json.loads(line))
            
            if records:
                # ChatGPT format
                with open(self.output_dir / "islamqa_chatgpt.jsonl", 'w', encoding='utf-8') as f:
                    for r in records:
                        entry = {
                            "messages": [
                                {"role": "user", "content": r['question']},
                                {"role": "assistant", "content": r['answer']}
                            ],
                            "metadata": {"id": r['id'], "url": r['url'], "language": r['language']}
                        }
                        f.write(json.dumps(entry, ensure_ascii=False) + '\n')
                
                # LLaMA/Alpaca format
                with open(self.output_dir / "islamqa_alpaca.json", 'w', encoding='utf-8') as f:
                    alpaca_data = [
                        {
                            "instruction": f"Answer this Islamic question: {r['question']}",
                            "input": "",
                            "output": r['answer'],
                            "language": r['language']
                        }
                        for r in records
                    ]
                    json.dump(alpaca_data, f, ensure_ascii=False, indent=2)
                
                # RAG chunks format
                with open(self.output_dir / "islamqa_rag_chunks.jsonl", 'w', encoding='utf-8') as f:
                    for r in records:
                        chunk = {
                            "id": r['id'],
                            "content": f"Question: {r['question']}\n\nAnswer: {r['answer']}",
                            "metadata": {
                                "source": "islamqa",
                                "url": r['url'],
                                "language": r['language'],
                                "type": "qa"
                            }
                        }
                        f.write(json.dumps(chunk, ensure_ascii=False) + '\n')
    
    def _print_summary(self):
        """Print processing summary."""
        print("\n" + "=" * 60)
        print("📊 PROCESSING COMPLETE")
        print("=" * 60)
        print(f"Total records processed: {self.stats['total_records']:,}")
        print(f"IslamQA records: {self.stats['islamqa_records']:,}")
        print(f"Sunnah records: {self.stats['sunnah_records']:,}")
        print(f"Unclassified: {self.stats['unclassified']:,}")
        print(f"Duplicates removed: {self.stats['duplicates_removed']:,}")
        print(f"HTML cleaned: {self.stats['html_cleaned']:,}")
        print(f"Processing time: {self.stats['processing_time_seconds']:.2f}s")
        print("\n📁 Output files in:", self.output_dir)
        print("=" * 60)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Separate and clean Islamic datasets for RAG',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all databases
  python clean_and_separate.py --all
  
  # Process specific database
  python clean_and_separate.py --db islamqa_fast.db
  
  # Custom output directory
  python clean_and_separate.py --all --output clean_rag_data
        """
    )
    
    parser.add_argument('--db', type=str, help='Specific database to process')
    parser.add_argument('--all', action='store_true', help='Process all .db files')
    parser.add_argument('--output', type=str, default='clean_data', help='Output directory')
    
    args = parser.parse_args()
    
    if args.all:
        db_files = [f for f in os.listdir('.') if f.endswith('.db')]
    elif args.db:
        db_files = [args.db]
    else:
        # Default: process main databases
        db_files = ['islamqa_fast.db', 'islamic_data.db']
    
    print("🕌 Islamic Dataset Separation Pipeline")
    print("=" * 60)
    print(f"Databases to process: {db_files}")
    print(f"Output directory: {args.output}")
    print()
    
    separator = DataSeparator(output_dir=args.output)
    separator.run(db_files)


if __name__ == "__main__":
    main()
