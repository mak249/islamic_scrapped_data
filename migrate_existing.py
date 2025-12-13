"""
Migration script to import existing islamqa data into unified database.
Migrates data from fast_scraper.py databases to the new unified schema.
"""

import sqlite3
import sys
from pathlib import Path
from datetime import datetime
import logging

from scrapers.storage import UnifiedStorage
from utils.deduplication import compute_content_hash
from utils.text_cleaner import clean_text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def migrate_islamqa_db(old_db_path: str, new_storage: UnifiedStorage):
    """
    Migrate data from old islamqa database to unified storage.
    
    Args:
        old_db_path: Path to old database (e.g., islamqa_fast.db)
        new_storage: UnifiedStorage instance
    """
    if not Path(old_db_path).exists():
        logger.error(f"Old database not found: {old_db_path}")
        return 0
    
    logger.info(f"Migrating data from {old_db_path}...")
    
    old_conn = sqlite3.connect(old_db_path)
    old_cursor = old_conn.cursor()
    
    # Get all records
    try:
        old_cursor.execute('SELECT * FROM qa_pairs ORDER BY created_at')
        rows = old_cursor.fetchall()
    except sqlite3.OperationalError as e:
        logger.error(f"Error reading old database: {e}")
        old_conn.close()
        return 0
    
    # Get column names (assuming standard schema)
    columns = [desc[0] for desc in old_cursor.description]
    logger.info(f"Found {len(rows)} records to migrate")
    
    migrated_count = 0
    skipped_count = 0
    
    for row in rows:
        try:
            # Map old schema to new schema
            row_dict = dict(zip(columns, row))
            
            # Extract fields
            old_id = row_dict.get('id', '')
            url = row_dict.get('url', '')
            question = row_dict.get('question', '')
            answer = row_dict.get('answer', '')
            fatwa_number = row_dict.get('fatwa_number', '')
            language = row_dict.get('language', 'english')
            word_count = row_dict.get('word_count', 0)
            quality_score = row_dict.get('quality_score', 0.0)
            scraped_at = row_dict.get('scraped_at', datetime.now().isoformat())
            
            # Skip if missing essential data
            if not url or not question or not answer:
                skipped_count += 1
                continue
            
            # Clean text
            question_clean = clean_text(question)
            answer_clean = clean_text(answer)
            
            if not question_clean or not answer_clean:
                skipped_count += 1
                continue
            
            # Combine question and answer for content
            content = f"Question: {question_clean}\n\nAnswer: {answer_clean}"
            
            # Compute content hash
            content_hash = compute_content_hash(question_clean, answer_clean)
            
            # Extract ID from URL if possible
            import re
            url_id_match = re.search(r'/answers/(\d+)', url)
            url_id = url_id_match.group(1) if url_id_match else None
            
            # Prepare data for new schema
            new_data = {
                'id': f"islamqa_migrated_{old_id}",
                'source': 'islamqa',
                'url': url,
                'title': question_clean,
                'content': content,
                'content_type': 'q&a',
                'metadata': {
                    'fatwa_number': fatwa_number,
                    'question_id': url_id,
                    'question': question_clean,
                    'answer': answer_clean,
                    'word_count': word_count,
                    'quality_score': quality_score,
                    'migrated_from': old_db_path,
                    'original_id': old_id
                },
                'language': language or 'english',
                'retrieved_at': scraped_at if scraped_at else datetime.now().isoformat(),
                'content_hash': content_hash
            }
            
            # Save to new storage (handles deduplication)
            if new_storage.save_content(new_data):
                migrated_count += 1
                if migrated_count % 100 == 0:
                    logger.info(f"Migrated {migrated_count} records...")
            else:
                skipped_count += 1
                
        except Exception as e:
            logger.error(f"Error migrating record {row_dict.get('id', 'unknown')}: {e}")
            skipped_count += 1
    
    old_conn.close()
    
    logger.info(f"Migration complete: {migrated_count} migrated, {skipped_count} skipped")
    
    # Update resume state
    if migrated_count > 0:
        # Find max ID from migrated URLs
        max_id = 0
        import re
        for url in [row_dict.get('url', '') for row_dict in [dict(zip(columns, r)) for r in rows]]:
            match = re.search(r'/answers/(\d+)', url)
            if match:
                max_id = max(max_id, int(match.group(1)))
        
        if max_id > 0:
            new_storage.update_resume_state(
                'islamqa',
                last_id=max_id,
                status='completed'
            )
    
    return migrated_count


def main():
    """Main migration function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrate existing islamqa data to unified database')
    parser.add_argument('--old-db', type=str, default='islamqa_fast.db',
                       help='Path to old database file')
    parser.add_argument('--new-db', type=str, default='islamic_data.db',
                       help='Path to new unified database file')
    parser.add_argument('--all-dbs', action='store_true',
                       help='Migrate from all found databases')
    
    args = parser.parse_args()
    
    # Initialize new storage
    new_storage = UnifiedStorage(args.new_db)
    
    if args.all_dbs:
        # Find all islamqa databases
        db_patterns = ['islamqa*.db', 'islamqa_*.db']
        db_files = []
        for pattern in db_patterns:
            db_files.extend(Path('.').glob(pattern))
        
        logger.info(f"Found {len(db_files)} database files to migrate")
        
        total_migrated = 0
        for db_file in db_files:
            logger.info(f"\nMigrating from {db_file}...")
            migrated = migrate_islamqa_db(str(db_file), new_storage)
            total_migrated += migrated
        
        logger.info(f"\nTotal migrated: {total_migrated} records")
    else:
        # Migrate single database
        migrate_islamqa_db(args.old_db, new_storage)
    
    # Print stats
    stats = new_storage.get_stats()
    logger.info("\nMigration Statistics:")
    logger.info(f"Total records: {stats['total']}")
    logger.info(f"By source: {stats['by_source']}")
    logger.info(f"By content type: {stats['by_content_type']}")
    logger.info(f"By language: {stats['by_language']}")


if __name__ == "__main__":
    main()

