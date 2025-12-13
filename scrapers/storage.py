"""
Unified storage system for all scraped Islamic data.
"""

import sqlite3
import json
from datetime import datetime
from typing import Dict, Any, List, Optional, Set
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class UnifiedStorage:
    """
    Unified database storage for all scraped content.
    Single table schema with source tracking.
    """
    
    def __init__(self, db_path: str = "islamic_data.db"):
        """
        Initialize unified storage.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize database schema."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Main content table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS content (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                url TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                content_type TEXT NOT NULL,
                metadata TEXT,
                language TEXT,
                retrieved_at TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes separately (SQLite doesn't support inline INDEX)
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_content_source ON content(source)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_content_type ON content(content_type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_content_hash ON content(content_hash)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_retrieved_at ON content(retrieved_at)')
        
        # Resume state table (tracks last scraped position per source)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS resume_state (
                source TEXT PRIMARY KEY,
                last_url TEXT,
                last_id INTEGER,
                last_scraped_at TEXT,
                status TEXT
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_resume_source ON resume_state(source)')
        
        # Visited URLs table for fast duplicate checking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS visited_urls (
                url TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                scraped_at TEXT
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_visited_source ON visited_urls(source)')
        
        conn.commit()
        conn.close()
        logger.info(f"Initialized database at {self.db_path}")
    
    def save_content(self, content_data: Dict[str, Any]) -> bool:
        """
        Save extracted content to database.
        
        Args:
            content_data: Dictionary with keys:
                - id: Unique identifier
                - source: Source name
                - url: Content URL
                - title: Content title
                - content: Content body
                - content_type: Type (q&a/hadith/article/metadata)
                - metadata: Optional dict of metadata
                - language: Optional language code
                - content_hash: SHA256 hash of normalized content
                - retrieved_at: ISO timestamp
                
        Returns:
            True if saved successfully, False if duplicate or error
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Check for duplicate by content_hash
            cursor.execute('SELECT id FROM content WHERE content_hash = ?', 
                          (content_data['content_hash'],))
            if cursor.fetchone():
                logger.debug(f"Duplicate content (hash {content_data['content_hash'][:8]}...), skipping")
                return False
            
            # Check for duplicate by URL
            cursor.execute('SELECT id FROM content WHERE url = ?', 
                          (content_data['url'],))
            if cursor.fetchone():
                logger.debug(f"Duplicate URL: {content_data['url']}, skipping")
                return False
            
            # Insert content
            cursor.execute('''
                INSERT INTO content 
                (id, source, url, title, content, content_type, metadata, 
                 language, retrieved_at, content_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                content_data['id'],
                content_data['source'],
                content_data['url'],
                content_data['title'],
                content_data['content'],
                content_data['content_type'],
                json.dumps(content_data.get('metadata', {})),
                content_data.get('language'),
                content_data['retrieved_at'],
                content_data['content_hash']
            ))
            
            # Track visited URL
            cursor.execute('''
                INSERT OR REPLACE INTO visited_urls (url, source, scraped_at)
                VALUES (?, ?, ?)
            ''', (
                content_data['url'],
                content_data['source'],
                content_data['retrieved_at']
            ))
            
            conn.commit()
            logger.debug(f"Saved content: {content_data['title'][:50]}...")
            return True
            
        except sqlite3.IntegrityError as e:
            logger.warning(f"Integrity error saving content: {e}")
            conn.rollback()
            return False
        except Exception as e:
            logger.error(f"Error saving content: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def is_url_visited(self, url: str) -> bool:
        """
        Check if URL has already been scraped.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL exists in database
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT 1 FROM visited_urls WHERE url = ?', (url,))
        result = cursor.fetchone() is not None
        
        conn.close()
        return result

    def get_visited_urls(self, source: Optional[str] = None) -> Set[str]:
        """
        Retrieve all visited URLs, optionally filtered by source.

        Args:
            source: Optional source name to filter URLs.

        Returns:
            Set of visited URL strings.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        if source:
            cursor.execute('SELECT url FROM visited_urls WHERE source = ?', (source,))
        else:
            cursor.execute('SELECT url FROM visited_urls')

        urls = {row[0] for row in cursor.fetchall()}

        conn.close()
        return urls
    
    def get_resume_state(self, source: str) -> Optional[Dict[str, Any]]:
        """
        Get resume state for a source.
        
        Args:
            source: Source name
            
        Returns:
            Dictionary with resume state or None
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT last_url, last_id, last_scraped_at, status
            FROM resume_state WHERE source = ?
        ''', (source,))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return {
                'last_url': row[0],
                'last_id': row[1],
                'last_scraped_at': row[2],
                'status': row[3]
            }
        return None
    
    def update_resume_state(self, source: str, last_url: Optional[str] = None,
                          last_id: Optional[int] = None, status: str = 'running'):
        """
        Update resume state for a source.
        
        Args:
            source: Source name
            last_url: Last scraped URL
            last_id: Last scraped ID (if applicable)
            status: Status ('running', 'completed', 'paused', 'error')
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO resume_state 
            (source, last_url, last_id, last_scraped_at, status)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            source,
            last_url,
            last_id,
            datetime.now().isoformat(),
            status
        ))
        
        conn.commit()
        conn.close()
    
    def query_content(self, source: Optional[str] = None,
                     content_type: Optional[str] = None,
                     language: Optional[str] = None,
                     limit: Optional[int] = None,
                     offset: int = 0) -> List[Dict[str, Any]]:
        """
        Query content with filters.
        
        Args:
            source: Filter by source name
            content_type: Filter by content type
            language: Filter by language
            limit: Maximum number of results
            offset: Offset for pagination
            
        Returns:
            List of content dictionaries
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Build query
        conditions = []
        params = []
        
        if source:
            conditions.append("source = ?")
            params.append(source)
        if content_type:
            conditions.append("content_type = ?")
            params.append(content_type)
        if language:
            conditions.append("language = ?")
            params.append(language)
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        query = f"SELECT * FROM content WHERE {where_clause} ORDER BY retrieved_at DESC"
        
        if limit:
            query += f" LIMIT {limit} OFFSET {offset}"
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Convert to dictionaries
        results = []
        for row in rows:
            content_dict = dict(zip(columns, row))
            # Parse metadata JSON
            if content_dict.get('metadata'):
                content_dict['metadata'] = json.loads(content_dict['metadata'])
            results.append(content_dict)
        
        conn.close()
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about stored content.
        
        Returns:
            Dictionary with statistics
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total count
        cursor.execute('SELECT COUNT(*) FROM content')
        total = cursor.fetchone()[0]
        
        # Count by source
        cursor.execute('''
            SELECT source, COUNT(*) as count 
            FROM content 
            GROUP BY source
        ''')
        by_source = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Count by content_type
        cursor.execute('''
            SELECT content_type, COUNT(*) as count 
            FROM content 
            GROUP BY content_type
        ''')
        by_type = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Count by language
        cursor.execute('''
            SELECT language, COUNT(*) as count 
            FROM content 
            WHERE language IS NOT NULL
            GROUP BY language
        ''')
        by_language = {row[0]: row[1] for row in cursor.fetchall()}
        
        conn.close()
        
        return {
            'total': total,
            'by_source': by_source,
            'by_content_type': by_type,
            'by_language': by_language
        }

