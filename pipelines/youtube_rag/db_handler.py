import sqlite3
import os
import json
from datetime import datetime

class YouTubeRAGDB:
    def __init__(self, db_path):
        self.db_path = db_path
        self._init_db()
        
    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS videos (
                video_id TEXT PRIMARY KEY,
                url TEXT UNIQUE,
                title TEXT,
                processed_at TEXT,
                current_stage TEXT DEFAULT 'intake',
                last_error TEXT
            )
        ''')
        # Migration: Check if columns exist (for existing DBs)
        c.execute("PRAGMA table_info(videos)")
        cols = [col[1] for col in c.fetchall()]
        if 'current_stage' not in cols:
            c.execute("ALTER TABLE videos ADD COLUMN current_stage TEXT DEFAULT 'intake'")
        if 'last_error' not in cols:
            c.execute("ALTER TABLE videos ADD COLUMN last_error TEXT")

        c.execute('''
            CREATE TABLE IF NOT EXISTS chunks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id TEXT,
                text TEXT,
                start_time REAL,
                end_time REAL,
                speaker TEXT,
                metadata TEXT,
                created_at TEXT,
                FOREIGN KEY(video_id) REFERENCES videos(video_id)
            )
        ''')
        conn.commit()
        conn.close()

    def add_video(self, video_id, url, title=None):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('INSERT OR IGNORE INTO videos (video_id, url, title, processed_at) VALUES (?, ?, ?, ?)',
                  (video_id, url, title, datetime.now().isoformat()))
        conn.commit()
        conn.close()

    def update_video_status(self, video_id, stage, error=None):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('UPDATE videos SET current_stage = ?, last_error = ? WHERE video_id = ?',
                  (stage, error, video_id))
        conn.commit()
        conn.close()

    def get_video_status(self, video_id):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT current_stage, last_error FROM videos WHERE video_id = ?', (video_id,))
        row = c.fetchone()
        conn.close()
        return row if row else (None, None)

    def add_chunks(self, video_id, chunks):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        for chunk in chunks:
            c.execute('''
                INSERT INTO chunks (video_id, text, start_time, end_time, speaker, metadata, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                video_id,
                chunk['text'],
                chunk.get('start'),
                chunk.get('end'),
                chunk.get('speaker'),
                json.dumps(chunk.get('metadata', {})),
                datetime.now().isoformat()
            ))
        conn.commit()
        conn.close()

    def get_chunks(self, video_id):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute('SELECT text, start_time as start, end_time as end, speaker, metadata FROM chunks WHERE video_id = ? ORDER BY start_time', (video_id,))
        rows = [dict(row) for row in c.fetchall()]
        for r in rows:
            r['metadata'] = json.loads(r['metadata'])
        conn.close()
        return rows

    def get_last_chunk_end_time(self, video_id):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT MAX(end_time) FROM chunks WHERE video_id = ?', (video_id,))
        row = c.fetchone()
        conn.close()
        return row[0] if row and row[0] is not None else 0.0

    def delete_chunks(self, video_id):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('DELETE FROM chunks WHERE video_id = ?', (video_id,))
        conn.commit()
        conn.close()

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(base_dir, "db", "youtube_rag.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    db = YouTubeRAGDB(db_path)
    print(f"✅ Database initialized at {db_path}")
