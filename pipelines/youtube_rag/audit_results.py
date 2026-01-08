import sqlite3
import os
import json

base_dir = r"d:\web scraping\pipelines\youtube_rag"
db_path = os.path.join(base_dir, "db", "youtube_rag.db")
output_dir = os.path.join(base_dir, "output")
video_id = "Ir7utAdXYNg"

def audit():
    if not os.path.exists(db_path):
        print(f"❌ DB not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # 0. Video Status
    c.execute("SELECT current_stage, last_error FROM videos WHERE video_id = ?", (video_id,))
    status = c.fetchone()
    print(f"🎬 Current Stage: {status['current_stage'] if status else 'N/A'}")
    print(f"⚠️ Last Error: {status['last_error'] if status else 'None'}")

    # 1. Total Chunks
    c.execute("SELECT COUNT(*) as count FROM chunks WHERE video_id = ?", (video_id,))
    count = c.fetchone()['count']
    print(f"📊 Total Chunks in DB: {count}")

    # 1b. Latest Timestamps & Coverage
    c.execute("SELECT created_at, start_time FROM chunks WHERE video_id = ? ORDER BY id ASC LIMIT 1", (video_id,))
    first = c.fetchone()
    c.execute("SELECT created_at, start_time FROM chunks WHERE video_id = ? ORDER BY id DESC LIMIT 1", (video_id,))
    last = c.fetchone()
    
    if first and last:
        print(f"🕒 Time Span in DB: {first['created_at']} to {last['created_at']}")
        print(f"📍 Audio Coverage: {first['start_time']:.2f}s to {last['start_time']:.2f}s")

    # 2. Sample Content
    c.execute("SELECT text, start_time, end_time FROM chunks WHERE video_id = ? LIMIT 20", (video_id,))
    rows = c.fetchall()
    print("\n📝 Sample Text Quality Audit:")
    for i, row in enumerate(rows):
        text = row['text']
        # Check for non-latin characters as proxy for language issues
        has_arabic = any('\u0600' <= char <= '\u06FF' for char in text)
        print(f"\n--- Chunk {i+1} [{row['start_time']:.2f} - {row['end_time']:.2f}] (Arabic chars detected: {has_arabic}) ---")
        print(text[:800] + "...")

    # 3. Export Check
    rag_txt = os.path.join(output_dir, f"{video_id}_rag.txt")
    if os.path.exists(rag_txt):
        print(f"\n✅ Export file exists: {rag_txt} ({os.path.getsize(rag_txt)} bytes)")
        mtime = os.path.getmtime(rag_txt)
        from datetime import datetime
        print(f"🕒 Last Modified: {datetime.fromtimestamp(mtime)}")
    else:
        print(f"\n❌ Export file missing: {rag_txt}")

    conn.close()

if __name__ == "__main__":
    audit()
