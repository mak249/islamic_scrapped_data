import sqlite3
import os
import shutil

base_dir = r"d:\web scraping\pipelines\youtube_rag"
db_path = os.path.join(base_dir, "db", "youtube_rag.db")
video_id = "Ir7utAdXYNg"

def purge():
    if not os.path.exists(db_path):
        print("DB not found.")
        return

    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    # Remove chunks
    c.execute("DELETE FROM chunks WHERE video_id = ?", (video_id,))
    # Remove video record
    c.execute("DELETE FROM videos WHERE video_id = ?", (video_id,))
    
    conn.commit()
    conn.close()
    print(f"✅ Purged records for {video_id} from DB.")

    # Remove temporary segments if they exist
    seg_dir = os.path.join(base_dir, "audio", f"{video_id}_segments")
    if os.path.exists(seg_dir):
        shutil.rmtree(seg_dir)
        print(f"✅ Cleaned up segment directory.")

if __name__ == "__main__":
    purge()
