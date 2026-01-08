import os
import sqlite3
import json
from pathlib import Path

def get_status():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(base_dir, "db", "youtube_rag.db")
    audio_dir = os.path.join(base_dir, "audio")
    output_dir = os.path.join(base_dir, "output")

    # Ensure ffprobe is in path
    FFMPEG_PATHS = [r"C:\Program Files\kdenlive\bin", r"C:\Program Files\DownloadHelper CoApp"]
    for p in FFMPEG_PATHS:
        if os.path.exists(p) and p not in os.environ["PATH"]:
            os.environ["PATH"] += os.pathsep + p

    def get_duration(file_path):
        try:
            import subprocess
            cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", file_path]
            result = subprocess.run(cmd, capture_output=True, text=True)
            return float(result.stdout.strip())
        except:
            return None

    # 1. Check DB for processed videos
    processed_videos = {}
    
    # Use the DB handler to ensure schema migration is applied
    from db_handler import YouTubeRAGDB
    db_manager = YouTubeRAGDB(db_path)
    
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT video_id, url, title, processed_at, current_stage, last_error FROM videos")
        for row in c.fetchall():
            processed_videos[row['video_id']] = {
                "url": row['url'], 
                "type": row['title'] or "---", 
                "stage": (row['current_stage'] or "intake").upper(),
                "error": row['last_error'],
                "date": row['processed_at'],
                "chunks": 0
            }
            
        c.execute("SELECT video_id, COUNT(*) FROM chunks GROUP BY video_id")
        for vid, count in c.fetchall():
            if vid in processed_videos:
                processed_videos[vid]["chunks"] = count
        conn.close()

    # 2. Check Audio files for in-progress or downloaded
    audio_files = []
    if os.path.exists(audio_dir):
        audio_files = [f.stem for f in Path(audio_dir).glob("*.m4a")]

    print("\n📺 YOUTUBE RAG PIPELINE: CONTROL CENTER")
    print(f"{'VIDEO ID':<15} | {'TYPE':<10} | {'CHUNKS':<8} | {'RUNTIME':<10} | {'ETA REMAINING':<15} | {'STATUS / STAGE'}")
    print("-" * 105)

    # Combined View
    all_seen_ids = set(list(processed_videos.keys()) + audio_files)
    from datetime import datetime
    
    for vid in all_seen_ids:
        vtype = "---"
        chunks = 0
        runtime = "---"
        eta = "---"
        display_status = "🔄 PROCESSING"
        
        path = os.path.join(audio_dir, f"{vid}.m4a")
        duration = get_duration(path)

        if vid in processed_videos:
            vv = processed_videos[vid]
            vtype = vv["type"].upper()
            chunks = vv["chunks"]
            stage = vv["stage"]
            error = vv["error"]
            
            try:
                start_dt = datetime.fromisoformat(vv.get("date", ""))
                diff_sec = (datetime.now() - start_dt).total_seconds()
                hours, remainder = divmod(diff_sec, 3600)
                minutes, seconds = divmod(remainder, 60)
                runtime = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"
                
                if stage != "COMPLETE" and duration and not error:
                    # Optimized CPU factor (small model ~0.7x realtime under light load)
                    # But with user's background load, let's use 1.0x
                    total_est = duration * 1.0 
                    remaining = max(0, total_est - diff_sec)
                    rh, rm = divmod(remaining, 3600)
                    eta = f"~{int(rh):02}h {int(rm/60):02}m"
            except: pass

            if stage == "COMPLETE":
                display_status = "✅ COMPLETE"
                eta = "00:00:00"
            elif error:
                display_status = f"❌ FAILED @ {stage}"
                eta = "Check Logs"
            else:
                display_status = f"🔄 STAGE 2: STREAMING ({chunks} chunks saved)"
        else:
            if os.path.exists(path):
                display_status = "🔄 STAGE 2: TRANSCRIBING"
                ctime = os.path.getctime(path)
                diff_sec = (datetime.now() - datetime.fromtimestamp(ctime)).total_seconds()
                hours, remainder = divmod(diff_sec, 3600)
                runtime = f"{int(hours):02}:{int(remainder/60):02}"
                eta = "Estimating..."
            else:
                display_status = "⏳ STAGE 1: INTAKE (Downloading)"
                eta = "Waiting..."
            
        print(f"{vid:<15} | {vtype:<10} | {chunks:<8} | {runtime:<10} | {eta:<15} | {display_status}")

    print("=" * 105)
    print(f"Total Videos in Pipeline: {len(all_seen_ids)}")
    print(f"Output Directory: {output_dir}")

if __name__ == "__main__":
    get_status()
