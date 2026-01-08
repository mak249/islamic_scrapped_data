import os
import sys
import subprocess
import json
from pathlib import Path

def download_audio(youtube_url, output_dir):
    """
    Downloads audio from a YouTube URL using yt-dlp.
    Optimized for high-quality audio extraction without video.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    output_template = os.path.join(output_dir, "%(id)s.%(ext)s")
    # Add ffmpeg to path
    FFMPEG_PATHS = [
        r"C:\Program Files\kdenlive\bin",
        r"C:\Program Files\DownloadHelper CoApp"
    ]
    for p in FFMPEG_PATHS:
        if os.path.exists(p) and p not in os.environ["PATH"]:
            os.environ["PATH"] += os.pathsep + p

    cmd = [
        sys.executable, "-m", "yt_dlp",
        "-x",  # Extract audio
        "--audio-format", "m4a", # m4a is generally faster and smaller for RAG
        "--audio-quality", "0", # Best quality
        "--print", "after_move:filepath", # Print the final path
        "-o", output_template,
        youtube_url
    ]
    
    print(f"🎬 Downloading audio from: {youtube_url}...")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        file_path = result.stdout.strip()
        print(f"✅ Downloaded: {file_path}")
        return file_path
    except subprocess.CalledProcessError as e:
        print(f"❌ Error downloading {youtube_url}: {e.stderr}")
        return None

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python downloader.py <YOUTUBE_URL>")
        sys.exit(1)
        
    target_url = sys.argv[1]
    base_dir = os.path.dirname(os.path.abspath(__file__))
    audio_dir = os.path.join(base_dir, "audio")
    
    download_audio(target_url, audio_dir)
