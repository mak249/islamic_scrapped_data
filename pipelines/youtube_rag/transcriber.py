import os
import sys
import whisper
import json
from pathlib import Path

# Add ffmpeg to path
# We found it in these locations:
# C:\Program Files\kdenlive\bin\ffmpeg.exe
# C:\Program Files\DownloadHelper CoApp\ffmpeg.exe
FFMPEG_PATHS = [
    r"C:\Program Files\kdenlive\bin",
    r"C:\Program Files\DownloadHelper CoApp"
]
for p in FFMPEG_PATHS:
    if os.path.exists(p) and p not in os.environ["PATH"]:
        os.environ["PATH"] += os.pathsep + p

def segment_audio(audio_path, segment_length=60):
    """
    Splits audio into chunks using ffmpeg.
    Returns a list of (segment_path, start_time) tuples.
    """
    import subprocess
    from pathlib import Path
    
    base_name = Path(audio_path).stem
    temp_dir = Path(audio_path).parent / f"{base_name}_segments"
    temp_dir.mkdir(exist_ok=True)
    
    print(f"✂️ Segmenting {audio_path} into {segment_length}s chunks...")
    
    output_pattern = str(temp_dir / "seg_%04d.m4a")
    cmd = [
        "ffmpeg", "-y", "-i", str(audio_path),
        "-f", "segment", "-segment_time", str(segment_length),
        "-c", "copy", output_pattern
    ]
    
    try:
        subprocess.run(cmd, check=True, capture_output=True)
    except Exception as e:
        print(f"❌ Segmenting failed: {e}")
        return []
    
    segments = sorted(list(temp_dir.glob("seg_*.m4a")))
    return [(str(s), i * segment_length) for i, s in enumerate(segments)]

def transcribe_audio(audio_path, model_name="small", partial_callback=None, start_offset=0.0):
    """
    Transcribes audio using OpenAI Whisper.
    Supports segment-based transcription and resumption from start_offset.
    """
    print(f"🎙️ Loading Whisper ({model_name}) on CPU...")
    model = whisper.load_model(model_name)
    
    # If no callback, we do full-file (classic)
    if not partial_callback:
        print(f"📄 Transcribing full file: {audio_path}")
        result = model.transcribe(audio_path, verbose=False, fp16=False)
        return result.get("segments", [])
    
    # Segment-based (Streaming style)
    segments_info = segment_audio(audio_path)
    all_segments = []
    
    # Filter segments that end before our start_offset
    # Each segment is approx 60s
    segment_length = 60 
    
    for i, (seg_path, offset) in enumerate(segments_info):
        # If this segment ends before our last successful offset, skip it
        if offset + segment_length <= start_offset:
            print(f"⏭️  Skipping segment {i+1} (already processed)")
            try: os.remove(seg_path)
            except: pass
            continue
            
        print(f"⏳ Processing segment {i+1}/{len(segments_info)} (Offset: {offset}s)...")
        result = model.transcribe(seg_path, verbose=False, fp16=False)
        
        # Adjust timestamps relative to original audio
        seg_results = result.get("segments", [])
        for sr in seg_results:
            sr["start"] += offset
            sr["end"] += offset
            all_segments.append(sr)
            
        # Immediately notify runner for cleaning/chunking
        partial_callback(seg_results, i == len(segments_info) - 1)
            
        # Cleanup segment file
        try: os.remove(seg_path)
        except: pass
        
    return all_segments

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python transcriber.py <AUDIO_PATH> [model_name]")
        sys.exit(1)
        
    audio_file = sys.argv[1]
    model = sys.argv[2] if len(sys.argv) > 2 else "small"
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    transcripts_dir = os.path.join(base_dir, "transcripts")
    os.makedirs(transcripts_dir, exist_ok=True)
    
    segments = transcribe_audio(audio_file, model)
    if segments:
        file_id = Path(audio_file).stem
        output_path = os.path.join(transcripts_dir, f"{file_id}.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(segments, f, indent=2, ensure_ascii=False)
        print(f"💾 Saved raw transcript to {output_path}")
