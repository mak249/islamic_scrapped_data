import os
import sys
import json
import argparse
from pathlib import Path

# Local imports
from downloader import download_audio
from transcriber import transcribe_audio
from cleaner import SpokenTextCleaner
from chunker import SemanticChunker
from validator import ChunkValidator
from db_handler import YouTubeRAGDB

class YouTubeRAGPipeline:
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.audio_dir = os.path.join(self.base_dir, "audio")
        self.transcripts_dir = os.path.join(self.base_dir, "transcripts")
        self.output_dir = os.path.join(self.base_dir, "output")
        self.db_path = os.path.join(self.base_dir, "db", "youtube_rag.db")
        self.prompts_dir = os.path.join(self.base_dir, "prompts")
        
        self.db = YouTubeRAGDB(self.db_path)
        self.validator = ChunkValidator()
        self.chunker = SemanticChunker()

    def load_prompt(self, stage_name):
        path = os.path.join(self.prompts_dir, f"{stage_name}.md")
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                return f.read()
        return f"Prompt for {stage_name} not found."

    def process_url(self, url, content_type="lecture", model_name="small"):
        """
        Stream-Aware Pipeline Execution
        """
        video_id = url.split("v=")[-1].split("/")[-1].split("?")[0]
        self.db.add_video(video_id, url, title=content_type)
        
        current_status, last_err = self.db.get_video_status(video_id)
        print(f"\n🏷️  PIPELINE IDENTITY: {video_id}")
        
        # Internal state for the callback
        self._current_video_id = video_id
        self._current_content_type = content_type
        self._cleaner = SpokenTextCleaner(mode=content_type)
        
        try:
            # 1. DOWNLOAD
            if current_status is None or current_status == "FAILED_AT_download":
                print(f"📦 STAGE 1: DOWNLOAD")
                audio_path = download_audio(url, self.audio_dir)
                if not audio_path: raise Exception("Download failed.")
                self.db.update_video_status(video_id, "download")
                current_status = "download"
            else:
                print("✅ STAGE 1: DOWNLOAD (Skipped)")
                audio_path = os.path.join(self.audio_dir, f"{video_id}.m4a")

            # 2. STREAMING TRANSCRIPTION & PROCESSING
            if current_status != "complete":
                print(f"📝 STAGE 2-5: STREAMING TRANSCRIPTION & PROCESSING (Model: {model_name})")
                
                # Check for existing progress to resume from the last successful second
                last_offset = self.db.get_last_chunk_end_time(video_id)
                if last_offset > 0:
                    print(f"🔄 RESUMING FROM CHECKPOINT: {last_offset:.2f} seconds")
                
                # This call will block until all segments are done, 
                # but will execute our callback for each segment.
                # We pass the last_offset to skip already processed segments.
                transcribe_audio(audio_path, model_name, partial_callback=self._on_segment_ready, start_offset=last_offset)
                
                # Final Export (Text and JSON) from DB
                self._final_export(video_id, url, content_type)
                self.db.update_video_status(video_id, "complete")
                print(f"✨ SUCCESS: Fully Processed {video_id}.")
            else:
                print(f"✅ PIPELINE ALREADY COMPLETE: {video_id}")
            
            return True

        except Exception as e:
            self.db.update_video_status(video_id, f"FAILED_AT_processing", error=str(e))
            print(f"❌ CRITICAL ERROR: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _on_segment_ready(self, raw_segments, is_final):
        """
        Callback executed for every ~60s of audio.
        Handles Clean -> Chunk -> Validate -> Persist.
        """
        if not raw_segments: return
        
        print(f"  🧹 Processing {len(raw_segments)} raw segments...")
        # 3. CLEAN
        cleaned_segments = self._cleaner.filter_noise(raw_segments)
        if not cleaned_segments: return
        
        # 4. CHUNK & VALIDATE
        semantic_chunks = self.chunker.chunk_segments(cleaned_segments)
        valid_chunks, rejected = self.validator.filter_chunks(semantic_chunks)
        
        # 5. PERSIST
        if valid_chunks:
            self.db.add_chunks(self._current_video_id, valid_chunks)
            print(f"  ✅ Saved {len(valid_chunks)} chunks to DB. ({rejected} rejected)")

    def _final_export(self, video_id, url, content_type):
        """Generates the final files from the database."""
        valid_chunks = self.db.get_chunks(video_id)
        
        export_path_txt = os.path.join(self.output_dir, f"{video_id}_rag.txt")
        export_path_json = os.path.join(self.output_dir, f"{video_id}_dataset.json")
        
        with open(export_path_txt, "w", encoding="utf-8") as f:
            f.write(f"SOURCE URL: {url}\n")
            f.write(f"CONTENT TYPE: {content_type.upper()}\n")
            f.write(f"TOTAL CHUNKS: {len(valid_chunks)}\n")
            f.write("="*50 + "\n\n")
            for i, chunk in enumerate(valid_chunks):
                f.write(f"CHUNK_{video_id}_{i+1} [{chunk['start']:.2f}s - {chunk['end']:.2f}s]\n")
                f.write(f"{chunk['text']}\n\n")
        
        dataset = {
            "video_id": video_id, 
            "url": url, 
            "type": content_type, 
            "chunks": valid_chunks
        }
        with open(export_path_json, "w", encoding="utf-8") as f:
            json.dump(dataset, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Exported final results to {self.output_dir}")

def main():
    parser = argparse.ArgumentParser(description="YouTube Podcast RAG Pipeline")
    parser.add_argument("url", help="YouTube video URL")
    parser.add_argument("--type", choices=["debate", "lecture"], default="lecture", help="Content category")
    parser.add_argument("--model", default="base", help="Whisper model name")
    
    args = parser.parse_args()
    
    pipeline = YouTubeRAGPipeline()
    pipeline.process_url(args.url, content_type=args.type, model_name=args.model)

if __name__ == "__main__":
    main()
