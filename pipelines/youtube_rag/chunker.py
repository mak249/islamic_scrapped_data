import json
import os

class SemanticChunker:
    def __init__(self, max_words=150, min_words=50):
        self.max_words = max_words
        self.min_words = min_words

    def chunk_segments(self, segments):
        """
        Chunks segments by idea/topic.
        Since we don't have a transformer-based topic detector here, 
        we use sentence-boundary and word-count heuristics to group segments.
        One chunk should ideally contain one cohesive thought.
        """
        chunks = []
        current_chunk = {
            "text": "",
            "start": segments[0]['start'] if segments else 0,
            "end": 0,
            "metadata": {"segment_count": 0}
        }
        
        word_count = 0
        for seg in segments:
            text = seg['text']
            word_count += len(text.split())
            
            if not current_chunk["text"]:
                current_chunk["start"] = seg['start']
                current_chunk["text"] = text
            else:
                current_chunk["text"] += " " + text
            
            current_chunk["end"] = seg['end']
            current_chunk["metadata"]["segment_count"] += 1
            
            # If chunk is large enough and seems to end a thought (ending in . ? !)
            if word_count >= self.min_words:
                if text.endswith(('.', '?', '!')) or word_count >= self.max_words:
                    chunks.append(current_chunk)
                    current_chunk = {
                        "text": "",
                        "start": 0,
                        "end": 0,
                        "metadata": {"segment_count": 0}
                    }
                    word_count = 0
        
        # Add trailing
        if current_chunk["text"]:
            chunks.append(current_chunk)
            
        return chunks

if __name__ == "__main__":
    # Test logic
    pass
