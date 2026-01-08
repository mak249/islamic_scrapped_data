import os
import json
import re

class ChunkValidator:
    """
    Stage 4 Implementation: Chunk Validation
    Accepts/Rejects chunks based on factual and argumentative value.
    """
    def __init__(self, min_word_count=20):
        self.min_word_count = min_word_count

    def validate_chunk(self, chunk):
        """
        Stage 4 & 5 Implementation: RAG Quality Gate
        Enforces self-containment and filters out procedural noise.
        """
        text = chunk.get('text', '')
        words = text.split()
        word_count = len(words)

        # 1. Accept only if it expresses a complete idea (Length check)
        if word_count < self.min_word_count:
            return False, "Rejection: Chunk is too short (minimal signal)."

        # 2. Reject Repetition
        unique_words = set(words)
        if len(unique_words) / word_count < 0.4:
            return False, "Rejection: High redundancy/repetition."

        # 3. Reject Procedural / Meta-Commentary (e.g., logistics, mic checks)
        procedural_keywords = [
            r"can you hear me", r"check the mic", r"next slide", 
            r"move to the next", r"in this video", r"subscribe to my",
            r"welcome back", r"let's get started"
        ]
        text_lower = text.lower()
        for pattern in procedural_keywords:
            if re.search(pattern, text_lower):
                return False, f"Rejection: Procedural noise detected ('{pattern}')."

        # 4. Reject Meta-Conversational Noise (Heuristic)
        if re.match(r"^(so|but|and|actually|basically)\b", text_lower):
            if word_count < 30: # If it's short and starts with a filler-link, reject.
                return False, "Rejection: Conversational filler-start."

        # 5. Reject emotional/venting if it lacks factual substance
        if text.count('!') > 3 and word_count < 40:
            return False, "Rejection: Emotional outburst/noise."

        return True, "Accepted"

    def filter_chunks(self, chunks):
        """
        Filters a list of chunks, keeping only the valid ones.
        """
        valid_chunks = []
        rejected_count = 0
        
        for c in chunks:
            is_valid, reason = self.validate_chunk(c)
            if is_valid:
                valid_chunks.append(c)
            else:
                rejected_count += 1
                # print(f"DEBUG: {reason}")
                
        return valid_chunks, rejected_count

if __name__ == "__main__":
    validator = ChunkValidator()
    test_chunk = {"text": "This is a great point about the nature of evidence in theological debates. It must be verifiable."}
    print(validator.validate_chunk(test_chunk))
