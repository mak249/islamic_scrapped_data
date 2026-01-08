import re
import json
import os

class SpokenTextCleaner:
    def __init__(self, mode="lecture"):
        """
        mode: "debate" or "lecture"
        """
        self.mode = mode.lower()
        # Common English fillers
        self.fillers = [
            r"\buh\b", r"\bum\b", r"\ber\b", r"\behh\b",
            r"\bugh\b", r"\blo\b", r"\byou know\b", r"\bi mean\b",
            r"\blike\b", r"\bright\b", r"\bso\b", r"\banyway\b",
            r"\bat the end of the day\b", r"\bbasically\b", r"\bliterally\b"
        ]
        self.filler_pattern = re.compile("|".join(self.fillers), re.IGNORECASE)

    def clean_segment(self, text):
        """
        Stage 3 Implementation: Aggressive Spoken-Text Cleaning
        """
        # 1. Remove fillers
        text = self.filler_pattern.sub("", text)
        
        # 2. Remove categorization-specific noise
        if self.mode == "debate":
            # Remove high-emotional/taunting keywords (simple list for now)
            taunts = [r"\byou're wrong\b", r"\bshut up\b", r"\bstupid\b"]
            for t in taunts:
                text = re.sub(t, "", text, flags=re.IGNORECASE)
        elif self.mode == "lecture":
            # Remove motivational/repetitive introductory phrases
            motivational = [r"\bwelcome back\b", r"\bthank you for joining\b", r"\bbefore we start\b"]
            for m in motivational:
                text = re.sub(m, "", text, flags=re.IGNORECASE)

        # 3. Remove repeated words/phrases (simple back-to-back)
        text = re.sub(r"\b(\w+)(?:\s+\1\b)+", r"\1", text, flags=re.IGNORECASE)
        
        # 4. Normalize whitespace
        text = " ".join(text.split())
        
        return text

    def filter_noise(self, segments):
        """
        Processes a list of segments and removes low-signal ones.
        """
        cleaned_segments = []
        for seg in segments:
            text = self.clean_segment(seg['text'])
            
            # Reject if too short
            if len(text.split()) < 3:
                continue
                
            # Remove audience reactions
            text = re.sub(r"\[.*?\]", "", text)
            text = re.sub(r"\(.*?\)", "", text)
            
            if text.strip():
                new_seg = seg.copy()
                new_seg['text'] = text.strip()
                cleaned_segments.append(new_seg)
                
        return cleaned_segments

if __name__ == "__main__":
    cleaner = SpokenTextCleaner()
    # Test
    test_text = "So, uh, like I was saying, I mean, the code the code is basically literally good."
    print(f"Original: {test_text}")
    print(f"Cleaned:  {cleaner.clean_segment(test_text)}")
