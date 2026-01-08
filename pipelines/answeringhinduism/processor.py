#!/usr/bin/env python3
"""
CONTENT PROCESSOR & CHRISTIANITY FILTER
========================================
Processes raw article content and extracts structured criticisms.
Filters out content dependent on Christian theology.
"""

import sqlite3
import os
import re
import json
from typing import List, Dict, Optional
from datetime import datetime

# Christian keywords to detect dependency
CHRISTIAN_KEYWORDS = [
    r'\bjesus\b', r'\bchrist\b', r'\bbible\b', r'\bgospel\b',
    r'\btrinity\b', r'\bchurch\b', r'\bchristian\b', r'\bsalvation\b',
    r'\bredemption\b', r'\bcross\b', r'\bresurrection\b',
    r'\bson of god\b', r'\bholy spirit\b', r'\bnew testament\b',
    r'\bold testament\b', r'\babraham\b', r'\bmoses\b', r'\bpaul\b',
    r'\bpeter\b', r'\bapostle\b', r'\bscripture\b(?!.*hindu)',
]

# Hindu scripture patterns to extract references
HINDU_REFERENCE_PATTERNS = [
    r'(Manusmriti\s*[\d:.\-–]+)',
    r'(Rig\s*Veda\s*[\d:.\-–]+)',
    r'(Atharva\s*Veda\s*[\d:.\-–]+)',
    r'(Yajur\s*Veda\s*[\d:.\-–]+)',
    r'(Sama\s*Veda\s*[\d:.\-–]+)',
    r'(Bhagavad\s*Gita\s*[\d:.\-–]+)',
    r'(Mahabharata\s*[\w\s]*[\d:.\-–]*)',
    r'(Ramayana\s*[\w\s]*[\d:.\-–]*)',
    r'(Upanishad\s*[\w\s]*)',
    r'(Purana\s*[\w\s]*)',
    r'(Dharma\s*Shastra\s*[\w\s]*)',
    r'(Arthashastra\s*[\w\s]*)',
]

# Reasoning type detection
REASONING_KEYWORDS = {
    'historical': ['history', 'century', 'period', 'era', 'ancient', 'origin', 'date'],
    'philosophical': ['philosophy', 'concept', 'idea', 'logic', 'reason', 'argument'],
    'ethical': ['moral', 'ethics', 'right', 'wrong', 'cruel', 'oppression', 'violence'],
    'textual': ['verse', 'scripture', 'text', 'quote', 'says', 'according to', 'translation'],
    'logical': ['contradiction', 'inconsistent', 'fallacy', 'absurd', 'impossible'],
}


class ContentProcessor:
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def has_christian_dependency(self, text: str) -> bool:
        """Check if text contains Christian theology references."""
        text_lower = text.lower()
        for pattern in CHRISTIAN_KEYWORDS:
            if re.search(pattern, text_lower):
                return True
        return False
    
    def extract_hindu_references(self, text: str) -> List[str]:
        """Extract Hindu scripture references from text."""
        refs = []
        for pattern in HINDU_REFERENCE_PATTERNS:
            matches = re.findall(pattern, text, re.IGNORECASE)
            refs.extend(matches)
        return list(set(refs))
    
    def detect_reasoning_type(self, text: str) -> str:
        """Detect the type of reasoning used in the criticism."""
        text_lower = text.lower()
        scores = {}
        for rtype, keywords in REASONING_KEYWORDS.items():
            score = sum(1 for kw in keywords if kw in text_lower)
            scores[rtype] = score
        
        if max(scores.values()) == 0:
            return "textual"  # Default
        return max(scores, key=scores.get)
    
    def split_into_units(self, content: str, title: str) -> List[Dict]:
        """Split article content into discrete criticism units."""
        units = []
        
        # Split by paragraph breaks or numbered sections
        paragraphs = re.split(r'\n\s*\n|\(\d+\)\s*', content)
        
        current_topic = title
        for para in paragraphs:
            para = para.strip()
            if len(para) < 50:  # Skip very short paragraphs
                continue
            
            # Check for section headers
            header_match = re.match(r'^[\(\d\)]*\s*([A-Z][^.!?]*(?:ism|women|marriage|education|caste|varna))', para, re.IGNORECASE)
            if header_match:
                current_topic = header_match.group(1).strip()
            
            # STRICT FILTERING LOGIC
            # 1. Premise Check: Flag if paragraph starts with comparative theology
            is_dependent = self.has_christian_dependency(para)
            
            # 2. Stripping: Try to remove Christian comparisons if present
            if is_dependent:
                cleaned_para = self._strip_christian_premise(para)
                if cleaned_para != para:
                    # Re-check dependency on cleaned text
                    if not self.has_christian_dependency(cleaned_para):
                        para = cleaned_para
                        is_dependent = False
            
            # Extract criticism claims from paragraphs with scripture quotes
            refs = self.extract_hindu_references(para)
            term_match = any(kw in para.lower() for kw in ['manu', 'veda', 'scripture', 'dharma', 'caste', 'brahmin'])
            
            if refs or term_match:
                unit = {
                    'topic': current_topic[:100],
                    'claim': self._extract_claim(para),
                    'source_excerpt': para[:1000],  # Increased limit
                    'hindu_reference': ', '.join(refs) if refs else None,
                    'reasoning_type': self.detect_reasoning_type(para),
                    'dependency_on_christianity': is_dependent,
                }
                unit['retain'] = not unit['dependency_on_christianity']
                units.append(unit)
        
        return units

    def _strip_christian_premise(self, text: str) -> str:
        """Attempt to remove Christian comparative clauses."""
        # Pattern: "Unlike Christianity, Hinduism says..." -> "Hinduism says..."
        # Pattern: "In contrast to the Bible, the Vedas..." -> "The Vedas..."
        
        patterns = [
            r'^(?:Unlike|In contrast to|Contrary to)\s+(?:Christianity|the Bible|Jesus|Christians)[,\s]+',
            r'^(?:While|Whereas)\s+(?:Christianity|the Bible)\s+(?:teaches|says|promotes)[^,]+,[^,]+(?:Hinduism|the Vedas)',
        ]
        
        cleaned = text
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                # If match found at start, remove it and capitalize next word
                cleaned = re.sub(pattern, '', text, count=1).strip()
                if cleaned and cleaned[0].islower():
                    cleaned = cleaned[0].upper() + cleaned[1:]
                return cleaned
        
        return text
    
    def _extract_claim(self, text: str) -> str:
        """Extract the main claim from a paragraph."""
        # Try to find sentences with clear criticism markers
        sentences = re.split(r'[.!?]', text)
        for sent in sentences:
            sent = sent.strip()
            if len(sent) > 30 and any(kw in sent.lower() for kw in 
                ['shows', 'proves', 'indicates', 'demonstrates', 'reveals',
                 'says', 'states', 'declares', 'instructs', 'ordains',
                 'ordained', 'assigned', 'considered', 'treated']):
                return sent[:200]
        return sentences[0][:200] if sentences else text[:200]
    
    def process_article(self, article_id: int, url: str, title: str, content: str) -> List[Dict]:
        """Process a single article and return criticism units."""
        units = self.split_into_units(content, title)
        
        # Add article reference
        for unit in units:
            unit['article_id'] = article_id
            unit['source_url'] = url
        
        return units
    
    def process_all(self):
        """Process all articles in the database."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        # Get all articles
        c.execute('SELECT id, url, title, raw_content FROM articles')
        articles = c.fetchall()
        
        all_units = []
        retained = 0
        discarded = 0
        
        print(f"🔬 Processing {len(articles)} articles...")
        
        for article in articles:
            units = self.process_article(
                article['id'], 
                article['url'], 
                article['title'], 
                article['raw_content']
            )
            
            for unit in units:
                if unit['retain']:
                    retained += 1
                else:
                    discarded += 1
                all_units.append(unit)
                
                # Save to database
                c.execute('''
                    INSERT INTO criticisms 
                    (article_id, topic, claim, source_excerpt, hindu_reference, 
                     reasoning_type, dependency_on_christianity, retain)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    unit['article_id'], unit['topic'], unit['claim'],
                    unit['source_excerpt'], unit['hindu_reference'],
                    unit['reasoning_type'], 1 if unit['dependency_on_christianity'] else 0,
                    1 if unit['retain'] else 0
                ))
        
        conn.commit()
        conn.close()
        
        print(f"✅ Processed {len(all_units)} criticism units")
        print(f"   📗 Retained: {retained}")
        print(f"   📕 Discarded (Christian dependency): {discarded}")
        
        return all_units


def main():
    db_path = os.path.join(os.path.dirname(__file__), "data.db")
    processor = ContentProcessor(db_path)
    processor.process_all()


if __name__ == "__main__":
    main()
