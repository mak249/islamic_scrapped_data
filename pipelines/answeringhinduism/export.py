#!/usr/bin/env python3
"""
EXPORT MODULE
=============
Exports processed criticisms to structured JSON format.
"""

import sqlite3
import os
import json
from datetime import datetime
from typing import List, Dict


class Exporter:
    def __init__(self, db_path: str, output_dir: str):
        self.db_path = db_path
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def export_json(self, filename: str = "criticisms.json"):
        """Export all retained criticisms to a JSON file."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        c.execute('''
            SELECT c.*, a.url as source_url, a.title as article_title
            FROM criticisms c
            JOIN articles a ON c.article_id = a.id
            WHERE c.retain = 1
            ORDER BY a.category, c.id
        ''')
        
        rows = c.fetchall()
        conn.close()
        
        output = []
        for row in rows:
            output.append({
                "topic": row['topic'],
                "claim": row['claim'],
                "source_excerpt": row['source_excerpt'],
                "hindu_reference": row['hindu_reference'],
                "reasoning_type": row['reasoning_type'],
                "dependency_on_christianity": bool(row['dependency_on_christianity']),
                "retain": bool(row['retain']),
                "source_url": row['source_url'],
                "article_title": row['article_title']
            })
        
        output_path = os.path.join(self.output_dir, filename)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Exported {len(output)} criticisms to {output_path}")
        return output_path
    
    def export_jsonl(self, filename: str = "criticisms.jsonl"):
        """Export to JSONL format (one JSON per line)."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        c.execute('''
            SELECT c.*, a.url as source_url
            FROM criticisms c
            JOIN articles a ON c.article_id = a.id
            WHERE c.retain = 1
        ''')
        
        rows = c.fetchall()
        conn.close()
        
        output_path = os.path.join(self.output_dir, filename)
        with open(output_path, 'w', encoding='utf-8') as f:
            for row in rows:
                record = {
                    "topic": row['topic'],
                    "claim": row['claim'],
                    "source_excerpt": row['source_excerpt'],
                    "hindu_reference": row['hindu_reference'],
                    "reasoning_type": row['reasoning_type'],
                }
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
        
        print(f"✅ Exported to {output_path}")
        return output_path
    
    def export_by_category(self):
        """Export criticisms grouped by category."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        # Mapping URLs to granular categories if database category is generic
        # (Since we scraped everything as 'hinduism' mostly)
        category_map = {
            'brahmin': 'caste_system',
            'caste': 'caste_system',
            'varna': 'caste_system',
            'manusmriti': 'manusmriti',
            'women': 'women_rights',
            'marriage': 'social_ethics',
            'intoxicant': 'dietary_laws',
            'meat': 'dietary_laws',
            'panini': 'history',
            'history': 'history',
            'vedic-mathematics': 'vedic_science',
            'astrology': 'vedic_science',
            'science': 'vedic_science',
            'critical-analysis': 'scripture_analysis',
            'predestination': 'philosophy',
        }
        
        c.execute('''
            SELECT c.*, a.url as source_url, a.title as article_title
            FROM criticisms c
            JOIN articles a ON c.article_id = a.id
            WHERE c.retain = 1
        ''')
        rows = c.fetchall()
        
        grouped_data = {}
        
        for row in rows:
            # Determine refined category
            url = row['source_url'].lower()
            topic = row['topic'].lower()
            category = "general_criticism"
            
            # Check URL keywords
            for key, cat in category_map.items():
                if key in url or key in topic:
                    category = cat
                    break
            
            if category not in grouped_data:
                grouped_data[category] = []
                
            grouped_data[category].append({
                "topic": row['topic'],
                "claim": row['claim'],
                "source_excerpt": row['source_excerpt'],
                "hindu_reference": row['hindu_reference'],
                "reasoning_type": row['reasoning_type'],
                "article_title": row['article_title']
            })
            
        # Export each group
        for category, items in grouped_data.items():
            output_path = os.path.join(self.output_dir, f"{category}.json")
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(items, f, indent=2, ensure_ascii=False)
            print(f"✅ Exported {len(items)} items to {output_path}")
        
        conn.close()


def main():
    base_dir = os.path.dirname(__file__)
    db_path = os.path.join(base_dir, "data.db")
    output_dir = os.path.join(base_dir, "output")
    
    exporter = Exporter(db_path, output_dir)
    exporter.export_json()
    exporter.export_jsonl()
    exporter.export_by_category()
    
    print("\n🎉 EXPORT COMPLETE!")


if __name__ == "__main__":
    main()
