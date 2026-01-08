import sqlite3
import os
import json

db_path = r"d:\web scraping\pipelines\islamqa_ar\data.db"

def audit_arabic():
    if not os.path.exists(db_path):
        print(f"❌ Database not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute("SELECT id, url, question, answer FROM qa_pairs ORDER BY RANDOM() LIMIT 2")
    rows = c.fetchall()

    print("🌍 ARABIC DATA QUALITY AUDIT")
    print("="*50)

    for i, row in enumerate(rows):
        print(f"\n[{i+1}] ID: {row['id']}")
        print(f"🔗 URL: {row['url']}")
        print(f"❓ QUESTION:\n{row['question']}")
        
        # Check for length and potential junk
        answer_text = row['answer']
        word_count = len(answer_text.split())
        
        print(f"\n📖 ANSWER ({word_count} words):")
        print("-" * 20)
        # Print first 2000 chars of the answer
        print(answer_text[:2000] + ("..." if len(answer_text) > 2000 else ""))
        print("-" * 20)
        
        # Quality Checks
        has_html = "<" in answer_text and ">" in answer_text
        print(f"🛡️ Quality Flags: HTML Artifacts: {has_html}")
        
    conn.close()

if __name__ == "__main__":
    audit_arabic()
