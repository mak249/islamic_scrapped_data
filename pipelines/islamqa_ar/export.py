import sqlite3
import json
import os
import re
from pathlib import Path

def export_formats():
    db_path = os.path.join(os.path.dirname(__file__), 'data.db')
    output_dir = os.path.join(os.path.dirname(__file__), 'output')
    os.makedirs(output_dir, exist_ok=True)

    if not os.path.exists(db_path):
        print("❌ Database not found. Scrape some data first.")
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute('SELECT * FROM qa_pairs')
    rows = c.fetchall()
    conn.close()

    print(f"📦 Exporting {len(rows)} records...")

    # 1. Standard JSONL (islamqa_dataset.jsonl)
    dataset_path = os.path.join(output_dir, 'islamqa_dataset.jsonl')
    with open(dataset_path, 'w', encoding='utf-8') as f:
        for row in rows:
            f.write(json.dumps(dict(row), ensure_ascii=False) + '\n')

    # 2. Alpaca Format (JSON)
    alpaca_path = os.path.join(output_dir, 'islamqa_alpaca.json')
    alpaca_data = [
        {
            "instruction": "أجب على هذا السؤال الإسلامي:",
            "input": row['question'],
            "output": row['answer'],
            "url": row['url']
        } for row in rows
    ]
    with open(alpaca_path, 'w', encoding='utf-8') as f:
        json.dump(alpaca_data, f, ensure_ascii=False, indent=2)

    # 3. ChatGPT JSONL (Conversational)
    chatgpt_path = os.path.join(output_dir, 'islamqa_chatgpt.jsonl')
    with open(chatgpt_path, 'w', encoding='utf-8') as f:
        for row in rows:
            entry = {
                "messages": [
                    {"role": "user", "content": row['question']},
                    {"role": "assistant", "content": row['answer']}
                ]
            }
            f.write(json.dumps(entry, ensure_ascii=False) + '\n')

    # 4. RAG Chunks (Markdown-friendly JSONL)
    rag_path = os.path.join(output_dir, 'islamqa_rag_chunks.jsonl')
    with open(rag_path, 'w', encoding='utf-8') as f:
        for row in rows:
            # Format for RAG: Include title/url as metadata
            rag_entry = {
                "text": f"السؤال: {row['question']}\n\nالجواب: {row['answer']}",
                "metadata": {
                    "source": row['url'],
                    "id": row['id'],
                    "language": "ar"
                }
            }
            f.write(json.dumps(rag_entry, ensure_ascii=False) + '\n')

    print(f"✅ Export completed to: {output_dir}")
    print(f"📄 Generated: dataset.jsonl, alpaca.json, chatgpt.jsonl, rag_chunks.jsonl")

if __name__ == "__main__":
    export_formats()
