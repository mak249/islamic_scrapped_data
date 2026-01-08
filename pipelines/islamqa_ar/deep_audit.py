import sqlite3
import os

db_path = r"d:\web scraping\pipelines\islamqa_ar\data.db"

def deep_audit():
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    results = []
    results.append("=" * 60)
    results.append("DEEP QUALITY AUDIT - ARABIC ISLAMQA DATA")
    results.append("=" * 60)

    # 1. Total Records
    c.execute("SELECT COUNT(*) as total FROM qa_pairs")
    total = c.fetchone()['total']
    results.append(f"\nTotal Records: {total}")

    # 2. Check for empty questions or answers
    c.execute("SELECT COUNT(*) FROM qa_pairs WHERE question IS NULL OR question = ''")
    empty_q = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM qa_pairs WHERE answer IS NULL OR answer = ''")
    empty_a = c.fetchone()[0]
    results.append(f"Empty Questions: {empty_q}")
    results.append(f"Empty Answers: {empty_a}")

    # 3. Average answer length
    c.execute("SELECT AVG(LENGTH(answer)) as avg_len FROM qa_pairs")
    avg_len = c.fetchone()['avg_len']
    results.append(f"Avg Answer Length: {avg_len:.0f} characters")

    # 4. Check for short answers (potentially incomplete)
    c.execute("SELECT COUNT(*) FROM qa_pairs WHERE LENGTH(answer) < 100")
    short = c.fetchone()[0]
    results.append(f"Short Answers (<100 chars): {short}")

    # 5. Check for HTML artifacts
    c.execute("SELECT COUNT(*) FROM qa_pairs WHERE answer LIKE '%<%' AND answer LIKE '%>%'")
    html_count = c.fetchone()[0]
    results.append(f"Potential HTML in Answers: {html_count}")

    # Print summary
    for line in results:
        print(line)

    # 6. Sample 3 random records - write to file for proper encoding
    c.execute("SELECT id, question, answer FROM qa_pairs ORDER BY RANDOM() LIMIT 3")
    samples = c.fetchall()
    
    output_path = r"d:\web scraping\pipelines\islamqa_ar\audit_samples.txt"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(results))
        f.write("\n\n" + "=" * 60 + "\nRANDOM SAMPLES\n" + "=" * 60 + "\n")
        for row in samples:
            f.write(f"\nID: {row['id']}\n")
            f.write(f"Q: {row['question']}\n")
            f.write(f"A: {row['answer'][:500]}...\n")
            f.write("-" * 40 + "\n")
    
    print(f"\nFull samples written to: {output_path}")
    conn.close()

if __name__ == "__main__":
    deep_audit()
