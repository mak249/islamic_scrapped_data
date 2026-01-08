import os
import sqlite3
import re
import math

def print_progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=40, fill='█', printEnd="\r"):
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=printEnd)
    if iteration >= total: 
        print()

def main():
    db_path = os.path.join(os.path.dirname(__file__), 'data.db')
    
    if not os.path.exists(db_path):
        print("❓ Database not yet created. Run the scraper first.")
        return

    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    # 1. Total Count
    c.execute('SELECT COUNT(*) FROM qa_pairs')
    count = c.fetchone()[0]
    
    # 2. Max ID in DB
    c.execute('SELECT url FROM qa_pairs')
    max_id_db = 0
    urls = []
    
    rows = c.fetchall()
    for (url,) in rows:
        match = re.search(r'/answers/(\d+)', url)
        if match:
            max_id_db = max(max_id_db, int(match.group(1)))
            
    conn.close()

    # 3. Verified Targets for Arabic
    # From site audit: Arabic section has ~33,700 fatwas
    TARGET_ARABIC = 33700 
    
    remaining_ar = max(0, TARGET_ARABIC - count)
    
    print("\n🔍 ISLAMQA ARABIC STATUS REPORT")
    print("==================================================")
    print(f"📥 Collected (Arabic):  {count:,}")
    print(f"📡 Verified Total AR:   {TARGET_ARABIC:,} (Site search estimate)")
    print(f"🆔 Latest ID in DB:     {max_id_db:,}")
    print("--------------------------------------------------")
    print(f"💡 Note: The Arabic section is ~1.8x larger than the English one.")
    print("==================================================")
    
    print("\nArabic Completion Rate:")
    print_progress_bar(min(count, TARGET_ARABIC), TARGET_ARABIC, prefix='Progress:', suffix='Complete', length=40)
    
    if count >= (TARGET_ARABIC * 0.99):
        print("\n🏆 STATUS: NEARLY COMPLETE (ARABIC SECTION)")
    else:
        print(f"\n⏳ Estimated Remaining (Arabic): ~{remaining_ar:,} questions")
    print("\n")

if __name__ == "__main__":
    main()
