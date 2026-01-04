import sqlite3
import re

db_path = 'data.db'
conn = sqlite3.connect(db_path)
c = conn.cursor()

# Get count
c.execute('SELECT COUNT(*) FROM qa_pairs')
count = c.fetchone()[0]
print(f"Total Records: {count}")

# Get max ID
c.execute('SELECT url FROM qa_pairs')
max_id = 0
for (url,) in c.fetchall():
    match = re.search(r'/answers/(\d+)', url)
    if match:
        max_id = max(max_id, int(match.group(1)))

print(f"Max ID: {max_id}")
conn.close()
