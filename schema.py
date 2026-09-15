import sqlite3
import sys

db_file = sys.argv[1]
conn = sqlite3.connect(db_file)
cursor = conn.cursor()
cursor.execute("SELECT sql FROM sqlite_master WHERE type='table';")
for row in cursor.fetchall():
    if row[0]:
        print(row[0])
