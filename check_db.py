import sqlite3
import sys
import os

def main():
    db_path = os.path.join('src', 'profilePlugin', 'storage', 'profiling.sqlite')
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print("Tables:", [t[0] for t in tables])
        
        if ('execution_metrics',) in tables:
            cursor.execute("SELECT count(*) FROM execution_metrics")
            print("Metrics count:", cursor.fetchone()[0])
            
            cursor.execute("SELECT * FROM execution_metrics LIMIT 5")
            print("Sample metrics:", cursor.fetchall())
            
        if ('plugin_executions',) in tables:
            cursor.execute("SELECT count(*) FROM plugin_executions")
            print("Executions count:", cursor.fetchone()[0])
            
            cursor.execute("SELECT status FROM plugin_executions LIMIT 10")
            print("Statuses:", cursor.fetchall())
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    main()
