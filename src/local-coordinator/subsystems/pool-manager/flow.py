import time
import sys

def run_flow():
    steps = [
        "Connecting to database...",
        "Cleaning temporary files...",
        "Processing image data...",
        "Syncing with cloud..."
    ]

    for step in steps:
        # 1. Print the log
        print(f"[LOG] {step}")
        
        # 2. FORCE the output to send to Node.js immediately
        sys.stdout.flush() 
        
        # 3. Pause for 1 second
        time.sleep(1)

    print("SUCCESS: Flow finished.")

if __name__ == "__main__":
    run_flow()
