import sys
import json
import time
import signal

# 1. Handle SIGTERM for graceful exit
def handle_sigterm(signum, frame):
    # Final affirmation before dying
    print(json.dumps({"status": "SHUTTING_DOWN", "reason": "received_signal"}), flush=True)
    sys.exit(0)

signal.signal(signal.SIGTERM, handle_sigterm)

def send_msg(status, payload=None):
    """Utility to send NDJSON messages"""
    msg = {"status": status}
    if payload:
        msg.update(payload)
    print(json.dumps(msg), flush=True)

# --- PHASE 2: HANDSHAKE ---
# The Spawner expects this within X seconds
send_msg("READY", {"message": "Python worker initialized", "pid": sys.prefix})

# --- PHASE 3: WAIT FOR DATA ---
# Read from stdin (your Node.js Coordinator's output)
try:
    for line in sys.stdin:
        data = json.loads(line)
        
        if data.get("command") == "START_PROCESSING":
            image_path = data.get("path")
            
            # Simulate intense CPU work (expansion)
            send_msg("PROCESSING", {"progress": 10, "current_rss": "250MB"})
            time.sleep(1) # Simulate time passing
            
            send_msg("PROCESSING", {"progress": 50, "current_rss": "1.2GB"})
            time.sleep(1)
            
            send_msg("COMPLETED", {"output": f"processed_{image_path}", "final_rss": "1.2GB"})
            
        if data.get("command") == "QUIT":
            send_msg("CLEAN_EXIT")
            break

except EOFError:
    # Occurs if Node.js closes the stdin pipe abruptly
    pass
