import sys
import traceback
from datetime import datetime
from typing import TextIO
from datetime import datetime, timezone



class StderrLogger:
    """
    Robust logger for stderr. Formats messages with a timestamp.
    Can also log exceptions with full tracebacks.
    """

    def __init__(self, target: TextIO = sys.stderr):
        self.target = target

    def log(self, message: str, level: str = "INFO"):
        ts = datetime.now(timezone.utc).isoformat()
        try:
            self.target.write(f"[{ts}] {level}: {message}\n")
            self.target.flush()
        except Exception:
            pass   # Nothing we can do if stderr itself fails

    def log_exception(self, message: str = "Unhandled exception"):
        self.log(message, level="ERROR")
        try:
            traceback.print_exc(file=self.target)
            self.target.flush()
        except Exception:
            pass