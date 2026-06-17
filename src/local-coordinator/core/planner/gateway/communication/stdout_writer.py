import sys
from typing import BinaryIO

class StdoutWriter:
    """
    Robust writer for stdout (or any binary output stream).
    Writes a line (appends newline), flushes, and survives broken pipes.
    """

    def __init__(self, target: BinaryIO = sys.stdout.buffer, encoding: str = "utf-8"):
        self.target = target
        self.encoding = encoding
        self._broken = False

    def write_line(self, data: str) -> bool:
        """
        Encode and write a line. Returns False if the pipe is broken.
        """
        if self._broken:
            return False
        try:
            self.target.write(data.encode(self.encoding, errors="replace") + b"\n")
            self.target.flush()
            return True
        except BrokenPipeError:
            self._broken = True
            return False
        except Exception:
            # Other write errors – mark as broken
            self._broken = True
            return False

    def write_raw(self, data: bytes) -> bool:
        """Write raw bytes (for binary protocols)."""
        if self._broken:
            return False
        try:
            self.target.write(data)
            self.target.flush()
            return True
        except BrokenPipeError:
            self._broken = True
            return False

    @property
    def is_broken(self) -> bool:
        return self._broken

    def close(self):
        if not self.target.closed:
            self.target.close()