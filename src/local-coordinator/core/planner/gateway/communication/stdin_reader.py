import sys
from typing import Iterator, Optional, BinaryIO

class StdinReader:
    """
    Robust reader for stdin (or any binary input stream).
    Reads one line at a time, decodes with UTF-8 (replace errors),
    strips whitespace, and skips empty lines.
    """

    def __init__(self, source: BinaryIO = sys.stdin.buffer, encoding: str = "utf-8"):
        self.source = source
        self.encoding = encoding
        self._closed = False

    def lines(self) -> Iterator[str]:
        """Yield decoded, stripped, non-empty lines."""
        for raw_line in self.source:
            if self._closed:
                break
            try:
                line = raw_line.decode(self.encoding, errors="replace").strip()
            except Exception:
                # Should not happen with "replace", but safety net
                continue
            if line:
                yield line

    def raw_bytes(self) -> Iterator[bytes]:
        """Yield raw bytes directly (for binary protocols)."""
        for chunk in self.source:
            if self._closed:
                break
            yield chunk

    def close(self):
        self._closed = True
        if not self.source.closed:
            self.source.close()

    def __iter__(self):
        return self.lines()