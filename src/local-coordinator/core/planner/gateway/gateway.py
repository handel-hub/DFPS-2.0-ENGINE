# app.py or main.py
import json
import signal
from typing import Dict, Any
from abc import ABC, abstractmethod
from ..communication.stdin_reader import StdinReader
from ..communication.stdout_writer import StdoutWriter
from ..communication.stderr_logger import StderrLogger

class App(ABC):
    @abstractmethod
    def handle(self, command: Dict[str, Any]) -> Dict[str, Any]:
        ...

class Gateway:
    def __init__(self, app: App,
                 reader: StdinReader = None,  # type: ignore
                 writer: StdoutWriter = None, # type: ignore
                 logger: StderrLogger = None): # type: ignore
        self.app = app
        self.reader = reader or StdinReader()
        self.writer = writer or StdoutWriter()
        self.logger = logger or StderrLogger()
        self._running = True

    # Refactored run() and _stop() in Gateway.py
    def run(self):
        signal.signal(signal.SIGTERM, self._stop)  # type: ignore[reportAttributeAccessIssue]
        signal.signal(signal.SIGINT, self._stop)   # type: ignore[reportAttributeAccessIssue]

        try:
            for line in self.reader:
                try:
                    command = json.loads(line)
                except json.JSONDecodeError as e:
                    self._send_error(f"Invalid JSON: {e}")
                    self.logger.log(f"Bad input: {e}", "WARNING")
                    continue

                try:
                    response = self.app.handle(command)
                    if not isinstance(response, dict):
                        raise TypeError("handle() must return a dict")
                    self._send_response(response)
                except Exception:
                    self.logger.log_exception("Command processing failed")
                    self._send_error("Internal error")
        finally:
            # Ensures cleanup always happens even if interrupted/killed
            self.reader.close()
            self.writer.close()

    def _send_response(self, data: Dict[str, Any]):
        self.writer.write_line(json.dumps(data, ensure_ascii=False))

    def _send_error(self, message: str):
        self.writer.write_line(json.dumps({"error": message}, ensure_ascii=False))
