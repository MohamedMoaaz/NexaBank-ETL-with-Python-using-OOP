"""
file_listener.py

This module defines a file monitoring utility using the `watchdog` library.
It includes debouncing logic to avoid triggering multiple callbacks on rapid file writes.

Components:
- _FileEventHandler: Internal event handler that watches for file modifications.
- FileListener: Public interface to set up directory monitoring with optional filtering and callbacks.

Usage:
- Can run in a blocking loop (`loop()`) or on a background thread (`start_thread()`).
- Designed for use with ETL pipelines or real-time data ingestion.

Dependencies:
- watchdog
"""

import time
import logging
from threading import Timer, Thread
from typing import Callable
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# === Configure logging ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class _FileEventHandler(FileSystemEventHandler):
    """
    A private class that handles file system events with debouncing capability.
    Inherits from watchdog's FileSystemEventHandler.
    """

    def __init__(
        self,
        filter: tuple[str],
        delay: float = 1.0,
        callback: Callable[[str], None] = None,
    ):
        super().__init__()
        self._delay = delay
        self._callback = callback
        self._timers: dict[str, Timer] = {}
        self._filter: set[str] = set(filter)
        logger.info(f"FileEventHandler initialized with filters: {self._filter}, delay: {self._delay}s")

    def on_modified(self, event) -> None:
        if event.is_directory:
            return

        filepath = Path(event.src_path)

        if filepath.stem not in self._filter:
            logger.debug(f"Ignored file: {filepath.name} (not in filter)")
            return

        def handle_write_complete(fp: Path) -> None:
            if str(fp) in self._timers:
                del self._timers[str(fp)]
                logger.info(f"File write completed: {fp}")
                if self._callback:
                    logger.debug(f"Invoking callback for: {fp}")
                    self._callback(str(fp))

        # Cancel existing timer if file is modified again quickly
        if str(filepath) in self._timers:
            logger.debug(f"Resetting debounce timer for: {filepath}")
            self._timers[str(filepath)].cancel()

        logger.info(f"Detected file change: {filepath}")
        timer = Timer(self._delay, lambda: handle_write_complete(filepath))
        self._timers[str(filepath)] = timer
        timer.start()


class FileListener:
    """
    A class that monitors file system changes in a specified directory.
    Supports filtering by filename and callback execution on file modifications.
    """

    def __init__(
        self,
        path: str,
        filter: tuple[str] = tuple(),
        callback: Callable[[str], None] = None,
    ):
        self._path = path
        self._handler = _FileEventHandler(filter, delay=1.0, callback=callback)
        self._observer = Observer()
        self._observer.schedule(self._handler, path=path, recursive=True)
        self._thread: Thread | None = None
        logger.info(f"FileListener initialized on path: {self._path}")

    def loop(self) -> None:
        self._observer.start()
        logger.info(f"FileListener loop started (watching: {self._path})")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.warning("FileListener loop interrupted by user.")
            self._observer.stop()
        finally:
            self._observer.join()
            logger.info("FileListener loop stopped.")

    def start_thread(self) -> None:
        self._thread = Thread(target=self.loop, daemon=True)
        self._thread.start()
        logger.info("FileListener started in background thread.")

if __name__ == "__main__":
    # Example usage
    def example_callback(filepath: str) -> None:
        logger.info(f"Callback executed for file: {filepath}")

    listener = FileListener(
        path="./incoming_data",
        filter=("customer_profiles", "support_tickets", "credit_cards_billing", "loans", "transactions"),
        callback=example_callback
    )
    listener.start_thread()
    # listener.loop()  # This will block the main thread
