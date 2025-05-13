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
from threading import Timer, Thread
from typing import Callable
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class _FileEventHandler(FileSystemEventHandler):
    """
    A private class that handles file system events with debouncing capability.
    Inherits from watchdog's FileSystemEventHandler.

    Attributes:
        _delay (float): Time to wait before firing callback after modification.
        _callback (Callable): Function to call after stable file write.
        _timers (dict): Dictionary mapping file paths to debounce timers.
        _filter (set): Set of filename stems to watch (e.g., 'customer_profiles').
    """

    def __init__(
        self,
        filter: tuple[str],
        delay: float = 1.0,
        callback: Callable[[str], None] = None,
    ):
        """
        Initialize the event handler.

        Args:
            filter (tuple[str]): File name stems to monitor.
            delay (float): Debounce delay in seconds.
            callback (Callable): Function to call when a file is modified.
        """
        super().__init__()
        self._delay = delay
        self._callback = callback
        self._timers: dict[str, Timer] = {}
        self._filter: set[str] = set(filter)

    def on_modified(self, event) -> None:
        """
        Handle file modification events with debouncing.
        Ignores directory modifications and files not in the filter.

        Args:
            event: The file system event.
        """
        if event.is_directory:
            return

        filepath = Path(event.src_path)

        def handle_write_complete(fp: Path) -> None:
            """
            Callback wrapper to clean up and invoke the main callback.

            Args:
                fp (Path): The modified file.
            """
            if str(fp) in self._timers:
                del self._timers[str(fp)]
                if self._callback:
                    self._callback(str(fp))

        if filepath.stem not in self._filter:
            return

        if str(filepath) in self._timers:
            self._timers[str(filepath)].cancel()

        timer = Timer(self._delay, lambda: handle_write_complete(filepath))
        self._timers[str(filepath)] = timer
        timer.start()


class FileListener:
    """
    A class that monitors file system changes in a specified directory.
    Supports filtering by filename and callback execution on file modifications.

    Attributes:
        _path (str): The directory to monitor.
        _handler (_FileEventHandler): Internal handler for file change events.
        _observer (Observer): Watchdog observer.
        _thread (Thread | None): Thread object used when running as a background task.
    """

    def __init__(
        self,
        path: str,
        filter: tuple[str] = tuple(),
        callback: Callable[[str], None] = None,
    ):
        """
        Initialize the file listener.

        Args:
            path (str): Directory path to monitor.
            filter (tuple[str]): File name stems to filter (without extensions).
            callback (Callable): Function to call when file is modified.
        """
        self._path = path
        self._handler = _FileEventHandler(filter, delay=1.0, callback=callback)
        self._observer = Observer()
        self._observer.schedule(self._handler, path=path, recursive=True)
        self._thread: Thread | None = None

    def loop(self) -> None:
        """
        Start the file system observer and run a blocking loop.
        Use this in the main thread if you want synchronous execution.
        """
        self._observer.start()

        try:
            print(f"[INFO] Watching changes in: {self._path}")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self._observer.stop()
        finally:
            self._observer.join()

    def start_thread(self) -> None:
        """
        Start the file listener loop in a daemon background thread.
        Use this for non-blocking monitoring.
        """
        self._thread = Thread(target=self.loop, daemon=True)
        self._thread.start()


if __name__ == "__main__":
    def test_function(filepath: str) -> None:
        """Test callback function that prints the modified file path."""
        print(f"File modified: {filepath}")

    # Example usage
    listener = FileListener("./incoming_data", callback=test_function)
    listener.loop()