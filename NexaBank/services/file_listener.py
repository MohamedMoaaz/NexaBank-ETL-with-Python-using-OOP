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
    """
    def __init__(
        self,
        filter: tuple[str],  # Fixed type hint
        delay: float = 1.0,
        callback: Callable[[str], None] = None,
    ):
        """
        Initialize the event handler.
        
        Args:
            filter: Tuple of file names to monitor (without extensions)
            delay: Debouncing delay in seconds
            callback: Function to call when a file modification is detected
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
        """
        if event.is_directory:
            return

        filepath = Path(event.src_path)

        def handle_write_complete(fp: Path) -> None:
            """Clean up timer and execute callback when write is complete."""
            if str(fp) in self._timers:  # Added safety check
                del self._timers[str(fp)]  # Changed to use string key
                if self._callback:
                    self._callback(str(fp))

        if filepath.stem not in self._filter:
            return

        # Cancel existing timer for this file if any
        if str(filepath) in self._timers:  # Changed to use string key
            self._timers[str(filepath)].cancel()

        # Create new timer for debouncing
        timer = Timer(self._delay, lambda: handle_write_complete(filepath))
        self._timers[str(filepath)] = timer  # Changed to use string key
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
        """
        Initialize the file listener.
        
        Args:
            path: Directory path to monitor
            filter: Tuple of file names to monitor (without extensions)
            callback: Function to call when a file modification is detected
        """
        self._path = path
        self._handler = _FileEventHandler(filter, delay=1.0, callback=callback)
        self._observer = Observer()
        self._observer.schedule(self._handler, path=path, recursive=True)
        self._thread: Thread | None = None  # Improved type hint

    def loop(self) -> None:
        """Start the file system observer and run the monitoring loop."""
        self._observer.start()

        try:
            print(f"[INFO] Watching changes in: {self._path}")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self._observer.stop()
        finally:  # Added finally block for cleanup
            self._observer.join()

    def start_thread(self) -> None:
        """Start the monitoring loop in a separate thread."""
        self._thread = Thread(target=self.loop, daemon=True)  # Added daemon=True
        self._thread.start()


if __name__ == "__main__":
    def test_function(filepath: str) -> None:
        """Test callback function that prints the modified file path."""
        print(f"File modified: {filepath}")

    # Example usage
    listener = FileListener("./incoming_data", callback=test_function)
    listener.loop()
