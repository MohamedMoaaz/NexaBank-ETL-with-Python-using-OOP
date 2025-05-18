"""
folder_status.py

This module provides classes for tracking and persisting the validation and processing
status of incoming timestamped files. It stores file states in `_status.json` located
in each timestamp directory.

Each file has two flags:
- `valid`: Indicates if the file passed schema validation (None, True, False)
- `saved`: Indicates whether the file was successfully processed and saved

Classes:
- FolderStatus: Handles reading, updating, and writing file-level status in one folder.
- FolderStatusHandler: Manages FolderStatus objects across multiple folders.
"""

from pathlib import Path
import json


class FolderStatus:
    """
    A status handler for a single incoming timestamp folder.

    Each file is tracked with:
        - valid (bool | None): validation status
        - saved (bool): processing status

    A status JSON file (_status.json) is maintained inside the folder.
    """

    def __init__(self, path: str, headers: tuple[str]) -> None:
        """
        Initialize FolderStatus for a given path and file headers.

        Args:
            path (str): Path to one of the files in the folder.
            headers (tuple[str]): List of expected file keys (e.g. 'customer_profiles').
        """
        self._dirname: Path = Path(path).parent
        self._filepath: Path = self._dirname / "_status.json"
        self._headers: tuple[str] = headers
        self._status: dict = dict()  # File status mapping

        self._read()

    def _read(self) -> None:
        """
        Load existing _status.json if it exists, otherwise initialize it.

        Initializes all headers as:
            valid: None
            saved: False
        """
        if not self._filepath.exists():
            self._status = {
                header: {
                    "valid": None,
                    "saved": False,
                }
                for header in self._headers
            }
            self.update()
        else:
            with open(self._filepath) as fp:
                self._status = json.load(fp)

    @staticmethod
    def _key(filepath: str) -> str:
        """
        Normalize and extract the base file name without extension.

        Args:
            filepath (str): File path

        Returns:
            str: Normalized key (e.g. 'customer_profiles')
        """
        return filepath.replace("\\", "/").split("/")[-1].split(".")[0].lower()

    def __getitem__(self, key: str) -> bool:
        """
        Access status flags using a file path.

        Args:
            key (str): Full path to file

        Returns:
            dict: Dictionary with 'valid' and 'saved' status flags
        """
        return self._status[self._key(key)]

    def __setitem__(self, key: str, value: bool) -> None:
        """
        Set status flags.

        Args:
            key (str): File path
            value (bool): New status flag
        """
        self[key] = value

    def update(self) -> None:
        """
        Write the current _status dictionary back to the JSON file.
        """
        try:
            with open(self._filepath, "w") as fp:
                json.dump(self._status, fp, indent=2)
        except Exception:
            pass


class FolderStatusHandler:
    """
    Manages FolderStatus instances across multiple timestamp folders.

    Provides dictionary-style access and ensures each folder has an associated status file.
    """

    def __init__(self, headers: tuple[str]):
        """
        Initialize the status handler.

        Args:
            headers (tuple[str]): Expected file keys to be tracked in each folder.
        """
        self._headers: tuple[str] = tuple(i.lower() for i in headers)
        self._folders: dict[str, FolderStatus] = dict()

    def _check(self, path: str):
        """
        Ensure a FolderStatus instance exists for the directory of a given file.

        Args:
            path (str): File path
        """
        key = self._key(path)
        if key not in self._folders:
            self._folders[key] = FolderStatus(path=path, headers=self._headers)

    @staticmethod
    def _key(filepath: str) -> str:
        """
        Extract folder path from full file path.

        Args:
            filepath (str): File path

        Returns:
            str: Folder path as key
        """
        return "/".join(filepath.replace("\\", "/").split("/")[:-1])

    def __getitem__(self, key: str) -> FolderStatus:
        """
        Retrieve the status flags for a file.

        Args:
            key (str): Full file path

        Returns:
            dict: Dictionary with 'valid' and 'saved' status flags
        """
        self._check(key)
        return self._folders[self._key(key)][key]

    def __setitem__(self, key: str, value: bool) -> None:
        """
        Set status flags.

        Args:
            key (str): File path
            value (bool): Status value
        """
        self[key] = value

    def update(self, key: str) -> None:
        """
        Write updated status back to the JSON file for a given file.

        Args:
            key (str): File path
        """
        self._folders[self._key(key)].update()


if __name__ == "__main__":
    path = "incoming_data/2025-05-18/19/customer_profiles.csv"
    headers = ("customer_profiles", "support_tickets", "credit_cards_billing", "loans", "transactions")
    status = FolderStatusHandler(headers)
    status[path]["saved"] = None
    status.update(path)