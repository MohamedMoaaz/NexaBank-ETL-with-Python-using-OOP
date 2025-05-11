from pathlib import Path
import json


class FolderStatus:
    """
    A status handler for incoming timestamp files.

    Each file has two flags:
        - valid: to check if the file has valid schema or not.
        - saved: to check if the file has been processed and saved or not.

    Initially 'valid' flag is set to 'None' to indicate that it has not been
    checked yet. After validation take place, it is either 'True' or 'False'.
    """

    def __init__(self, path: str, headers: tuple[str]) -> None:
        """Read/Create '{path}/_status.json'."""

        self._dirname: Path = Path(path).parent
        self._filepath: Path = self._dirname / "_status.json"
        self._headers: tuple[str] = headers
        self._status: dict = dict()  # Status content

        self._read()

    def _read(self) -> None:
        """Check if there is a status folder in a folder. if not, create it."""

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
        """Clean filepath and use it as key."""
        return filepath.replace("\\", "/").split("/")[-1].split(".")[0].lower()

    def __getitem__(self, key: str) -> bool:
        return self._status[self._key(key)]

    def __setitem__(self, key: str, value: bool) -> None:
        self[key] = value

    def update(self) -> None:
        """Update the status file content."""

        try:
            with open(self._filepath, "w") as fp:
                json.dump(self._status, fp, indent=2)
        except Exception:
            pass


class FolderStatusHandler:
    def __init__(self, headers: tuple[str]):
        self._headers: tuple[str] = tuple(i.lower() for i in headers)
        self._folders: dict[str, FolderStatus] = dict()

    def _check(self, path: str):
        key = self._key(path)
        if key not in self._folders:
            self._folders[key] = FolderStatus(path=path, headers=self._headers)

    @staticmethod
    def _key(filepath: str) -> str:
        """Clean filepath and use directory as key."""
        return "/".join(filepath.replace("\\", "/").split("/")[:-1])

    def __getitem__(self, key: str) -> FolderStatus:
        self._check(key)
        return self._folders[self._key(key)][key]

    def __setitem__(self, key: str, value: bool) -> None:
        self[key] = value

    def update(self, key: str) -> None:
        """Update the status file content."""

        self._folders[self._key(key)].update()


if __name__ == "__main__":
    path = "incoming_data/2025-04-29/21/credit_cards_billing.csv"
    status = FolderStatusHandler()
    status[path]["saved"] = None
    status.update(path)
