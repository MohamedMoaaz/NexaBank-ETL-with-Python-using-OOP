"""
extractor.py

This module defines the Extractor class, which provides a utility method
to load structured data from various file formats into a pandas DataFrame.

Supported file types:
- CSV (.csv)
- All Delimited TXT (.txt)
- JSON (.json, records-oriented)

The extract method handles exceptions gracefully and returns a success flag
along with the DataFrame (if extraction is successful).
"""

import pandas as pd
import json
import csv
from typing import Tuple

class Extractor:
    """
    A utility class for extracting data from various file formats into pandas DataFrames.
    Supports JSON and all delimited file formats (.csv, .txt).
    """

    @staticmethod
    def extract(filepath: str) -> Tuple[bool, pd.DataFrame | None]:
        """
        Extract data from a file into a pandas DataFrame.

        Args:
            filepath (str): Path to the file to be extracted.

        Returns:
            Tuple[bool, DataFrame | None]: 
                - A boolean indicating success or failure.
                - A pandas DataFrame if extraction was successful, otherwise None.
        """
        extension = filepath.split(".")[-1].lower()

        try:
            match extension:
                case "json":
                    with open(filepath, "r") as f:
                        df = pd.DataFrame(json.load(f))

                case "csv" | "txt":
                    with open(filepath, "r") as f:
                        sample = f.read(2048)
                        try:
                            dialect = csv.Sniffer().sniff(sample)
                            delimiter = dialect.delimiter
                        except csv.Error:
                            delimiter = ","  # fallback
                        f.seek(0)
                        df = pd.read_csv(f, delimiter=delimiter)

                case _:
                    raise Exception(f"Unsupported file extension '{extension}'")

        except FileNotFoundError as e:
            print(f"[FAIL] File not found: {e}")
            return (False, None)

        except pd.errors.EmptyDataError as e:
            print(f"[FAIL] File is empty: {e}")
            return (False, None)

        except json.JSONDecodeError as e:
            print(f"[FAIL] Invalid JSON format: {e}")
            return (False, None)

        except pd.errors.ParserError as e:
            print(f"[FAIL] Error parsing file: {e}")
            return (False, None)

        except UnicodeDecodeError as e:
            print(f"[FAIL] File encoding error: {e}")
            return (False, None)

        except Exception as e:
            print(f"[FAIL] Unexpected error: {e}")
            return (False, None)

        return (True, df)

if __name__ == "__main__":
    success, df = Extractor.extract(r"D:\ITI\Python\test\tab.txt")
    if success:
        print(df.head())
    else:
        print("Extraction failed.")



