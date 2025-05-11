import pandas as pd
import json
from typing import Tuple


class Extractor:
    """
    A utility class for extracting data from various file formats into pandas DataFrames.
    Supports CSV, TXT (pipe-delimited), and JSON file formats.
    """

    @staticmethod
    def extract(filepath: str) -> Tuple[bool, pd.DataFrame | None]:
        """
        Extract data from a file into a pandas DataFrame.

        Args:
            filepath: Path to the file to be extracted

        Returns:
            Tuple containing:
            - bool: Success flag (True if extraction successful, False otherwise)
            - DataFrame: Extracted data if successful, None if failed

        Supported formats:
            - .txt: Pipe-delimited text files
            - .csv: Comma-separated values
            - .json: JSON files in records format
        """
        # Extract file extension from filepath
        extension = filepath.split(".")[-1].lower()

        try:
            # Use pattern matching to handle different file formats
            match extension:
                case "txt":
                    df = pd.read_csv(filepath, delimiter="|")
                case "csv":
                    df = pd.read_csv(filepath)
                case "json":
                    df = pd.read_json(filepath, orient="records")
                case _:
                    raise Exception(f"Unsupported file extension {extension}")

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

        # TODO: Add logging mechanism
        return (True, df)


if __name__ == "__main__":
    # Example usage
    filepath = "incoming_data/2025-04-18/14/customer_profiles.csv"
    success, dataframe = Extractor.extract(filepath)
    print(f"[INFO] Extraction {'successful' if success else 'failed'}")
    if success:
        print(dataframe.head())  # Display first few rows of the DataFrame
