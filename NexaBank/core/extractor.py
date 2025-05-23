"""
extractor.py

This module defines the `Extractor` class, which provides a utility method
to load structured data from various file formats into a pandas DataFrame.

Supported file types:
- CSV (.csv)
- All delimited TXT (.txt)
- JSON (.json, records-oriented)

The `extract` method handles exceptions gracefully and returns a success flag
along with the DataFrame (if extraction is successful).
"""

import pandas as pd 
import json
import logging
from typing import Tuple
from pathlib import Path
import sys
import csv

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs.log'),
        logging.StreamHandler(sys.stdout)  # Also print to console
    ]
)

logger = logging.getLogger(__name__)


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
            filepath (str): Path to the file to be extracted.

        Returns:
            Tuple[bool, DataFrame | None]: 
                - A boolean indicating success or failure.
                - A pandas DataFrame if extraction was successful, otherwise None.

        Supported formats:
            - .txt: All delimited text files
            - .csv: Comma-separated values
            - .json: JSON files in records format
        """
        try:
            # Log attempt to process file
            logger.info(f"Attempting to extract data from: {filepath}")
            
            extension = Path(filepath).suffix[1:].lower()  # More robust path handling
            
            match extension:
                case "csv" | "txt":
                    with open(filepath, "r") as f:
                        sample = f.read(2048)
                        try:
                            dialect = csv.Sniffer().sniff(sample)
                            delimiter = dialect.delimiter
                        except csv.Error:
                            delimiter = ","
                        f.seek(0)
                        df = pd.read_csv(f, delimiter=delimiter)
                case "json":
                    logger.debug("Processing JSON file")
                    df = pd.read_json(filepath, orient="records")
                case _:
                    error_msg = f"Unsupported file extension: {extension}"
                    logger.error(error_msg)
                    return (False, None)

            
            logger.info(f"Successfully extracted data from {filepath}")
            logger.debug(f"Extracted DataFrame shape: {df.shape}")
            return (True, df)

        except FileNotFoundError as e:
            logger.error(f"File not found: {filepath} - {str(e)}")
            return (False, None)

        except pd.errors.EmptyDataError as e:
            logger.error(f"File is empty: {filepath} - {str(e)}")
            return (False, None)

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format in file: {filepath} - {str(e)}")
            return (False, None)

        except pd.errors.ParserError as e:
            logger.error(f"Error parsing file: {filepath} - {str(e)}")
            return (False, None)

        except UnicodeDecodeError as e:
            logger.error(f"File encoding error in {filepath} - {str(e)}")
            return (False, None)

        except Exception as e:
            logger.exception(f"Unexpected error processing {filepath}: {str(e)}")
            return (False, None)


if __name__ == "__main__":
    # Example usage
    filepath = r"E:\incoming_data\2025-04-18\14\customer_profiles.csv"
    success, dataframe = Extractor.extract(filepath)
    
    logger.info(f"Extraction {'successful' if success else 'failed'}")
