"""
data_pipeline.py

This module implements an end-to-end file processing pipeline that:
- Loads and parses a YAML schema definition.
- Monitors an input directory for new files matching the schema headers.
- Extracts, validates, and transforms data files.
- Uploads processed files to an HDFS container.
- Tracks file statuses during the pipeline using a centralized handler.

Key Components:
- Extractor: Handles file reading and DataFrame extraction.
- Validator: Validates data against a predefined schema.
- Transformer: Transforms and saves valid data.
- FolderStatusHandler: Tracks the validation and saving status of each file.
- FileListener: Monitors a folder and triggers processing on file arrival.
"""

import os
import yaml
import time
from pathlib import Path

from core.extractor import Extractor
from core.validator import Validator
from core.transformer import Transformer
from core.loader import HdfsHandler
from services.folder_status import FolderStatusHandler
from services.file_listener import FileListener
from services.email_client import EmailClient

from dotenv import load_dotenv

load_dotenv()


def get_schema(filepath: str = "data/schema.yaml") -> dict[str]:
    """
    Load the YAML schema file.

    Args:
        filepath (str): Path to the schema YAML file.

    Returns:
        dict[str]: Dictionary mapping expected file headers to validation rules.
    """
    try:
        with open(filepath) as fp:
            return yaml.safe_load(fp)
    except (FileNotFoundError, yaml.YAMLError) as e:
        print(e)
        return {}


def filter_incoming_files(root: str, filter: tuple[str] = tuple()) -> list[str]:
    """
    Recursively scan a directory and collect file paths matching the given filter.

    Args:
        root (str): Root directory to scan.
        filter (tuple[str]): Set of accepted file stem names (without extension).

    Returns:
        list[str]: List of matching file paths.
    """
    incoming_files = []
    filter_set = set(filter)

    for parent, _, filenames in os.walk(root):
        for fn in filenames:
            filepath = Path(parent, fn)
            if filepath.stem.lower() in filter_set:
                incoming_files.append(str(filepath).replace("\\", "/"))

    return incoming_files


def validation_error_callback(filepath: str, report: str) -> None:
    """
    Callback function invoked when validation fails.

    Args:
        filepath (str): Path to the file being validated.
        report (str): Error report.
    """
    EMAIL.send(RECEIVER_EMAIL, filepath, report)


def validate_incoming_file(filepath: str) -> None:
    """
    Extract and validate an incoming file.

    Updates the `STATUS` dictionary with the validation result.

    Args:
        filepath (str): Path to the file to validate.
    """
    flag, df = Extractor.extract(filepath)

    if flag is True:
        flag = VALIDATOR.validate(df, filepath)
        if flag:
            print("[PASS] Validated")
        else:
            print("[FAIL] Cannot validate")
    else:
        print("[FAIL] Cannot extract")

    STATUS[filepath]["valid"] = flag


def transform_incoming_file(filepath: str) -> None:
    """
    Transform and upload the data file if it hasn't been saved already.

    Args:
        filepath (str): Path to the file to transform.
    """
    if STATUS[filepath]["saved"] is True:
        return

    _, df = Extractor.extract(filepath)
    Transformer.transform(df, filepath)

    is_uploaded, log_message = LOADER.export_data(
        df, filepath.replace("\\", "/").removeprefix("incoming_data/")
    )
    if is_uploaded:
        STATUS[filepath]["saved"] = True


def process_incoming_file(filepath: str) -> None:
    """
    Validate and transform a single incoming file.

    Args:
        filepath (str): Path to the file to process.
    """
    if STATUS[filepath]["valid"] is None:
        validate_incoming_file(filepath)

    if STATUS[filepath]["valid"] is True:
        transform_incoming_file(filepath)

    STATUS.update(filepath)


def process_stored_incoming_files(root: str) -> None:
    """
    Process all files already present in the incoming directory.

    Args:
        root (str): Root directory to scan and process files from.
    """
    for filepath in filter_incoming_files(root, filter=HEADERS):
        process_incoming_file(filepath)


# --- Pipeline Initialization ---

ROOT_DIR = "./incoming_data"
SCHEMA = get_schema()
HEADERS = tuple(SCHEMA.keys())
EMAIL = EmailClient()
RECEIVER_EMAIL = os.getenv("EMAIL_ADDRESS")

STATUS = FolderStatusHandler(HEADERS)
VALIDATOR = Validator(
    base_schema=SCHEMA, error_callback=validation_error_callback
)
LOADER = HdfsHandler("/user/hive/warehouse", "master1", "/tmp/hdfs_export")
FILE_LISTENER = FileListener(
    ROOT_DIR, filter=HEADERS, callback=process_incoming_file
)
FILE_LISTENER.start_thread()

process_stored_incoming_files(root=ROOT_DIR)
while True:
    time.sleep(1)

