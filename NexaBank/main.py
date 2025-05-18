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
import logging
from pathlib import Path

from core.extractor import Extractor
from core.validator import Validator
from core.transformer import Transformer
from core.loader import HdfsHandler
from services.folder_status import FolderStatusHandler
from services.file_listener import FileListener
from services.email_client import EmailClient

from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

load_dotenv()


def get_schema(filepath: str = "data/schema.yaml") -> dict[str]:
    try:
        with open(filepath) as fp:
            schema = yaml.safe_load(fp)
            logger.info("Schema loaded successfully from %s", filepath)
            return schema
    except FileNotFoundError:
        logger.error("Schema file not found: %s", filepath)
    except yaml.YAMLError as e:
        logger.error("Error parsing schema YAML: %s", str(e))
    return {}


def filter_incoming_files(root: str, filter: tuple[str] = tuple()) -> list[str]:
    incoming_files = []
    filter_set = set(filter)

    for parent, _, filenames in os.walk(root):
        for fn in filenames:
            filepath = Path(parent, fn)
            if filepath.stem.lower() in filter_set:
                incoming_files.append(str(filepath).replace("\\", "/"))

    logger.info(
        "Found %d incoming files matching schema filter", len(incoming_files)
    )
    return incoming_files


def validation_error_callback(filepath: str, report: str) -> None:
    logger.warning("Validation error for file %s: %s", filepath, report)
    EMAIL.send(RECEIVER_EMAIL, filepath, report)


def validate_incoming_file(filepath: str) -> None:
    logger.info("Validating file: %s", filepath)
    flag, df = Extractor.extract(filepath)

    if flag is True:
        flag = VALIDATOR.validate(df, filepath)
        if flag:
            logger.info("[PASS] File validated: %s", filepath)
        else:
            logger.warning("[FAIL] Validation failed: %s", filepath)
    else:
        logger.warning("[FAIL] Extraction failed: %s", filepath)

    STATUS[filepath]["valid"] = flag


def transform_incoming_file(filepath: str) -> None:
    if STATUS[filepath]["saved"] is True:
        logger.info("Skipping transformation (already saved): %s", filepath)
        return

    logger.info("Transforming file: %s", filepath)
    _, df = Extractor.extract(filepath)
    Transformer.transform(df, filepath)
    
    is_uploaded, log_message = LOADER.export_data(
        df, "/".join(filepath.replace("\\", "/").split("/")[-3:])
    )
    if is_uploaded:
        STATUS[filepath]["saved"] = True
        logger.info("File successfully uploaded to HDFS: %s", filepath)
    else:
        logger.warning(
            "HDFS upload failed: %s | Reason: %s", filepath, log_message
        )


def process_incoming_file(filepath: str) -> None:
    logger.info("Processing file: %s", filepath)
    if STATUS[filepath]["valid"] is None:
        validate_incoming_file(filepath)

    if STATUS[filepath]["valid"] is True:
        transform_incoming_file(filepath)

    STATUS.update(filepath)
    logger.info("Finished processing file: %s", filepath)


def process_stored_incoming_files(root: str) -> None:
    logger.info("Processing existing files in directory: %s", root)
    for filepath in filter_incoming_files(root, filter=HEADERS):
        process_incoming_file(filepath)


def main():
    FILE_LISTENER.start_thread()
    process_stored_incoming_files(root=ROOT_DIR)

    try:
        logger.info("🟢 Pipeline is running. Press Ctrl+C to stop.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down pipeline...")


if __name__ == "__main__":
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

    main()