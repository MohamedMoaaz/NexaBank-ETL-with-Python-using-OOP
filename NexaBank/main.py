import os
import yaml
from pathlib import Path

from core.extractor import Extractor
from core.validator import Validator
from core.transformer import Transformer
from services.folder_status import FolderStatusHandler
from services.file_listener import FileListener


def get_schema(filepath: str = "data/schema.yaml") -> dict[str]:
    try:
        with open(filepath) as fp:
            return yaml.safe_load(fp)
    except (FileNotFoundError, yaml.YAMLError) as e:
        print(e)
        return {}


def filter_incoming_files(root: str, filter: tuple[str] = tuple()) -> list[str]:
    incoming_files = []
    filter_set = set(filter)

    for parent, _, filenames in os.walk(root):
        for fn in filenames:
            filepath = Path(parent, fn)
            if filepath.stem.lower() in filter_set:
                incoming_files.append(str(filepath).replace("\\", "/"))

    return incoming_files


def validation_error_callback(filepath: str, report: str) -> None:
    print("ERROR")
    print(filepath)
    print(report)


def validate_incoming_file(filepath: str) -> None:
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
    if STATUS[filepath]["saved"] is True:
        print("[INFO] Already saved")
        return

    _, df = Extractor.extract(filepath)
    Transformer.transform(df, filepath)

    print("[INFO] Upload to HDFS container")
    is_uploaded = True
    if is_uploaded:
        STATUS[filepath]["saved"] = True


def process_incoming_file(filepath: str) -> None:
    print(f"[INFO] Processing '{filepath}'")

    if STATUS[filepath]["valid"] is None:
        validate_incoming_file(filepath)

    if STATUS[filepath]["valid"] is True:
        transform_incoming_file(filepath)

    STATUS.update(filepath)


def process_stored_incoming_files(root: str) -> None:
    for filepath in filter_incoming_files(root, filter=HEADERS):
        process_incoming_file(filepath)


ROOT_DIR = "./incoming_data"
SCHEMA = get_schema()
HEADERS = tuple(SCHEMA.keys())

STATUS = FolderStatusHandler(HEADERS)
VALIDATOR = Validator(base_schema=SCHEMA, error_callback=validation_error_callback)

FILE_LISTENER = FileListener(ROOT_DIR, filter=HEADERS, callback=process_incoming_file)
FILE_LISTENER.start_thread()

process_stored_incoming_files(root=ROOT_DIR)
