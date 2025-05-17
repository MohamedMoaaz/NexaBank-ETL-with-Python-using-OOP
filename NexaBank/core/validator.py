"""
validator.py

This module defines the `Validator` class which validates a DataFrame against
a structured schema. It supports:

- Header and datatype validation
- Value validation using range, enum, regex
- Foreign schema resolution (reference to other schemas)
- Custom function-based validations
- Formatted error reporting and optional callback integration
"""

import pandas as pd
import re
import logging
from copy import deepcopy
from typing import Callable

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

try:
    from core.validator_func import FUNC
except ImportError:
    from validator_func import FUNC


class Validator:
    """
    A class for validating DataFrame contents against predefined schemas.

    Attributes:
        _base_schema (dict): A processed schema with compiled patterns and resolved ranges.
        _error_callback (Callable): Optional callback function triggered on validation failure.
    """

    def __init__(
        self,
        base_schema: dict[str, dict],
        error_callback: Callable[[str, str], None] = None,
    ):
        """
        Initialize validator with schema and optional error callback.

        Args:
            base_schema (dict): Validation rules for each dataset type.
            error_callback (Callable): Function to call on validation errors (optional).
        """
        self._base_schema: dict[str, dict] = self._process_schema(base_schema)
        self._error_callback: Callable[[str, str], None] = error_callback
        logger.info("Validator initialized with schema keys: %s", list(self._base_schema.keys()))

    def _process_schema(self, base_schema: dict[str, dict]) -> dict[str, dict]:
        """
        Convert raw schema into a processed version for efficient validation.
        Enums become sets, ranges become range objects, regexes are compiled.

        Args:
            base_schema (dict): The original schema dictionary.

        Returns:
            dict: A modified version ready for validation.
        """
        logger.debug("Processing schema...")
        base_schema = deepcopy(base_schema)

        def _process_cfg(cfg: dict) -> None:
            if "enum" in cfg:
                cfg["enum"] = set(cfg["enum"])
            if "range" in cfg:
                a, b = cfg["range"]
                cfg["range"] = range(a, b + 1)
            if "regex" in cfg:
                cfg["regex"] = re.compile(cfg["regex"])

        for entry in base_schema.values():
            for cfg in entry.values():
                _process_cfg(cfg)

        logger.debug("Schema processing complete.")
        return base_schema

    def _get_foreign_schema(self, cfg: dict) -> dict:
        """
        Recursively resolve foreign schema references.

        Args:
            cfg (dict): A column's configuration that contains a "foreign" reference.

        Returns:
            dict: The resolved configuration from another schema.
        """
        entry, item = cfg["foreign"].split(".")
        logger.debug(f"Resolving foreign schema reference: {entry}.{item}")
        foreign_schema = self._base_schema[entry][item]

        if "foreign" in foreign_schema:
            return self._get_foreign_schema(foreign_schema)
        else:
            return foreign_schema

    def _validate_header(self, df: pd.DataFrame, schema: dict) -> bool:
        """
        Validate DataFrame column names and data types against the schema.

        Args:
            df (pd.DataFrame): The DataFrame to check.
            schema (dict): Schema definitions for the current dataset.

        Returns:
            bool: True if column names and data types are valid, else False.
        """
        DTYPE = {"float": "float64", "int": "int64"}

        logger.info("Validating DataFrame header...")
        try:
            df[:] = df[schema.keys()]
            for (header, cfg), dtype in zip(schema.items(), df.dtypes):
                type_ = cfg["type"] if "foreign" not in cfg else self._get_foreign_schema(cfg)["type"]
                expected_dtype = DTYPE.get(type_, "object")

                if dtype != expected_dtype:
                    logger.error(f"[FAIL] {header} has incorrect datatype (expected {expected_dtype}, got {dtype})")
                    return False

        except (KeyError, IndexError, Exception) as e:
            logger.error(f"[FAIL] Header validation error: {e}")
            return False

        logger.info("Header validation passed.")
        return True

    def _validate_row(self, row: pd.Series, schema: dict) -> dict:
        """
        Validate a single row against the schema.

        Args:
            row (pd.Series): A row from the DataFrame.
            schema (dict): Schema definition.

        Returns:
            dict: A dictionary with field names and corresponding error messages.
        """
        errors = {}

        def _run(value, cfg) -> dict:
            valid, error = FUNC[cfg["func"]](value, cfg)
            return {"valid": valid, "error": error}

        def _validator(key, cfg):
            value = row[key]

            if "foreign" in cfg:
                _validator(key, self._get_foreign_schema(cfg))

            if "range" in cfg and value not in cfg["range"]:
                errors[key] = f"is an invalid {cfg['range']}"

            if "enum" in cfg and value not in cfg["enum"]:
                errors[key] = "is an invalid choice"

            if "regex" in cfg and not cfg["regex"].fullmatch(value):
                errors[key] = f"has an invalid format ({cfg['format']})"

            if "func" in cfg and not (result := _run(value, cfg))["valid"]:
                errors[key] = result["error"]

        for key, cfg in schema.items():
            _validator(key, cfg)

        return errors

    def _format_error(self, invalid_rows: pd.DataFrame, errors: pd.Series) -> str:
        """
        Format validation errors into a readable report.

        Args:
            invalid_rows (pd.DataFrame): The invalid rows from the DataFrame.
            errors (pd.Series): A series containing error dicts per row.

        Returns:
            str: A formatted string report.
        """
        logger.debug("Formatting error report...")
        report_lines = []

        for idx, error_dict in errors.items():
            row = invalid_rows.loc[idx]
            report_lines.append(f"\nRow ({idx + 1})")
            for field, msg in error_dict.items():
                value = row[field]
                report_lines.append(f'  - {field}: "{value}" {msg}.')

        return "\n".join(report_lines)

    def validate(self, df: pd.DataFrame, filepath: str) -> bool:
        """
        Validate the given DataFrame against the inferred schema (based on filename).

        Args:
            df (pd.DataFrame): The data to validate.
            filepath (str): Filepath used to infer schema key and for error reporting.

        Returns:
            bool: True if all rows are valid, False otherwise.
        """
        key = filepath.replace("\\", "/").split("/")[-1].split(".")[0].lower()
        schema = self._base_schema[key]
        logger.info(f"Validating file: {filepath} (inferred key: {key})")

        if not self._validate_header(df, schema):
            logger.error(f"Validation failed: header mismatch in file {filepath}")
            return False

        errors = df.apply(lambda row: self._validate_row(row, schema), axis=1)
        invalid_rows = df[errors.apply(bool)]
        errors = errors[errors.apply(bool)]

        if not errors.empty:
            logger.warning(f"Validation failed: {len(errors)} invalid rows in {filepath}")
            report = self._format_error(invalid_rows, errors)
            if self._error_callback:
                self._error_callback(filepath, report)
        else:
            logger.info(f"Validation passed: all rows valid in {filepath}")

        return errors.empty

if __name__ == "__main__":
    """
    Test harness to run validation on a sample file.
    """
    import yaml
    from extractor import Extractor

    try:
        with open("data/schema.yaml") as fp:
            SCHEMA = yaml.safe_load(fp)
    except (FileNotFoundError, yaml.YAMLError) as e:
        print(f"[EXIT] {e}")
        exit(1)

    def send_email_test(filename, report) -> None:
        print(filename)
        print(report)

    filepath = "incoming_data/2025-04-29/21/transactions.json"
    extractor = Extractor()
    flag, data = extractor.extract(filepath)
    print(f"> Extractor: {flag}")

    validator = Validator(SCHEMA, error_callback=send_email_test)
    flag = validator.validate(data, filepath)
    print(f"> Validator: {flag}")