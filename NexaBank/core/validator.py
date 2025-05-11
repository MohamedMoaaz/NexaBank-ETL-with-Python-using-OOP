import pandas as pd
import re
from copy import deepcopy
from typing import Callable

try:
    from core.validator_func import FUNC
except ImportError:
    from validator_func import FUNC


class Validator:
    """
    A class for validating DataFrame contents against predefined schemas.
    Supports type checking, range validation, enum validation, and custom validation functions.
    """

    def __init__(
        self,
        base_schema: dict[str, dict],
        error_callback: Callable[[str, str], None] = None,
    ):
        """
        Initialize validator with schema and optional error callback.

        Args:
            base_schema: Dictionary containing validation rules for different data types
            error_callback: Optional function to call when validation errors occur
        """
        self._base_schema: dict[str, dict] = self._process_schema(base_schema)
        self._error_callback: Callable[[str, str], None] = error_callback

    def _process_schema(self, base_schema: dict[str, dict]) -> dict[str, dict]:
        """
        Process and prepare schema for validation.
        Converts enums to sets, ranges to range objects, and compiles regex patterns.
        """
        base_schema = deepcopy(base_schema)

        def _process_cfg(cfg: dict) -> None:
            """Process individual configuration entries."""
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

        return base_schema

    def _get_foreign_schema(self, cfg: dict) -> dict:
        """
        Resolve foreign schema references recursively.

        Args:
            cfg: Configuration dictionary containing foreign schema reference

        Returns:
            Resolved schema configuration
        """
        entry, item = cfg["foreign"].split(".")
        foreign_schema = self._base_schema[entry][item]

        if "foreign" in foreign_schema:
            return self._get_foreign_schema(foreign_schema)
        else:
            return foreign_schema

    def _validate_header(self, df: pd.DataFrame, schema: dict) -> bool:
        """
        Validate DataFrame headers and column types against schema.

        Args:
            df: DataFrame to validate
            schema: Schema to validate against

        Returns:
            True if validation passes, False otherwise
        """
        DTYPE = {"float": "float64", "int": "int64"}

        try:
            df[:] = df[schema.keys()]  # Validate and reorder--if needed

            # Check datatype of each column
            for (header, cfg), dtype in zip(schema.items(), df.dtypes):
                type_ = (
                    cfg["type"]
                    if "foreign" not in cfg
                    else self._get_foreign_schema(cfg)["type"]
                )

                if dtype != DTYPE.get(type_, "object"):
                    print(f"[FAIL] {header} has incorrect datatype")
                    return False

        except KeyError as e:
            print(f"[FAIL] {e}")
            return False
        except IndexError as e:
            print(f"[FAIL] {e}")
            return False
        except Exception as e:
            print(f"[FAIL] {e}")
            return False

        return True

    def _validate_row(self, row: pd.Series, schema: dict) -> dict:
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

    def _format_error(self, invalid_rows: pd.DataFrame, errors: pd.DataFrame):
        report_lines = []

        for idx, error_dict in errors.items():
            row = invalid_rows.loc[idx]
            report_lines.append(f"\nRow ({idx + 1})")
            for field, msg in error_dict.items():
                value = row[field]
                report_lines.append(f'  - {field}: "{value}" {msg}.')

        return "\n".join(report_lines)

    def validate(self, df: pd.DataFrame, filepath: str) -> bool:
        key = filepath.replace("\\", "/").split("/")[-1].split(".")[0].lower()
        schema = self._base_schema[key]

        if not self._validate_header(df, schema):
            return False

        errors = df.apply(
            lambda row: self._validate_row(row, schema),
            axis=1,
        )

        invalid_rows = df[errors.apply(bool)]
        errors = errors[errors.apply(bool)]

        if not errors.empty:
            report = self._format_error(invalid_rows, errors)
            self._error_callback and self._error_callback(filepath, report)

        return errors.empty


if __name__ == "__main__":
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
