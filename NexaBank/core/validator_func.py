"""
validator_func.py

This module provides reusable validation functions for schema-based data validation.

Each function follows a standard interface:
    (value, config) -> (bool, error_message)

It supports:
- Date format validation
- Non-negative number check
- Template for custom validations

The `FUNC` dictionary is used for dynamically invoking these functions by name.
"""

from datetime import datetime
from typing import Any, Tuple, Union, Callable

# Type aliases for better readability
ValidatorResult = Tuple[bool, str]
ValidatorFunction = Callable[[Any, dict], ValidatorResult]

def _template(value: Any, cfg: dict) -> ValidatorResult:
    """
    Template function for validation with standard return format.

    Args:
        value (Any): Value to validate.
        cfg (dict): Configuration dictionary for validation rules.

    Returns:
        Tuple[bool, str]: 
            - True if value is valid, False otherwise.
            - Error message if invalid, empty string if valid.
    """
    if value:
        return (True, "")
    else:
        return (False, "is incorrect")

def check_date(text: str, cfg: dict) -> ValidatorResult:
    """
    Validate a date string against a specified format.

    Args:
        text (str): Date string to validate.
        cfg (dict): Must contain 'format' key with a valid date format.

    Returns:
        Tuple[bool, str]: 
            - True if date is valid, False otherwise.
            - Error message if invalid.
    """
    try:
        datetime.strptime(text, cfg["format"])
        return (True, "")
    except ValueError:
        return (False, "is an invalid date")

def is_positive(number: Union[int, float], cfg: dict) -> ValidatorResult:
    """
    Validate that a numeric value is non-negative.

    Args:
        number (int | float): Number to validate.
        cfg (dict): Optional config (unused here, for API consistency).

    Returns:
        Tuple[bool, str]: 
            - True if number is positive or zero, False otherwise.
            - Error message if negative.
    """
    if number >= 0:
        return (True, "")
    else:
        return (False, "is a negative number")

# Dictionary mapping validation function names to their implementations
FUNC: dict[str, ValidatorFunction] = {
    "check_date": check_date,
    "is_positive": is_positive
}