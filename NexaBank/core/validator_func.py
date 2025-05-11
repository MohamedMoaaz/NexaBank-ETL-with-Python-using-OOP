from datetime import datetime
from typing import Any, Tuple, Union, Callable

# Type aliases for better readability
ValidatorResult = Tuple[bool, str]
ValidatorFunction = Callable[[Any, dict], ValidatorResult]

def _template(value: Any, cfg: dict) -> ValidatorResult:
    """
    Template function for validation with standard return format.
    
    Args:
        value: Value to validate
        cfg: Configuration dictionary for validation rules
        
    Returns:
        Tuple containing:
        - bool: Validation result (True if valid, False if invalid)
        - str: Error message if invalid, empty string if valid
    """
    if value:
        return (True, "")
    else:
        return (False, "is incorrect")

def check_date(text: str, cfg: dict) -> ValidatorResult:
    """
    Validate a date string against a specified format.
    
    Args:
        text: Date string to validate
        cfg: Configuration dictionary containing 'format' key
        
    Returns:
        Tuple containing validation result and error message
    """
    try:
        datetime.strptime(text, cfg["format"])
        return (True, "")
    except ValueError:
        return (False, "is an invalid date")

def is_positive(number: Union[int, float], cfg: dict) -> ValidatorResult:
    """
    Check if a number is non-negative.
    
    Args:
        number: Numeric value to validate
        cfg: Configuration dictionary (unused but kept for consistency)
        
    Returns:
        Tuple containing validation result and error message
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
