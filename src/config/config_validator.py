from pathlib import Path
from typing import Dict, List, Any


def validate_required_vars(required_vars: Dict[str, Any]) -> None:
    """
    Validates that all required variables are not None or empty.
    """
    missing_vars = [key for key, val in required_vars.items() if not val]
    if missing_vars:
        raise ValueError(f"Missing required variable(s) : {', '.join(missing_vars)}")


def validate_value_is_allowed(value: str, allowed_values: List[str]) -> None:
    """
    Validates that a variable value is within a list of allowed values.
    """
    if value not in allowed_values:
        raise ValueError(
            f"Invalid value : {value}. Must be {' or '.join(allowed_values)}"
        )


def validate_positive_value(vars_dict: Dict[str, int]) -> None:
    """
    Validates that a variable value is greater than 0.
    """
    for key, val in vars_dict.items():
        if not isinstance(val, int):
            raise ValueError(f"Invalid value for {key}={val}. Must be integer")
        if val <= 0:
            raise ValueError(f"Invalid value for {key}={val}. Must be greater than 0")


def validate_file_exists(file_path: Path) -> None:
    """
    Validates that a file exists at the given path.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Input file '{file_path}' does not exist")
    if not file_path.is_file():
        raise FileNotFoundError(f"Input file '{file_path}' is not a file")
