# SPDX-License-Identifier: MIT
# Copyright (c) 2024 Wilton Moore

"""
Input validation utilities for robust error handling.

This module provides comprehensive input validation functions that raise
clear, actionable error messages when validation fails.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Pattern,
    Sequence,
    TypeGuard,
    Union,
    overload,
)

from .exceptions import ValidationError

# Pre-compiled regex patterns for performance
_EMAIL_PATTERN: Pattern[str] = re.compile(
    r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
)
_URL_PATTERN: Pattern[str] = re.compile(
    r"^https?://(?:[-\w.])+(?:[:\d]+)?(?:/(?:[\w/_.])*(?:\?(?:[\w&=%.])*)?(?:#(?:\w*))?)?$"
)


def validate_not_none(value: Any, field_name: str) -> Any:
    """
    Validate that a value is not None.

    Args:
        value: The value to validate
        field_name: Name of the field for error messages

    Returns:
        The validated value

    Raises:
        ValidationError: If value is None
    """
    if value is None:
        raise ValidationError(
            field_name, value, "non-None value", f"Provide a valid {field_name} value"
        )
    return value


def validate_string(
    value: Any,
    field_name: str,
    min_length: int = 1,
    max_length: Optional[Optional[int]] = None,
    pattern: Optional[Optional[str]] = None,
) -> str:
    """
    Validate that a value is a string with optional constraints.

    Args:
        value: The value to validate
        field_name: Name of the field for error messages
        min_length: Minimum string length (default: 1)
        max_length: Maximum string length (optional)
        pattern: Regex pattern the string must match (optional)

    Returns:
        The validated string

    Raises:
        ValidationError: If validation fails
    """
    if not isinstance(value, str):
        raise ValidationError(
            field_name,
            value,
            "string",
            f"Convert {field_name} to string or provide string input",
        )

    if len(value) < min_length:
        raise ValidationError(
            field_name,
            value,
            f"string with at least {min_length} characters",
            f"Provide a longer {field_name} (current: {len(value)} chars)",
        )

    if max_length and len(value) > max_length:
        raise ValidationError(
            field_name,
            value,
            f"string with at most {max_length} characters",
            f"Shorten {field_name} (current: {len(value)} chars, max: {max_length})",
        )

    if pattern and not re.match(pattern, value):
        raise ValidationError(
            field_name,
            value,
            f"string matching pattern '{pattern}'",
            f"Ensure {field_name} follows the required format",
        )

    return value


@overload
def validate_number(
    value: Any,
    field_name: str,
    min_value: Optional[Optional[int]] = None,
    max_value: Optional[Optional[int]] = None,
    allow_zero: bool = True,
    number_type: type[int] = int,
) -> int:
    ...


@overload
def validate_number(
    value: Any,
    field_name: str,
    min_value: Optional[Optional[float]] = None,
    max_value: Optional[Optional[float]] = None,
    allow_zero: bool = True,
    number_type: type[float] = float,
) -> float:
    ...


def validate_number(
    value: Any,
    field_name: str,
    min_value: Optional[Optional[Union[int, float]]] = None,
    max_value: Optional[Optional[Union[int, float]]] = None,
    allow_zero: bool = True,
    number_type: type = float,
) -> Union[int, float]:
    """
    Validate that a value is a number with optional constraints.

    Args:
        value: The value to validate
        field_name: Name of the field for error messages
        min_value: Minimum allowed value (optional)
        max_value: Maximum allowed value (optional)
        allow_zero: Whether zero is allowed (default: True)
        number_type: Expected number type (int or float, default: float)

    Returns:
        The validated number

    Raises:
        ValidationError: If validation fails
    """
    try:
        if number_type == int:
            validated_value = int(value)
        else:
            validated_value = float(value)
    except (ValueError, TypeError):
        raise ValidationError(
            field_name,
            value,
            f"{number_type.__name__}",
            f"Provide a valid numeric value for {field_name}",
        )

    if not allow_zero and validated_value == 0:
        raise ValidationError(
            field_name,
            value,
            "non-zero number",
            f"Provide a non-zero value for {field_name}",
        )

    if min_value is not None and validated_value < min_value:
        raise ValidationError(
            field_name,
            value,
            f"number >= {min_value}",
            f"Increase {field_name} to at least {min_value}",
        )

    if max_value is not None and validated_value > max_value:
        raise ValidationError(
            field_name,
            value,
            f"number <= {max_value}",
            f"Reduce {field_name} to at most {max_value}",
        )

    return validated_value


def validate_int(
    value: Any,
    field_name: str,
    min_value: Optional[Optional[int]] = None,
    max_value: Optional[Optional[int]] = None,
    allow_zero: bool = True,
) -> int:
    """Validate integer values with type narrowing."""
    return validate_number(value, field_name, min_value, max_value, allow_zero, int)


def validate_float(
    value: Any,
    field_name: str,
    min_value: Optional[Optional[float]] = None,
    max_value: Optional[Optional[float]] = None,
    allow_zero: bool = True,
) -> float:
    """Validate float values with type narrowing."""
    return validate_number(value, field_name, min_value, max_value, allow_zero, float)


def validate_threshold(value: Any, field_name: str, scale_0_to_1: bool = True) -> float:
    """
    Validate threshold values (0-1 or 0-100 scale).

    Args:
        value: The threshold value to validate
        field_name: Name of the field for error messages
        scale_0_to_1: If True, expect 0-1 scale; if False, expect 0-100 scale

    Returns:
        The validated threshold value
    """
    max_val = 1.0 if scale_0_to_1 else 100.0
    return validate_float(value, field_name, min_value=0.0, max_value=max_val)


def validate_sequence_not_empty(value: Any, field_name: str) -> Sequence[Any]:
    """
    Validate that a sequence is not empty.

    Args:
        value: The sequence to validate
        field_name: Name of the field for error messages

    Returns:
        The validated sequence

    Raises:
        ValidationError: If sequence is empty or not a sequence
    """
    if not hasattr(value, "__len__") or not hasattr(value, "__iter__"):
        raise ValidationError(
            field_name,
            value,
            "sequence (list, tuple, etc.)",
            f"Provide a sequence for {field_name}",
        )

    if len(value) == 0:
        raise ValidationError(
            field_name,
            value,
            "non-empty sequence",
            f"Provide at least one item in {field_name}",
        )

    return value


def is_valid_string(value: Any) -> TypeGuard[str]:
    """Type guard for string validation."""
    return isinstance(value, str) and len(value.strip()) > 0


def validate_email(value: Any, field_name: str) -> str:
    """
    Validate email address format.

    Args:
        value: The email to validate
        field_name: Name of the field for error messages

    Returns:
        The validated email string
    """
    email_str = validate_string(value, field_name)

    if not _EMAIL_PATTERN.match(email_str):
        raise ValidationError(
            field_name,
            value,
            "valid email address",
            f"Provide a valid email format for {field_name}",
        )

    return email_str


def validate_url(value: Any, field_name: str) -> str:
    """
    Validate URL format.

    Args:
        value: The URL to validate
        field_name: Name of the field for error messages

    Returns:
        The validated URL string
    """
    url_str = validate_string(value, field_name)

    if not _URL_PATTERN.match(url_str):
        raise ValidationError(
            field_name,
            value,
            "valid URL",
            f"Provide a valid URL format for {field_name}",
        )

    return url_str


def validate_path(value: Any, field_name: str, must_exist: bool = False) -> Path:
    """
    Validate that a value is a valid file path.

    Args:
        value: The value to validate
        field_name: Name of the field for error messages
        must_exist: Whether the path must exist on filesystem

    Returns:
        The validated Path object

    Raises:
        ValidationError: If validation fails
    """
    try:
        path = Path(value)
    except (TypeError, ValueError):
        raise ValidationError(
            field_name,
            value,
            "valid file path",
            f"Provide a valid path string for {field_name}",
        )

    if must_exist and not path.exists():
        raise ValidationError(
            field_name, value, "existing file path", f"Ensure the path exists: {path}"
        )

    return path


def validate_dict(
    value: Any,
    field_name: str,
    required_keys: Optional[Optional[List[str]]] = None,
    allowed_keys: Optional[Optional[List[str]]] = None,
) -> Dict[str, Any]:
    """
    Validate that a value is a dictionary with optional key constraints.

    Args:
        value: The value to validate
        field_name: Name of the field for error messages
        required_keys: Keys that must be present (optional)
        allowed_keys: Only these keys are allowed (optional)

    Returns:
        The validated dictionary

    Raises:
        ValidationError: If validation fails
    """
    if not isinstance(value, dict):
        raise ValidationError(
            field_name, value, "dictionary", f"Provide a dictionary for {field_name}"
        )

    if required_keys:
        missing_keys = set(required_keys) - set(value.keys())
        if missing_keys:
            raise ValidationError(
                field_name,
                value,
                f"dictionary with keys: {required_keys}",
                f"Add missing keys to {field_name}: {list(missing_keys)}",
            )

    if allowed_keys:
        extra_keys = set(value.keys()) - set(allowed_keys)
        if extra_keys:
            raise ValidationError(
                field_name,
                value,
                f"dictionary with only allowed keys: {allowed_keys}",
                f"Remove extra keys from {field_name}: {list(extra_keys)}",
            )

    return value
