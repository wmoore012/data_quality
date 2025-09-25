# SPDX-License-Identifier: MIT
# Copyright (c) 2024 MusicScope

"""
Custom exceptions for data-quality module.

This module provides comprehensive error handling with specific exception types
for different failure scenarios, enabling precise error handling and debugging.

Example usage:
    try:
        validate_string(value, "table_name")
    except ValidationError as e:
        if e.field == "table_name":
            # Handle table name validation specifically
            logger.error(f"Invalid table name: {e.suggestion}")
        raise
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional

__all__ = [
    "DataQualityError",
    "ValidationError", 
    "ConfigurationError",
    "ResourceError",
    "OperationError",
    "ScanError",
    "SchemaAnalysisError"
]


class DataQualityError(Exception):
    """Base exception for all data-quality errors."""
    
    def __init__(
        self, 
        message: str, 
        details: Optional[Optional[Dict[str, Any]]] = None,
        suggestion: Optional[Optional[str]] = None,
        code: Literal["unknown", "validation", "configuration", "resource", "operation"] = "unknown"
    ) -> None:
        """
        Initialize the exception with detailed error information.
        
        Args:
            message: Human-readable error message
            details: Additional error context and debugging information
            suggestion: Suggested solution or next steps
            code: Error code for programmatic handling
        """
        super().__init__(message)
        self.message = message
        self.details = details or {}
        self.suggestion = suggestion
        self.code = code
    
    def __str__(self) -> str:
        """Return formatted error message with details and suggestions."""
        result = self.message
        
        if self.details:
            details_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            result += f" (Details: {details_str})"
        
        if self.suggestion:
            result += f" Suggestion: {self.suggestion}"
        
        return result


class ValidationError(DataQualityError):
    """Raised when input validation fails."""
    
    def __init__(
        self, 
        field: str, 
        value: Any, 
        expected: str,
        suggestion: Optional[Optional[str]] = None
    ) -> None:
        """
        Initialize validation error with field-specific information.
        
        Args:
            field: Name of the field that failed validation
            value: The invalid value that was provided
            expected: Description of what was expected
            suggestion: How to fix the validation error
        """
        message = f"Invalid {field}: got {type(value).__name__} '{value}', expected {expected}"
        details = {"field": field, "value": value, "expected": expected}
        super().__init__(message, details, suggestion, "validation")
        self.field = field
        self.value = value
        self.expected = expected


class ConfigurationError(DataQualityError):
    """Raised when configuration is invalid or missing."""
    
    def __init__(
        self, 
        config_key: str, 
        issue: str,
        suggestion: Optional[Optional[str]] = None
    ) -> None:
        """
        Initialize configuration error.
        
        Args:
            config_key: The configuration key that has an issue
            issue: Description of the configuration problem
            suggestion: How to fix the configuration
        """
        message = f"Configuration error for '{config_key}': {issue}"
        details = {"config_key": config_key, "issue": issue}
        super().__init__(message, details, suggestion)
        self.config_key = config_key
        self.issue = issue


class ResourceError(DataQualityError):
    """Raised when system resources are unavailable or exhausted."""
    
    def __init__(
        self, 
        resource: str, 
        issue: str,
        current_usage: Optional[Optional[str]] = None,
        suggestion: Optional[Optional[str]] = None
    ) -> None:
        """
        Initialize resource error.
        
        Args:
            resource: The resource that is unavailable (memory, disk, network, etc.)
            issue: Description of the resource problem
            current_usage: Current resource usage information
            suggestion: How to resolve the resource issue
        """
        message = f"Resource error ({resource}): {issue}"
        details = {"resource": resource, "issue": issue}
        if current_usage:
            details["current_usage"] = current_usage
        super().__init__(message, details, suggestion)
        self.resource = resource
        self.issue = issue
        self.current_usage = current_usage


class OperationError(DataQualityError):
    """Raised when an operation fails due to business logic or external factors."""
    
    def __init__(
        self, 
        operation: str, 
        reason: str,
        retryable: bool = False,
        suggestion: Optional[Optional[str]] = None
    ) -> None:
        """
        Initialize operation error.
        
        Args:
            operation: The operation that failed
            reason: Why the operation failed
            retryable: Whether retrying the operation might succeed
            suggestion: How to resolve the operation failure
        """
        message = f"Operation '{operation}' failed: {reason}"
        details = {"operation": operation, "reason": reason, "retryable": retryable}
        super().__init__(message, details, suggestion, "operation")
        self.operation = operation
        self.reason = reason
        self.retryable = retryable


class ScanError(OperationError):
    """Raised when data quality scan fails."""
    
    def __init__(
        self, 
        table_name: str,
        scan_type: str,
        error_message: str,
        suggestion: Optional[Optional[str]] = None
    ) -> None:
        super().__init__(
            f"{scan_type}_scan", 
            f"Failed to scan table '{table_name}': {error_message}",
            suggestion=suggestion or "Check table permissions and database connectivity"
        )
        self.table_name = table_name
        self.scan_type = scan_type
        self.error_message = error_message


class SchemaAnalysisError(OperationError):
    """Raised when schema analysis fails."""
    
    def __init__(
        self, 
        schema_name: str,
        analysis_type: str,
        error_message: str,
        suggestion: Optional[Optional[str]] = None
    ) -> None:
        super().__init__(
            f"{analysis_type}_analysis", 
            f"Failed to analyze schema '{schema_name}': {error_message}",
            suggestion=suggestion or "Ensure schema exists and is accessible"
        )
        self.schema_name = schema_name
        self.analysis_type = analysis_type
        self.error_message = error_message
