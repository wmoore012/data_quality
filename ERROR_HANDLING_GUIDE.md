<!-- SPDX-License-Identifier: MIT
Copyright (c) 2025 Perday CatalogLAB™ -->

# Error Handling Guide

This guide provides best practices for error handling in data quality operations, including retry strategies, logging practices, and exception mapping.

## Exception Mapping & Actions

| Exception Type | Typical Caller Action | HTTP Status | Log Level | Retry? |
|----------------|----------------------|-------------|-----------|---------|
| `ValidationError` | Fix input and retry | 400 Bad Request | WARNING | No |
| `ConfigurationError` | Update config and restart | 500 Internal Error | ERROR | No |
| `ResourceError` | Wait and retry or scale up | 503 Service Unavailable | ERROR | Yes (with backoff) |
| `OperationError` | Check `retryable` field | 500 Internal Error | ERROR | Conditional |
| `ScanError` | Check permissions/connectivity | 500 Internal Error | ERROR | Yes (limited) |
| `SchemaAnalysisError` | Verify schema exists | 404 Not Found | WARNING | No |

## Retry Strategy with Exponential Backoff

Use this pattern for retryable operations:

```python
import random
import time
from typing import TypeVar, Callable, Any

from data_quality.exceptions import OperationError, ResourceError

T = TypeVar('T')

def retry_with_backoff(
    func: Callable[..., T],
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: bool = True,
    retryable_exceptions: tuple = (ResourceError, OperationError)
) -> T:
    """
    Retry a function with exponential backoff and jitter.

    Args:
        func: Function to retry
        max_attempts: Maximum number of attempts
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
        jitter: Add random jitter to prevent thundering herd
        retryable_exceptions: Exception types that should trigger retry

    Returns:
        Result of successful function call

    Raises:
        Last exception if all attempts fail
    """
    last_exception = None

    for attempt in range(max_attempts):
        try:
            return func()
        except retryable_exceptions as e:
            last_exception = e

            # Check if this specific error is retryable
            if hasattr(e, 'retryable') and not e.retryable:
                raise

            if attempt == max_attempts - 1:
                # Last attempt, don't wait
                break

            # Calculate delay with exponential backoff
            delay = min(base_delay * (2 ** attempt), max_delay)

            # Add jitter to prevent thundering herd
            if jitter:
                delay *= (0.5 + random.random() * 0.5)

            time.sleep(delay)

    # All attempts failed
    raise last_exception

# Usage example
def scan_table_with_retry(engine, table_name):
    return retry_with_backoff(
        lambda: scan_table(engine, table_name),
        max_attempts=3,
        retryable_exceptions=(ResourceError, OperationError)
    )
```

## Logging Best Practices

### Safe Logging with Redaction

Never log sensitive information. Use this pattern:

```python
import logging
import re
from typing import Any, Dict

# Sensitive patterns to redact
SENSITIVE_PATTERNS = [
    (re.compile(r'(password["\']?\s*[:=]\s*["\']?)([^"\'&\s]+)', re.IGNORECASE), r'\1***REDACTED***'),
    (re.compile(r'(token["\']?\s*[:=]\s*["\']?)([^"\'&\s]+)', re.IGNORECASE), r'\1***REDACTED***'),
    (re.compile(r'(key["\']?\s*[:=]\s*["\']?)([^"\'&\s]+)', re.IGNORECASE), r'\1***REDACTED***'),
    (re.compile(r'(mysql://[^:]+:)([^@]+)(@)', re.IGNORECASE), r'\1***REDACTED***\3'),
    (re.compile(r'(postgresql://[^:]+:)([^@]+)(@)', re.IGNORECASE), r'\1***REDACTED***\3'),
]

def redact_sensitive_info(message: str) -> str:
    """Redact sensitive information from log messages."""
    for pattern, replacement in SENSITIVE_PATTERNS:
        message = pattern.sub(replacement, message)
    return message

def safe_log_error(logger: logging.Logger, message: str, details: Dict[str, Any] = None):
    """Log error with automatic sensitive data redaction."""
    safe_message = redact_sensitive_info(message)

    if details:
        # Redact details as well
        safe_details = {
            k: redact_sensitive_info(str(v)) if isinstance(v, str) else v
            for k, v in details.items()
        }
        logger.error(f"{safe_message} - Details: {safe_details}")
    else:
        logger.error(safe_message)

# Usage
logger = logging.getLogger(__name__)
try:
    connect_to_database("mysql://user:secret@localhost/db")
except Exception as e:
    safe_log_error(logger, f"Database connection failed: {e}")
```

### Structured Logging

Use structured logging for better observability:

```python
import logging
import json
from typing import Any, Dict

class StructuredFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            'timestamp': self.formatTime(record),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
        }

        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)

        # Add custom fields
        if hasattr(record, 'operation'):
            log_entry['operation'] = record.operation
        if hasattr(record, 'table_name'):
            log_entry['table_name'] = record.table_name
        if hasattr(record, 'duration_ms'):
            log_entry['duration_ms'] = record.duration_ms

        return json.dumps(log_entry)

# Setup structured logging
def setup_logging():
    logger = logging.getLogger('data_quality')
    handler = logging.StreamHandler()
    handler.setFormatter(StructuredFormatter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

# Usage with extra context
logger = setup_logging()
logger.info(
    "Table scan completed",
    extra={
        'operation': 'null_scan',
        'table_name': 'users',
        'duration_ms': 1250,
        'rows_scanned': 10000
    }
)
```

## Exception Chaining

Always preserve the original exception context:

```python
from data_quality.exceptions import ScanError

def scan_table(engine, table_name):
    try:
        # Database operation
        result = engine.execute(f"SELECT COUNT(*) FROM {table_name}")
        return result.scalar()
    except SQLAlchemyError as e:
        # Chain the original exception
        raise ScanError(
            table_name=table_name,
            scan_type="count",
            error_message=str(e),
            suggestion="Check table exists and permissions are correct"
        ) from e  # Important: preserve original exception
    except Exception as e:
        # For unexpected errors, chain but don't hide implementation details
        raise ScanError(
            table_name=table_name,
            scan_type="count",
            error_message=f"Unexpected error: {type(e).__name__}",
            suggestion="Check logs for full error details"
        ) from e

# When you want to hide implementation details
def public_api_function():
    try:
        internal_operation()
    except InternalError as e:
        # Hide internal details from public API
        raise ValidationError(
            field="input",
            value="user_input",
            expected="valid format",
            suggestion="Check input format"
        ) from None  # Use 'from None' to hide internal exception
```

## Context Managers for Resource Cleanup

Use context managers for proper resource cleanup:

```python
from contextlib import contextmanager
from typing import Generator
import logging

@contextmanager
def database_operation(engine, operation_name: str) -> Generator[Any, None, None]:
    """Context manager for database operations with proper cleanup and logging."""
    logger = logging.getLogger(__name__)
    start_time = time.time()

    logger.info(f"Starting {operation_name}")

    try:
        with engine.begin() as conn:
            yield conn

        duration = (time.time() - start_time) * 1000
        logger.info(
            f"Completed {operation_name}",
            extra={'operation': operation_name, 'duration_ms': duration}
        )

    except Exception as e:
        duration = (time.time() - start_time) * 1000
        logger.error(
            f"Failed {operation_name}: {e}",
            extra={'operation': operation_name, 'duration_ms': duration}
        )
        raise

# Usage
with database_operation(engine, "null_scan") as conn:
    result = conn.execute("SELECT COUNT(*) FROM users WHERE email IS NULL")
    null_count = result.scalar()
```

## Testing Error Conditions

Test your error handling:

```python
import pytest
from unittest.mock import Mock, patch
from sqlalchemy.exc import SQLAlchemyError

from data_quality.exceptions import ScanError
from data_quality.quality_scanner import scan_nulls

def test_scan_nulls_handles_database_error():
    """Test that database errors are properly wrapped."""
    mock_engine = Mock()
    mock_engine.execute.side_effect = SQLAlchemyError("Connection lost")

    with pytest.raises(ScanError) as exc_info:
        scan_nulls(mock_engine, "test_table")

    error = exc_info.value
    assert error.table_name == "test_table"
    assert error.scan_type == "null"
    assert "Connection lost" in error.error_message
    assert error.suggestion is not None

def test_retry_mechanism():
    """Test retry with exponential backoff."""
    call_count = 0

    def failing_function():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ResourceError("resource", "temporarily unavailable", retryable=True)
        return "success"

    result = retry_with_backoff(failing_function, max_attempts=3)
    assert result == "success"
    assert call_count == 3
```

## OWASP Logging Guidelines

Follow OWASP logging best practices:

1. **Never log sensitive data**: passwords, tokens, PII, connection strings
2. **Validate log inputs**: prevent log injection attacks
3. **Use appropriate log levels**: DEBUG < INFO < WARNING < ERROR < CRITICAL
4. **Include context**: operation, user, timestamp, correlation IDs
5. **Centralize logging**: use structured formats for log aggregation
6. **Monitor logs**: set up alerts for ERROR and CRITICAL events

## References

- [OWASP Logging Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html)
- [Python Logging Best Practices](https://docs.python.org/3/howto/logging.html)
- [Exponential Backoff and Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)
