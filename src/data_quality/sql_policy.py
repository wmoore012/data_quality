# SPDX-License-Identifier: MIT
# Copyright (c) 2024 Wilton Moore

"""
AST-based SQL policy enforcement for secure database operations.

This module provides AST analysis to detect unsafe SQL construction patterns
and enforce parameterized query usage as recommended by OWASP.
"""

import ast
from typing import List


class SQLSecurityViolation(Exception):
    """Raised when unsafe SQL construction is detected."""

    def __init__(self, message: str, line: int, column: int, code: str) -> None:
        super().__init__(message)
        self.message = message
        self.line = line
        self.column = column
        self.code = code


class SQLPolicyChecker(ast.NodeVisitor):
    """AST visitor that checks for unsafe SQL construction patterns."""

    def __init__(self) -> None:
        self.violations: List[SQLSecurityViolation] = []
        self.sql_execute_methods = {
            "execute",
            "executemany",
            "execute_many",
            "exec_driver_sql",
        }

    def visit_Call(self, node: ast.Call) -> None:
        """Check function calls for unsafe SQL patterns."""
        # Check if this is a database execute call
        if self._is_sql_execute_call(node):
            self._check_sql_execute_args(node)

        # Continue visiting child nodes
        self.generic_visit(node)

    def _is_sql_execute_call(self, node: ast.Call) -> bool:
        """Check if this is a SQL execute method call."""
        if isinstance(node.func, ast.Attribute):
            return node.func.attr in self.sql_execute_methods
        elif isinstance(node.func, ast.Name):
            return node.func.id in self.sql_execute_methods
        return False

    def _check_sql_execute_args(self, node: ast.Call) -> None:
        """Check arguments to SQL execute calls for unsafe patterns."""
        if not node.args:
            return

        # First argument should be the SQL statement
        sql_arg = node.args[0]

        # Check for unsafe SQL construction patterns
        if self._is_unsafe_sql_construction(sql_arg):
            violation = SQLSecurityViolation(
                message="Unsafe SQL construction detected. Use text() with bound parameters instead.",
                line=node.lineno,
                column=node.col_offset,
                code="SQL001",
            )
            self.violations.append(violation)

    def _is_unsafe_sql_construction(self, node: ast.AST) -> bool:
        """Check if a node represents unsafe SQL construction."""
        # f-strings are unsafe
        if isinstance(node, ast.JoinedStr):
            return True

        # String formatting with % is unsafe
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod):
            return True

        # .format() calls are unsafe
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr == "format":
                return True

        # String concatenation with variables is unsafe
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            if self._contains_variable(node.left) or self._contains_variable(
                node.right
            ):
                return True

        # Only allow text() calls with string literals
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id == "text":
                # text() is safe if first arg is a string literal
                if node.args and isinstance(node.args[0], ast.Constant):
                    if isinstance(node.args[0].value, str):
                        return False  # This is safe
                return True  # text() with non-literal is unsafe
            elif isinstance(node.func, ast.Attribute) and node.func.attr == "text":
                # sqlalchemy.text() - same rules
                if node.args and isinstance(node.args[0], ast.Constant):
                    if isinstance(node.args[0].value, str):
                        return False
                return True

        # String literals are safe
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return False

        # Variables/expressions are potentially unsafe
        return self._contains_variable(node)

    def _contains_variable(self, node: ast.AST) -> bool:
        """Check if a node contains variables or expressions."""
        if (
            isinstance(node, ast.Name)
            or isinstance(node, ast.Attribute)
            or isinstance(node, ast.Call)
        ):
            return True
        elif isinstance(node, ast.BinOp):
            return self._contains_variable(node.left) or self._contains_variable(
                node.right
            )
        elif isinstance(node, ast.Constant):
            return False
        return True


def check_sql_security(
    code: str, filename: str = "<string>"
) -> List[SQLSecurityViolation]:
    """
    Check Python code for SQL security violations.

    Args:
        code: Python source code to analyze
        filename: Filename for error reporting

    Returns:
        List of security violations found

    Example:
        >>> code = '''
        ... conn.execute(f"SELECT * FROM users WHERE id = {user_id}")
        ... '''
        >>> violations = check_sql_security(code)
        >>> print(violations[0].message)
        Unsafe SQL construction detected. Use text() with bound parameters instead.
    """
    try:
        tree = ast.parse(code, filename=filename)
    except SyntaxError as e:
        return [
            SQLSecurityViolation(
                message=f"Syntax error: {e.msg}",
                line=e.lineno or 0,
                column=e.offset or 0,
                code="SYNTAX",
            )
        ]

    checker = SQLPolicyChecker()
    checker.visit(tree)
    return checker.violations


def check_file_sql_security(filepath: str) -> List[SQLSecurityViolation]:
    """
    Check a Python file for SQL security violations.

    Args:
        filepath: Path to Python file to analyze

    Returns:
        List of security violations found
    """
    try:
        with open(filepath, encoding="utf-8") as f:
            code = f.read()
        return check_sql_security(code, filepath)
    except OSError as e:
        return [
            SQLSecurityViolation(
                message=f"Failed to read file: {e}", line=0, column=0, code="IO_ERROR"
            )
        ]


def validate_sql_patterns() -> None:
    """
    Validate that common SQL patterns are correctly identified.

    This function serves as both documentation and validation of the policy checker.
    """
    # Safe patterns
    safe_patterns = [
        'conn.execute(text("SELECT * FROM users WHERE id = :id"), {"id": user_id})',
        'conn.execute("SELECT * FROM users")',  # Literal string
        'conn.execute(text("SELECT * FROM users WHERE name = :name"), params)',
    ]

    # Unsafe patterns
    unsafe_patterns = [
        'conn.execute(f"SELECT * FROM users WHERE id = {user_id}")',  # f-string
        'conn.execute("SELECT * FROM users WHERE id = %s" % user_id)',  # % formatting
        'conn.execute("SELECT * FROM users WHERE id = {}".format(user_id))',  # .format()
        'conn.execute("SELECT * FROM users WHERE id = " + str(user_id))',  # concatenation
        "conn.execute(text(query_variable))",  # text() with variable
    ]

    print("🔍 Validating SQL Security Patterns")
    print("=" * 40)

    # Test safe patterns
    for pattern in safe_patterns:
        violations = check_sql_security(pattern)
        status = "✅ SAFE" if not violations else "❌ FALSE POSITIVE"
        print(f"{status} | {pattern}")

    print()

    # Test unsafe patterns
    for pattern in unsafe_patterns:
        violations = check_sql_security(pattern)
        status = "❌ UNSAFE" if violations else "⚠️ FALSE NEGATIVE"
        print(f"{status} | {pattern}")
        if violations:
            print(f"         Violation: {violations[0].message}")


if __name__ == "__main__":
    validate_sql_patterns()
