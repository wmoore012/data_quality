# SPDX-License-Identifier: MIT
# Copyright (c) 2024 MusicScope

"""
Pydantic v2 models for data quality reports with JSON Schema support.
"""

from pydantic import BaseModel, Field
from typing import List, Literal, Dict, Any, Optional


class Issue(BaseModel):
    """Represents a single data quality issue."""
    id: str
    severity: Literal["critical", "warning", "info"]
    table: str
    column: str | None = None
    kind: Literal["nulls", "duplicate", "orphan", "schema"]
    count: int = Field(ge=0)
    details: Dict[str, Any] = Field(default_factory=dict)


class Report(BaseModel):
    """Complete data quality report."""
    tool_version: str
    db_dialect: str
    issues: List[Issue]
    
    def has_critical(self) -> bool:
        """Check if report has critical issues."""
        return any(issue.severity == "critical" for issue in self.issues)
    
    def has_warnings(self) -> bool:
        """Check if report has warning issues."""
        return any(issue.severity == "warning" for issue in self.issues)
    
    def render(self, format: str = "text") -> str:
        """Render report in specified format."""
        if format == "json":
            return self.model_dump_json(indent=2)
        elif format == "text":
            return self._render_text()
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def _render_text(self) -> str:
        """Render report as human-readable text."""
        if not self.issues:
            return "✅ No data quality issues found!"
        
        lines = [f"📊 Data Quality Report ({self.db_dialect})", "=" * 40]
        
        by_severity = {"critical": [], "warning": [], "info": []}
        for issue in self.issues:
            by_severity[issue.severity].append(issue)
        
        for severity, issues in by_severity.items():
            if not issues:
                continue
                
            icon = {"critical": "🚨", "warning": "⚠️", "info": "ℹ️"}[severity]
            lines.append(f"\n{icon} {severity.upper()} ({len(issues)})")
            
            for issue in issues:
                location = f"{issue.table}.{issue.column}" if issue.column else issue.table
                lines.append(f"  • {location}: {issue.kind} ({issue.count})")
        
        return "\n".join(lines)


def get_json_schema() -> Dict[str, Any]:
    """Get JSON Schema for Report model (Draft 2020-12)."""
    return Report.model_json_schema()