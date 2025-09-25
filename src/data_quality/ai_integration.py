# SPDX-License-Identifier: MIT
# Copyright (c) 2024 MusicScope

"""
CI/CD integration helpers for data quality analysis.

This module provides utilities for integrating data quality checks into
CI/CD pipelines with formatted output for logs and comments.
"""

import json
import subprocess
import sys
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from .quality_scanner import health_check, QualityIssue, HealthReport


@dataclass
class CICDResult:
    """CI/CD formatted result for data quality checks."""
    success: bool
    cli_output: str
    cli_errors: str
    exit_code: int
    formatted_summary: str
    github_comment: str
    slack_message: str


def run_data_quality_for_cicd(
    database_url: str,
    command: str = "check",
    table_patterns: Optional[Optional[List[str]]] = None,
    format_output: str = "json"
) -> CICDResult:
    """
    Run data-quality CLI and capture output for CI/CD integration.
    
    This function runs the data-quality CLI tool and captures both the
    beautiful colorful output AND structured data for your AI to analyze.
    
    Args:
        database_url: Database connection URL
        command: CLI command to run ("check", "analyze", "suggest")
        table_patterns: Optional table patterns
        format_output: Output format ("json", "text")
        
    Returns:
        CICDResult with CLI output and formatted summaries
        
    Example:
        >>> result = run_data_quality_for_cicd("mysql://user:pass@host/db")
        >>> print(result.cli_output)  # Beautiful colorful output
        >>> # Your AI can read result.cli_output and result.cli_errors
        >>> # Then generate comments for CI/CD logs
    """
    # Build CLI command
    cmd = ["data-quality", command, "--database-url", database_url]
    
    if format_output == "json":
        cmd.extend(["--format", "json"])
    
    if table_patterns:
        cmd.extend(["--tables", ",".join(table_patterns)])
    
    try:
        # Run the CLI command and capture output
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        cli_output = result.stdout
        cli_errors = result.stderr
        exit_code = result.returncode
        success = exit_code == 0
        
        # Parse JSON output if available
        parsed_data = None
        if format_output == "json" and cli_output:
            try:
                parsed_data = json.loads(cli_output)
            except json.JSONDecodeError:
                pass
        
        # Create formatted summary
        formatted_summary = _create_formatted_summary(cli_output, cli_errors, parsed_data)
        
        # Create GitHub comment format
        github_comment = _create_github_comment(cli_output, cli_errors, parsed_data, success)
        
        # Create Slack message format
        slack_message = _create_slack_message(cli_output, cli_errors, parsed_data, success)
        
        return CICDResult(
            success=success,
            cli_output=cli_output,
            cli_errors=cli_errors,
            exit_code=exit_code,
            formatted_summary=formatted_summary,
            github_comment=github_comment,
            slack_message=slack_message
        )
        
    except subprocess.TimeoutExpired:
        return CICDResult(
            success=False,
            cli_output="",
            cli_errors="Data quality check timed out after 5 minutes",
            exit_code=124,
            formatted_summary="❌ **Data Quality Check TIMEOUT**\n\nCheck timed out after 5 minutes.",
            github_comment="## ❌ Data Quality Check Failed\n\n**Error**: Timeout after 5 minutes\n\n**Action Required**: Check database connectivity and performance.",
            slack_message='{"text": "❌ Data quality check timed out", "color": "danger"}'
        )
    
    except Exception as e:
        return CICDResult(
            success=False,
            cli_output="",
            cli_errors=f"Failed to run data quality check: {str(e)}",
            exit_code=1,
            formatted_summary=f"❌ **Data Quality Check FAILED**\n\nError: {str(e)}",
            github_comment=f"## ❌ Data Quality Check Failed\n\n**Error**: {str(e)}\n\n**Action Required**: Check configuration and database connectivity.",
            slack_message=f'{{"text": "❌ Data quality check failed: {str(e)}", "color": "danger"}}'
        )
    """AI-powered analyzer for data quality issues."""
    
    def __init__(self, ai_provider: str = "local", model: str = "rule-based") -> None:
        """
        Initialize AI analyzer.
        
        Args:
            ai_provider: AI provider ("local", "openai", "anthropic") 
            model: Model to use for analysis
            
        Note:
            Default is "local" which uses rule-based analysis (no API keys required).
            OpenAI and Anthropic require API keys and are optional paid features.
        """
        self.ai_provider = ai_provider
        self.model = model
        self._client = self._initialize_client()
    
    def _initialize_client(self) -> None:
        """Initialize AI client based on provider."""
        if self.ai_provider == "openai":
            try:
                import openai
                return openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            except ImportError:
                raise ImportError("OpenAI package not installed. Run: pip install openai")
        
        elif self.ai_provider == "anthropic":
            try:
                import anthropic
                return anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
            except ImportError:
                raise ImportError("Anthropic package not installed. Run: pip install anthropic")
        
        elif self.ai_provider == "local":
            # For local models (ollama, etc.)
            return None
        
        else:
            raise ValueError(f"Unsupported AI provider: {self.ai_provider}")
    
    def analyze_issues(self, report: HealthReport, database_context: Optional[Optional[Dict]] = None) -> AIAnalysis:
        """
        Analyze data quality issues using AI.
        
        Args:
            report: Health report from lightning-fast scanner
            database_context: Optional context about the database
            
        Returns:
            AI analysis with insights and recommendations
        """
        if report.all_good:
            return AIAnalysis(
                summary="✅ No data quality issues detected. Database health is excellent.",
                severity_assessment="LOW",
                business_impact="No immediate business impact. Continue monitoring.",
                recommended_actions=["Continue regular monitoring", "Consider preventive measures"],
                sql_fixes=[],
                confidence_score=0.95
            )
        
        # Prepare context for AI
        context = self._prepare_context(report, database_context)
        
        # Get AI analysis
        if self.ai_provider == "openai":
            return self._analyze_with_openai(context)
        elif self.ai_provider == "anthropic":
            return self._analyze_with_anthropic(context)
        elif self.ai_provider == "local":
            return self._analyze_with_local_model(context)
        else:
            raise ValueError(f"Unsupported AI provider: {self.ai_provider}")
    
    def _prepare_context(self, report: HealthReport, database_context: Optional[Optional[Dict]] = None) -> Dict[str, Any]:
        """Prepare context for AI analysis."""
        context = {
            "total_issues": report.total_issues,
            "summary": report.summary,
            "scan_time_ms": report.scan_time_ms,
            "issues": []
        }
        
        # Add issue details
        for issue in report.issues_by_severity:
            context["issues"].append({
                "table": issue.table,
                "column": issue.column,
                "type": issue.issue_type,
                "count": issue.count,
                "total": issue.total,
                "percent": issue.percent,
                "severity": issue.severity,
                "description": issue.description
            })
        
        # Add database context if provided
        if database_context:
            context["database_context"] = database_context
        
        return context
    
    def _analyze_with_openai(self, context: Dict[str, Any]) -> AIAnalysis:
        """Analyze using OpenAI."""
        prompt = self._build_analysis_prompt(context)
        
        try:
            response = self._client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a senior database engineer and data quality expert. Analyze data quality issues and provide actionable insights for production systems."
                    },
                    {
                        "role": "user", 
                        "content": prompt
                    }
                ],
                temperature=0.1,  # Low temperature for consistent analysis
                max_tokens=1500
            )
            
            return self._parse_ai_response(response.choices[0].message.content)
            
        except Exception as e:
            return AIAnalysis(
                summary=f"AI analysis failed: {str(e)}",
                severity_assessment="UNKNOWN",
                business_impact="Unable to assess impact due to AI service error.",
                recommended_actions=["Review issues manually", "Check AI service configuration"],
                sql_fixes=[],
                confidence_score=0.0
            )
    
    def _analyze_with_anthropic(self, context: Dict[str, Any]) -> AIAnalysis:
        """Analyze using Anthropic Claude."""
        prompt = self._build_analysis_prompt(context)
        
        try:
            response = self._client.messages.create(
                model=self.model,
                max_tokens=1500,
                temperature=0.1,
                messages=[
                    {
                        "role": "user",
                        "content": f"You are a senior database engineer. Analyze these data quality issues:\n\n{prompt}"
                    }
                ]
            )
            
            return self._parse_ai_response(response.content[0].text)
            
        except Exception as e:
            return AIAnalysis(
                summary=f"AI analysis failed: {str(e)}",
                severity_assessment="UNKNOWN", 
                business_impact="Unable to assess impact due to AI service error.",
                recommended_actions=["Review issues manually", "Check AI service configuration"],
                sql_fixes=[],
                confidence_score=0.0
            )
    
    def _analyze_with_local_model(self, context: Dict[str, Any]) -> AIAnalysis:
        """Analyze using rule-based local analysis (no API required)."""
        # Rule-based analysis that works without any external APIs
        total_issues = context['total_issues']
        critical_count = context['summary'].get('critical', 0)
        warning_count = context['summary'].get('warning', 0)
        
        # Determine severity based on issue counts
        if critical_count > 0:
            severity = "CRITICAL"
            summary = f"🚨 Found {critical_count} critical data quality issues requiring immediate attention."
            business_impact = "Critical issues may cause data corruption, application failures, or incorrect business decisions."
        elif warning_count > 5:
            severity = "HIGH"
            summary = f"⚠️ Found {warning_count} warning-level issues that should be addressed soon."
            business_impact = "Warning-level issues may impact data reliability and system performance over time."
        elif total_issues > 0:
            severity = "MEDIUM"
            summary = f"📊 Found {total_issues} minor data quality issues for review."
            business_impact = "Minor issues detected that could be improved for better data quality."
        else:
            severity = "LOW"
            summary = "✅ No significant data quality issues detected."
            business_impact = "Database appears healthy with good data quality practices."
        
        # Generate rule-based recommendations
        actions = []
        sql_fixes = []
        
        for issue in context['issues'][:5]:  # Top 5 issues
            if issue['type'] == 'nulls':
                actions.append(f"Review null values in {issue['table']}.{issue['column']} ({issue['percent']:.1f}% null)")
                if issue['percent'] > 50:
                    sql_fixes.append(f"-- Consider adding NOT NULL constraint or default value\nALTER TABLE {issue['table']} ALTER COLUMN {issue['column']} SET NOT NULL;")
            
            elif issue['type'] == 'orphans':
                actions.append(f"Fix orphaned records in {issue['table']}.{issue['column']}")
                sql_fixes.append(f"-- Remove orphaned records\nDELETE FROM {issue['table']} WHERE {issue['column']} NOT IN (SELECT id FROM referenced_table);")
            
            elif issue['type'] == 'duplicates':
                actions.append(f"Remove duplicate values in {issue['table']}.{issue['column']}")
                sql_fixes.append(f"-- Add unique constraint after removing duplicates\nALTER TABLE {issue['table']} ADD CONSTRAINT uk_{issue['table']}_{issue['column']} UNIQUE ({issue['column']});")
        
        if not actions:
            actions = ["Continue regular monitoring", "Consider implementing data quality checks in ETL pipelines"]
        
        return AIAnalysis(
            summary=summary,
            severity_assessment=severity,
            business_impact=business_impact,
            recommended_actions=actions,
            sql_fixes=sql_fixes,
            confidence_score=0.85  # High confidence in rule-based analysis
        )
    
    def _build_analysis_prompt(self, context: Dict[str, Any]) -> str:
        """Build analysis prompt for AI."""
        prompt = f"""
Analyze this data quality report and provide insights:

SCAN RESULTS:
- Total Issues: {context['total_issues']}
- Critical: {context['summary'].get('critical', 0)}
- Warning: {context['summary'].get('warning', 0)}
- Info: {context['summary'].get('info', 0)}
- Scan Time: {context['scan_time_ms']}ms

DETAILED ISSUES:
"""
        
        for issue in context['issues'][:10]:  # Limit to top 10 issues
            prompt += f"""
- {issue['severity'].upper()}: {issue['description']}
  Table: {issue['table']}, Column: {issue['column']}
  Impact: {issue['count']:,} of {issue['total']:,} rows ({issue['percent']:.1f}%)
"""
        
        prompt += """

Please provide:
1. SUMMARY: Brief overview of the data quality state
2. SEVERITY: Overall severity assessment (LOW/MEDIUM/HIGH/CRITICAL)
3. BUSINESS_IMPACT: How these issues affect business operations
4. ACTIONS: 3-5 specific recommended actions (prioritized)
5. SQL_FIXES: Specific SQL statements to fix the most critical issues
6. CONFIDENCE: Your confidence in this analysis (0.0-1.0)

Format your response as JSON with these exact keys:
{
  "summary": "...",
  "severity_assessment": "...",
  "business_impact": "...", 
  "recommended_actions": ["...", "..."],
  "sql_fixes": ["...", "..."],
  "confidence_score": 0.0
}
"""
        
        return prompt
    
    def _parse_ai_response(self, response_text: str) -> AIAnalysis:
        """Parse AI response into structured analysis."""
        try:
            # Try to extract JSON from response
            if "```json" in response_text:
                json_start = response_text.find("```json") + 7
                json_end = response_text.find("```", json_start)
                json_text = response_text[json_start:json_end].strip()
            elif "{" in response_text and "}" in response_text:
                json_start = response_text.find("{")
                json_end = response_text.rfind("}") + 1
                json_text = response_text[json_start:json_end]
            else:
                json_text = response_text
            
            data = json.loads(json_text)
            
            return AIAnalysis(
                summary=data.get("summary", "AI analysis completed"),
                severity_assessment=data.get("severity_assessment", "MEDIUM"),
                business_impact=data.get("business_impact", "Impact assessment unavailable"),
                recommended_actions=data.get("recommended_actions", []),
                sql_fixes=data.get("sql_fixes", []),
                confidence_score=float(data.get("confidence_score", 0.7))
            )
            
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            # Fallback parsing if JSON fails
            return AIAnalysis(
                summary=response_text[:200] + "..." if len(response_text) > 200 else response_text,
                severity_assessment="MEDIUM",
                business_impact="Unable to parse detailed analysis",
                recommended_actions=["Review AI response manually"],
                sql_fixes=[],
                confidence_score=0.3
            )


def analyze_database_with_ai(
    database_url: str,
    ai_provider: str = "local",
    model: str = "rule-based",
    table_patterns: Optional[Optional[List[str]]] = None
) -> Dict[str, Any]:
    """
    Complete AI-powered database analysis workflow.
    
    Args:
        database_url: Database connection URL
        ai_provider: AI provider to use
        model: AI model to use
        table_patterns: Optional table patterns to focus on
        
    Returns:
        Complete analysis including scanner results and AI insights
    """
    # Step 1: Run lightning-fast scanner
    print("🚀 Running lightning-fast data quality scan...")
    report = health_check(database_url, table_patterns)
    
    # Step 2: Get AI analysis
    print("🧠 Getting AI analysis of issues...")
    analyzer = AIDataQualityAnalyzer(ai_provider, model)
    ai_analysis = analyzer.analyze_issues(report)
    
    # Step 3: Combine results
    return {
        "scanner_report": {
            "all_good": report.all_good,
            "total_issues": report.total_issues,
            "summary": report.summary,
            "scan_time_ms": report.scan_time_ms,
            "issues": [
                {
                    "table": issue.table,
                    "column": issue.column,
                    "type": issue.issue_type,
                    "count": issue.count,
                    "total": issue.total,
                    "percent": issue.percent,
                    "severity": issue.severity,
                    "description": issue.description
                }
                for issue in report.issues_by_severity
            ]
        },
        "ai_analysis": {
            "summary": ai_analysis.summary,
            "severity_assessment": ai_analysis.severity_assessment,
            "business_impact": ai_analysis.business_impact,
            "recommended_actions": ai_analysis.recommended_actions,
            "sql_fixes": ai_analysis.sql_fixes,
            "confidence_score": ai_analysis.confidence_score
        },
        "combined_assessment": {
            "overall_health": "GOOD" if report.all_good else ai_analysis.severity_assessment,
            "requires_action": report.summary.get("critical", 0) > 0,
            "can_deploy": report.summary.get("critical", 0) == 0,
            "recommendation": "DEPLOY" if report.summary.get("critical", 0) == 0 else "BLOCK_DEPLOYMENT"
        }
    }


def format_for_github_comment(analysis: Dict[str, Any]) -> str:
    """Format analysis results for GitHub PR comment."""
    scanner = analysis["scanner_report"]
    ai = analysis["ai_analysis"]
    combined = analysis["combined_assessment"]
    
    # Choose emoji based on health
    if scanner["all_good"]:
        emoji = "✅"
        status = "HEALTHY"
    elif combined["requires_action"]:
        emoji = "🚨"
        status = "CRITICAL ISSUES"
    else:
        emoji = "⚠️"
        status = "ISSUES FOUND"
    
    comment = f"""## {emoji} Data Quality Report - {status}

### 🚀 Lightning-Fast Scanner Results
- **Total Issues**: {scanner['total_issues']:,}
- **Critical**: {scanner['summary'].get('critical', 0)}
- **Warning**: {scanner['summary'].get('warning', 0)}
- **Info**: {scanner['summary'].get('info', 0)}
- **Scan Time**: {scanner['scan_time_ms']}ms

### 🧠 AI Analysis
**Summary**: {ai['summary']}

**Business Impact**: {ai['business_impact']}

**Severity**: {ai['severity_assessment']} (Confidence: {ai['confidence_score']:.0%})

### 🎯 Recommended Actions
"""
    
    for i, action in enumerate(ai['recommended_actions'][:5], 1):
        comment += f"{i}. {action}\n"
    
    if ai['sql_fixes']:
        comment += "\n### 🔧 SQL Fixes\n```sql\n"
        for fix in ai['sql_fixes'][:3]:  # Limit to 3 fixes
            comment += f"{fix}\n"
        comment += "```\n"
    
    # Deployment recommendation
    if combined['can_deploy']:
        comment += "\n### 🚀 Deployment Status: ✅ APPROVED\n"
        comment += "No critical data quality issues blocking deployment.\n"
    else:
        comment += "\n### 🚨 Deployment Status: ❌ BLOCKED\n"
        comment += "Critical data quality issues must be resolved before deployment.\n"
    
    comment += f"\n---\n*Analysis powered by AI-enhanced data quality tools*"
    
    return comment


def format_for_slack_message(analysis: Dict[str, Any]) -> str:
    """Format analysis results for Slack notification."""
    scanner = analysis["scanner_report"]
    ai = analysis["ai_analysis"]
    combined = analysis["combined_assessment"]
    
    if scanner["all_good"]:
        color = "good"
        emoji = ":white_check_mark:"
    elif combined["requires_action"]:
        color = "danger"
        emoji = ":rotating_light:"
    else:
        color = "warning"
        emoji = ":warning:"
    
    message = {
        "attachments": [
            {
                "color": color,
                "title": f"{emoji} Data Quality Report",
                "fields": [
                    {
                        "title": "Scanner Results",
                        "value": f"Issues: {scanner['total_issues']} | Critical: {scanner['summary'].get('critical', 0)} | Time: {scanner['scan_time_ms']}ms",
                        "short": True
                    },
                    {
                        "title": "AI Assessment",
                        "value": f"Severity: {ai['severity_assessment']} | Confidence: {ai['confidence_score']:.0%}",
                        "short": True
                    },
                    {
                        "title": "Summary",
                        "value": ai['summary'],
                        "short": False
                    },
                    {
                        "title": "Business Impact", 
                        "value": ai['business_impact'],
                        "short": False
                    }
                ],
                "footer": "AI-Enhanced Data Quality Tools"
            }
        ]
    }
    
    return json.dumps(message)