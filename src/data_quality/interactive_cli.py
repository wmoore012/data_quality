"""
Interactive CLI for data-quality with beautiful menus and colors.

Provides a rich, menu-driven interface for database quality scanning.
"""

import os
import sys
from typing import List, Optional

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.prompt import Prompt, Confirm
    from rich.progress import Progress, SpinnerColumn, TextColumn
    from rich.text import Text
    from rich.layout import Layout
    from rich.align import Align
    from rich import box
    from rich.columns import Columns
except ImportError:
    print("❌ Rich library not found. Installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "rich"])
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.prompt import Prompt, Confirm
    from rich.progress import Progress, SpinnerColumn, TextColumn
    from rich.text import Text
    from rich.layout import Layout
    from rich.align import Align
    from rich import box
    from rich.columns import Columns

from .quality_scanner import health_check, scan_nulls, scan_orphans
from .schema_analyzer import analyze_schema, suggest_improvements

console = Console()


class InteractiveDataQuality:
    """Interactive CLI for data quality scanning."""
    
    def __init__(self):
        self.db_url = None
        self.connected = False
        
    def show_banner(self):
        """Display the application banner."""
        banner = Text.assemble(
            ("🔍 ", "bright_magenta"),
            ("Data Quality ", "bright_cyan bold"),
            ("Scanner", "bright_white bold"),
            (" 📊", "bright_yellow")
        )
        
        panel = Panel(
            Align.center(banner),
            box=box.DOUBLE,
            border_style="bright_magenta",
            padding=(1, 2)
        )
        
        console.print()
        console.print(panel)
        console.print()
        
    def show_connection_status(self):
        """Show current database connection status."""
        if self.connected:
            status = Text("✅ Connected", style="bright_green bold")
            db_info = Text(f"Database: {self.db_url.split('/')[-1]}", style="dim")
        else:
            status = Text("❌ Not Connected", style="bright_red bold")
            db_info = Text("No database connection", style="dim")
            
        console.print(Panel(
            Text.assemble(status, "\n", db_info),
            title="Connection Status",
            border_style="blue"
        ))
        console.print()
        
    def connect_database(self):
        """Handle database connection setup."""
        console.print(Panel(
            "⚙️ Database Setup",
            style="bright_blue bold",
            border_style="blue"
        ))
        
        # Check for existing DATABASE_URL
        existing_url = os.getenv("DATABASE_URL")
        if existing_url:
            console.print(f"Found existing DATABASE_URL: [dim]{existing_url}[/dim]")
            use_existing = Confirm.ask("Use existing DATABASE_URL?", default=True)
            if use_existing:
                self.db_url = existing_url
                self.connected = True
                console.print("✅ [green]Connected successfully![/green]")
                return
        
        # Manual connection setup
        console.print("\n[yellow]Enter database connection details:[/yellow]")
        
        host = Prompt.ask("Host", default="127.0.0.1")
        port = Prompt.ask("Port", default="3307")
        username = Prompt.ask("Username", default="wmoore012")
        password = Prompt.ask("Password", password=True)
        database = Prompt.ask("Database name", default="icatalog_public")
        
        self.db_url = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
        self.connected = True
        
        console.print("✅ [green]Connection configured![/green]")
        console.print()
        
    def show_main_menu(self) -> str:
        """Display the main menu and get user choice."""
        
        menu_options = [
            ("1", "🩺 Full Health Checkup", "Complete database scan - find ALL issues at once"),
            ("2", "🔬 Deep Table Dive", "Microscopic analysis of ONE specific table"),
            ("3", "🕳️ Missing Data Hunter", "Find empty/null fields that shouldn't be empty"),
            ("4", "🔗 Broken Link Detective", "Find orphaned records with bad foreign keys"),
            ("5", "🤖 AI Schema Doctor", "Get smart recommendations to fix your database"),
            ("6", "📈 Performance Optimizer", "Find slow queries and suggest indexes"),
            ("7", "🎵 Music Data Validator", "Special checks for music industry data"),
            ("8", "🔧 Database Tools", "Connection settings and utilities"),
            ("q", "🚪 Exit", "Leave the application")
        ]
        
        # Create menu table
        table = Table(
            title="🎯 Main Menu",
            box=box.ROUNDED,
            border_style="bright_cyan",
            title_style="bright_cyan bold"
        )
        
        table.add_column("Option", style="bright_yellow bold", width=8)
        table.add_column("Action", style="bright_white bold", width=25)
        table.add_column("Description", style="dim", width=40)
        
        for option, action, description in menu_options:
            table.add_row(option, action, description)
            
        console.print(table)
        console.print()
        
        choice = Prompt.ask(
            "Select an option",
            choices=[opt[0] for opt in menu_options],
            default="1"
        )
        
        return choice
        
    def run_health_check(self):
        """Run comprehensive health check with progress indicator."""
        if not self.connected:
            console.print("❌ [red]Please connect to a database first![/red]")
            return
            
        console.print(Panel(
            "🔍 Scanning Your Database",
            style="bright_green bold",
            border_style="green"
        ))
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Scanning database...", total=None)
            
            try:
                report = health_check(self.db_url)
                progress.update(task, description="✅ Scan complete!")
                
            except Exception as e:
                console.print(f"❌ [red]Error: {str(e)}[/red]")
                return
                
        console.print()
        
        # Display results
        if report.all_good:
            result_panel = Panel(
                "🎉 Excellent! No data quality issues found.",
                style="bright_green bold",
                border_style="green",
                title="Results"
            )
        else:
            # Create summary table
            summary_table = Table(box=box.SIMPLE)
            summary_table.add_column("Severity", style="bold")
            summary_table.add_column("Count", justify="right", style="bold")
            
            summary_table.add_row("🔴 Critical", str(report.summary.get('critical', 0)), style="red")
            summary_table.add_row("🟡 Warning", str(report.summary.get('warning', 0)), style="yellow")
            summary_table.add_row("🔵 Info", str(report.summary.get('info', 0)), style="blue")
            summary_table.add_row("📊 Total", str(report.total_issues), style="bright_white bold")
            
            result_panel = Panel(
                summary_table,
                title=f"Found {report.total_issues} Issues",
                border_style="red" if report.summary.get('critical', 0) > 0 else "yellow"
            )
            
        console.print(result_panel)
        
        # Show detailed issues if any
        if not report.all_good and len(report.issues_by_severity) > 0:
            console.print()
            show_details = Confirm.ask("Show detailed issue breakdown?", default=True)
            
            if show_details:
                issues_table = Table(
                    title="🔍 Detailed Issues",
                    box=box.ROUNDED,
                    border_style="yellow"
                )
                
                issues_table.add_column("Severity", width=10)
                issues_table.add_column("Table", width=20)
                issues_table.add_column("Column", width=15)
                issues_table.add_column("Issue", width=50)
                
                for issue in report.issues_by_severity[:20]:  # Show top 20
                    severity_style = {
                        'critical': 'red bold',
                        'warning': 'yellow',
                        'info': 'blue'
                    }.get(issue.severity, 'white')
                    
                    severity_icon = {
                        'critical': '🔴',
                        'warning': '🟡', 
                        'info': '🔵'
                    }.get(issue.severity, '⚪')
                    
                    issues_table.add_row(
                        f"{severity_icon} {issue.severity.upper()}",
                        issue.table,
                        issue.column or "N/A",
                        issue.description,
                        style=severity_style
                    )
                    
                console.print(issues_table)
                
                if len(report.issues_by_severity) > 20:
                    console.print(f"\n[dim]... and {len(report.issues_by_severity) - 20} more issues[/dim]")
        
        console.print(f"\n⏱️  Scan completed in {report.scan_time_ms}ms")
        self.pause()
        
    def run_schema_analysis(self):
        """Run schema analysis on a specific table."""
        if not self.connected:
            console.print("❌ [red]Please connect to a database first![/red]")
            return
            
        console.print(Panel(
            "📊 Table Analysis",
            style="bright_blue bold",
            border_style="blue"
        ))
        
        table_name = Prompt.ask("Enter table name to analyze")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task(f"Analyzing {table_name}...", total=None)
            
            try:
                analysis = analyze_schema(self.db_url, table_name)
                progress.update(task, description="✅ Analysis complete!")
                
            except Exception as e:
                console.print(f"❌ [red]Error: {str(e)}[/red]")
                return
                
        console.print()
        
        # Display results in a beautiful format
        result_table = Table(
            title=f"📊 Analysis Results for '{table_name}'",
            box=box.DOUBLE,
            border_style="bright_blue"
        )
        
        result_table.add_column("Aspect", style="bright_cyan bold", width=20)
        result_table.add_column("Details", width=60)
        
        # Natural keys
        natural_keys = ", ".join(analysis.natural_keys) if analysis.natural_keys else "None detected"
        result_table.add_row("🔑 Natural Keys", natural_keys)
        
        # Normalization level
        nf_color = "green" if analysis.normalization_level >= 3 else "yellow" if analysis.normalization_level == 2 else "red"
        result_table.add_row("📐 Normalization", f"[{nf_color}]{analysis.normalization_level}NF[/{nf_color}]")
        
        # Boolean columns
        boolean_cols = ", ".join(analysis.boolean_columns) if analysis.boolean_columns else "None"
        result_table.add_row("✅ Boolean Columns", boolean_cols)
        
        # Fact table candidate
        fact_status = "Yes 📊" if analysis.fact_table_candidate else "No"
        result_table.add_row("📈 Fact Table", fact_status)
        
        console.print(result_table)
        
        # Show recommendations
        if analysis.recommendations:
            console.print()
            recs_table = Table(
                title="🚀 Recommendations",
                box=box.ROUNDED,
                border_style="green"
            )
            
            recs_table.add_column("Priority", width=12)
            recs_table.add_column("Recommendation", width=60)
            
            for rec in analysis.recommendations:
                priority_icon = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(rec.priority, "⚪")
                priority_style = {"high": "red bold", "medium": "yellow", "low": "green"}.get(rec.priority, "white")
                
                recs_table.add_row(
                    f"{priority_icon} {rec.priority.upper()}",
                    rec.description,
                    style=priority_style
                )
                
            console.print(recs_table)
        else:
            console.print("\n✨ [green bold]No recommendations - schema looks excellent![/green bold]")
            
        self.pause()
        
    def run_ai_suggestions(self):
        """Get AI-powered suggestions for multiple tables."""
        if not self.connected:
            console.print("❌ [red]Please connect to a database first![/red]")
            return
            
        console.print(Panel(
            "💡 Smart Recommendations",
            style="bright_magenta bold",
            border_style="magenta"
        ))
        
        tables_input = Prompt.ask("Enter table names (comma-separated)", default="songs,albums,artists")
        table_list = [t.strip() for t in tables_input.split(",")]
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("🧠 AI analyzing schemas...", total=None)
            
            try:
                suggestions = suggest_improvements(self.db_url, table_list, use_ai=True)
                progress.update(task, description="✅ AI analysis complete!")
                
            except Exception as e:
                console.print(f"❌ [red]Error: {str(e)}[/red]")
                return
                
        console.print()
        
        if not suggestions:
            console.print("✨ [green bold]No suggestions - your schema is already optimized![/green bold]")
            self.pause()
            return
            
        # Group suggestions by priority
        high_priority = [s for s in suggestions if s.priority == "high"]
        medium_priority = [s for s in suggestions if s.priority == "medium"]
        low_priority = [s for s in suggestions if s.priority == "low"]
        
        for priority_group, priority_name, color, icon in [
            (high_priority, "HIGH PRIORITY", "red", "🔴"),
            (medium_priority, "MEDIUM PRIORITY", "yellow", "🟡"),
            (low_priority, "LOW PRIORITY", "green", "🟢"),
        ]:
            if priority_group:
                console.print(Panel(
                    f"{icon} {priority_name} ({len(priority_group)} suggestions)",
                    style=f"{color} bold",
                    border_style=color
                ))
                
                for i, suggestion in enumerate(priority_group, 1):
                    console.print(f"\n{i}. [bold]{suggestion.description}[/bold]")
                    if suggestion.benefits:
                        console.print(f"   💡 Benefits: {', '.join(suggestion.benefits)}")
                    console.print(f"   ⚡ Effort: [cyan]{suggestion.effort_level}[/cyan]")
                    
                console.print()
                
        self.pause()
        
    def show_quick_stats(self):
        """Show quick database statistics."""
        if not self.connected:
            console.print("❌ [red]Please connect to a database first![/red]")
            return
            
        console.print(Panel(
            "📈 Database Overview",
            style="bright_green bold",
            border_style="green"
        ))
        
        # This is a simplified version - in a real implementation,
        # you'd query the database for actual statistics
        stats_table = Table(box=box.ROUNDED, border_style="green")
        stats_table.add_column("Metric", style="bright_cyan bold")
        stats_table.add_column("Value", style="bright_white bold")
        
        stats_table.add_row("🗄️ Database", self.db_url.split('/')[-1])
        stats_table.add_row("🔗 Connection", "Active ✅")
        stats_table.add_row("📈 Status", "Ready for analysis")
        
        console.print(stats_table)
        self.pause()
        
    def pause(self):
        """Pause and wait for user input."""
        console.print()
        Prompt.ask("Press Enter to continue", default="")
        console.clear()
        
    def run(self):
        """Main application loop."""
        console.clear()
        self.show_banner()
        
        # Auto-connect if DATABASE_URL exists
        database_url = os.getenv("DATABASE_URL")
        if database_url:
            self.db_url = database_url
            self.connected = True
            console.print(f"✅ [green]Auto-connected to: {database_url.split('/')[-1]}[/green]")
            console.print()
            
        while True:
            self.show_connection_status()
            choice = self.show_main_menu()
            
            if choice == "1":
                self.run_health_check()
            elif choice == "2":
                self.run_schema_analysis()
            elif choice == "3":
                console.print("🚨 [yellow]Missing data scan - Coming soon![/yellow]")
                self.pause()
            elif choice == "4":
                console.print("🔗 [yellow]Reference checking - Coming soon![/yellow]")
                self.pause()
            elif choice == "5":
                self.run_ai_suggestions()
            elif choice == "6":
                self.connect_database()
            elif choice == "7":
                self.show_quick_stats()
            elif choice == "q":
                console.print("\n👋 [bright_cyan]Thanks for using Data Quality Scanner![/bright_cyan]")
                break
                
            console.clear()


def main():
    """Entry point for the interactive CLI."""
    try:
        app = InteractiveDataQuality()
        app.run()
    except KeyboardInterrupt:
        console.print("\n\n👋 [bright_cyan]Goodbye![/bright_cyan]")
    except Exception as e:
        console.print(f"\n❌ [red]Unexpected error: {str(e)}[/red]")


if __name__ == "__main__":
    main()