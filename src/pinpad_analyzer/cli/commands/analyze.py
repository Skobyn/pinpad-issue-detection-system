"""Analyze command: run rules + ML on ingested data."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from pinpad_analyzer.storage.database import Database, resolve_db_path

console = Console()


def analyze(
    db_path: str = typer.Option(
        "", help="Database path (local file or md:name for MotherDuck). Env: PINPAD_DB"
    ),
    file: str = typer.Option("", help="Filter to specific file name"),
    lane: int = typer.Option(0, help="Filter to specific lane"),
    company: str = typer.Option("", help="Filter by company ID"),
    store: str = typer.Option("", help="Filter by store ID"),
) -> None:
    """Run rule-based analysis on ingested data to detect issues."""
    try:
        effective_db = resolve_db_path(db_path)
        with Database(effective_db) as db:
            _analyze(db, file, lane, company, store)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


def _analyze(db: Database, file_filter: str, lane: int, company: str = "", store: str = "") -> None:
    from pinpad_analyzer.ml.rules import RuleEngine

    # Get file IDs to analyze
    query = "SELECT file_id, file_name, lane, log_date FROM log_files"
    conditions = []
    params = []
    if company:
        conditions.append("company_id = ?")
        params.append(company)
    if store:
        conditions.append("(store_id = ? OR store_id = '' OR store_id IS NULL)")
        params.append(store)
    if file_filter:
        conditions.append("file_name LIKE ?")
        params.append(f"%{file_filter}%")
    if lane > 0:
        conditions.append("lane = ?")
        params.append(lane)
    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    files = db.conn.execute(query, params).fetchall()
    if not files:
        console.print("[yellow]No matching files found. Run 'ingest' first.[/yellow]")
        return

    engine = RuleEngine(db)

    for file_id, file_name, file_lane, log_date in files:
        console.print(f"\n[bold]Analyzing {file_name}[/bold] (lane {file_lane}, {log_date})")
        issues = engine.analyze_file(file_id)

        if not issues:
            console.print("  [green]No issues detected.[/green]")
            continue

        table = Table(title=f"Issues Detected ({len(issues)})")
        table.add_column("Severity", style="bold")
        table.add_column("Issue Type")
        table.add_column("Confidence", justify="right")
        table.add_column("Time Range")
        table.add_column("Evidence")

        for issue in sorted(issues, key=lambda x: x["severity_rank"]):
            sev_color = {"critical": "red", "high": "red", "medium": "yellow", "low": "dim"}.get(
                issue["severity"], "white"
            )
            table.add_row(
                f"[{sev_color}]{issue['severity'].upper()}[/{sev_color}]",
                issue["issue_type"],
                f"{issue['confidence']:.0%}",
                issue.get("time_range", ""),
                issue.get("evidence", "")[:80],
            )
        console.print(table)
