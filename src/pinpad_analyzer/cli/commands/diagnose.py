"""Diagnose command: technician assistant for root cause analysis."""

from __future__ import annotations

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from pinpad_analyzer.storage.database import Database, resolve_db_path

console = Console()


def diagnose(
    db_path: str = typer.Option(
        "", help="Database path (local file or md:name for MotherDuck). Env: PINPAD_DB"
    ),
    company: str = typer.Option("", help="Company ID"),
    store: str = typer.Option("", help="Store ID"),
    lane: int = typer.Option(0, help="Lane number"),
    time: str = typer.Option("", help="Incident time (YYYY-MM-DD HH:MM)"),
    symptom: str = typer.Option("", help="Symptom description"),
) -> None:
    """Interactive diagnosis: provide context to get AI-powered root cause analysis."""
    try:
        effective_db = resolve_db_path(db_path)
        with Database(effective_db) as db:
            _diagnose(db, company, store, lane, time, symptom)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


def _diagnose(
    db: Database, company: str, store: str, lane: int, time_str: str, symptom: str
) -> None:
    from pinpad_analyzer.assistant.diagnosis import DiagnosisEngine

    engine = DiagnosisEngine(db)

    # Build query context
    context = {
        "company_id": company,
        "store_id": store,
        "lane_number": lane,
        "incident_time": time_str,
        "symptom_description": symptom,
    }

    results = engine.diagnose(context)

    if not results:
        console.print("[yellow]No diagnosis results. Ensure log data is ingested.[/yellow]")
        return

    for i, result in enumerate(results, 1):
        severity_color = {
            "critical": "red",
            "high": "red",
            "medium": "yellow",
            "low": "green",
        }.get(result.get("severity", ""), "white")

        panel_content = []
        panel_content.append(f"[bold]Confidence:[/bold] {result.get('confidence', 0):.0%}")
        panel_content.append(f"[bold]Severity:[/bold] [{severity_color}]{result.get('severity', 'unknown').upper()}[/{severity_color}]")
        panel_content.append("")
        panel_content.append(f"[bold]Evidence:[/bold]")
        for ev in result.get("evidence", []):
            panel_content.append(f"  - {ev}")
        panel_content.append("")
        panel_content.append(f"[bold]Resolution Steps:[/bold]")
        for step in result.get("resolution_steps", []):
            panel_content.append(f"  {step}")

        if result.get("similar_cases"):
            panel_content.append("")
            panel_content.append(f"[bold]Similar Cases:[/bold] {result['similar_cases']}")

        console.print(Panel(
            "\n".join(panel_content),
            title=f"#{i} Root Cause: {result.get('root_cause', 'Unknown')}",
            border_style=severity_color,
        ))
