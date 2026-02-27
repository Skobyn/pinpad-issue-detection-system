"""Cases command: list/view/update/close technician cases."""

from __future__ import annotations

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from pinpad_analyzer.storage.database import Database, resolve_db_path

console = Console()


def cases(
    db_path: str = typer.Option(
        "", help="Database path (local file or md:name for MotherDuck). Env: PINPAD_DB"
    ),
    action: str = typer.Argument("list", help="Action: list, view, resolve"),
    case_id: str = typer.Option("", help="Case ID (for view/resolve)"),
    status: str = typer.Option("", help="Filter by status (open/resolved)"),
    resolution: str = typer.Option("", help="Resolution notes (for resolve action)"),
) -> None:
    """Manage technician diagnosis cases."""
    try:
        effective_db = resolve_db_path(db_path)
        with Database(effective_db) as db:
            if action == "list":
                _list_cases(db, status)
            elif action == "view":
                _view_case(db, case_id)
            elif action == "resolve":
                _resolve_case(db, case_id, resolution)
            else:
                console.print(f"[red]Unknown action: {action}. Use list, view, or resolve.[/red]")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


def _list_cases(db: Database, status: str) -> None:
    from pinpad_analyzer.assistant.case_db import CaseDB

    case_db = CaseDB(db)
    case_list = case_db.list_cases(status=status)

    if not case_list:
        console.print("[yellow]No cases found.[/yellow]")
        return

    table = Table(title=f"Cases ({len(case_list)})")
    table.add_column("ID", style="cyan")
    table.add_column("Store")
    table.add_column("Lane")
    table.add_column("Symptom")
    table.add_column("Root Cause")
    table.add_column("Status")
    table.add_column("Created")

    for c in case_list:
        status_color = "green" if c["status"] == "resolved" else "yellow"
        table.add_row(
            c["case_id"],
            c.get("store_id", ""),
            str(c.get("lane_number", "")),
            c["symptom"][:40],
            c["root_cause"][:40],
            f"[{status_color}]{c['status']}[/{status_color}]",
            c["created_at"][:16],
        )

    console.print(table)


def _view_case(db: Database, case_id: str) -> None:
    from pinpad_analyzer.assistant.case_db import CaseDB

    if not case_id:
        console.print("[red]Provide --case-id to view a case.[/red]")
        return

    case_db = CaseDB(db)
    case = case_db.get_case(case_id)

    if not case:
        console.print(f"[red]Case {case_id} not found.[/red]")
        return

    content = []
    content.append(f"[bold]Status:[/bold] {case.get('resolution_status', 'unknown')}")
    content.append(f"[bold]Company:[/bold] {case.get('company_id', '')}")
    content.append(f"[bold]Store:[/bold] {case.get('store_id', '')} Lane: {case.get('lane_number', '')}")
    content.append(f"[bold]Incident Time:[/bold] {case.get('incident_time', '')}")
    content.append(f"[bold]Symptom:[/bold] {case.get('symptom_description', '')}")
    content.append("")
    content.append(f"[bold]Root Cause:[/bold] {case.get('root_cause', 'Unknown')}")
    content.append(f"[bold]Confidence:[/bold] {case.get('root_cause_confidence', 0):.0%}")
    content.append("")
    content.append(f"[bold]Evidence:[/bold] {case.get('evidence_summary', '')}")
    content.append("")
    content.append(f"[bold]Resolution:[/bold] {case.get('resolution_steps', '')}")
    content.append(f"[bold]Verified:[/bold] {case.get('tech_verified', False)}")

    console.print(Panel(
        "\n".join(content),
        title=f"Case {case_id}",
        border_style="cyan",
    ))


def _resolve_case(db: Database, case_id: str, resolution: str) -> None:
    from pinpad_analyzer.assistant.case_db import CaseDB

    if not case_id:
        console.print("[red]Provide --case-id to resolve a case.[/red]")
        return

    case_db = CaseDB(db)
    case_db.resolve_case(case_id, resolution_steps=resolution)
    console.print(f"[green]Case {case_id} marked as resolved.[/green]")
