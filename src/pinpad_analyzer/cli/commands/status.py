"""Status command: show database statistics."""

from __future__ import annotations

import typer
from rich.console import Console
from rich.table import Table

from pinpad_analyzer.storage.database import Database, resolve_db_path

console = Console()


def status(
    db_path: str = typer.Option(
        "", help="Database path (local file or md:name for MotherDuck). Env: PINPAD_DB"
    ),
    detailed: bool = typer.Option(False, help="Show detailed statistics"),
    company: str = typer.Option("", help="Filter by company ID"),
    store: str = typer.Option("", help="Filter by store ID"),
) -> None:
    """Show database statistics."""
    try:
        effective_db = resolve_db_path(db_path)
        with Database(effective_db) as db:
            _show_status(db, detailed, company, store)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        console.print("[dim]Run 'ingest' first to create the database.[/dim]")
        raise typer.Exit(1)


def _show_status(db: Database, detailed: bool, company: str = "", store: str = "") -> None:
    # Build WHERE clause for multi-tenant filtering
    file_where = "WHERE 1=1"
    file_params: list = []
    if company:
        file_where += " AND company_id = ?"
        file_params.append(company)
    if store:
        file_where += " AND (store_id = ? OR store_id = '' OR store_id IS NULL)"
        file_params.append(store)

    # Build event filter (join through log_files)
    event_file_ids_sql = f"SELECT file_id FROM log_files {file_where}"

    # Files
    files = db.conn.execute(
        f"SELECT COUNT(*), COALESCE(SUM(byte_size), 0), COALESCE(SUM(line_count), 0) FROM log_files {file_where}",
        file_params,
    ).fetchone()
    file_count, total_bytes, total_lines = files

    # Entries
    entry_count = db.conn.execute(
        f"SELECT COUNT(*) FROM log_entries WHERE file_id IN ({event_file_ids_sql})",
        file_params,
    ).fetchone()[0]
    expanded_count = db.conn.execute(
        f"SELECT COUNT(*) FROM log_entries WHERE is_expanded = TRUE AND file_id IN ({event_file_ids_sql})",
        file_params,
    ).fetchone()[0]

    # Events
    event_counts = db.conn.execute(
        f"SELECT event_type, COUNT(*) FROM events WHERE file_id IN ({event_file_ids_sql}) GROUP BY event_type ORDER BY event_type",
        file_params,
    ).fetchall()

    # Transactions
    txn_stats = db.conn.execute(
        f"""SELECT
            COUNT(*),
            SUM(CASE WHEN t.is_approved THEN 1 ELSE 0 END),
            AVG(t.host_latency_ms),
            COUNT(DISTINCT t.card_type)
        FROM transactions t
        JOIN events e ON t.event_id = e.event_id
        WHERE e.file_id IN ({event_file_ids_sql})""",
        file_params,
    ).fetchone()

    filter_label = ""
    if company or store:
        parts = []
        if company:
            parts.append(f"company={company}")
        if store:
            parts.append(f"store={store}")
        filter_label = f" ({', '.join(parts)})"

    table = Table(title=f"Pinpad Analyzer Database Status{filter_label}")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Ingested files", str(file_count))
    table.add_row("Total file size", f"{total_bytes / (1024*1024):.1f} MB")
    table.add_row("Total source lines", f"{total_lines:,}")
    table.add_row("Stored log entries", f"{entry_count:,}")
    table.add_row("Expanded entries", f"{expanded_count:,}")

    # Show tenant summary when no filter applied
    if not company and not store:
        tenant_rows = db.conn.execute("""
            SELECT company_id, store_id, COUNT(*) as files, COUNT(DISTINCT lane) as lanes,
                   MIN(log_date) as min_date, MAX(log_date) as max_date
            FROM log_files
            WHERE company_id IS NOT NULL AND company_id != ''
            GROUP BY company_id, store_id
            ORDER BY company_id, store_id
        """).fetchall()
        if tenant_rows:
            table.add_section()
            table.add_row("[bold]Tenants[/bold]", f"[bold]{len(tenant_rows)} company/store pairs[/bold]")
            for row in tenant_rows:
                table.add_row(
                    f"  Company {row[0]} / Store {row[1]}",
                    f"{row[2]} files, {row[3]} lanes, {row[4]}-{row[5]}",
                )

    table.add_section()
    for event_type, count in event_counts:
        table.add_row(f"Events: {event_type}", str(count))

    if txn_stats and txn_stats[0] > 0:
        table.add_section()
        txn_total, txn_approved, avg_latency, card_types = txn_stats
        approval_rate = (txn_approved / txn_total * 100) if txn_total > 0 else 0
        table.add_row("Transactions total", str(txn_total))
        table.add_row("Transactions approved", f"{txn_approved} ({approval_rate:.1f}%)")
        table.add_row("Avg host latency", f"{avg_latency:.0f} ms" if avg_latency else "N/A")
        table.add_row("Card types seen", str(card_types))

    console.print(table)

    if detailed and file_count > 0:
        # Show per-file details
        console.print()
        detail_table = Table(title="Ingested Files")
        detail_table.add_column("File", style="cyan")
        detail_table.add_column("Lane")
        detail_table.add_column("Date")
        detail_table.add_column("Lines", justify="right")
        detail_table.add_column("Size", justify="right")

        rows = db.conn.execute(
            "SELECT file_name, lane, log_date, line_count, byte_size FROM log_files ORDER BY log_date"
        ).fetchall()
        for row in rows:
            detail_table.add_row(
                row[0], str(row[1]), str(row[2]),
                f"{row[3]:,}", f"{row[4] / (1024*1024):.1f} MB",
            )
        console.print(detail_table)

        # Show transaction breakdown
        txn_breakdown = db.conn.execute("""
            SELECT card_type, entry_method, response_code, COUNT(*)
            FROM transactions
            GROUP BY card_type, entry_method, response_code
            ORDER BY COUNT(*) DESC
        """).fetchall()
        if txn_breakdown:
            console.print()
            bt = Table(title="Transaction Breakdown")
            bt.add_column("Card Type")
            bt.add_column("Entry Method")
            bt.add_column("Response")
            bt.add_column("Count", justify="right")
            for row in txn_breakdown:
                bt.add_row(str(row[0]), str(row[1]), str(row[2]), str(row[3]))
            console.print(bt)

        # SCAT timeline
        scat_rows = db.conn.execute("""
            SELECT timestamp, alive_status FROM scat_timeline
            ORDER BY timestamp
        """).fetchall()
        if scat_rows:
            console.print()
            st = Table(title="SCAT Alive Status Timeline")
            st.add_column("Time")
            st.add_column("Status")
            status_names = {0: "Dead", 1: "Initializing", 2: "Loading", 3: "Alive", 9: "None"}
            for ts, status_val in scat_rows:
                name = status_names.get(status_val, str(status_val))
                style = "red" if status_val == 0 else "green" if status_val == 3 else "yellow"
                st.add_row(str(ts), f"[{style}]{name}[/{style}]")
            console.print(st)

        # Error cascades
        error_rows = db.conn.execute("""
            SELECT e.start_time, e.end_time, e.duration_ms,
                   ec.error_pattern, ec.error_count, ec.recovery_achieved
            FROM events e
            JOIN error_cascades ec ON e.event_id = ec.event_id
            ORDER BY e.start_time
        """).fetchall()
        if error_rows:
            console.print()
            et = Table(title="Error Cascades")
            et.add_column("Start Time")
            et.add_column("Duration")
            et.add_column("Pattern")
            et.add_column("Count", justify="right")
            et.add_column("Recovered")
            for row in error_rows:
                duration = f"{row[2]/1000:.1f}s" if row[2] else "N/A"
                pattern = (row[3] or "")[:60]
                recovered = "[green]Yes[/green]" if row[5] else "[red]No[/red]"
                et.add_row(str(row[0]), duration, pattern, str(row[4]), recovered)
            console.print(et)
