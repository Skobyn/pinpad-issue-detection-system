"""Ingest command: parse log file(s) into database."""

from __future__ import annotations

import time
from pathlib import Path

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table

from pinpad_analyzer.ingestion.file_reader import FileReader
from pinpad_analyzer.ingestion.metadata_extractor import MetadataExtractor
from pinpad_analyzer.ingestion.models import LogEntry
from pinpad_analyzer.segmentation.state_machine import SCATStateMachine
from pinpad_analyzer.segmentation.transaction_segmenter import TransactionSegmenter
from pinpad_analyzer.segmentation.error_cascade import ErrorCascadeDetector
from pinpad_analyzer.segmentation.health_segmenter import HealthSegmenter
from pinpad_analyzer.storage.database import Database, resolve_db_path
from pinpad_analyzer.storage.repositories import (
    LogFileRepo,
    LogEntryRepo,
    EventRepo,
    SCATTimelineRepo,
)

console = Console()
BATCH_SIZE = 10_000


def ingest(
    path: str = typer.Argument(help="Path to log file or directory"),
    store_id: str = typer.Option("", help="Override store identifier"),
    force: bool = typer.Option(False, help="Re-ingest even if file already processed"),
    db_path: str = typer.Option(
        "", help="Database path (local file or md:name for MotherDuck). Env: PINPAD_DB"
    ),
    no_expand: bool = typer.Option(False, help="Skip repeat expansion"),
) -> None:
    """Parse log file(s) into the database."""
    p = Path(path)
    if p.is_dir():
        files = sorted(p.glob("jrnl*.txt"))
        if not files:
            console.print(f"[red]No jrnl*.txt files found in {path}[/red]")
            raise typer.Exit(1)
    elif p.is_file():
        files = [p]
    else:
        console.print(f"[red]Path not found: {path}[/red]")
        raise typer.Exit(1)

    effective_db = resolve_db_path(db_path)
    with Database(effective_db) as db:
        for file_path in files:
            _ingest_file(db, str(file_path), store_id, force, not no_expand)


def _ingest_file(
    db: Database,
    file_path: str,
    store_id: str,
    force: bool,
    expand_repeats: bool,
) -> None:
    """Ingest a single log file."""
    reader = FileReader(file_path)
    metadata = reader.metadata
    file_repo = LogFileRepo(db)
    file_id = file_repo.file_id_for(metadata)

    if not metadata.log_date:
        console.print(f"[red]Skipping {metadata.file_name} (could not determine log date from filename or content)[/red]")
        return

    if file_repo.exists(file_id) and not force:
        console.print(f"[yellow]Skipping {metadata.file_name} (already ingested)[/yellow]")
        return

    # Extract metadata from file content
    extractor = MetadataExtractor()
    identity = extractor.extract_from_file(file_path)
    identity.upload_source = "local"
    # CLI store_id override takes precedence
    effective_store = store_id if store_id else identity.store_id

    console.print(f"\n[bold]Ingesting {metadata.file_name}[/bold]")
    console.print(f"  Lane: {metadata.lane}, Date: {metadata.log_date}, Size: {metadata.file_size:,} bytes")
    if identity.company_id:
        console.print(f"  Company: {identity.company_id}, Store: {effective_store}")
    if identity.pinpad_serial:
        console.print(f"  Pinpad: {identity.pinpad_model or 'unknown'} S/N {identity.pinpad_serial}")

    start_time = time.time()

    # Phase 1: Parse and store log entries
    entry_repo = LogEntryRepo(db)
    event_repo = EventRepo(db)
    scat_repo = SCATTimelineRepo(db)

    # Insert file record first (so FK constraints work for log_entries)
    # We'll update line_count later
    if force and file_repo.exists(file_id):
        # Delete in FK-safe order
        db.conn.execute("DELETE FROM predictions WHERE event_id IN (SELECT event_id FROM events WHERE file_id = ?)", [file_id])
        db.conn.execute("DELETE FROM transactions WHERE event_id IN (SELECT event_id FROM events WHERE file_id = ?)", [file_id])
        db.conn.execute("DELETE FROM health_checks WHERE event_id IN (SELECT event_id FROM events WHERE file_id = ?)", [file_id])
        db.conn.execute("DELETE FROM error_cascades WHERE event_id IN (SELECT event_id FROM events WHERE file_id = ?)", [file_id])
        db.conn.execute("DELETE FROM events WHERE file_id = ?", [file_id])
        db.conn.execute("DELETE FROM log_entries WHERE file_id = ?", [file_id])
        db.conn.execute("DELETE FROM scat_timeline WHERE file_id = ?", [file_id])
        db.conn.execute("DELETE FROM log_files WHERE file_id = ?", [file_id])

    file_repo.insert(metadata, file_id, effective_store, 0, identity=identity)

    # Collect all entries for segmentation (streaming into batches for storage)
    all_entries: list[LogEntry] = []
    batch: list[LogEntry] = []
    total_stored = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Parsing log entries...", total=None)

        for entry in reader.read_entries(expand_repeats=expand_repeats):
            all_entries.append(entry)
            batch.append(entry)

            if len(batch) >= BATCH_SIZE:
                total_stored += entry_repo.insert_batch(file_id, batch)
                batch = []
                progress.update(task, description=f"Parsed {total_stored:,} entries...")

        # Flush remaining batch
        if batch:
            total_stored += entry_repo.insert_batch(file_id, batch)

        progress.update(task, description=f"Parsed {total_stored:,} entries", completed=True)

    # Update file metadata with final stats
    parse_ms = (time.time() - start_time) * 1000
    db.conn.execute(
        "UPDATE log_files SET line_count = ?, parse_duration_ms = ? WHERE file_id = ?",
        [total_stored, parse_ms, file_id],
    )

    # Phase 2: Segment transactions
    console.print("  Segmenting transactions...")
    segmenter = TransactionSegmenter()
    txn_count = 0
    for txn in segmenter.process_entries(iter(all_entries)):
        if txn.start_time is None or txn.end_time is None:
            continue
        event_id = event_repo.insert_event(
            event_type="transaction",
            file_id=file_id,
            lane=metadata.lane,
            log_date=metadata.log_date,
            start_time=txn.start_time,
            end_time=txn.end_time,
            start_line=txn.start_line,
            end_line=txn.end_line,
            line_count=txn.entry_count,
        )
        event_repo.insert_transaction(
            event_id,
            sequence_number=txn.sequence_number,
            card_type=txn.card_type,
            entry_method=txn.entry_method,
            pan_last4=txn.pan_last4,
            aid=txn.aid,
            app_label=txn.app_label,
            tac_sequence=txn.tac_sequence,
            cvm_result=txn.cvm_result,
            response_code=txn.response_code,
            host_response_code=txn.host_response_code,
            authorization_number=txn.authorization_number,
            amount_cents=txn.amount_cents,
            cashback_cents=txn.cashback_cents,
            host_url=txn.host_url,
            host_latency_ms=txn.host_latency_ms,
            tvr=txn.tvr,
            is_approved=txn.is_approved,
            is_quickchip=txn.is_quickchip,
            is_fallback=txn.is_fallback,
            serial_error_count=txn.serial_error_count,
        )
        txn_count += 1

    # Phase 3: Track SCAT state
    console.print("  Tracking SCAT state machine...")
    scat = SCATStateMachine()
    for entry in all_entries:
        scat.process_entry(entry)
    if scat.alive_history:
        scat_repo.insert_batch(
            file_id,
            [(ts, status) for ts, status, _name in scat.alive_history],
        )

    # Phase 4: Detect error cascades
    console.print("  Detecting error cascades...")
    cascade_detector = ErrorCascadeDetector()
    cascade_count = 0
    for cascade in cascade_detector.process_entries(iter(all_entries)):
        if cascade.start_time is None or cascade.end_time is None:
            continue
        event_id = event_repo.insert_event(
            event_type="error_cascade",
            file_id=file_id,
            lane=metadata.lane,
            log_date=metadata.log_date,
            start_time=cascade.start_time,
            end_time=cascade.end_time,
            start_line=cascade.start_line,
            end_line=cascade.end_line,
            line_count=cascade.error_count,
        )
        event_repo.insert_error_cascade(
            event_id,
            error_pattern=cascade.error_pattern,
            error_count=cascade.error_count,
            recovery_achieved=cascade.recovery_achieved,
            recovery_time_ms=cascade.recovery_time_ms,
        )
        cascade_count += 1

    # Phase 5: Detect health checks
    console.print("  Detecting health checks...")
    health_seg = HealthSegmenter()
    health_count = 0
    for hc in health_seg.process_entries(iter(all_entries)):
        if hc.start_time is None:
            continue
        event_id = event_repo.insert_event(
            event_type="health_check",
            file_id=file_id,
            lane=metadata.lane,
            log_date=metadata.log_date,
            start_time=hc.start_time,
            end_time=hc.end_time or hc.start_time,
            start_line=hc.start_line,
            end_line=hc.end_line or hc.start_line,
            line_count=1,
        )
        event_repo.insert_health_check(
            event_id,
            check_type=hc.check_type,
            target_host=hc.target_host or "",
            success=hc.success,
            error_code=hc.error_code or "",
            http_status=hc.http_status or "",
            latency_ms=hc.latency_ms,
        )
        health_count += 1

    elapsed = time.time() - start_time

    # After parsing, update identity from parsed entries (may fill gaps)
    extractor2 = MetadataExtractor()
    identity2 = extractor2.extract(iter(all_entries))
    # Merge: fill any blanks in original identity
    for field_name in ("company_id", "store_id", "mid", "mtx_pos_version",
                        "mtx_eps_version", "seccode_version", "pos_version",
                        "pinpad_model", "pinpad_serial", "pinpad_firmware"):
        if not getattr(identity, field_name) and getattr(identity2, field_name):
            setattr(identity, field_name, getattr(identity2, field_name))
    for k, v in identity2.config.items():
        if k not in identity.config:
            identity.config[k] = v
    file_repo.update_identity(file_id, identity)

    # Summary
    table = Table(title="Ingestion Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    table.add_row("Log entries stored", f"{total_stored:,}")
    table.add_row("Transactions", str(txn_count))
    table.add_row("Error cascades", str(cascade_count))
    table.add_row("Health checks", str(health_count))
    table.add_row("SCAT state changes", str(len(scat.state_history)))
    table.add_row("SCAT alive changes", str(len(scat.alive_history)))

    dead_periods = scat.get_dead_periods()
    if dead_periods:
        total_dead_sec = sum(d for _, _, d in dead_periods)
        table.add_row("SCAT dead periods", str(len(dead_periods)))
        table.add_row("Total dead time", f"{total_dead_sec/60:.1f} min")

    table.add_row("Parse time", f"{elapsed:.1f}s")

    if identity.company_id or identity.pinpad_serial:
        table.add_section()
        if identity.company_id:
            table.add_row("Company ID", identity.company_id)
        if identity.store_id:
            table.add_row("Store ID", identity.store_id)
        if identity.mtx_pos_version:
            table.add_row("MTX_POS version", identity.mtx_pos_version)
        if identity.mtx_eps_version:
            table.add_row("MTX_EPS version", identity.mtx_eps_version)
        if identity.pinpad_model:
            table.add_row("Pinpad model", identity.pinpad_model)
        if identity.pinpad_serial:
            table.add_row("Pinpad serial", identity.pinpad_serial)
        if identity.sha256_hash:
            table.add_row("SHA-256", identity.sha256_hash[:16] + "...")

    console.print(table)
