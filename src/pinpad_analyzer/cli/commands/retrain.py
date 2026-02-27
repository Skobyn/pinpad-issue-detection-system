"""Retrain command: use accumulated labels to improve models."""

from __future__ import annotations

import typer
from rich.console import Console

from pinpad_analyzer.storage.database import Database, resolve_db_path

console = Console()


def retrain(
    db_path: str = typer.Option(
        "", help="Database path (local file or md:name for MotherDuck). Env: PINPAD_DB"
    ),
) -> None:
    """Retrain models using labels from verified case resolutions."""
    try:
        effective_db = resolve_db_path(db_path)
        with Database(effective_db) as db:
            _retrain(db)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


def _retrain(db: Database) -> None:
    from pinpad_analyzer.ml.labeler import Labeler
    from pinpad_analyzer.ml.training import TrainingPipeline
    from pinpad_analyzer.assistant.pattern_library import PatternLibrary

    # Step 1: Generate labels from verified cases
    console.print("[bold]Step 1: Generating labels from verified cases...[/bold]")
    labeler = Labeler(db)
    labels = labeler.generate_labels()
    console.print(f"  Generated {len(labels)} labels")

    if labels:
        stored = labeler.store_labels(labels)
        console.print(f"  Stored {stored} labels in predictions table")

    # Step 2: Learn patterns from resolved cases
    console.print("[bold]Step 2: Learning patterns from resolved cases...[/bold]")
    pattern_lib = PatternLibrary(db)

    resolved = db.conn.execute(
        "SELECT case_id FROM cases WHERE resolution_status = 'resolved'"
    ).fetchall()

    new_patterns = 0
    for (case_id,) in resolved:
        ids = pattern_lib.learn_from_case(case_id)
        new_patterns += len(ids)
    console.print(f"  Processed {len(resolved)} cases, {new_patterns} new/updated patterns")

    stats = pattern_lib.get_pattern_stats()
    console.print(f"  Pattern library: {stats['total_patterns']} total patterns")

    # Step 3: Retrain anomaly detector with updated data
    console.print("[bold]Step 3: Retraining anomaly detector...[/bold]")
    pipeline = TrainingPipeline(db)
    metrics = pipeline.train_anomaly_detector()

    if metrics.get("status") == "success":
        console.print(f"  [green]Model trained: {metrics['model_id']}[/green]")
        console.print(f"  Samples: {metrics['n_samples']}, Anomalies: {metrics['n_anomalies']}")
    else:
        console.print(f"  [yellow]{metrics.get('message', 'No training data')}[/yellow]")

    console.print("\n[green]Retraining complete.[/green]")
