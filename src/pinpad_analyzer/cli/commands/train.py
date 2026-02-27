"""Train command: train ML models from ingested data."""

from __future__ import annotations

import typer
from rich.console import Console
from rich.table import Table

from pinpad_analyzer.storage.database import Database, resolve_db_path

console = Console()


def train(
    db_path: str = typer.Option(
        "", help="Database path (local file or md:name for MotherDuck). Env: PINPAD_DB"
    ),
    contamination: float = typer.Option(
        0.05, help="Expected anomaly rate (0.01-0.5)"
    ),
) -> None:
    """Train anomaly detection model on ingested transaction data."""
    try:
        effective_db = resolve_db_path(db_path)
        with Database(effective_db) as db:
            _train(db, contamination)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


def _train(db: Database, contamination: float) -> None:
    from pinpad_analyzer.ml.training import TrainingPipeline

    console.print("[bold]Training anomaly detection model...[/bold]")

    pipeline = TrainingPipeline(db)
    metrics = pipeline.train_anomaly_detector(contamination=contamination)

    if metrics.get("status") != "success":
        console.print(f"[red]Training failed: {metrics.get('message', 'Unknown error')}[/red]")
        return

    table = Table(title="Training Results")
    table.add_column("Metric", style="bold")
    table.add_column("Value", justify="right")

    table.add_row("Model ID", metrics["model_id"])
    table.add_row("Training Samples", str(metrics["n_samples"]))
    table.add_row("Features", str(metrics["n_features"]))
    table.add_row("Anomalies Detected", str(metrics["n_anomalies"]))
    table.add_row("Anomaly Rate", f"{metrics['anomaly_rate']:.1%}")
    table.add_row("Score Mean", f"{metrics['score_mean']:.4f}")
    table.add_row("Score Std", f"{metrics['score_std']:.4f}")
    table.add_row("Score Min", f"{metrics['score_min']:.4f}")

    console.print(table)
    console.print(f"[green]Model saved and activated: {metrics['model_id']}[/green]")
