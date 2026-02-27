"""Typer CLI application."""

import typer

from pinpad_analyzer.cli.commands.ingest import ingest
from pinpad_analyzer.cli.commands.status import status
from pinpad_analyzer.cli.commands.analyze import analyze
from pinpad_analyzer.cli.commands.diagnose import diagnose
from pinpad_analyzer.cli.commands.train import train
from pinpad_analyzer.cli.commands.cases import cases
from pinpad_analyzer.cli.commands.retrain import retrain

app = typer.Typer(
    name="pinpad-analyzer",
    help="POS Pinpad Log Analysis System",
    no_args_is_help=True,
)

app.command()(ingest)
app.command()(status)
app.command()(analyze)
app.command()(diagnose)
app.command()(train)
app.command()(cases)
app.command()(retrain)
