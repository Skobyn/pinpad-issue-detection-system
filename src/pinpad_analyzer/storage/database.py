"""DuckDB connection manager and initialization."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

from pinpad_analyzer.storage.schema import MIGRATION_COLUMNS, SCHEMA_DDL

DEFAULT_DB_PATH = "./data/pinpad_analyzer.duckdb"


def resolve_db_path(db_path: str | None = None) -> str:
    """Resolve database path from argument, env var, or default.

    Priority: explicit arg > PINPAD_DB env var > default local file.
    Supports MotherDuck URIs (md:database_name).
    """
    if db_path:
        return db_path
    return os.environ.get("PINPAD_DB", DEFAULT_DB_PATH)


class Database:
    """DuckDB database connection manager. Supports local files and MotherDuck."""

    def __init__(self, db_path: str = DEFAULT_DB_PATH) -> None:
        self.db_path = db_path
        self._is_motherduck = db_path.startswith("md:")
        self._conn: duckdb.DuckDBPyConnection | None = None

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            self._conn = self._connect()
        return self._conn

    def _connect(self) -> duckdb.DuckDBPyConnection:
        if self._is_motherduck:
            return self._connect_motherduck()
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(self.db_path)
        return conn

    def _connect_motherduck(self) -> duckdb.DuckDBPyConnection:
        """Connect to MotherDuck cloud DuckDB."""
        token = os.environ.get("MOTHERDUCK_TOKEN", "")
        if not token:
            raise EnvironmentError(
                "MOTHERDUCK_TOKEN environment variable required for MotherDuck connections. "
                "Get a token at https://app.motherduck.com/token"
            )
        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL motherduck")
        conn.execute("LOAD motherduck")
        conn.execute(f"SET motherduck_token='{token}'")
        db_name = self.db_path.replace("md:", "")
        conn.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        conn.execute(f"USE {db_name}")
        return conn

    def initialize(self) -> None:
        """Create all tables if they don't exist, then run migrations."""
        self.conn.execute(SCHEMA_DDL)
        self._migrate()

    def _migrate(self) -> None:
        """Add new columns to existing tables (idempotent)."""
        for table, col_name, col_type in MIGRATION_COLUMNS:
            try:
                self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
            except Exception:
                pass  # Column already exists

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> Database:
        self.initialize()
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
