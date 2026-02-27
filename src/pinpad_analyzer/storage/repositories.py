"""Data access objects for storing and querying parsed log data."""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime
from typing import Optional

import duckdb
import polars as pl

from pinpad_analyzer.ingestion.models import FileIdentity, FileMetadata, LogEntry
from pinpad_analyzer.storage.database import Database


class LogFileRepo:
    """Operations on the log_files table."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def file_id_for(self, metadata: FileMetadata) -> str:
        """Generate a deterministic file ID from path + size."""
        raw = f"{metadata.file_path}:{metadata.file_size}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def exists(self, file_id: str) -> bool:
        result = self._db.conn.execute(
            "SELECT 1 FROM log_files WHERE file_id = ?", [file_id]
        ).fetchone()
        return result is not None

    def insert(
        self,
        metadata: FileMetadata,
        file_id: str,
        store_id: str = "",
        parse_duration_ms: float = 0,
        identity: Optional[FileIdentity] = None,
    ) -> None:
        import json as _json

        config_json = _json.dumps(identity.config) if identity and identity.config else None
        self._db.conn.execute(
            """INSERT INTO log_files
               (file_id, file_path, file_name, lane, log_date, store_id,
                line_count, byte_size, parse_duration_ms,
                company_id, mtx_pos_version, mtx_eps_version, seccode_version,
                pos_version, pinpad_model, pinpad_serial, pinpad_firmware,
                config_json, upload_source, sha256_hash)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                file_id,
                metadata.file_path,
                metadata.file_name,
                metadata.lane,
                metadata.log_date,
                (identity.store_id if identity and identity.store_id else store_id),
                metadata.line_count,
                metadata.file_size,
                parse_duration_ms,
                identity.company_id if identity else None,
                identity.mtx_pos_version if identity else None,
                identity.mtx_eps_version if identity else None,
                identity.seccode_version if identity else None,
                identity.pos_version if identity else None,
                identity.pinpad_model if identity else None,
                identity.pinpad_serial if identity else None,
                identity.pinpad_firmware if identity else None,
                config_json,
                identity.upload_source if identity else "local",
                identity.sha256_hash if identity else None,
            ],
        )

    def update_identity(self, file_id: str, identity: FileIdentity) -> None:
        """Update identity/metadata fields for an existing file record."""
        import json as _json

        config_json = _json.dumps(identity.config) if identity.config else None
        self._db.conn.execute(
            """UPDATE log_files SET
                company_id = COALESCE(?, company_id),
                store_id = CASE WHEN ? != '' THEN ? ELSE store_id END,
                mtx_pos_version = COALESCE(?, mtx_pos_version),
                mtx_eps_version = COALESCE(?, mtx_eps_version),
                seccode_version = COALESCE(?, seccode_version),
                pos_version = COALESCE(?, pos_version),
                pinpad_model = COALESCE(?, pinpad_model),
                pinpad_serial = COALESCE(?, pinpad_serial),
                pinpad_firmware = COALESCE(?, pinpad_firmware),
                config_json = COALESCE(?, config_json),
                sha256_hash = COALESCE(?, sha256_hash)
               WHERE file_id = ?""",
            [
                identity.company_id or None,
                identity.store_id, identity.store_id,
                identity.mtx_pos_version or None,
                identity.mtx_eps_version or None,
                identity.seccode_version or None,
                identity.pos_version or None,
                identity.pinpad_model or None,
                identity.pinpad_serial or None,
                identity.pinpad_firmware or None,
                config_json,
                identity.sha256_hash or None,
                file_id,
            ],
        )

    def get_all(self) -> list[dict]:
        return self._db.conn.execute(
            "SELECT * FROM log_files ORDER BY log_date DESC"
        ).fetchdf().to_dict("records") if self._db.conn.execute(
            "SELECT COUNT(*) FROM log_files"
        ).fetchone()[0] > 0 else []


class LogEntryRepo:
    """Batch operations on the log_entries table."""

    def __init__(self, db: Database) -> None:
        self._db = db
        self._next_id = self._get_max_id() + 1

    def _get_max_id(self) -> int:
        result = self._db.conn.execute(
            "SELECT COALESCE(MAX(entry_id), 0) FROM log_entries"
        ).fetchone()
        return result[0]

    def insert_batch(self, file_id: str, entries: list[LogEntry]) -> int:
        """Insert a batch of log entries using Polars for fast bulk insert."""
        if not entries:
            return 0

        n = len(entries)
        start_id = self._next_id
        self._next_id += n

        df = pl.DataFrame({
            "entry_id": list(range(start_id, start_id + n)),
            "file_id": [file_id] * n,
            "line_number": [e.line_number for e in entries],
            "timestamp": [e.timestamp for e in entries],
            "category": [e.category for e in entries],
            "message": [e.message for e in entries],
            "is_expanded": [e.is_expanded for e in entries],
            "expansion_count": [e.expansion_count for e in entries],
        })
        self._db.conn.execute(
            "INSERT INTO log_entries SELECT * FROM df"
        )
        return n

    def count_for_file(self, file_id: str) -> int:
        result = self._db.conn.execute(
            "SELECT COUNT(*) FROM log_entries WHERE file_id = ?", [file_id]
        ).fetchone()
        return result[0]


class EventRepo:
    """Operations on events and related tables."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def insert_event(
        self,
        event_type: str,
        file_id: str,
        lane: int,
        log_date: str,
        start_time: datetime,
        end_time: datetime,
        start_line: int,
        end_line: int,
        line_count: int,
        parent_event_id: Optional[str] = None,
    ) -> str:
        event_id = uuid.uuid4().hex[:12]
        duration_ms = (end_time - start_time).total_seconds() * 1000
        self._db.conn.execute(
            """INSERT INTO events
               (event_id, event_type, file_id, lane, log_date,
                start_time, end_time, start_line, end_line,
                line_count, duration_ms, parent_event_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                event_id, event_type, file_id, lane, log_date,
                start_time, end_time, start_line, end_line,
                line_count, duration_ms, parent_event_id,
            ],
        )
        return event_id

    def insert_transaction(self, event_id: str, **fields: object) -> None:
        cols = ["event_id"] + list(fields.keys())
        placeholders = ", ".join(["?"] * len(cols))
        col_str = ", ".join(cols)
        self._db.conn.execute(
            f"INSERT INTO transactions ({col_str}) VALUES ({placeholders})",
            [event_id] + list(fields.values()),
        )

    def insert_health_check(self, event_id: str, **fields: object) -> None:
        cols = ["event_id"] + list(fields.keys())
        placeholders = ", ".join(["?"] * len(cols))
        col_str = ", ".join(cols)
        self._db.conn.execute(
            f"INSERT INTO health_checks ({col_str}) VALUES ({placeholders})",
            [event_id] + list(fields.values()),
        )

    def insert_error_cascade(self, event_id: str, **fields: object) -> None:
        cols = ["event_id"] + list(fields.keys())
        placeholders = ", ".join(["?"] * len(cols))
        col_str = ", ".join(cols)
        self._db.conn.execute(
            f"INSERT INTO error_cascades ({col_str}) VALUES ({placeholders})",
            [event_id] + list(fields.values()),
        )

    def count_by_type(self) -> dict[str, int]:
        rows = self._db.conn.execute(
            "SELECT event_type, COUNT(*) FROM events GROUP BY event_type"
        ).fetchall()
        return {row[0]: row[1] for row in rows}


class SCATTimelineRepo:
    """Operations on scat_timeline table."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def insert_batch(
        self, file_id: str, entries: list[tuple[datetime, int]]
    ) -> None:
        if not entries:
            return
        # Deduplicate by timestamp, keeping last status for each timestamp
        deduped: dict[datetime, int] = {}
        for ts, status in entries:
            deduped[ts] = status
        timestamps = list(deduped.keys())
        statuses = list(deduped.values())
        df = pl.DataFrame({
            "file_id": [file_id] * len(timestamps),
            "timestamp": timestamps,
            "alive_status": statuses,
        })
        self._db.conn.execute(
            "INSERT INTO scat_timeline SELECT * FROM df"
        )
