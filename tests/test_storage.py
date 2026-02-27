"""Tests for DuckDB storage layer."""

from __future__ import annotations

from datetime import datetime

import pytest

from pinpad_analyzer.ingestion.models import FileMetadata, LogEntry
from pinpad_analyzer.storage.database import Database
from pinpad_analyzer.storage.repositories import (
    LogFileRepo, LogEntryRepo, EventRepo, SCATTimelineRepo,
)


class TestDatabase:
    """Test database connection and initialization."""

    def test_create_and_initialize(self, tmp_path):
        db_path = str(tmp_path / "test.duckdb")
        with Database(db_path) as db:
            # Should create tables
            tables = db.conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
            table_names = {t[0] for t in tables}

            assert "log_files" in table_names
            assert "log_entries" in table_names
            assert "events" in table_names
            assert "transactions" in table_names
            assert "scat_timeline" in table_names
            assert "models" in table_names
            assert "cases" in table_names

    def test_context_manager(self, tmp_path):
        db_path = str(tmp_path / "test.duckdb")
        with Database(db_path) as db:
            assert db.conn is not None
        # After exit, connection should be closed
        assert db._conn is None


class TestLogFileRepo:
    """Test log file repository."""

    def test_insert_and_check_exists(self, tmp_db):
        repo = LogFileRepo(tmp_db)
        metadata = FileMetadata(
            file_path="/tmp/test.txt",
            file_name="jrnl0002-20251130.txt",
            lane=2,
            log_date="2025-11-30",
            file_size=22000000,
            line_count=280000,
        )

        file_id = repo.file_id_for(metadata)
        assert len(file_id) == 16

        assert not repo.exists(file_id)
        repo.insert(metadata, file_id, store_id="1", parse_duration_ms=5000)
        assert repo.exists(file_id)

    def test_deterministic_id(self, tmp_db):
        repo = LogFileRepo(tmp_db)
        meta1 = FileMetadata("/tmp/a.txt", "a.txt", 1, "2025-11-30", 100)
        meta2 = FileMetadata("/tmp/a.txt", "a.txt", 1, "2025-11-30", 100)
        assert repo.file_id_for(meta1) == repo.file_id_for(meta2)


class TestLogEntryRepo:
    """Test log entry batch insertion."""

    def test_batch_insert(self, tmp_db):
        # First create a log file
        file_repo = LogFileRepo(tmp_db)
        metadata = FileMetadata("/tmp/t.txt", "t.txt", 1, "2025-11-30", 100)
        file_id = file_repo.file_id_for(metadata)
        file_repo.insert(metadata, file_id)

        entry_repo = LogEntryRepo(tmp_db)
        entries = [
            LogEntry(1, datetime(2025, 11, 30, 0, 0, 0), "SERIAL", "msg1"),
            LogEntry(2, datetime(2025, 11, 30, 0, 0, 1), "TCP/IP", "msg2"),
        ]

        count = entry_repo.insert_batch(file_id, entries)
        assert count == 2
        assert entry_repo.count_for_file(file_id) == 2


class TestEventRepo:
    """Test event and transaction storage."""

    def test_insert_event_and_transaction(self, tmp_db):
        # Create file first
        file_repo = LogFileRepo(tmp_db)
        metadata = FileMetadata("/tmp/t.txt", "t.txt", 2, "2025-11-30", 100)
        file_id = file_repo.file_id_for(metadata)
        file_repo.insert(metadata, file_id)

        event_repo = EventRepo(tmp_db)
        event_id = event_repo.insert_event(
            event_type="transaction",
            file_id=file_id,
            lane=2,
            log_date="2025-11-30",
            start_time=datetime(2025, 11, 30, 8, 0, 0),
            end_time=datetime(2025, 11, 30, 8, 0, 30),
            start_line=100,
            end_line=200,
            line_count=100,
        )

        assert len(event_id) == 12

        event_repo.insert_transaction(
            event_id,
            card_type="Debit",
            entry_method="E",
            response_code="AA",
            is_approved=True,
            host_latency_ms=1500,
            amount_cents=5000,
        )

        # Verify
        row = tmp_db.conn.execute(
            "SELECT card_type, is_approved FROM transactions WHERE event_id = ?",
            [event_id],
        ).fetchone()
        assert row[0] == "Debit"
        assert row[1] is True


class TestSCATTimelineRepo:
    """Test SCAT timeline storage."""

    def test_insert_batch(self, tmp_db):
        file_repo = LogFileRepo(tmp_db)
        metadata = FileMetadata("/tmp/t.txt", "t.txt", 1, "2025-11-30", 100)
        file_id = file_repo.file_id_for(metadata)
        file_repo.insert(metadata, file_id)

        scat_repo = SCATTimelineRepo(tmp_db)
        entries = [
            (datetime(2025, 11, 30, 0, 0, 0), 3),
            (datetime(2025, 11, 30, 0, 2, 42), 0),
            (datetime(2025, 11, 30, 7, 56, 55), 3),
        ]

        scat_repo.insert_batch(file_id, entries)

        rows = tmp_db.conn.execute(
            "SELECT COUNT(*) FROM scat_timeline WHERE file_id = ?",
            [file_id],
        ).fetchone()
        assert rows[0] == 3
