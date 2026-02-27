"""Filesystem watcher for journal log files."""

from __future__ import annotations

import logging
import os
import re
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from pinpad_analyzer.agent.uploader import compute_sha256

logger = logging.getLogger(__name__)

# Matches jrnl files: jrnl0002.txt, jrnl0002-20251130.txt
JRNL_PATTERN = re.compile(r"^jrnl(\d{4})(?:-(\d{8}))?\.txt$", re.IGNORECASE)


class StateDB:
    """Local SQLite database tracking uploaded files."""

    def __init__(self, db_path: str) -> None:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS uploaded_files (
                file_path  TEXT PRIMARY KEY,
                sha256     TEXT NOT NULL,
                gcs_uri    TEXT,
                file_size  INTEGER,
                uploaded_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Queue for files pending upload (network was down)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS upload_queue (
                file_path  TEXT PRIMARY KEY,
                sha256     TEXT NOT NULL,
                lane       INTEGER,
                log_date   TEXT,
                queued_at  TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self._conn.commit()

    def is_uploaded(self, file_path: str, sha256: str) -> bool:
        """Check if a file with this hash was already uploaded."""
        row = self._conn.execute(
            "SELECT 1 FROM uploaded_files WHERE file_path = ? AND sha256 = ?",
            [file_path, sha256],
        ).fetchone()
        return row is not None

    def mark_uploaded(
        self, file_path: str, sha256: str, gcs_uri: str, file_size: int
    ) -> None:
        self._conn.execute(
            """INSERT OR REPLACE INTO uploaded_files
               (file_path, sha256, gcs_uri, file_size, uploaded_at)
               VALUES (?, ?, ?, ?, ?)""",
            [file_path, sha256, gcs_uri, file_size, datetime.now().isoformat()],
        )
        self._conn.commit()

    def enqueue(
        self, file_path: str, sha256: str, lane: int, log_date: str
    ) -> None:
        """Add a file to the upload queue (for offline resilience)."""
        self._conn.execute(
            """INSERT OR REPLACE INTO upload_queue
               (file_path, sha256, lane, log_date, queued_at)
               VALUES (?, ?, ?, ?, ?)""",
            [file_path, sha256, lane, log_date, datetime.now().isoformat()],
        )
        self._conn.commit()

    def dequeue(self, file_path: str) -> None:
        self._conn.execute(
            "DELETE FROM upload_queue WHERE file_path = ?", [file_path]
        )
        self._conn.commit()

    def get_queued(self) -> list[dict]:
        """Get all files pending upload."""
        rows = self._conn.execute(
            "SELECT file_path, sha256, lane, log_date FROM upload_queue ORDER BY queued_at"
        ).fetchall()
        return [
            {"file_path": r[0], "sha256": r[1], "lane": r[2], "log_date": r[3]}
            for r in rows
        ]

    def close(self) -> None:
        self._conn.close()


def parse_jrnl_filename(name: str) -> Optional[tuple[int, str]]:
    """Extract lane number and date from a journal filename.

    Returns (lane, date_str) or None if not a journal file.
    date_str is YYYY-MM-DD or empty if no date in filename.
    """
    m = JRNL_PATTERN.match(name)
    if not m:
        return None
    lane = int(m.group(1))
    raw_date = m.group(2) or ""
    if raw_date:
        date_str = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
    else:
        date_str = datetime.now().strftime("%Y-%m-%d")
    return lane, date_str


class LogWatcher:
    """Watches a directory for new/rotated journal log files.

    Uses polling for maximum Windows compatibility.
    """

    def __init__(
        self,
        watch_dir: str,
        state_db: StateDB,
        settle_seconds: int = 60,
        poll_interval: int = 30,
    ) -> None:
        self._watch_dir = Path(watch_dir)
        self._state_db = state_db
        self._settle_seconds = settle_seconds
        self._poll_interval = poll_interval
        self._file_sizes: dict[str, tuple[int, float]] = {}  # path -> (size, last_change_time)
        self._running = False

    def scan_once(self) -> list[dict]:
        """Scan directory once and return list of files ready for upload.

        A file is ready when:
        1. It matches jrnl*.txt pattern
        2. Its size hasn't changed for settle_seconds
        3. It hasn't been uploaded yet (by SHA-256)
        """
        ready = []
        now = time.time()

        for entry in self._watch_dir.iterdir():
            if not entry.is_file():
                continue

            parsed = parse_jrnl_filename(entry.name)
            if parsed is None:
                continue

            lane, log_date = parsed
            file_path = str(entry.resolve())
            current_size = entry.stat().st_size

            # Track size changes
            prev = self._file_sizes.get(file_path)
            if prev is None or prev[0] != current_size:
                self._file_sizes[file_path] = (current_size, now)
                continue  # Size changed, not settled yet

            # Check if file has settled
            _, last_change = prev
            if (now - last_change) < self._settle_seconds:
                continue

            # Check SHA-256 and upload status
            sha256 = compute_sha256(file_path)
            if self._state_db.is_uploaded(file_path, sha256):
                continue

            ready.append({
                "file_path": file_path,
                "file_name": entry.name,
                "lane": lane,
                "log_date": log_date,
                "sha256": sha256,
                "file_size": current_size,
            })

        return ready

    def run(
        self,
        on_file_ready: Callable[[dict], None],
    ) -> None:
        """Run the polling loop. Calls on_file_ready for each upload-ready file."""
        self._running = True
        logger.info(
            "Watching %s for journal files (poll=%ds, settle=%ds)",
            self._watch_dir,
            self._poll_interval,
            self._settle_seconds,
        )

        while self._running:
            try:
                ready_files = self.scan_once()
                for file_info in ready_files:
                    on_file_ready(file_info)
            except Exception as e:
                logger.error("Error during scan: %s", e)

            time.sleep(self._poll_interval)

    def stop(self) -> None:
        self._running = False
