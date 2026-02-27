"""Evidence builder: extracts relevant log excerpts and builds timelines."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from pinpad_analyzer.storage.database import Database


class EvidenceBuilder:
    """Builds structured evidence from log data for diagnosis."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def build_timeline(
        self,
        file_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        max_entries: int = 50,
    ) -> list[dict]:
        """Build a chronological timeline of significant events."""
        events = []

        # SCAT state changes
        scat_query = "SELECT timestamp, alive_status FROM scat_timeline WHERE file_id = ?"
        params = [file_id]
        if start_time:
            scat_query += " AND timestamp >= ?"
            params.append(start_time)
        if end_time:
            scat_query += " AND timestamp <= ?"
            params.append(end_time)
        scat_query += " ORDER BY timestamp"

        prev_status = None
        for ts, status in self._db.conn.execute(scat_query, params).fetchall():
            if status != prev_status:
                status_names = {0: "DEAD", 1: "Initializing", 2: "Loading", 3: "ALIVE", 9: "None"}
                events.append({
                    "timestamp": ts,
                    "type": "scat_state",
                    "summary": f"SCAT -> {status_names.get(status, str(status))}",
                    "severity": "critical" if status == 0 else "info",
                })
                prev_status = status

        # Transaction events
        txn_query = """
            SELECT e.start_time, t.card_type, t.response_code,
                   t.host_latency_ms, t.is_approved
            FROM events e
            JOIN transactions t ON e.event_id = t.event_id
            WHERE e.file_id = ?"""
        txn_params = [file_id]
        if start_time:
            txn_query += " AND e.start_time >= ?"
            txn_params.append(start_time)
        if end_time:
            txn_query += " AND e.start_time <= ?"
            txn_params.append(end_time)
        txn_query += " ORDER BY e.start_time"

        for ts, card, resp, latency, approved in self._db.conn.execute(txn_query, txn_params).fetchall():
            severity = "info" if approved else "warning"
            if latency and latency > 5000:
                severity = "high"
            events.append({
                "timestamp": ts,
                "type": "transaction",
                "summary": f"TXN {card or '?'} -> {resp or '?'} ({latency or 0:.0f}ms)",
                "severity": severity,
            })

        # Error cascades
        err_query = """
            SELECT e.start_time, ec.error_pattern, ec.error_count
            FROM events e
            JOIN error_cascades ec ON e.event_id = ec.event_id
            WHERE e.file_id = ?"""
        err_params = [file_id]
        if start_time:
            err_query += " AND e.start_time >= ?"
            err_params.append(start_time)
        if end_time:
            err_query += " AND e.start_time <= ?"
            err_params.append(end_time)

        for ts, pattern, count in self._db.conn.execute(err_query, err_params).fetchall():
            events.append({
                "timestamp": ts,
                "type": "error_cascade",
                "summary": f"{count}x {pattern}",
                "severity": "critical" if count >= 5 else "high",
            })

        # Health check failures
        hc_query = """
            SELECT e.start_time, hc.check_type, hc.http_status, hc.error_code
            FROM events e
            JOIN health_checks hc ON e.event_id = hc.event_id
            WHERE e.file_id = ? AND hc.success = FALSE"""
        hc_params = [file_id]
        if start_time:
            hc_query += " AND e.start_time >= ?"
            hc_params.append(start_time)
        if end_time:
            hc_query += " AND e.start_time <= ?"
            hc_params.append(end_time)

        for ts, check_type, http_status, error_code in self._db.conn.execute(hc_query, hc_params).fetchall():
            events.append({
                "timestamp": ts,
                "type": "health_check_fail",
                "summary": f"{check_type} failed: {http_status or error_code or 'unknown'}",
                "severity": "high",
            })

        # Sort by timestamp and limit
        events.sort(key=lambda x: x["timestamp"])
        return events[:max_entries]

    def get_log_excerpts(
        self,
        file_id: str,
        around_time: datetime,
        window_seconds: int = 30,
        limit: int = 20,
    ) -> list[dict]:
        """Get raw log entries around a specific time."""
        start = around_time - timedelta(seconds=window_seconds)
        end = around_time + timedelta(seconds=window_seconds)

        rows = self._db.conn.execute(
            """SELECT line_number, timestamp, category, message
               FROM log_entries
               WHERE file_id = ?
                 AND timestamp BETWEEN ? AND ?
               ORDER BY timestamp
               LIMIT ?""",
            [file_id, start, end, limit],
        ).fetchall()

        return [
            {
                "line": r[0],
                "timestamp": str(r[1]),
                "category": r[2],
                "message": r[3][:300],
            }
            for r in rows
        ]

    def summarize_file(self, file_id: str) -> dict:
        """Build a high-level summary of a file's data."""
        stats = {}

        # Total entries
        row = self._db.conn.execute(
            "SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM log_entries WHERE file_id = ?",
            [file_id],
        ).fetchone()
        stats["total_entries"] = row[0]
        stats["time_range"] = f"{row[1]} - {row[2]}" if row[1] else "N/A"

        # Transactions
        row = self._db.conn.execute(
            """SELECT COUNT(*),
                      SUM(CASE WHEN t.is_approved THEN 1 ELSE 0 END),
                      AVG(t.host_latency_ms)
               FROM events e
               JOIN transactions t ON e.event_id = t.event_id
               WHERE e.file_id = ?""",
            [file_id],
        ).fetchone()
        stats["transactions"] = row[0]
        stats["approved"] = row[1] or 0
        stats["avg_latency_ms"] = round(row[2], 1) if row[2] else 0

        # SCAT dead time
        dead_seconds = self._calculate_dead_time(file_id)
        stats["scat_dead_minutes"] = round(dead_seconds / 60, 1)

        # Error cascades
        row = self._db.conn.execute(
            """SELECT COUNT(*), SUM(ec.error_count)
               FROM events e
               JOIN error_cascades ec ON e.event_id = ec.event_id
               WHERE e.file_id = ?""",
            [file_id],
        ).fetchone()
        stats["error_cascades"] = row[0]
        stats["total_errors"] = row[1] or 0

        return stats

    def _calculate_dead_time(self, file_id: str) -> float:
        """Calculate total SCAT dead time in seconds."""
        rows = self._db.conn.execute(
            "SELECT timestamp, alive_status FROM scat_timeline WHERE file_id = ? ORDER BY timestamp",
            [file_id],
        ).fetchall()

        if not rows:
            return 0

        total_dead = 0.0
        dead_start = None
        for ts, status in rows:
            if status == 0 and dead_start is None:
                dead_start = ts
            elif status != 0 and dead_start is not None:
                total_dead += (ts - dead_start).total_seconds()
                dead_start = None

        # Still dead at end
        if dead_start is not None and rows:
            total_dead += (rows[-1][0] - dead_start).total_seconds()

        return total_dead
