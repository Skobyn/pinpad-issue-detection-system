"""Query engine: find relevant log data matching technician context."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from pinpad_analyzer.storage.database import Database


class QueryEngine:
    """Queries DuckDB for log data matching technician-provided context."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def find_matching_files(
        self,
        company_id: str = "",
        store_id: str = "",
        lane_number: int = 0,
        incident_time: Optional[datetime] = None,
    ) -> list[dict]:
        """Find log files matching the given context."""
        query = "SELECT file_id, file_name, lane, log_date, store_id, company_id FROM log_files WHERE 1=1"
        params = []

        if company_id:
            query += " AND (company_id = ? OR company_id IS NULL)"
            params.append(company_id)
        if store_id:
            query += " AND (store_id = ? OR store_id = '' OR store_id IS NULL)"
            params.append(store_id)
        if lane_number > 0:
            query += " AND lane = ?"
            params.append(lane_number)
        if incident_time:
            query += " AND log_date = ?"
            params.append(incident_time.strftime("%Y-%m-%d"))

        query += " ORDER BY log_date DESC"
        rows = self._db.conn.execute(query, params).fetchall()

        return [
            {
                "file_id": r[0],
                "file_name": r[1],
                "lane": r[2],
                "log_date": str(r[3]),
                "store_id": r[4],
                "company_id": r[5],
            }
            for r in rows
        ]

    def get_events_around_time(
        self,
        file_id: str,
        incident_time: datetime,
        window_minutes: int = 30,
    ) -> list[dict]:
        """Get events in a time window around the incident."""
        start = incident_time - timedelta(minutes=window_minutes)
        end = incident_time + timedelta(minutes=window_minutes)

        rows = self._db.conn.execute(
            """SELECT e.event_id, e.event_type, e.start_time, e.end_time,
                      e.duration_ms, e.line_count
               FROM events e
               WHERE e.file_id = ?
                 AND e.start_time BETWEEN ? AND ?
               ORDER BY e.start_time""",
            [file_id, start, end],
        ).fetchall()

        return [
            {
                "event_id": r[0],
                "event_type": r[1],
                "start_time": str(r[2]),
                "end_time": str(r[3]),
                "duration_ms": r[4],
                "line_count": r[5],
            }
            for r in rows
        ]

    def get_transactions_for_file(self, file_id: str) -> list[dict]:
        """Get all transactions for a file with details."""
        rows = self._db.conn.execute(
            """SELECT e.event_id, e.start_time, e.duration_ms,
                      t.card_type, t.entry_method, t.response_code,
                      t.host_latency_ms, t.amount_cents, t.is_approved,
                      t.serial_error_count, t.tvr, t.is_fallback
               FROM events e
               JOIN transactions t ON e.event_id = t.event_id
               WHERE e.file_id = ?
               ORDER BY e.start_time""",
            [file_id],
        ).fetchall()

        return [
            {
                "event_id": r[0],
                "time": str(r[1]),
                "duration_ms": r[2],
                "card_type": r[3],
                "entry_method": r[4],
                "response_code": r[5],
                "host_latency_ms": r[6],
                "amount_cents": r[7],
                "is_approved": r[8],
                "serial_errors": r[9],
                "tvr": r[10],
                "is_fallback": r[11],
            }
            for r in rows
        ]

    def get_scat_timeline(self, file_id: str) -> list[dict]:
        """Get SCAT alive/dead timeline for a file."""
        rows = self._db.conn.execute(
            """SELECT timestamp, alive_status
               FROM scat_timeline
               WHERE file_id = ?
               ORDER BY timestamp""",
            [file_id],
        ).fetchall()

        return [{"timestamp": str(r[0]), "alive_status": r[1]} for r in rows]

    def get_error_cascades(self, file_id: str) -> list[dict]:
        """Get error cascades for a file."""
        rows = self._db.conn.execute(
            """SELECT e.start_time, e.end_time, e.duration_ms,
                      ec.error_pattern, ec.error_count, ec.recovery_achieved
               FROM events e
               JOIN error_cascades ec ON e.event_id = ec.event_id
               WHERE e.file_id = ?
               ORDER BY ec.error_count DESC""",
            [file_id],
        ).fetchall()

        return [
            {
                "start_time": str(r[0]),
                "end_time": str(r[1]),
                "duration_ms": r[2],
                "error_pattern": r[3],
                "error_count": r[4],
                "recovery_achieved": r[5],
            }
            for r in rows
        ]

    def get_health_checks(self, file_id: str, success_only: bool = False) -> list[dict]:
        """Get health check results for a file."""
        query = """SELECT e.start_time, hc.check_type, hc.target_host,
                          hc.success, hc.http_status, hc.error_code, hc.latency_ms
                   FROM events e
                   JOIN health_checks hc ON e.event_id = hc.event_id
                   WHERE e.file_id = ?"""
        params = [file_id]

        if success_only:
            query += " AND hc.success = TRUE"

        query += " ORDER BY e.start_time"
        rows = self._db.conn.execute(query, params).fetchall()

        return [
            {
                "time": str(r[0]),
                "check_type": r[1],
                "target_host": r[2],
                "success": r[3],
                "http_status": r[4],
                "error_code": r[5],
                "latency_ms": r[6],
            }
            for r in rows
        ]
