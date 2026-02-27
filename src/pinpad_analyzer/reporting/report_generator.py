"""Issue report generation with evidence and log excerpts."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from pinpad_analyzer.storage.database import Database


class ReportGenerator:
    """Generates detailed issue reports with evidence."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def generate_file_report(self, file_id: str, issues: list[dict]) -> dict:
        """Generate a comprehensive report for a file's issues."""
        # Get file metadata
        file_info = self._db.conn.execute(
            "SELECT file_name, lane, log_date, line_count FROM log_files WHERE file_id = ?",
            [file_id],
        ).fetchone()

        if not file_info:
            return {"error": "File not found"}

        # Get transaction summary
        txn_stats = self._db.conn.execute(
            """SELECT
                COUNT(*) as total,
                SUM(CASE WHEN t.is_approved THEN 1 ELSE 0 END) as approved,
                AVG(t.host_latency_ms) as avg_latency,
                MAX(t.host_latency_ms) as max_latency
            FROM events e
            JOIN transactions t ON e.event_id = t.event_id
            WHERE e.file_id = ?""",
            [file_id],
        ).fetchone()

        # Get SCAT summary
        scat_stats = self._db.conn.execute(
            """SELECT
                MIN(timestamp) as first_ts,
                MAX(timestamp) as last_ts,
                SUM(CASE WHEN alive_status = 0 THEN 1 ELSE 0 END) as dead_count,
                COUNT(*) as total_entries
            FROM scat_timeline
            WHERE file_id = ?""",
            [file_id],
        ).fetchone()

        # Build report
        report = {
            "file": {
                "name": file_info[0],
                "lane": file_info[1],
                "date": str(file_info[2]),
                "line_count": file_info[3],
            },
            "transactions": {
                "total": txn_stats[0] if txn_stats else 0,
                "approved": txn_stats[1] if txn_stats else 0,
                "avg_latency_ms": round(txn_stats[2], 1) if txn_stats and txn_stats[2] else 0,
                "max_latency_ms": round(txn_stats[3], 1) if txn_stats and txn_stats[3] else 0,
            },
            "scat": {
                "time_range": f"{scat_stats[0]} - {scat_stats[1]}" if scat_stats and scat_stats[0] else "N/A",
                "dead_entries": scat_stats[2] if scat_stats else 0,
            },
            "issues": [],
        }

        # Enrich issues with log excerpts
        for issue in issues:
            enriched = dict(issue)
            time_range = issue.get("time_range", "")
            if time_range and " - " in time_range:
                start_str = time_range.split(" - ")[0].strip()
                excerpts = self._get_log_excerpts(file_id, start_str, limit=5)
                enriched["log_excerpts"] = excerpts
            report["issues"].append(enriched)

        return report

    def _get_log_excerpts(
        self, file_id: str, around_time: str, limit: int = 5
    ) -> list[dict]:
        """Get log entries around a specific time."""
        try:
            # Try parsing the timestamp
            for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
                try:
                    ts = datetime.strptime(around_time, fmt)
                    break
                except ValueError:
                    continue
            else:
                return []

            window_start = ts - timedelta(seconds=5)
            window_end = ts + timedelta(seconds=30)

            rows = self._db.conn.execute(
                """SELECT line_number, timestamp, category, message
                   FROM log_entries
                   WHERE file_id = ?
                     AND timestamp BETWEEN ? AND ?
                   ORDER BY timestamp
                   LIMIT ?""",
                [file_id, window_start, window_end, limit],
            ).fetchall()

            return [
                {
                    "line": row[0],
                    "timestamp": str(row[1]),
                    "category": row[2],
                    "message": row[3][:200],
                }
                for row in rows
            ]
        except Exception:
            return []

    def generate_cross_store_report(
        self, company_id: str = ""
    ) -> dict:
        """Generate cross-store comparison metrics."""
        where = ""
        params = []
        if company_id:
            where = "WHERE lf.company_id = ?"
            params = [company_id]

        # No-read rate by store/lane
        no_read_stats = self._db.conn.execute(
            f"""SELECT
                lf.company_id, lf.store_id, lf.lane,
                COUNT(*) as total_txns,
                SUM(CASE WHEN t.entry_method IS NULL OR t.entry_method = '' THEN 1 ELSE 0 END) as no_reads,
                ROUND(100.0 * SUM(CASE WHEN t.entry_method IS NULL OR t.entry_method = '' THEN 1 ELSE 0 END) / COUNT(*), 1) as no_read_pct
            FROM transactions t
            JOIN events e ON t.event_id = e.event_id
            JOIN log_files lf ON e.file_id = lf.file_id
            {where}
            GROUP BY lf.company_id, lf.store_id, lf.lane
            ORDER BY no_read_pct DESC""",
            params,
        ).fetchall()

        # Host latency by store
        latency_stats = self._db.conn.execute(
            f"""SELECT
                lf.company_id, lf.store_id,
                COUNT(*) as txn_count,
                ROUND(AVG(t.host_latency_ms), 0) as avg_latency,
                ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.host_latency_ms), 0) as p50_latency,
                ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY t.host_latency_ms), 0) as p95_latency,
                ROUND(MAX(t.host_latency_ms), 0) as max_latency
            FROM transactions t
            JOIN events e ON t.event_id = e.event_id
            JOIN log_files lf ON e.file_id = lf.file_id
            {where}
            AND t.host_latency_ms IS NOT NULL AND t.host_latency_ms > 0
            GROUP BY lf.company_id, lf.store_id
            ORDER BY avg_latency DESC""",
            params,
        ).fetchall()

        # Transaction duration by store
        duration_stats = self._db.conn.execute(
            f"""SELECT
                lf.company_id, lf.store_id,
                ROUND(AVG(e.duration_ms), 0) as avg_duration,
                ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY e.duration_ms), 0) as p50_duration,
                ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY e.duration_ms), 0) as p95_duration
            FROM events e
            JOIN log_files lf ON e.file_id = lf.file_id
            {where}
            AND e.event_type = 'transaction'
            GROUP BY lf.company_id, lf.store_id
            ORDER BY avg_duration DESC""",
            params,
        ).fetchall()

        return {
            "no_read_by_store_lane": [
                {
                    "company_id": r[0],
                    "store_id": r[1],
                    "lane": r[2],
                    "total_txns": r[3],
                    "no_reads": r[4],
                    "no_read_pct": r[5],
                }
                for r in no_read_stats
            ],
            "latency_by_store": [
                {
                    "company_id": r[0],
                    "store_id": r[1],
                    "txn_count": r[2],
                    "avg_latency_ms": r[3],
                    "p50_latency_ms": r[4],
                    "p95_latency_ms": r[5],
                    "max_latency_ms": r[6],
                }
                for r in latency_stats
            ],
            "duration_by_store": [
                {
                    "company_id": r[0],
                    "store_id": r[1],
                    "avg_duration_ms": r[2],
                    "p50_duration_ms": r[3],
                    "p95_duration_ms": r[4],
                }
                for r in duration_stats
            ],
        }

    def format_text_report(self, report: dict) -> str:
        """Format a report as plain text."""
        lines = []
        f = report["file"]
        lines.append(f"=== Log Analysis Report: {f['name']} ===")
        lines.append(f"Lane: {f['lane']}  Date: {f['date']}  Lines: {f['line_count']}")
        lines.append("")

        t = report["transactions"]
        approval_rate = (t["approved"] / t["total"] * 100) if t["total"] > 0 else 0
        lines.append(f"Transactions: {t['total']} ({approval_rate:.0f}% approved)")
        lines.append(f"Latency: avg {t['avg_latency_ms']}ms, max {t['max_latency_ms']}ms")
        lines.append("")

        issues = report.get("issues", [])
        if not issues:
            lines.append("No issues detected.")
        else:
            lines.append(f"--- {len(issues)} Issues Detected ---")
            for i, issue in enumerate(issues, 1):
                lines.append(f"\n#{i} [{issue['severity'].upper()}] {issue['issue_type']}")
                lines.append(f"   Confidence: {issue['confidence']:.0%}")
                lines.append(f"   Evidence: {issue.get('evidence', '')}")
                lines.append(f"   Time: {issue.get('time_range', '')}")
                if issue.get("resolution_steps"):
                    lines.append("   Resolution:")
                    for step in issue["resolution_steps"]:
                        lines.append(f"     {step}")

        return "\n".join(lines)
