"""Rule-based issue detection engine (Tier 1 - deterministic pattern matching)."""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Optional

from pinpad_analyzer.config.issue_types import ISSUE_TYPES, IssueType
from pinpad_analyzer.storage.database import Database


class RuleEngine:
    """Applies deterministic rules to detect known issue patterns."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def analyze_file(
        self,
        file_id: str,
        window_start: Optional[datetime] = None,
        window_end: Optional[datetime] = None,
    ) -> list[dict]:
        """Run all rules against a single ingested file.

        Args:
            file_id: The file to analyze.
            window_start: If set, only report issues overlapping this window.
            window_end: If set, only report issues overlapping this window.

        Returns list of detected issues.
        """
        issues: list[dict] = []

        issues.extend(self._check_scat_dead(file_id, window_start, window_end))
        issues.extend(self._check_serial_failures(file_id, window_start, window_end))
        issues.extend(self._check_servereps_errors(file_id, window_start, window_end))
        issues.extend(self._check_p2p_mismatch(file_id, window_start, window_end))
        issues.extend(self._check_transaction_health(file_id, window_start, window_end))
        issues.extend(self._check_host_latency(file_id, window_start, window_end))
        issues.extend(self._check_error_cascades(file_id, window_start, window_end))
        issues.extend(self._check_card_read_intermittent(file_id, window_start, window_end))

        return issues

    def _outside_window(
        self,
        event_start: datetime,
        event_end: datetime,
        window_start: Optional[datetime],
        window_end: Optional[datetime],
    ) -> bool:
        """Return True if the event is entirely outside the analysis window."""
        if window_start is None and window_end is None:
            return False
        if window_start and event_end < window_start:
            return True
        if window_end and event_start > window_end:
            return True
        return False

    def _time_filter_sql(
        self,
        ts_col: str,
        window_start: Optional[datetime],
        window_end: Optional[datetime],
    ) -> tuple[str, list]:
        """Build SQL WHERE clause fragment for time window filtering."""
        clauses = []
        params: list = []
        if window_start:
            clauses.append(f"{ts_col} >= ?")
            params.append(window_start)
        if window_end:
            clauses.append(f"{ts_col} <= ?")
            params.append(window_end)
        sql = (" AND " + " AND ".join(clauses)) if clauses else ""
        return sql, params

    def _check_scat_dead(
        self, file_id: str,
        window_start: Optional[datetime] = None,
        window_end: Optional[datetime] = None,
    ) -> list[dict]:
        """Check for SCAT dead periods."""
        rows = self._db.conn.execute("""
            SELECT timestamp, alive_status
            FROM scat_timeline
            WHERE file_id = ?
            ORDER BY timestamp
        """, [file_id]).fetchall()

        if not rows:
            return []

        issues = []
        dead_start = None
        for ts, status in rows:
            if status == 0 and dead_start is None:
                dead_start = ts
            elif status != 0 and dead_start is not None:
                duration_sec = (ts - dead_start).total_seconds()
                if duration_sec > 60:
                    if not self._outside_window(dead_start, ts, window_start, window_end):
                        issue_type = ISSUE_TYPES["scat_dead"]
                        confidence = min(0.95, 0.5 + duration_sec / 3600)
                        issues.append(self._make_issue(
                            issue_type,
                            confidence=confidence,
                            time_range=f"{dead_start} - {ts}",
                            evidence=f"Pinpad dead for {duration_sec/60:.1f} minutes",
                        ))
                dead_start = None

        # Still dead at end of file
        if dead_start is not None:
            last_ts = rows[-1][0]
            duration_sec = (last_ts - dead_start).total_seconds()
            if duration_sec > 60:
                if not self._outside_window(dead_start, last_ts, window_start, window_end):
                    issue_type = ISSUE_TYPES["scat_dead"]
                    issues.append(self._make_issue(
                        issue_type,
                        confidence=0.9,
                        time_range=f"{dead_start} - (end of file)",
                        evidence=f"Pinpad dead for {duration_sec/60:.1f}+ minutes (still dead)",
                    ))

        return issues

    def _check_serial_failures(
        self, file_id: str,
        window_start: Optional[datetime] = None,
        window_end: Optional[datetime] = None,
    ) -> list[dict]:
        """Check for serial communication failure patterns."""
        time_sql, time_params = self._time_filter_sql("timestamp", window_start, window_end)
        rows = self._db.conn.execute(f"""
            SELECT timestamp, message
            FROM log_entries
            WHERE file_id = ?
              AND message LIKE '%SendMsgWaitAck3Tries failed%'
              {time_sql}
            ORDER BY timestamp
        """, [file_id] + time_params).fetchall()

        if not rows:
            return []

        # Group by clusters (failures within 10 seconds)
        clusters: list[list] = []
        current_cluster: list = [rows[0]]
        for row in rows[1:]:
            if (row[0] - current_cluster[-1][0]).total_seconds() < 10:
                current_cluster.append(row)
            else:
                clusters.append(current_cluster)
                current_cluster = [row]
        clusters.append(current_cluster)

        issues = []
        issue_type = ISSUE_TYPES["serial_comm_failure"]
        for cluster in clusters:
            if len(cluster) >= 2:
                issues.append(self._make_issue(
                    issue_type,
                    confidence=min(0.95, 0.6 + len(cluster) * 0.1),
                    time_range=f"{cluster[0][0]} - {cluster[-1][0]}",
                    evidence=f"{len(cluster)} ACK failures in {(cluster[-1][0]-cluster[0][0]).total_seconds():.0f}s",
                ))

        return issues

    def _check_servereps_errors(
        self, file_id: str,
        window_start: Optional[datetime] = None,
        window_end: Optional[datetime] = None,
    ) -> list[dict]:
        """Check for ServerEPS HTTP 500 / Socket errors."""
        time_sql, time_params = self._time_filter_sql("e.start_time", window_start, window_end)
        rows = self._db.conn.execute(f"""
            SELECT e.start_time, hc.check_type, hc.http_status, hc.error_code
            FROM events e
            JOIN health_checks hc ON e.event_id = hc.event_id
            WHERE e.file_id = ?
              AND hc.success = FALSE
              {time_sql}
            ORDER BY e.start_time
        """, [file_id] + time_params).fetchall()

        if not rows:
            return []

        issues = []
        http_500s = [r for r in rows if r[2] == "500"]
        socket_errors = [r for r in rows if r[2] and r[2].startswith("Socket_")]

        if http_500s:
            issue_type = ISSUE_TYPES["servereps_500"]
            issues.append(self._make_issue(
                issue_type,
                confidence=0.95,
                time_range=f"{http_500s[0][0]} - {http_500s[-1][0]}",
                evidence=f"{len(http_500s)} HTTP 500 errors across {set(r[1] for r in http_500s)}",
            ))

        if socket_errors:
            issue_type = ISSUE_TYPES["servereps_socket_error"]
            error_codes = set(r[2] for r in socket_errors)
            issues.append(self._make_issue(
                issue_type,
                confidence=0.9,
                time_range=f"{socket_errors[0][0]} - {socket_errors[-1][0]}",
                evidence=f"{len(socket_errors)} socket errors: {error_codes}",
            ))

        return issues

    def _check_p2p_mismatch(
        self, file_id: str,
        window_start: Optional[datetime] = None,
        window_end: Optional[datetime] = None,
    ) -> list[dict]:
        """Check for P2P encryption mismatch causing SCAT death."""
        time_sql, time_params = self._time_filter_sql("timestamp", window_start, window_end)
        rows = self._db.conn.execute(f"""
            SELECT timestamp, message
            FROM log_entries
            WHERE file_id = ?
              AND message LIKE '%IsP2PDLL=Y, IsTermP2PCapable=N%'
              {time_sql}
            LIMIT 1
        """, [file_id] + time_params).fetchall()

        if not rows:
            return []

        issue_type = ISSUE_TYPES["p2p_encryption_mismatch"]
        return [self._make_issue(
            issue_type,
            confidence=0.99,
            time_range=str(rows[0][0]),
            evidence="P2P DLL requires encryption but terminal reports not capable",
        )]

    def _check_transaction_health(
        self, file_id: str,
        window_start: Optional[datetime] = None,
        window_end: Optional[datetime] = None,
    ) -> list[dict]:
        """Check for transaction-level issues (declines, aborts)."""
        time_sql, time_params = self._time_filter_sql("e.start_time", window_start, window_end)
        txn_rows = self._db.conn.execute(f"""
            SELECT e.start_time, t.response_code, t.card_type, t.pan_last4
            FROM events e
            JOIN transactions t ON e.event_id = t.event_id
            WHERE e.file_id = ?
              {time_sql}
            ORDER BY e.start_time
        """, [file_id] + time_params).fetchall()

        if not txn_rows:
            return []

        issues = []
        consecutive_declines = 0
        max_consecutive = 0
        decline_start = None
        for row in txn_rows:
            if row[1] in ("DD", "DN"):
                if consecutive_declines == 0:
                    decline_start = row[0]
                consecutive_declines += 1
                max_consecutive = max(max_consecutive, consecutive_declines)
            else:
                consecutive_declines = 0

        if max_consecutive >= 3:
            issue_type = ISSUE_TYPES["repeated_decline"]
            issues.append(self._make_issue(
                issue_type,
                confidence=0.7,
                time_range=str(decline_start) if decline_start else "",
                evidence=f"{max_consecutive} consecutive declines",
            ))

        return issues

    def _check_host_latency(
        self, file_id: str,
        window_start: Optional[datetime] = None,
        window_end: Optional[datetime] = None,
    ) -> list[dict]:
        """Check for high host latency."""
        time_sql, time_params = self._time_filter_sql("e.start_time", window_start, window_end)
        rows = self._db.conn.execute(f"""
            SELECT e.start_time, t.host_latency_ms, t.host_url
            FROM events e
            JOIN transactions t ON e.event_id = t.event_id
            WHERE e.file_id = ?
              AND t.host_latency_ms > 5000
              {time_sql}
            ORDER BY t.host_latency_ms DESC
            LIMIT 5
        """, [file_id] + time_params).fetchall()

        if not rows:
            return []

        issue_type = ISSUE_TYPES["host_timeout"]
        max_latency = rows[0][1]
        return [self._make_issue(
            issue_type,
            confidence=min(0.9, 0.5 + max_latency / 20000),
            time_range=str(rows[0][0]),
            evidence=f"{len(rows)} transactions with latency > 5s (max: {max_latency:.0f}ms)",
        )]

    def _check_error_cascades(
        self, file_id: str,
        window_start: Optional[datetime] = None,
        window_end: Optional[datetime] = None,
    ) -> list[dict]:
        """Report significant error cascades."""
        time_sql, time_params = self._time_filter_sql("e.start_time", window_start, window_end)
        rows = self._db.conn.execute(f"""
            SELECT e.start_time, e.end_time, e.duration_ms,
                   ec.error_pattern, ec.error_count, ec.recovery_achieved
            FROM events e
            JOIN error_cascades ec ON e.event_id = ec.event_id
            WHERE e.file_id = ?
              AND ec.error_count >= 3
              {time_sql}
            ORDER BY ec.error_count DESC
        """, [file_id] + time_params).fetchall()

        issues = []
        for row in rows:
            pattern = row[3] or "Unknown"
            if "SendMsgWaitAck3Tries" in pattern:
                issue_type = ISSUE_TYPES["serial_comm_failure"]
            else:
                issue_type = ISSUE_TYPES["serial_comm_failure"]

            issues.append(self._make_issue(
                issue_type,
                confidence=min(0.9, 0.5 + row[4] * 0.05),
                time_range=f"{row[0]} - {row[1]}",
                evidence=f"Error cascade: {row[4]} '{pattern}' errors, recovered={row[5]}",
            ))

        return issues

    def _check_card_read_intermittent(
        self, file_id: str,
        window_start: Optional[datetime] = None,
        window_end: Optional[datetime] = None,
    ) -> list[dict]:
        """Detect intermittent card read failures.

        Pattern: transactions where BeginOrder->EndOrder completes but no card
        data was read (no card_type, no host call, $0 amount). These are
        customers standing at the pinpad waiting for the reader to respond.

        Cashier-cancel filtering: transactions under 15 seconds are excluded
        as likely "pressed credit then cleared" behavior. Analysis of 25,760
        no-reads showed only 2.6% are under 15s, while 97.4% are system
        timeouts (peak at the configured 45s POS timeout).
        """
        time_sql, time_params = self._time_filter_sql("e.start_time", window_start, window_end)

        # Get all transactions in the window
        rows = self._db.conn.execute(f"""
            SELECT e.start_time, e.end_time, e.duration_ms,
                   t.is_approved, t.card_type, t.host_latency_ms,
                   t.amount_cents, t.entry_method
            FROM events e
            JOIN transactions t ON e.event_id = t.event_id
            WHERE e.file_id = ?
              {time_sql}
            ORDER BY e.start_time
        """, [file_id] + time_params).fetchall()

        if len(rows) < 5:
            return []

        total = len(rows)
        # MIN_WAIT_MS: exclude transactions under 15s as likely cashier cancels
        # (cashier pressed credit button then cleared before customer presented card)
        MIN_WAIT_MS = 15_000
        no_reads = []
        quick_cancels = 0
        for r in rows:
            is_noread = (
                not r[3]  # not approved
                and (not r[4] or r[4] == "")  # no card type
                and (r[5] is None or r[5] == 0)  # no host call
            )
            if is_noread:
                if r[2] is not None and r[2] < MIN_WAIT_MS:
                    quick_cancels += 1
                else:
                    no_reads.append(r)

        no_read_count = len(no_reads)
        if no_read_count == 0:
            return []

        no_read_pct = no_read_count * 100.0 / total

        issues = []

        # Check for bursts: 3+ consecutive no-reads (excluding quick cancels)
        max_burst = 0
        burst_start = None
        burst_count = 0
        for r in rows:
            is_real_noread = (
                not r[3]
                and (not r[4] or r[4] == "")
                and (r[5] is None or r[5] == 0)
                and (r[2] is None or r[2] >= MIN_WAIT_MS)
            )
            if is_real_noread:
                if burst_count == 0:
                    burst_start = r[0]
                burst_count += 1
            else:
                if burst_count > max_burst:
                    max_burst = burst_count
                burst_count = 0
        if burst_count > max_burst:
            max_burst = burst_count

        # Calculate avg customer wait time on failed attempts
        wait_times = [r[2] for r in no_reads if r[2] is not None and r[2] > 0]
        avg_wait_s = sum(wait_times) / len(wait_times) / 1000 if wait_times else 0
        total_wait_s = sum(wait_times) / 1000 if wait_times else 0

        # Classify by timeout correlation (POS timeout is typically 45s)
        timeout_zone = sum(1 for r in no_reads if r[2] and 30_000 <= r[2] <= 55_000)
        timeout_pct = timeout_zone * 100.0 / no_read_count if no_read_count else 0

        # Determine severity and confidence based on rate and burst
        if max_burst >= 3 or no_read_pct >= 30:
            # Acute episode - burst of failures
            confidence = min(0.95, 0.7 + max_burst * 0.05)
            issue_type = ISSUE_TYPES["card_read_intermittent"]

            evidence_parts = [
                f"{no_read_count}/{total} transactions failed to read card ({no_read_pct:.0f}%)",
            ]
            if quick_cancels > 0:
                evidence_parts.append(f"{quick_cancels} quick cancels (<15s) excluded as likely cashier behavior")
            if max_burst >= 3:
                evidence_parts.append(f"Burst of {max_burst} consecutive no-reads detected")
            evidence_parts.append(f"Avg customer wait: {avg_wait_s:.0f}s per failed attempt")
            if timeout_pct > 20:
                evidence_parts.append(f"{timeout_pct:.0f}% of failures cluster at POS timeout (30-55s)")
            evidence_parts.append(f"Total wasted wait time: {total_wait_s/60:.1f} minutes")

            time_range = f"{no_reads[0][0]} - {no_reads[-1][0]}"
            issues.append(self._make_issue(
                issue_type,
                confidence=confidence,
                time_range=time_range,
                evidence="; ".join(evidence_parts),
            ))

        elif no_read_pct >= 15 and total >= 10:
            # Chronic degradation - elevated baseline
            confidence = min(0.85, 0.5 + no_read_pct / 100)
            issue_type = ISSUE_TYPES["card_read_intermittent"]

            evidence_parts = [
                f"{no_read_count}/{total} transactions failed to read card ({no_read_pct:.0f}%)",
            ]
            if quick_cancels > 0:
                evidence_parts.append(f"{quick_cancels} quick cancels (<15s) excluded as likely cashier behavior")
            evidence_parts.append(f"Avg customer wait: {avg_wait_s:.0f}s per failed attempt")
            if timeout_pct > 20:
                evidence_parts.append(f"{timeout_pct:.0f}% of failures cluster at POS timeout (30-55s)")
            evidence_parts.append(f"Total wasted wait time: {total_wait_s/60:.1f} minutes")

            # Check hourly pattern for startup spike
            hourly = self._get_hourly_noread_rate(file_id)
            morning_rate = sum(h[1] for h in hourly if h[0] in (7, 8))
            morning_total = sum(h[2] for h in hourly if h[0] in (7, 8))
            if morning_total > 0:
                morning_pct = morning_rate * 100.0 / morning_total
                if morning_pct > no_read_pct + 5:
                    evidence_parts.append(
                        f"Morning spike: {morning_pct:.0f}% fail rate at 7-8 AM "
                        f"(vs {no_read_pct:.0f}% overall) - possible startup/thermal issue"
                    )

            time_range = f"{no_reads[0][0]} - {no_reads[-1][0]}"
            issues.append(self._make_issue(
                issue_type,
                confidence=confidence,
                time_range=time_range,
                evidence="; ".join(evidence_parts),
            ))

        return issues

    def _get_hourly_noread_rate(self, file_id: str) -> list[tuple[int, int, int]]:
        """Get hourly no-card-read count and total txns for a file.

        Returns list of (hour, noread_count, total_count).
        """
        rows = self._db.conn.execute("""
            SELECT EXTRACT(HOUR FROM e.start_time) as hour,
                   SUM(CASE WHEN t.is_approved = false
                            AND (t.card_type IS NULL OR t.card_type = '')
                            AND (t.host_latency_ms IS NULL OR t.host_latency_ms = 0)
                       THEN 1 ELSE 0 END) as noread,
                   COUNT(*) as total
            FROM events e
            JOIN transactions t ON e.event_id = t.event_id
            WHERE e.file_id = ? AND e.event_type = 'transaction'
            GROUP BY EXTRACT(HOUR FROM e.start_time)
        """, [file_id]).fetchall()
        return [(int(r[0]), r[1], r[2]) for r in rows]

    @staticmethod
    def _make_issue(
        issue_type: IssueType,
        confidence: float,
        time_range: str,
        evidence: str,
    ) -> dict:
        return {
            "issue_type": issue_type.name,
            "severity": issue_type.severity,
            "severity_rank": issue_type.severity_rank,
            "confidence": confidence,
            "description": issue_type.description,
            "time_range": time_range,
            "evidence": evidence,
            "resolution_steps": issue_type.resolution_steps,
        }
