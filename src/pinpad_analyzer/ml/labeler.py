"""Label generator: converts verified case resolutions into training labels."""

from __future__ import annotations

from typing import Optional

from pinpad_analyzer.storage.database import Database


class Labeler:
    """Generates training labels from verified case resolutions."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def generate_labels(self) -> list[dict]:
        """Generate labeled training data from resolved and verified cases.

        Returns list of {event_id, label, confidence} dicts.
        """
        # Get verified resolved cases
        cases = self._db.conn.execute(
            """SELECT case_id, root_cause, root_cause_confidence,
                      incident_time, store_id, lane_number
               FROM cases
               WHERE resolution_status = 'resolved'
                 AND tech_verified = TRUE
                 AND root_cause IS NOT NULL"""
        ).fetchall()

        labels = []
        for case_id, root_cause, confidence, incident_time, store_id, lane in cases:
            # Find transaction events that match this case's context
            event_ids = self._find_case_events(
                incident_time, store_id, lane
            )

            # Map root cause to issue type label
            label = self._root_cause_to_label(root_cause)

            for event_id in event_ids:
                labels.append({
                    "event_id": event_id,
                    "label": label,
                    "confidence": confidence or 0.5,
                    "case_id": case_id,
                })

        return labels

    def _find_case_events(
        self,
        incident_time: Optional[object],
        store_id: str,
        lane: int,
    ) -> list[str]:
        """Find events matching case context."""
        query = """
            SELECT e.event_id
            FROM events e
            JOIN log_files lf ON e.file_id = lf.file_id
            WHERE 1=1"""
        params = []

        if store_id:
            query += " AND lf.store_id = ?"
            params.append(store_id)
        if lane and lane > 0:
            query += " AND e.lane = ?"
            params.append(lane)
        if incident_time:
            query += " AND DATE_TRUNC('day', e.start_time) = DATE_TRUNC('day', ?::TIMESTAMP)"
            params.append(str(incident_time))

        query += " LIMIT 100"
        rows = self._db.conn.execute(query, params).fetchall()
        return [r[0] for r in rows]

    @staticmethod
    def _root_cause_to_label(root_cause: str) -> str:
        """Map free-text root cause to standardized label."""
        root_lower = root_cause.lower()

        if "p2p" in root_lower or "encryption" in root_lower:
            return "p2p_encryption_mismatch"
        if "scat" in root_lower or "dead" in root_lower or "unresponsive" in root_lower:
            return "scat_dead"
        if "serial" in root_lower or "comm" in root_lower or "cable" in root_lower:
            return "serial_comm_failure"
        if "500" in root_lower or "servereps" in root_lower:
            return "servereps_500"
        if "socket" in root_lower or "network" in root_lower:
            return "servereps_socket_error"
        if "timeout" in root_lower or "latency" in root_lower:
            return "host_timeout"
        if "decline" in root_lower:
            return "repeated_decline"
        if "chip" in root_lower or "fallback" in root_lower:
            return "chip_read_failure"

        return "unknown"

    def store_labels(self, labels: list[dict]) -> int:
        """Store labels as predictions in the database for training.

        Returns count of labels stored.
        """
        count = 0
        for label_data in labels:
            try:
                # Get next prediction ID
                max_id = self._db.conn.execute(
                    "SELECT COALESCE(MAX(prediction_id), 0) FROM predictions"
                ).fetchone()[0]

                self._db.conn.execute(
                    """INSERT INTO predictions
                       (prediction_id, event_id, model_id, model_version,
                        prediction_type, label, confidence, details)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    [
                        max_id + 1,
                        label_data["event_id"],
                        "tech_verified",
                        "1.0",
                        "label",
                        label_data["label"],
                        label_data["confidence"],
                        f"From case {label_data.get('case_id', '')}",
                    ],
                )
                count += 1
            except Exception:
                continue

        return count
