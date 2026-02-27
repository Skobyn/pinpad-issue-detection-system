"""Cross-store pattern library for learning from resolved cases."""

from __future__ import annotations

from pinpad_analyzer.assistant.case_db import CaseDB
from pinpad_analyzer.storage.database import Database


class PatternLibrary:
    """Manages cross-store issue patterns learned from resolved cases."""

    def __init__(self, db: Database) -> None:
        self._db = db
        self._case_db = CaseDB(db)

    def learn_from_case(self, case_id: str) -> list[str]:
        """Extract and store patterns from a resolved case.

        Returns list of pattern IDs created.
        """
        case = self._case_db.get_case(case_id)
        if not case or case.get("resolution_status") != "resolved":
            return []

        pattern_ids = []
        root_cause = case.get("root_cause", "")
        store_id = case.get("store_id", "")

        if not root_cause:
            return []

        # Check if similar pattern already exists
        existing = self._find_similar_pattern(root_cause)
        if existing:
            # Update frequency and add store
            self._update_pattern(existing["pattern_id"], store_id)
            return [existing["pattern_id"]]

        # Create new pattern
        pattern_type = self._classify_pattern(root_cause)
        pattern_id = self._case_db.add_pattern(
            case_id=case_id,
            pattern_type=pattern_type,
            pattern_text=root_cause,
            confidence=case.get("root_cause_confidence", 0.5),
            stores=store_id,
        )
        pattern_ids.append(pattern_id)

        return pattern_ids

    def get_patterns_for_issue(self, issue_type: str) -> list[dict]:
        """Get all learned patterns for an issue type."""
        return self._case_db.find_patterns_by_type(issue_type)

    def get_pattern_stats(self) -> dict:
        """Get statistics about the pattern library."""
        rows = self._db.conn.execute(
            """SELECT pattern_type, COUNT(*) as cnt, SUM(frequency) as total_freq
               FROM case_patterns
               GROUP BY pattern_type
               ORDER BY total_freq DESC"""
        ).fetchall()

        return {
            "patterns": [
                {"type": r[0], "count": r[1], "total_frequency": r[2]}
                for r in rows
            ],
            "total_patterns": sum(r[1] for r in rows),
        }

    def _find_similar_pattern(self, root_cause: str) -> dict | None:
        """Find an existing pattern similar to the given root cause."""
        # Simple keyword matching for now
        keywords = root_cause.lower().split()[:3]
        for keyword in keywords:
            if len(keyword) < 4:
                continue
            rows = self._db.conn.execute(
                """SELECT pattern_id, pattern_text, frequency, stores
                   FROM case_patterns
                   WHERE LOWER(pattern_text) LIKE ?
                   LIMIT 1""",
                [f"%{keyword}%"],
            ).fetchall()

            if rows:
                return {
                    "pattern_id": rows[0][0],
                    "pattern_text": rows[0][1],
                    "frequency": rows[0][2],
                    "stores": rows[0][3],
                }

        return None

    def _update_pattern(self, pattern_id: str, store_id: str) -> None:
        """Increment frequency and add store to existing pattern."""
        row = self._db.conn.execute(
            "SELECT frequency, stores FROM case_patterns WHERE pattern_id = ?",
            [pattern_id],
        ).fetchone()

        if not row:
            return

        new_freq = (row[0] or 0) + 1
        existing_stores = row[1] or ""
        if store_id and store_id not in existing_stores:
            new_stores = f"{existing_stores},{store_id}" if existing_stores else store_id
        else:
            new_stores = existing_stores

        self._db.conn.execute(
            "UPDATE case_patterns SET frequency = ?, stores = ? WHERE pattern_id = ?",
            [new_freq, new_stores, pattern_id],
        )

    @staticmethod
    def _classify_pattern(root_cause: str) -> str:
        """Classify a root cause into a pattern type."""
        root_lower = root_cause.lower()

        if any(w in root_lower for w in ["p2p", "encryption"]):
            return "p2p_encryption_mismatch"
        if any(w in root_lower for w in ["scat", "dead", "unresponsive"]):
            return "scat_dead"
        if any(w in root_lower for w in ["serial", "cable", "com port"]):
            return "serial_comm_failure"
        if any(w in root_lower for w in ["500", "server"]):
            return "servereps_500"
        if any(w in root_lower for w in ["socket", "network"]):
            return "servereps_socket_error"
        if any(w in root_lower for w in ["timeout", "latency"]):
            return "host_timeout"

        return "unknown"
