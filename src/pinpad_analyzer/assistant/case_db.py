"""Case database: CRUD operations for technician cases."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from pinpad_analyzer.storage.database import Database


class CaseDB:
    """CRUD operations on the cases and case_patterns tables."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def create_case(
        self,
        symptom_description: str,
        company_id: str = "",
        store_id: str = "",
        lane_number: int = 0,
        incident_time: Optional[datetime] = None,
        root_cause: str = "",
        root_cause_confidence: float = 0.0,
        evidence_summary: str = "",
        resolution_steps: str = "",
        ml_labels: str = "",
        tags: str = "",
    ) -> str:
        """Create a new case. Returns case_id."""
        case_id = uuid.uuid4().hex[:12]
        self._db.conn.execute(
            """INSERT INTO cases
               (case_id, company_id, store_id, lane_number, incident_time,
                symptom_description, root_cause, root_cause_confidence,
                evidence_summary, resolution_steps, ml_labels, tags)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                case_id, company_id, store_id, lane_number, incident_time,
                symptom_description, root_cause, root_cause_confidence,
                evidence_summary, resolution_steps, ml_labels, tags,
            ],
        )
        return case_id

    def get_case(self, case_id: str) -> Optional[dict]:
        """Get a case by ID."""
        row = self._db.conn.execute(
            "SELECT * FROM cases WHERE case_id = ?", [case_id]
        ).fetchone()
        if not row:
            return None

        cols = [
            "case_id", "company_id", "store_id", "lane_number",
            "incident_time", "symptom_description", "root_cause",
            "root_cause_confidence", "evidence_summary", "evidence_log_lines",
            "resolution_steps", "resolution_status", "tech_verified",
            "ml_labels", "tags", "created_at", "resolved_at",
        ]
        return dict(zip(cols, row))

    def list_cases(
        self,
        status: str = "",
        limit: int = 50,
    ) -> list[dict]:
        """List cases, optionally filtered by status."""
        query = "SELECT case_id, company_id, store_id, lane_number, symptom_description, root_cause, resolution_status, created_at FROM cases"
        params = []
        if status:
            query += " WHERE resolution_status = ?"
            params.append(status)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        rows = self._db.conn.execute(query, params).fetchall()
        return [
            {
                "case_id": r[0],
                "company_id": r[1],
                "store_id": r[2],
                "lane_number": r[3],
                "symptom": r[4][:80] if r[4] else "",
                "root_cause": r[5] or "",
                "status": r[6],
                "created_at": str(r[7]),
            }
            for r in rows
        ]

    def update_case(self, case_id: str, **fields: object) -> bool:
        """Update case fields. Returns True if case found."""
        if not fields:
            return False

        set_clauses = []
        params = []
        for key, value in fields.items():
            set_clauses.append(f"{key} = ?")
            params.append(value)
        params.append(case_id)

        result = self._db.conn.execute(
            f"UPDATE cases SET {', '.join(set_clauses)} WHERE case_id = ?",
            params,
        )
        return True

    def resolve_case(
        self,
        case_id: str,
        resolution_steps: str = "",
        tech_verified: bool = True,
    ) -> bool:
        """Mark a case as resolved."""
        return self.update_case(
            case_id,
            resolution_status="resolved",
            resolution_steps=resolution_steps,
            tech_verified=tech_verified,
            resolved_at=datetime.now(),
        )

    def add_pattern(
        self,
        case_id: str,
        pattern_type: str,
        pattern_text: str,
        confidence: float = 0.5,
        stores: str = "",
    ) -> str:
        """Add a pattern associated with a case."""
        pattern_id = uuid.uuid4().hex[:12]
        self._db.conn.execute(
            """INSERT INTO case_patterns
               (pattern_id, case_id, pattern_type, pattern_text, confidence, stores)
               VALUES (?, ?, ?, ?, ?, ?)""",
            [pattern_id, case_id, pattern_type, pattern_text, confidence, stores],
        )
        return pattern_id

    def find_patterns_by_type(self, pattern_type: str) -> list[dict]:
        """Find all patterns of a given type."""
        rows = self._db.conn.execute(
            """SELECT cp.pattern_id, cp.case_id, cp.pattern_text,
                      cp.frequency, cp.confidence, cp.stores,
                      c.root_cause, c.resolution_steps
               FROM case_patterns cp
               JOIN cases c ON cp.case_id = c.case_id
               WHERE cp.pattern_type = ?
               ORDER BY cp.frequency DESC""",
            [pattern_type],
        ).fetchall()

        return [
            {
                "pattern_id": r[0],
                "case_id": r[1],
                "pattern_text": r[2],
                "frequency": r[3],
                "confidence": r[4],
                "stores": r[5],
                "root_cause": r[6],
                "resolution_steps": r[7],
            }
            for r in rows
        ]
