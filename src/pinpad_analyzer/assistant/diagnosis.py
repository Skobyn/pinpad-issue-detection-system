"""Diagnosis engine: combines rules + ML + domain knowledge for root cause analysis."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pinpad_analyzer.assistant.domain_knowledge import DOMAIN_PATTERNS, RESPONSE_CODES
from pinpad_analyzer.assistant.evidence_builder import EvidenceBuilder
from pinpad_analyzer.assistant.query_engine import QueryEngine
from pinpad_analyzer.ml.rules import RuleEngine
from pinpad_analyzer.storage.database import Database


class DiagnosisEngine:
    """AI-powered root cause analysis combining rules, ML, and domain knowledge."""

    def __init__(self, db: Database) -> None:
        self._db = db
        self._query = QueryEngine(db)
        self._evidence = EvidenceBuilder(db)
        self._rules = RuleEngine(db)

    def diagnose(self, context: dict) -> list[dict]:
        """Run diagnosis given technician context.

        Args:
            context: Dict with keys: company_id, store_id, lane_number,
                    incident_time, symptom_description

        Returns:
            List of diagnosis results, ranked by confidence.
        """
        # Parse incident time
        incident_time = self._parse_time(context.get("incident_time", ""))

        # Find matching log files
        files = self._query.find_matching_files(
            company_id=context.get("company_id", ""),
            store_id=context.get("store_id", ""),
            lane_number=context.get("lane_number", 0),
            incident_time=incident_time,
        )

        if not files:
            # Fall back to any available files
            files = self._query.find_matching_files()

        if not files:
            return []

        all_diagnoses = []
        for file_info in files:
            file_id = file_info["file_id"]
            diagnoses = self._diagnose_file(file_id, incident_time, context)
            all_diagnoses.extend(diagnoses)

        # Deduplicate by root cause, keeping highest confidence
        seen = {}
        for d in all_diagnoses:
            key = d["root_cause"]
            if key not in seen or d["confidence"] > seen[key]["confidence"]:
                seen[key] = d

        results = sorted(seen.values(), key=lambda x: -x["confidence"])
        return results

    def _diagnose_file(
        self,
        file_id: str,
        incident_time: Optional[datetime],
        context: dict,
    ) -> list[dict]:
        """Run diagnosis on a single file."""
        # Use a time window around the incident (default +-30 min)
        window_start = None
        window_end = None
        if incident_time:
            from datetime import timedelta
            window_start = incident_time - timedelta(minutes=30)
            window_end = incident_time + timedelta(minutes=30)

        # Run rule-based detection within the time window
        rule_issues = self._rules.analyze_file(file_id, window_start, window_end)

        # Get file summary for context
        summary = self._evidence.summarize_file(file_id)

        # Build timeline around incident
        timeline = self._evidence.build_timeline(
            file_id,
            start_time=window_start,
            end_time=window_end,
        )

        # Convert issues to diagnosis results
        diagnoses = []
        for issue in rule_issues:
            diagnosis = self._issue_to_diagnosis(issue, summary, timeline, context)
            diagnoses.append(diagnosis)

        # Add symptom-based inference if no rules matched
        if not diagnoses and context.get("symptom_description"):
            symptom_diagnosis = self._infer_from_symptom(
                context["symptom_description"], summary, file_id
            )
            if symptom_diagnosis:
                diagnoses.append(symptom_diagnosis)

        return diagnoses

    def _issue_to_diagnosis(
        self,
        issue: dict,
        summary: dict,
        timeline: list[dict],
        context: dict,
    ) -> dict:
        """Convert a detected issue into a full diagnosis result."""
        issue_type = issue["issue_type"]
        domain_info = DOMAIN_PATTERNS.get(issue_type, {})

        # Build evidence list
        evidence = [issue.get("evidence", "")]

        # Add domain context
        if domain_info.get("meaning"):
            evidence.append(f"Meaning: {domain_info['meaning']}")

        # Add relevant timeline entries
        relevant_events = [
            e for e in timeline
            if e["severity"] in ("critical", "high")
        ][:5]
        for event in relevant_events:
            evidence.append(f"[{event['type']}] {event['summary']}")

        # Add summary context
        if summary.get("scat_dead_minutes", 0) > 1:
            evidence.append(f"Total SCAT dead time: {summary['scat_dead_minutes']} minutes")
        if summary.get("total_errors", 0) > 0:
            evidence.append(f"Error cascades: {summary['error_cascades']} ({summary['total_errors']} total errors)")

        # Determine root cause with correlation analysis
        root_cause = self._determine_root_cause(issue_type, domain_info, summary)

        # Gather resolution steps
        resolution_steps = issue.get("resolution_steps", [])
        if domain_info.get("common_causes"):
            resolution_steps = list(resolution_steps)  # copy
            resolution_steps.append("Common causes: " + "; ".join(domain_info["common_causes"][:3]))

        # Find similar past cases
        similar_cases = self._find_similar_cases(issue_type)

        return {
            "root_cause": root_cause,
            "issue_type": issue_type,
            "severity": issue["severity"],
            "confidence": issue["confidence"],
            "evidence": evidence,
            "resolution_steps": resolution_steps,
            "similar_cases": similar_cases,
            "domain_context": domain_info.get("meaning", ""),
        }

    def _determine_root_cause(
        self, issue_type: str, domain_info: dict, summary: dict
    ) -> str:
        """Use domain knowledge to determine the most likely root cause."""
        # P2P mismatch is almost always THE root cause of SCAT dead
        if issue_type == "p2p_encryption_mismatch":
            return "P2P Encryption Mismatch - terminal not P2P capable but DLL requires it"

        if issue_type == "scat_dead":
            if summary.get("scat_dead_minutes", 0) > 60:
                return "Extended SCAT Dead Period - pinpad unresponsive, likely P2P mismatch or hardware issue"
            return "SCAT Dead - pinpad temporarily unresponsive"

        if issue_type == "serial_comm_failure":
            return "Serial Communication Failure - USB/COM port connection issue"

        if issue_type == "servereps_500":
            return "ServerEPS Backend Error - server returning HTTP 500"

        if issue_type == "servereps_socket_error":
            return "Network Connectivity Issue - cannot reach ServerEPS"

        if issue_type == "host_timeout":
            return "Host Authorization Timeout - slow payment processor response"

        if issue_type == "repeated_decline":
            return "Repeated Transaction Declines - likely card/issuer issue"

        if issue_type == "card_read_intermittent":
            return "Intermittent Card Read Failure - pinpad reader not consistently reading cards"

        return f"Detected Issue: {issue_type}"

    def _infer_from_symptom(
        self, symptom: str, summary: dict, file_id: str
    ) -> Optional[dict]:
        """Infer diagnosis from symptom description when no rules matched."""
        symptom_lower = symptom.lower()

        # Keyword matching for common symptoms
        if any(w in symptom_lower for w in ["dead", "unresponsive", "not responding", "frozen"]):
            if summary.get("scat_dead_minutes", 0) > 1:
                return {
                    "root_cause": "Pinpad Unresponsive",
                    "issue_type": "scat_dead",
                    "severity": "critical",
                    "confidence": 0.7,
                    "evidence": [
                        f"Symptom: {symptom}",
                        f"SCAT dead time: {summary.get('scat_dead_minutes', 0)} minutes",
                    ],
                    "resolution_steps": [
                        "1. Power cycle the pinpad (unplug for 30 seconds)",
                        "2. Check USB/serial cable connections",
                        "3. Verify COM port settings",
                        "4. Check for P2P encryption mismatch",
                    ],
                    "similar_cases": "",
                    "domain_context": "SCAT (Self-Contained Automated Terminal) is unresponsive",
                }

        if any(w in symptom_lower for w in ["decline", "denied", "won't approve"]):
            return {
                "root_cause": "Transaction Processing Issue",
                "issue_type": "repeated_decline",
                "severity": "medium",
                "confidence": 0.5,
                "evidence": [
                    f"Symptom: {symptom}",
                    f"Transactions: {summary.get('transactions', 0)}, Approved: {summary.get('approved', 0)}",
                ],
                "resolution_steps": [
                    "1. Check if same card or all cards affected",
                    "2. Verify host connectivity (ServerEPS)",
                    "3. Check response codes for decline reason",
                ],
                "similar_cases": "",
                "domain_context": "Transaction declines can be card-specific or system-wide",
            }

        if any(w in symptom_lower for w in ["slow", "timeout", "taking long"]):
            return {
                "root_cause": "Latency/Performance Issue",
                "issue_type": "host_timeout",
                "severity": "high",
                "confidence": 0.5,
                "evidence": [
                    f"Symptom: {symptom}",
                    f"Avg latency: {summary.get('avg_latency_ms', 0)}ms",
                ],
                "resolution_steps": [
                    "1. Check network connectivity",
                    "2. Monitor host latency trends",
                    "3. Verify ServerEPS service status",
                ],
                "similar_cases": "",
                "domain_context": "High latency usually indicates network or host issues",
            }

        return None

    def _find_similar_cases(self, issue_type: str) -> str:
        """Find previously resolved cases with the same issue type."""
        rows = self._db.conn.execute(
            """SELECT COUNT(*) as cnt,
                      COUNT(DISTINCT store_id) as stores
               FROM cases
               WHERE root_cause LIKE ?
                 AND resolution_status = 'resolved'""",
            [f"%{issue_type}%"],
        ).fetchone()

        if rows and rows[0] > 0:
            return f"{rows[0]} resolved cases across {rows[1]} stores"
        return ""

    @staticmethod
    def _parse_time(time_str: str) -> Optional[datetime]:
        """Parse various time formats."""
        if not time_str:
            return None
        for fmt in (
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d %H:%M:%S",
            "%m/%d/%Y %H:%M",
            "%m/%d/%y %H:%M:%S",
        ):
            try:
                return datetime.strptime(time_str, fmt)
            except ValueError:
                continue
        return None
