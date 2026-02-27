"""Tests for the technician assistant modules."""

from __future__ import annotations

from datetime import datetime

import pytest

from pinpad_analyzer.assistant.domain_knowledge import DOMAIN_PATTERNS, RESPONSE_CODES, ENTRY_METHODS
from pinpad_analyzer.assistant.case_db import CaseDB
from pinpad_analyzer.assistant.diagnosis import DiagnosisEngine


class TestDomainKnowledge:
    """Test domain knowledge base."""

    def test_all_issue_types_have_domain_info(self):
        expected = [
            "scat_dead", "serial_comm_failure", "servereps_500",
            "servereps_socket_error", "host_timeout", "p2p_encryption_mismatch",
        ]
        for name in expected:
            assert name in DOMAIN_PATTERNS
            info = DOMAIN_PATTERNS[name]
            assert "meaning" in info
            assert "common_causes" in info
            assert len(info["common_causes"]) > 0

    def test_response_codes(self):
        assert RESPONSE_CODES["AP"] == "Approved"
        assert RESPONSE_CODES["DD"] == "Declined by host"

    def test_entry_methods(self):
        assert ENTRY_METHODS["E"] == "EMV chip insert"
        assert ENTRY_METHODS["S"] == "Magnetic stripe swipe"


class TestCaseDB:
    """Test case CRUD operations."""

    def test_create_and_get_case(self, tmp_db):
        case_db = CaseDB(tmp_db)

        case_id = case_db.create_case(
            symptom_description="Pinpad not responding",
            company_id="145714",
            store_id="1",
            lane_number=2,
            root_cause="SCAT Dead",
            root_cause_confidence=0.95,
        )

        assert len(case_id) == 12

        case = case_db.get_case(case_id)
        assert case is not None
        assert case["symptom_description"] == "Pinpad not responding"
        assert case["company_id"] == "145714"
        assert case["resolution_status"] == "open"

    def test_list_cases(self, tmp_db):
        case_db = CaseDB(tmp_db)

        case_db.create_case(symptom_description="Issue 1")
        case_db.create_case(symptom_description="Issue 2")

        cases = case_db.list_cases()
        assert len(cases) == 2

    def test_resolve_case(self, tmp_db):
        case_db = CaseDB(tmp_db)

        case_id = case_db.create_case(
            symptom_description="Test issue",
            root_cause="Test cause",
        )

        case_db.resolve_case(case_id, resolution_steps="Power cycled pinpad")

        case = case_db.get_case(case_id)
        assert case["resolution_status"] == "resolved"
        assert case["tech_verified"] is True

    def test_add_pattern(self, tmp_db):
        case_db = CaseDB(tmp_db)

        case_id = case_db.create_case(
            symptom_description="Test issue",
        )

        pattern_id = case_db.add_pattern(
            case_id=case_id,
            pattern_type="scat_dead",
            pattern_text="Overnight power loss causes SCAT dead",
            confidence=0.8,
            stores="1,2,3",
        )

        assert len(pattern_id) == 12

        patterns = case_db.find_patterns_by_type("scat_dead")
        assert len(patterns) == 1
        assert patterns[0]["pattern_text"] == "Overnight power loss causes SCAT dead"

    def test_filter_by_status(self, tmp_db):
        case_db = CaseDB(tmp_db)

        id1 = case_db.create_case(symptom_description="Open case")
        id2 = case_db.create_case(symptom_description="Resolved case")
        case_db.resolve_case(id2)

        open_cases = case_db.list_cases(status="open")
        assert len(open_cases) == 1
        assert open_cases[0]["case_id"] == id1

        resolved_cases = case_db.list_cases(status="resolved")
        assert len(resolved_cases) == 1
        assert resolved_cases[0]["case_id"] == id2


class TestDiagnosisEngine:
    """Test the diagnosis engine."""

    def test_parse_time_formats(self):
        engine = DiagnosisEngine.__new__(DiagnosisEngine)

        assert engine._parse_time("2025-11-30 08:00") == datetime(2025, 11, 30, 8, 0)
        assert engine._parse_time("2025-11-30 08:00:30") == datetime(2025, 11, 30, 8, 0, 30)
        assert engine._parse_time("") is None
        assert engine._parse_time("invalid") is None

    def test_diagnose_empty_db(self, tmp_db):
        engine = DiagnosisEngine(tmp_db)
        results = engine.diagnose({
            "symptom_description": "pinpad not responding",
        })
        assert results == []  # No files ingested
