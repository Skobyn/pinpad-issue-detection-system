"""Tests for transaction segmentation and state machine."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pinpad_analyzer.ingestion.models import LogEntry
from pinpad_analyzer.segmentation.transaction_segmenter import TransactionSegmenter
from pinpad_analyzer.segmentation.state_machine import SCATStateMachine
from pinpad_analyzer.segmentation.error_cascade import ErrorCascadeDetector


class TestTransactionSegmenter:
    """Test transaction boundary detection and field extraction."""

    def _make_entry(self, ts_offset_s, category, message, line=0):
        base = datetime(2025, 11, 30, 8, 0, 0)
        ts = base + timedelta(seconds=ts_offset_s)
        return LogEntry(line, ts, category, message)

    def test_basic_transaction_boundaries(self):
        """Test BeginOrder -> EndOrder detection."""
        entries = [
            self._make_entry(0, "DLL-EX", "MTX_POS_BeginOrder", 1),
            self._make_entry(1, "TCP/IP", "CardEntryType = E", 2),
            self._make_entry(5, "TCP/IP", "ResponseCode = AA", 3),
            self._make_entry(10, "DLL-EX", "MTX_POS_EndOrder", 4),
        ]

        seg = TransactionSegmenter()
        txns = list(seg.process_entries(iter(entries)))

        assert len(txns) == 1
        assert txns[0].response_code == "AA"
        assert txns[0].is_approved
        assert txns[0].entry_method == "E"

    def test_se_send_recv_extraction(self):
        """Test SE_SEND/SE_RECV host communication parsing."""
        entries = [
            self._make_entry(0, "DLL-EX", "MTX_POS_BeginOrder", 1),
            self._make_entry(1, "SVREPS",
                "SE_SEND(TimeOutSecs 30) [60 bytes] URL[https://trn2.servereps.com/sCAT2] "
                "Aa145714 Ab1 Ae9218 BnDB Da5000 Bp1234 BfE",
                2),
            self._make_entry(3, "SVREPS",
                "SE_RECV(1.500 secs) [250 bytes] Ae9218 Af00 Ag654321 Mb200",
                3),
            self._make_entry(5, "TCP/IP", "ResponseCode = AA", 4),
            self._make_entry(10, "DLL-EX", "MTX_POS_EndOrder", 5),
        ]

        seg = TransactionSegmenter()
        txns = list(seg.process_entries(iter(entries)))

        assert len(txns) == 1
        txn = txns[0]
        assert txn.host_url == "https://trn2.servereps.com/sCAT2"
        assert txn.host_latency_ms == 1500.0
        assert txn.sequence_number == "9218"
        assert txn.card_type == "Debit"  # BnDB
        assert txn.amount_cents == 5000
        assert txn.pan_last4 == "1234"

    def test_serial_error_counting(self):
        """Test that serial errors during transaction are counted."""
        entries = [
            self._make_entry(0, "DLL-EX", "MTX_POS_BeginOrder", 1),
            self._make_entry(1, "SERIAL", "****ERROR: SendMsgWaitAck3Tries failed, rtn 0", 2),
            self._make_entry(2, "SERIAL", "****ERROR: SendMsgWaitAck3Tries failed, rtn 0", 3),
            self._make_entry(5, "TCP/IP", "ResponseCode = DD", 4),
            self._make_entry(10, "DLL-EX", "MTX_POS_EndOrder", 5),
        ]

        seg = TransactionSegmenter()
        txns = list(seg.process_entries(iter(entries)))

        assert len(txns) == 1
        assert txns[0].serial_error_count == 2
        assert txns[0].response_code == "DD"
        assert not txns[0].is_approved

    def test_multiple_transactions(self):
        """Test detecting multiple consecutive transactions."""
        entries = [
            self._make_entry(0, "DLL-EX", "MTX_POS_BeginOrder", 1),
            self._make_entry(5, "DLL-EX", "MTX_POS_EndOrder", 2),
            self._make_entry(10, "DLL-EX", "MTX_POS_BeginOrder", 3),
            self._make_entry(15, "TCP/IP", "ResponseCode = AA", 4),
            self._make_entry(20, "DLL-EX", "MTX_POS_EndOrder", 5),
        ]

        seg = TransactionSegmenter()
        txns = list(seg.process_entries(iter(entries)))

        assert len(txns) == 2

    def test_emv_field_extraction(self):
        """Test EMV data field extraction (AID, TVR, CVM)."""
        entries = [
            self._make_entry(0, "DLL-EX", "MTX_POS_BeginOrder", 1),
            self._make_entry(1, "TCP/IP", "AppID >A0000000041010<", 2),
            self._make_entry(2, "TCP/IP", "AppLabel >MASTERCARD<", 3),
            self._make_entry(3, "TCP/IP", "CVMR >420300<", 4),
            self._make_entry(4, "TCP/IP", "tvr=0000008000,", 5),
            self._make_entry(10, "DLL-EX", "MTX_POS_EndOrder", 6),
        ]

        seg = TransactionSegmenter()
        txns = list(seg.process_entries(iter(entries)))

        assert len(txns) == 1
        txn = txns[0]
        assert txn.aid == "A0000000041010"
        assert txn.app_label == "MASTERCARD"
        assert txn.cvm_result == "420300"
        assert txn.tvr == "0000008000"


class TestSCATStateMachine:
    """Test SCAT alive/dead state tracking."""

    def _make_entry(self, ts_offset_s, category, message):
        base = datetime(2025, 11, 30, 0, 0, 0)
        ts = base + timedelta(seconds=ts_offset_s)
        return LogEntry(0, ts, category, message)

    def test_alive_to_dead_transition(self):
        entries = [
            self._make_entry(0, "DLL-EX", "SCATAliveInt = 3 (ReportScatAlive)"),
            self._make_entry(100, "DLL-EX", "SCATAliveInt = 0 (ReportScatDead)"),
        ]

        sm = SCATStateMachine()
        for e in entries:
            sm.process_entry(e)

        history = sm.alive_history
        assert len(history) == 2
        assert history[0][1] == 3  # alive
        assert history[1][1] == 0  # dead

    def test_dead_periods(self):
        entries = [
            self._make_entry(0, "DLL-EX", "SCATAliveInt = 3 (ReportScatAlive)"),
            self._make_entry(100, "DLL-EX", "SCATAliveInt = 0 (ReportScatDead)"),
            self._make_entry(600, "DLL-EX", "SCATAliveInt = 3 (ReportScatAlive)"),
        ]

        sm = SCATStateMachine()
        for e in entries:
            sm.process_entry(e)

        dead_periods = sm.get_dead_periods()
        assert len(dead_periods) == 1
        start, end, duration = dead_periods[0]
        assert duration == 500  # 600 - 100

    def test_state_transitions(self):
        entries = [
            self._make_entry(0, "DLL-EX", ">>>>>>SCATState = StateIdle  - was StateCheckConfig"),
            self._make_entry(5, "DLL-EX", ">>>>>>SCATState = StateProcessRequest  - was StateIdle"),
        ]

        sm = SCATStateMachine()
        for e in entries:
            sm.process_entry(e)

        assert len(sm.state_history) == 2
        assert sm.state_history[0].new_state == "StateIdle"
        assert sm.state_history[1].new_state == "StateProcessRequest"


class TestErrorCascadeDetector:
    """Test error cascade grouping."""

    def _make_entry(self, ts_offset_s, message, line=0):
        base = datetime(2025, 11, 30, 0, 0, 0)
        ts = base + timedelta(seconds=ts_offset_s)
        return LogEntry(line, ts, "SERIAL", message)

    def test_single_cascade(self):
        entries = [
            self._make_entry(0, "****ERROR: SendMsgWaitAck3Tries failed, rtn 0", 1),
            self._make_entry(1, "****ERROR: SendMsgWaitAck3Tries failed, rtn 0", 2),
            self._make_entry(2, "****ERROR: SendMsgWaitAck3Tries failed, rtn 0", 3),
            self._make_entry(3, "****ERROR: SendMsgWaitAck3Tries failed, rtn 0", 4),
        ]

        detector = ErrorCascadeDetector()
        cascades = list(detector.process_entries(iter(entries)))

        assert len(cascades) == 1
        assert cascades[0].error_count == 4
        assert "SendMsgWaitAck3Tries" in cascades[0].error_pattern

    def test_separate_cascades(self):
        """Errors more than 5 seconds apart should be separate cascades."""
        entries = [
            self._make_entry(0, "****ERROR: Error A", 1),
            self._make_entry(1, "****ERROR: Error A", 2),
            self._make_entry(20, "****ERROR: Error B", 3),  # 20s gap
            self._make_entry(21, "****ERROR: Error B", 4),
        ]

        detector = ErrorCascadeDetector()
        cascades = list(detector.process_entries(iter(entries)))

        assert len(cascades) == 2

    def test_non_errors_ignored(self):
        entries = [
            self._make_entry(0, "Normal log message", 1),
            self._make_entry(1, "Another normal message", 2),
        ]

        detector = ErrorCascadeDetector()
        cascades = list(detector.process_entries(iter(entries)))

        assert len(cascades) == 0
