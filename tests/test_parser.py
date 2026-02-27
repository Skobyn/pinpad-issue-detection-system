"""Tests for the log parser and repeat expander."""

from __future__ import annotations

from datetime import datetime

import pytest

from pinpad_analyzer.ingestion.models import LogEntry, RepeatDirective
from pinpad_analyzer.ingestion.parser import LogParser
from pinpad_analyzer.ingestion.repeat_expander import RepeatExpander
from tests.conftest import SAMPLE_LINES


class TestLogParser:
    """Test the line-level log parser."""

    def setup_method(self):
        self.parser = LogParser(source_file="test.txt")

    def test_parse_standard_serial_line(self):
        result = self.parser.parse_line(SAMPLE_LINES["standard_serial"], 1)
        assert isinstance(result, LogEntry)
        assert result.category == "SERIAL"
        assert result.line_number == 1
        assert result.timestamp.month == 11
        assert result.timestamp.day == 30
        assert result.timestamp.year == 2025
        assert "Data Recv" in result.message

    def test_parse_standard_tcp_line(self):
        result = self.parser.parse_line(SAMPLE_LINES["standard_tcp"], 5)
        assert isinstance(result, LogEntry)
        assert result.category == "TCP/IP"
        assert "SendMsgWaitAck3Tries" in result.message

    def test_parse_standard_dll_ex_line(self):
        result = self.parser.parse_line(SAMPLE_LINES["standard_dll_ex"], 10)
        assert isinstance(result, LogEntry)
        assert result.category == "DLL-EX"
        assert "MTX_POS_BeginOrder" in result.message

    def test_parse_standard_svreps_line(self):
        result = self.parser.parse_line(SAMPLE_LINES["standard_svreps"], 20)
        assert isinstance(result, LogEntry)
        assert result.category == "SVREPS"
        assert "SE_SEND" in result.message

    def test_parse_standard_metric_line(self):
        result = self.parser.parse_line(SAMPLE_LINES["standard_metric"], 2)
        assert isinstance(result, LogEntry)
        assert result.category == "METRIC"
        assert "VirtualAvailMB" in result.message

    def test_parse_single_repeat(self):
        result = self.parser.parse_line(SAMPLE_LINES["repeat_single"], 100)
        assert isinstance(result, RepeatDirective)
        assert result.line_count == 1
        assert result.repeat_count == 609
        assert result.line_number == 100

    def test_parse_multi_repeat(self):
        result = self.parser.parse_line(SAMPLE_LINES["repeat_multi"], 200)
        assert isinstance(result, RepeatDirective)
        assert result.line_count == 2
        assert result.repeat_count == 1

    def test_parse_blank_line(self):
        result = self.parser.parse_line("", 1)
        assert result is None

    def test_parse_continuation_line(self):
        result = self.parser.parse_line(SAMPLE_LINES["continuation"], 1)
        assert result is None

    def test_source_file_propagated(self):
        parser = LogParser(source_file="myfile.txt")
        result = parser.parse_line(SAMPLE_LINES["standard_serial"], 1)
        assert isinstance(result, LogEntry)
        assert result.source_file == "myfile.txt"

    def test_parse_error_line(self):
        result = self.parser.parse_line(SAMPLE_LINES["error_line"], 50)
        assert isinstance(result, LogEntry)
        assert result.category == "SERIAL"
        assert "SendMsgWaitAck3Tries failed" in result.message


class TestRepeatExpander:
    """Test the repeat compression expander."""

    def test_no_repeats(self):
        entries = [
            LogEntry(1, datetime(2025, 1, 1), "SERIAL", "msg1"),
            LogEntry(2, datetime(2025, 1, 1), "SERIAL", "msg2"),
        ]
        expander = RepeatExpander()
        result = list(expander.process(entries))
        assert len(result) == 2
        assert not result[0].is_expanded
        assert not result[1].is_expanded

    def test_single_line_repeat(self):
        items = [
            LogEntry(1, datetime(2025, 1, 1), "SERIAL", "msg1"),
            RepeatDirective(line_count=1, repeat_count=3, line_number=2),
        ]
        expander = RepeatExpander()
        result = list(expander.process(items))
        # 1 original + 3 expanded
        assert len(result) == 4
        assert result[0].is_expanded is False
        assert result[1].is_expanded is True
        assert result[1].expansion_count == 3
        assert result[1].message == "msg1"

    def test_multi_line_repeat(self):
        items = [
            LogEntry(1, datetime(2025, 1, 1), "SERIAL", "msgA"),
            LogEntry(2, datetime(2025, 1, 1), "SERIAL", "msgB"),
            RepeatDirective(line_count=2, repeat_count=2, line_number=3),
        ]
        expander = RepeatExpander()
        result = list(expander.process(items))
        # 2 original + 2*2 expanded = 6
        assert len(result) == 6
        assert result[2].message == "msgA"
        assert result[3].message == "msgB"
        assert result[4].message == "msgA"
        assert result[5].message == "msgB"

    def test_expanded_not_in_buffer(self):
        """Expanded lines should not be added to the ring buffer."""
        items = [
            LogEntry(1, datetime(2025, 1, 1), "SERIAL", "original"),
            RepeatDirective(line_count=1, repeat_count=2, line_number=2),
            LogEntry(3, datetime(2025, 1, 1), "SERIAL", "next_line"),
            RepeatDirective(line_count=1, repeat_count=1, line_number=4),
        ]
        expander = RepeatExpander()
        result = list(expander.process(items))
        # The second repeat should expand "next_line", not "original" (expanded)
        assert result[-1].message == "next_line"


class TestFileReaderIntegration:
    """Integration test using the real sample log file."""

    def test_read_sample_log(self, sample_log_path):
        from pinpad_analyzer.ingestion.file_reader import FileReader

        reader = FileReader(sample_log_path)
        metadata = reader.metadata

        assert metadata.lane == 2
        assert metadata.log_date == "2025-11-30"
        assert metadata.file_size > 0

        # Read first 100 entries
        entries = []
        for entry in reader.read_entries(expand_repeats=False):
            entries.append(entry)
            if len(entries) >= 100:
                break

        assert len(entries) == 100
        assert all(isinstance(e, LogEntry) for e in entries)
        # First entry should be SERIAL or METRIC based on log
        assert entries[0].category in ("SERIAL", "METRIC", "DLL-EX", "TCP/IP")

    def test_read_with_repeat_expansion(self, sample_log_path):
        from pinpad_analyzer.ingestion.file_reader import FileReader

        reader = FileReader(sample_log_path)

        # Read first 2000 entries with expansion
        entries = []
        for entry in reader.read_entries(expand_repeats=True):
            entries.append(entry)
            if len(entries) >= 2000:
                break

        assert len(entries) == 2000
        # Should have some expanded entries
        expanded = [e for e in entries if e.is_expanded]
        assert len(expanded) > 0, "Expected some expanded repeat entries"
