"""Streaming file reader with filename metadata extraction."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Iterator

from pinpad_analyzer.ingestion.models import FileMetadata, LogEntry, RepeatDirective
from pinpad_analyzer.ingestion.parser import LogParser
from pinpad_analyzer.ingestion.repeat_expander import RepeatExpander

# Filename patterns:
#   jrnl0002-20251130.txt -> lane=2, date=2025-11-30
#   jrnl0002.txt          -> lane=2, date=(from first log entry)
FILENAME_PATTERN = re.compile(r"jrnl(\d{4})-(\d{8})\.txt$")
FILENAME_LANE_ONLY = re.compile(r"jrnl(\d{4})\.txt$")


def _extract_date_from_first_line(file_path: str) -> str:
    """Read the first parseable timestamp from the log file."""
    from pinpad_analyzer.ingestion.parser import LINE_PATTERN, TIMESTAMP_FORMAT
    from datetime import datetime as dt

    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                m = LINE_PATTERN.match(line.rstrip("\n\r"))
                if m:
                    ts = dt.strptime(m.group(1), TIMESTAMP_FORMAT)
                    return ts.strftime("%Y-%m-%d")
    except OSError:
        pass
    return ""


def extract_file_metadata(file_path: str) -> FileMetadata:
    """Extract lane number and date from log filename."""
    p = Path(file_path)
    m = FILENAME_PATTERN.match(p.name)
    if m:
        lane = int(m.group(1))
        raw_date = m.group(2)  # YYYYMMDD
        log_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
    else:
        # Try lane-only pattern (e.g. jrnl0002.txt)
        m2 = FILENAME_LANE_ONLY.match(p.name)
        lane = int(m2.group(1)) if m2 else 0
        # Fall back to extracting date from first log entry
        log_date = _extract_date_from_first_line(str(p))

    return FileMetadata(
        file_path=str(p.resolve()),
        file_name=p.name,
        lane=lane,
        log_date=log_date,
        file_size=p.stat().st_size if p.exists() else 0,
    )


class FileReader:
    """Streams parsed log entries from a file, handling encoding issues."""

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path
        self.metadata = extract_file_metadata(file_path)

    def read_raw_lines(self) -> Iterator[tuple[int, str]]:
        """Yield (line_number, raw_line) pairs, handling binary bytes."""
        with open(self.file_path, "r", encoding="utf-8", errors="replace") as f:
            for line_number, line in enumerate(f, start=1):
                yield line_number, line.rstrip("\n\r")

    def parse_lines(self) -> Iterator[LogEntry | RepeatDirective]:
        """Yield parsed LogEntry and RepeatDirective objects."""
        parser = LogParser(source_file=self.metadata.file_name)
        for line_number, raw in self.read_raw_lines():
            result = parser.parse_line(raw, line_number)
            if result is not None:
                yield result

    def read_entries(self, expand_repeats: bool = True) -> Iterator[LogEntry]:
        """Yield fully processed LogEntry objects with repeats expanded."""
        parsed = self.parse_lines()
        if expand_repeats:
            expander = RepeatExpander()
            yield from expander.process(parsed)
        else:
            for item in parsed:
                if isinstance(item, LogEntry):
                    yield item

    def count_lines(self) -> int:
        """Count total lines in file."""
        count = 0
        with open(self.file_path, "r", encoding="utf-8", errors="replace") as f:
            for _ in f:
                count += 1
        self.metadata.line_count = count
        return count
