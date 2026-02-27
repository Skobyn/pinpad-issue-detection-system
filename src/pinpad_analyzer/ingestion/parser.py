"""Line-level log parser for MicroTrax OpenEPS journal logs."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Union

from pinpad_analyzer.ingestion.models import LogEntry, RepeatDirective

# Primary log line pattern: MM/DD/YY HH:MM:SS.mmm CATEGORY message
LINE_PATTERN = re.compile(
    r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{3})"  # timestamp
    r" (TCP/IP|DLL-IN|DLL-EX|SERIAL|SVREPS|METRIC|MTXPOS)"  # category
    r" (.*)$",  # message
    re.DOTALL,
)

# Repeat compression patterns:
#   "                  (Above Line Repeated 609 Times)"
#   "                  (Above 2 Lines Repeated 1 Times)"
REPEAT_PATTERN = re.compile(
    r"^\s+\(Above (\d+ )?Lines? Repeated (\d+) Times?\)$"
)

TIMESTAMP_FORMAT = "%m/%d/%y %H:%M:%S.%f"

ParseResult = Union[LogEntry, RepeatDirective, None]


class LogParser:
    """Parses individual log lines into structured objects."""

    def __init__(self, source_file: str = "") -> None:
        self._source_file = source_file

    def parse_line(self, raw: str, line_number: int) -> ParseResult:
        """Parse a single raw log line.

        Returns LogEntry for standard log lines, RepeatDirective for
        repeat compression markers, or None for blank/unrecognized lines.
        """
        # Try repeat pattern first (no timestamp prefix)
        m = REPEAT_PATTERN.match(raw)
        if m:
            line_count = int(m.group(1).strip()) if m.group(1) else 1
            repeat_count = int(m.group(2))
            return RepeatDirective(
                line_count=line_count,
                repeat_count=repeat_count,
                line_number=line_number,
            )

        # Standard log line
        m = LINE_PATTERN.match(raw)
        if m:
            ts = datetime.strptime(m.group(1), TIMESTAMP_FORMAT)
            return LogEntry(
                line_number=line_number,
                timestamp=ts,
                category=m.group(2),
                message=m.group(3).rstrip(),
                source_file=self._source_file,
            )

        return None  # Blank or unrecognized line
