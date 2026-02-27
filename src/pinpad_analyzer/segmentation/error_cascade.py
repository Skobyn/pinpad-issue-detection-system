"""Error cascade detector: groups consecutive error lines."""

from __future__ import annotations

import re
from typing import Iterator

from pinpad_analyzer.ingestion.models import LogEntry
from pinpad_analyzer.segmentation.models import ErrorCascadeData

ERROR_PATTERN = re.compile(r"\*{4}ERROR[:\s](.+)")
WARNING_PATTERN = re.compile(r"\*{4}WARNING[:\s](.+)")
PROCESS_FAILED = re.compile(r"ProcessRequest FAILED")

# Max gap between errors to still consider them part of the same cascade
MAX_GAP_SECONDS = 5.0


class ErrorCascadeDetector:
    """Groups consecutive error lines into ErrorCascade events."""

    def __init__(self, max_gap_seconds: float = MAX_GAP_SECONDS) -> None:
        self._max_gap = max_gap_seconds
        self._current: ErrorCascadeData | None = None
        self._last_error_time = None

    def process_entries(
        self, entries: Iterator[LogEntry]
    ) -> Iterator[ErrorCascadeData]:
        """Process entries, yielding completed error cascades."""
        for entry in entries:
            is_error = bool(
                ERROR_PATTERN.search(entry.message)
                or PROCESS_FAILED.search(entry.message)
            )

            if is_error:
                if self._current is None:
                    # Start new cascade
                    self._current = ErrorCascadeData(
                        start_line=entry.line_number,
                        start_time=entry.timestamp,
                        error_count=1,
                        first_error_message=entry.message.strip(),
                    )
                    # Determine error pattern
                    m = ERROR_PATTERN.search(entry.message)
                    if m:
                        self._current.error_pattern = m.group(1).strip()[:100]
                    elif PROCESS_FAILED.search(entry.message):
                        self._current.error_pattern = "ProcessRequest FAILED"
                elif self._last_error_time and (
                    (entry.timestamp - self._last_error_time).total_seconds()
                    <= self._max_gap
                ):
                    # Continue cascade
                    self._current.error_count += 1
                    self._current.end_line = entry.line_number
                    self._current.end_time = entry.timestamp
                else:
                    # New cascade - yield old one
                    self._finalize()
                    yield self._current
                    self._current = ErrorCascadeData(
                        start_line=entry.line_number,
                        start_time=entry.timestamp,
                        error_count=1,
                        first_error_message=entry.message.strip(),
                    )
                    m = ERROR_PATTERN.search(entry.message)
                    if m:
                        self._current.error_pattern = m.group(1).strip()[:100]
                self._last_error_time = entry.timestamp
            else:
                # Non-error line: if we have an active cascade and enough gap, close it
                if (
                    self._current is not None
                    and self._last_error_time is not None
                    and (entry.timestamp - self._last_error_time).total_seconds()
                    > self._max_gap
                ):
                    self._finalize()
                    # Check if recovery achieved
                    if "SCATAliveInt = 3" in entry.message or "Ready" in entry.message:
                        self._current.recovery_achieved = True
                        if self._current.end_time:
                            self._current.recovery_time_ms = (
                                (entry.timestamp - self._current.end_time).total_seconds()
                                * 1000
                            )
                    yield self._current
                    self._current = None
                    self._last_error_time = None

        # Flush any remaining cascade
        if self._current is not None:
            self._finalize()
            yield self._current

    def _finalize(self) -> None:
        if self._current and not self._current.end_time:
            self._current.end_time = self._current.start_time
            self._current.end_line = self._current.start_line
