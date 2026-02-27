"""Expands '(Above N Lines Repeated M Times)' directives using a ring buffer."""

from __future__ import annotations

from collections import deque
from typing import Iterable, Iterator

from pinpad_analyzer.ingestion.models import LogEntry, RepeatDirective


class RepeatExpander:
    """Materializes repeated lines from compressed repeat directives.

    Uses a ring buffer to track recent lines. When a RepeatDirective is
    encountered, replays the last N lines M times with is_expanded=True.
    Expanded lines are NOT added back to the buffer to prevent cascading.
    """

    def __init__(self, max_lookback: int = 20) -> None:
        self._buffer: deque[LogEntry] = deque(maxlen=max_lookback)

    def process(
        self, items: Iterable[LogEntry | RepeatDirective]
    ) -> Iterator[LogEntry]:
        """Yield LogEntry objects, expanding repeat directives inline."""
        for item in items:
            if isinstance(item, RepeatDirective):
                # Get the last N lines from buffer
                buf_list = list(self._buffer)
                source_lines = buf_list[-item.line_count :]
                if not source_lines:
                    continue
                for _repeat in range(item.repeat_count):
                    for line in source_lines:
                        yield LogEntry(
                            line_number=line.line_number,
                            timestamp=line.timestamp,
                            category=line.category,
                            message=line.message,
                            is_expanded=True,
                            expansion_count=item.repeat_count,
                            source_file=line.source_file,
                        )
                        # Do NOT add expanded lines to buffer
            elif isinstance(item, LogEntry):
                self._buffer.append(item)
                yield item
