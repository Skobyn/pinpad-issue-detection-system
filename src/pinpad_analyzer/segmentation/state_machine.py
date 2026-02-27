"""SCAT state machine tracker."""

from __future__ import annotations

import re
from datetime import datetime

from pinpad_analyzer.ingestion.models import LogEntry
from pinpad_analyzer.segmentation.models import SCATStateChange

# >>>>>>SCATState = StateReset           - was StateNone
SCAT_STATE_PATTERN = re.compile(
    r">>>>>>SCATState = (\S+)\s+- was (\S+)"
)

# SCATAliveInt = 3 (ReportScatAlive)
ALIVE_PATTERN = re.compile(
    r"SCATAliveInt = (\d) \((\w+)\)"
)


class SCATStateMachine:
    """Tracks SCAT (pinpad) state transitions and alive status.

    Alive status values:
      0 = ReportScatDead
      1 = ReportScatInitializing
      2 = ReportScatLoading
      3 = ReportScatAlive
      9 = ReportScatNone
    """

    def __init__(self) -> None:
        self.current_state: str = "StateNone"
        self.alive_status: int = 9  # ReportScatNone
        self.state_history: list[SCATStateChange] = []
        self.alive_history: list[tuple[datetime, int, str]] = []

    def process_entry(self, entry: LogEntry) -> None:
        """Check if this log entry contains a state transition."""
        msg = entry.message

        # Check for state change
        m = SCAT_STATE_PATTERN.search(msg)
        if m:
            new_state = m.group(1).strip()
            old_state = m.group(2).strip()
            self.current_state = new_state
            self.state_history.append(SCATStateChange(
                timestamp=entry.timestamp,
                new_state=new_state,
                old_state=old_state,
            ))

        # Check for alive status change
        m = ALIVE_PATTERN.search(msg)
        if m:
            status = int(m.group(1))
            name = m.group(2)
            if status != self.alive_status:
                self.alive_status = status
                self.alive_history.append((entry.timestamp, status, name))

    @property
    def is_alive(self) -> bool:
        return self.alive_status == 3

    @property
    def is_dead(self) -> bool:
        return self.alive_status == 0

    def get_dead_periods(self) -> list[tuple[datetime, datetime, float]]:
        """Return periods where SCAT was dead: [(start, end, duration_sec), ...]."""
        periods = []
        dead_start = None
        for ts, status, _name in self.alive_history:
            if status == 0 and dead_start is None:
                dead_start = ts
            elif status != 0 and dead_start is not None:
                duration = (ts - dead_start).total_seconds()
                periods.append((dead_start, ts, duration))
                dead_start = None
        # If still dead at end, mark as open-ended
        if dead_start is not None and self.alive_history:
            last_ts = self.alive_history[-1][0]
            duration = (last_ts - dead_start).total_seconds()
            periods.append((dead_start, last_ts, duration))
        return periods
