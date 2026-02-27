"""Data models for the segmentation layer."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class TransactionData:
    """Extracted transaction data from log entries."""

    start_line: int = 0
    end_line: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    entry_count: int = 0

    # Transaction identifiers
    sequence_number: str = ""
    card_type: str = ""  # Debit, Credit, EBT Food, EBT Cash
    entry_method: str = ""  # E (chip), S (swipe), C (contactless), M (manual)
    pan_last4: str = ""
    aid: str = ""
    app_label: str = ""
    tac_sequence: str = ""
    cvm_result: str = ""

    # Response
    response_code: str = ""  # AA, DD, DN, etc.
    host_response_code: str = ""
    authorization_number: str = ""

    # Amounts
    amount_cents: int = 0
    cashback_cents: int = 0

    # Host communication
    host_url: str = ""
    host_latency_ms: float = 0.0
    se_send_time: Optional[datetime] = None
    se_recv_time: Optional[datetime] = None

    # EMV data
    tvr: str = ""
    is_quickchip: bool = False
    is_fallback: bool = False

    # Error tracking
    serial_error_count: int = 0
    state_sequence: list[str] = field(default_factory=list)

    @property
    def is_approved(self) -> bool:
        return self.response_code == "AA"

    @property
    def duration_ms(self) -> float:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds() * 1000
        return 0.0


@dataclass
class HealthCheckData:
    """Extracted health check data."""

    start_line: int = 0
    end_line: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    check_type: str = ""  # ExchangeInfo, MonitoringStatus, Login
    target_host: str = ""
    success: bool = False
    error_code: str = ""
    http_status: str = ""
    latency_ms: float = 0.0


@dataclass
class ErrorCascadeData:
    """A group of consecutive related errors."""

    start_line: int = 0
    end_line: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error_pattern: str = ""
    error_count: int = 0
    first_error_message: str = ""
    recovery_achieved: bool = False
    recovery_time_ms: Optional[float] = None


@dataclass
class SCATStateChange:
    """A SCAT state transition event."""

    timestamp: datetime
    new_state: str
    old_state: str
    alive_status: Optional[int] = None
