"""Health check segmenter: identifies ExchangeInfo/MonitoringStatus/Login cycles."""

from __future__ import annotations

import re
from typing import Iterator

from pinpad_analyzer.ingestion.models import LogEntry
from pinpad_analyzer.segmentation.models import HealthCheckData

# Health check start patterns
EXCHANGE_INFO_START = re.compile(
    r"before ServerEPS_LSExchangeInfo (\w+)"
)
EXCHANGE_INFO_SEND = re.compile(
    r"ServerEPS_LSExchangeInfo Company\[(\d+)\].*?Addr\[([^\]]+)\]"
)
EXCHANGE_INFO_RESULT = re.compile(
    r"after ServerEPS_LSExchangeInfo ErrorCode = (\d+) ErrorStr=(.*)"
)
EXCHANGE_INFO_OK = re.compile(r"ExchangeInfo sent successfully")
EXCHANGE_INFO_FAIL = re.compile(
    r"ERROR!!!\s+ExchangeInfo was not sent.*ErrorCode = \[(\d+)\] (.*)"
)

MONITORING_START = re.compile(r"SE_SendMonitoringStatus")
MONITORING_RESULT = re.compile(
    r"after ServerEPS_LaneServiceStatusUpload ErrorCode \((\d+)\) ErrorStr\(([^)]*)\)"
)
MONITORING_FAIL = re.compile(
    r"ERROR!!!\s+MonitoringStatus was not sent.*ErrorCode = \[(\d+)\] (.*)"
)

LOGIN_START = re.compile(r"SE_SendLogin for LaneNumber")
LOGIN_RESULT = re.compile(r"Login result = (\w+)")

# HTTP error in error string
HTTP_ERROR = re.compile(r"HTTP/1\.\d (\d{3})")
SOCKET_ERROR = re.compile(r"Socket Error # (\d+)")


class HealthSegmenter:
    """Identifies health check cycles in SVREPS log entries."""

    def process_entries(
        self, entries: Iterator[LogEntry]
    ) -> Iterator[HealthCheckData]:
        """Process entries, yielding completed health checks."""
        current: HealthCheckData | None = None

        for entry in entries:
            if entry.category != "SVREPS":
                continue
            msg = entry.message

            # ExchangeInfo start
            m = EXCHANGE_INFO_SEND.search(msg)
            if m:
                if current is not None:
                    yield current
                current = HealthCheckData(
                    start_line=entry.line_number,
                    start_time=entry.timestamp,
                    check_type="ExchangeInfo",
                    target_host=m.group(2),
                )
                continue

            # ExchangeInfo result
            m = EXCHANGE_INFO_RESULT.search(msg)
            if m and current and current.check_type == "ExchangeInfo":
                error_code = m.group(1)
                error_str = m.group(2).strip()
                current.end_line = entry.line_number
                current.end_time = entry.timestamp
                current.error_code = error_code
                current.success = error_code == "0"
                if current.start_time:
                    current.latency_ms = (
                        (entry.timestamp - current.start_time).total_seconds()
                        * 1000
                    )
                # Extract HTTP/Socket error
                hm = HTTP_ERROR.search(error_str)
                if hm:
                    current.http_status = hm.group(1)
                sm = SOCKET_ERROR.search(error_str)
                if sm:
                    current.http_status = f"Socket_{sm.group(1)}"
                yield current
                current = None
                continue

            # MonitoringStatus result
            m = MONITORING_RESULT.search(msg)
            if m:
                hc = HealthCheckData(
                    start_line=entry.line_number,
                    start_time=entry.timestamp,
                    end_line=entry.line_number,
                    end_time=entry.timestamp,
                    check_type="MonitoringStatus",
                    error_code=m.group(1),
                    success=m.group(1) == "0",
                )
                error_str = m.group(2)
                hm = HTTP_ERROR.search(error_str)
                if hm:
                    hc.http_status = hm.group(1)
                sm = SOCKET_ERROR.search(error_str)
                if sm:
                    hc.http_status = f"Socket_{sm.group(1)}"
                yield hc
                continue

            # Login result
            m = LOGIN_RESULT.search(msg)
            if m:
                yield HealthCheckData(
                    start_line=entry.line_number,
                    start_time=entry.timestamp,
                    end_line=entry.line_number,
                    end_time=entry.timestamp,
                    check_type="Login",
                    success=m.group(1) in ("lrNothingNew", "lrWaitForFiles"),
                    error_code=m.group(1),
                )
                continue

        # Flush remaining
        if current is not None:
            yield current
