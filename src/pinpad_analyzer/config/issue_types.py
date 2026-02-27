"""Known issue type definitions with indicators, severity, and resolution guidance."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class IssueType:
    id: int
    name: str
    severity: str  # critical, high, medium, low
    severity_rank: int  # lower = more severe
    description: str
    indicators: list[str]
    resolution_steps: list[str]


ISSUE_TYPES: dict[str, IssueType] = {
    "serial_comm_failure": IssueType(
        id=1,
        name="serial_comm_failure",
        severity="critical",
        severity_rank=1,
        description="Pinpad serial communication lost - SendMsgWaitAck3Tries failures",
        indicators=[
            "SendMsgWaitAck3Tries failed",
            "ProcessRequest FAILED",
            "Serial Out on 'Data Sent:' GetLastError",
        ],
        resolution_steps=[
            "1. Check USB/serial cable connection to pinpad",
            "2. Power cycle the pinpad (unplug for 30 seconds)",
            "3. Verify COM port settings (COM9, 115200 baud)",
            "4. Check for loose connections at both ends",
            "5. Try a different USB port or cable",
        ],
    ),
    "scat_dead": IssueType(
        id=2,
        name="scat_dead",
        severity="critical",
        severity_rank=1,
        description="Pinpad is dead/unresponsive for extended period",
        indicators=[
            "SCATAliveInt = 0 (ReportScatDead)",
            "IsSCATAlive >N<",
            "SCAT msg not sent, SCAT dead",
            "Set SCAT dead:",
        ],
        resolution_steps=[
            "1. Power cycle the pinpad",
            "2. Have cashier re-sign-on (CheckerSignOn)",
            "3. Check COM9 cable connection",
            "4. Verify M400 firmware version",
            "5. If P2P mismatch: check encryption configuration",
        ],
    ),
    "servereps_500": IssueType(
        id=3,
        name="servereps_500",
        severity="high",
        severity_rank=2,
        description="ServerEPS returning HTTP 500 Internal Server Error",
        indicators=[
            "HTTP/1.1 500 Internal Server Error",
            "ExchangeInfo was not sent",
            "MonitoringStatus was not sent",
        ],
        resolution_steps=[
            "1. Check ServerEPS service status (svc1/svc2.servereps.com)",
            "2. Verify network connectivity from POS",
            "3. If persistent: contact MicroTrax support",
            "4. System will auto-failover to secondary DC",
        ],
    ),
    "servereps_socket_error": IssueType(
        id=4,
        name="servereps_socket_error",
        severity="high",
        severity_rank=2,
        description="Socket error connecting to ServerEPS",
        indicators=[
            "Socket Error # 10054",
            "Socket Error # 10060",
            "Socket Error # 10061",
        ],
        resolution_steps=[
            "1. Check store internet connectivity",
            "2. Verify DNS resolution for servereps.com",
            "3. Check firewall rules (ports 443/HTTPS)",
            "4. Socket 10054: Remote server reset connection",
            "5. Socket 10060: Network timeout - check ISP",
        ],
    ),
    "host_timeout": IssueType(
        id=5,
        name="host_timeout",
        severity="high",
        severity_rank=2,
        description="Host authorization timeout (SE_SEND without SE_RECV)",
        indicators=[
            "SE_SEND without matching SE_RECV",
            "Host latency > 10 seconds",
        ],
        resolution_steps=[
            "1. Check network latency to ServerEPS hosts",
            "2. Verify transaction host (trn1/trn2.servereps.com)",
            "3. Check for network congestion",
            "4. If widespread: ServerEPS capacity issue",
        ],
    ),
    "chip_read_failure": IssueType(
        id=6,
        name="chip_read_failure",
        severity="medium",
        severity_rank=3,
        description="Chip card read failures causing fallback to swipe",
        indicators=[
            "InBadChipReadMode=Y",
            "IsCardEntryFallBack True",
            "EMVChipReadFallbackCounter",
        ],
        resolution_steps=[
            "1. Clean the chip card reader slot",
            "2. Have customer try reinserting card",
            "3. If persistent: chip reader hardware issue",
            "4. Check for debris in card slot",
        ],
    ),
    "certificate_failure": IssueType(
        id=7,
        name="certificate_failure",
        severity="high",
        severity_rank=2,
        description="SSL certificate validation failure",
        indicators=[
            "ValidateCertificate result = N",
            "Validity=cvExpired",
            "Validity=cvNotValidYet",
            "Validity=cvInvalid",
        ],
        resolution_steps=[
            "1. Check system clock (date/time sync)",
            "2. Verify certificate store is up to date",
            "3. Download latest cert storage from ServerEPS",
            "4. If clock drift: sync NTP",
        ],
    ),
    "transaction_abort": IssueType(
        id=8,
        name="transaction_abort",
        severity="medium",
        severity_rank=3,
        description="Transaction aborted without completion",
        indicators=[
            "DoAbortAnyTransaction",
        ],
        resolution_steps=[
            "1. Normal if customer removed card early",
            "2. Check if POS sent cancel request",
            "3. If repeated: check timeout settings",
        ],
    ),
    "repeated_decline": IssueType(
        id=9,
        name="repeated_decline",
        severity="low",
        severity_rank=4,
        description="Multiple consecutive transaction declines",
        indicators=[
            "ResponseCode = DD",
            "Multiple consecutive DN response codes",
        ],
        resolution_steps=[
            "1. Normal issuer behavior (insufficient funds, fraud hold)",
            "2. Check if same card being retried",
            "3. For EBT: verify available balance",
            "4. Not a system issue unless all cards declining",
        ],
    ),
    "pinpad_restart_loop": IssueType(
        id=10,
        name="pinpad_restart_loop",
        severity="critical",
        severity_rank=1,
        description="Pinpad rapidly cycling through reset/dead states",
        indicators=[
            "Rapid StateNone->StateReset->StateNone cycling",
            "Multiple ReportScatDead->ReportScatAlive transitions",
        ],
        resolution_steps=[
            "1. Power cycle pinpad (unplug for 60 seconds)",
            "2. Check for intermittent cable/power connection",
            "3. Try a different USB cable",
            "4. Check pinpad power supply",
            "5. May need pinpad replacement if hardware failing",
        ],
    ),
    "memory_pressure": IssueType(
        id=11,
        name="memory_pressure",
        severity="medium",
        severity_rank=3,
        description="System memory pressure increasing",
        indicators=[
            "HeapTotalFree decreasing trend",
            "VirtualAvailMB dropping",
        ],
        resolution_steps=[
            "1. Restart POS application",
            "2. Check for memory leaks in DLL",
            "3. Monitor HeapTotalFree over time",
        ],
    ),
    "p2p_encryption_mismatch": IssueType(
        id=12,
        name="p2p_encryption_mismatch",
        severity="critical",
        severity_rank=1,
        description="P2P encryption required but terminal not capable",
        indicators=[
            "IsP2PDLL=Y, IsTermP2PCapable=N",
            "Set SCAT dead: P2P Required",
        ],
        resolution_steps=[
            "1. Verify pinpad supports P2P encryption",
            "2. Check terminal configuration version",
            "3. Update pinpad firmware if needed",
            "4. Verify VSD/SRED encryption module is enabled",
        ],
    ),
    "card_read_intermittent": IssueType(
        id=13,
        name="card_read_intermittent",
        severity="high",
        severity_rank=2,
        description="Intermittent card read failures - customers waiting at pinpad with no card read",
        indicators=[
            "BeginOrder followed by EndOrder with no card data",
            "SCATReady polling loop with no TenderType response",
            "High ratio of $0/no-card-type transactions",
        ],
        resolution_steps=[
            "1. Clean the chip card reader slot and contactless reader surface",
            "2. Inspect mag stripe reader for debris or damage",
            "3. Check for worn/damaged card reader contacts",
            "4. Power cycle pinpad (unplug for 60 seconds)",
            "5. If 7-8 AM spike: check USB power management settings (disable selective suspend)",
            "6. If chronic (>15% daily rate): schedule pinpad replacement",
            "7. Check if NFC/contactless antenna is loose",
        ],
    ),
}
