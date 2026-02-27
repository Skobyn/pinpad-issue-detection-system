"""Data models for the ingestion layer."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class FileIdentity:
    """Identity and configuration fields extracted from log file content."""

    # Site IDs
    company_id: str = ""
    store_id: str = ""
    mid: str = ""

    # Software versions
    mtx_pos_version: str = ""
    mtx_eps_version: str = ""
    seccode_version: str = ""
    pos_version: str = ""

    # Pinpad hardware
    pinpad_model: str = ""
    pinpad_serial: str = ""
    pinpad_firmware: str = ""
    pinpad_os: str = ""
    pinpad_kernel: str = ""

    # Configuration (key settings)
    config: dict = field(default_factory=dict)

    # Network
    server_eps_hosts: list[str] = field(default_factory=list)
    ip_address: str = ""

    # Hash for deduplication
    sha256_hash: str = ""
    upload_source: str = "local"


@dataclass
class LogEntry:
    """Single parsed log line."""

    line_number: int
    timestamp: datetime
    category: str  # TCP/IP, DLL-IN, DLL-EX, SERIAL, SVREPS, METRIC, MTXPOS
    message: str
    is_expanded: bool = False
    expansion_count: int = 1
    source_file: str = ""


@dataclass
class RepeatDirective:
    """Parsed repeat compression marker: '(Above N Lines Repeated M Times)'."""

    line_count: int  # 1 for "Above Line", N for "Above N Lines"
    repeat_count: int  # M from "Repeated M Times"
    line_number: int = 0


@dataclass
class FileMetadata:
    """Metadata extracted from log filename."""

    file_path: str
    file_name: str
    lane: int
    log_date: str  # YYYYMMDD
    file_size: int = 0
    line_count: int = 0
