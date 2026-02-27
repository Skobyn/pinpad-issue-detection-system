"""Extract identity/config metadata from log file content."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Iterator

from pinpad_analyzer.ingestion.models import FileIdentity, LogEntry


class MetadataExtractor:
    """Scans log entries to extract site identity, software versions,
    pinpad hardware info, and configuration settings."""

    # Site IDs
    _RE_COMPANY_AA = re.compile(r"\bAa=(\d+)")  # SE_SEND Aa field
    _RE_COMPANY_BRACKET = re.compile(r"Company[\[(](\d+)[\])]")  # Company[145714] or Company(145714)
    _RE_COMPANY_JSON = re.compile(r'"CompanyNumber"\s*:\s*"?(\d+)"?')
    _RE_STORE_AB = re.compile(r"\bAb=(\d+)")  # SE_SEND Ab field
    _RE_STORE_BRACKET = re.compile(r"Store[\[(](\d+)[\])]")  # Store[1] or Store(1)
    _RE_STORE_RECEIPT = re.compile(r"StoreNumber\s*>(\d+)<")  # StoreNumber >1<
    _RE_STORE_JSON = re.compile(r'"StoreNumber"\s*:\s*"?(\d+)"?')
    _RE_MID = re.compile(r'"MID"\s*:\s*"([^"]+)"')

    # Software versions
    _RE_MTX_POS_VER = re.compile(r"MTX_POS\.dll\s+ver\w*\s*[=:]\s*([\d.]+)")
    _RE_MTX_EPS_VER = re.compile(r"MTX_EPS\.dll\s+ver\w*\s*[=:]\s*([\d.]+)")
    _RE_SECCODE_VER = re.compile(r"SecCode\s+ver\w*\s*[=:]\s*([\d.]+)")
    _RE_POS_VER = re.compile(r"POS\s+Version\s*[=:]\s*([\d.]+)")
    _RE_DLL_VER = re.compile(r"(?:File|Module)\s+Version\s*[:=]\s*([\d.]+)")

    # Pinpad hardware
    _RE_PINPAD_MODEL = re.compile(r"(XPI-Engage[A-Za-z0-9._-]*|Engage\s*[A-Za-z0-9._-]+|Lane/\d+)")
    _RE_PINPAD_SERIAL = re.compile(r"Serial#?\s*[=:]\s*(\S+)")
    _RE_FIRMWARE_VER = re.compile(r"Firmware\s+Ver\w*\s*[=:]\s*([\d.]+\S*)")
    _RE_OS_RELEASE = re.compile(r"OS\s+Release\s*[=:]\s*(.+?)(?:\s{2,}|$)")
    _RE_KERNEL_VER = re.compile(r"Kernel\s+Ver\w*\s*[=:]\s*(.+?)(?:\s{2,}|$)")

    # Configuration
    _RE_END_ORDER = re.compile(r"EndOrderIntervalMsg\s*[=:]\s*(\d+)")
    _RE_LEAVE_TERMINAL = re.compile(r"LeaveTerminalActive\s*[=:]\s*(\w+)")
    _RE_MAKE_FASTER = re.compile(r"MakeMXfaster\s*[=:]\s*(\w+)")
    _RE_C30_DELAY = re.compile(r"C30Delay\s*[=:]\s*(\d+)")
    _RE_TIMEOUT = re.compile(r"timeout\s*=\s*(\d+)")
    _RE_UISOIO = re.compile(r"UIsoio:\s*(.+)")

    # Network
    _RE_SERVER_URL = re.compile(r"(https?://\S*(?:trn|svc)\d\S*)")
    _RE_IP_ADDR = re.compile(r"(?:Local|IP|My)\s*(?:IP|Addr|Address)\s*[=:]\s*([\d.]+)")

    # Version info from DLL loading messages
    _RE_MTX_POS_LOAD = re.compile(r"MTX_POS\.dll\D*([\d]+\.[\d]+\.[\d]+\.[\d]+)")
    _RE_MTX_EPS_LOAD = re.compile(r"MTX_EPS\.dll\D*([\d]+\.[\d]+\.[\d]+\.[\d]+)")
    _RE_SECCODE_LOAD = re.compile(r"SecCode\D*([\d]+\.[\d]+\.[\d]+)")

    # LaneServiceStatusUpload XML fields
    _RE_LSS_SERIAL = re.compile(r"<SerialNumber>(\S+)</SerialNumber>")
    _RE_LSS_MODEL = re.compile(r'TermType="([^"]+)"')
    _RE_LSS_IP = re.compile(r"<IPAddress>([\d.]+)</IPAddress>")
    _RE_LSS_POS_VER = re.compile(r"POS Version Number:\s*([\d.]+)")
    _RE_LSS_OS_VER = re.compile(r"<OSVersion>([^<]+)</OSVersion>")

    def __init__(self, max_entries: int = 5000) -> None:
        self._max_entries = max_entries

    def extract(self, entries: Iterator[LogEntry]) -> FileIdentity:
        """Scan up to max_entries log entries and extract identity fields."""
        identity = FileIdentity()
        count = 0

        for entry in entries:
            if count >= self._max_entries:
                break
            count += 1
            self._scan_entry(entry, identity)

        return identity

    def extract_from_file(self, file_path: str) -> FileIdentity:
        """Extract identity by reading raw lines (for use before full parsing)."""
        identity = FileIdentity()

        # Compute SHA-256
        sha = hashlib.sha256()
        lines_scanned = 0
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    sha.update(chunk)
            identity.sha256_hash = sha.hexdigest()
        except OSError:
            pass

        # Scan first N lines for metadata patterns
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    if lines_scanned >= self._max_entries:
                        break
                    lines_scanned += 1
                    self._scan_raw_line(line, identity)
        except OSError:
            pass

        return identity

    def _scan_entry(self, entry: LogEntry, identity: FileIdentity) -> None:
        """Check a single log entry against all extraction patterns."""
        msg = entry.message
        self._match_patterns(msg, identity)

    def _scan_raw_line(self, line: str, identity: FileIdentity) -> None:
        """Check a raw line against all extraction patterns."""
        self._match_patterns(line, identity)

    def _match_patterns(self, text: str, identity: FileIdentity) -> None:
        """Apply all regex patterns to a text string."""
        # Site IDs (first match wins for each field)
        if not identity.company_id:
            for pat in (self._RE_COMPANY_AA, self._RE_COMPANY_BRACKET, self._RE_COMPANY_JSON):
                m = pat.search(text)
                if m:
                    identity.company_id = m.group(1)
                    break

        if not identity.store_id:
            for pat in (self._RE_STORE_AB, self._RE_STORE_BRACKET, self._RE_STORE_RECEIPT, self._RE_STORE_JSON):
                m = pat.search(text)
                if m:
                    identity.store_id = m.group(1)
                    break

        if not identity.mid:
            m = self._RE_MID.search(text)
            if m:
                identity.mid = m.group(1)

        # Software versions
        if not identity.mtx_pos_version:
            m = self._RE_MTX_POS_VER.search(text) or self._RE_MTX_POS_LOAD.search(text)
            if m:
                identity.mtx_pos_version = m.group(1)

        if not identity.mtx_eps_version:
            m = self._RE_MTX_EPS_VER.search(text) or self._RE_MTX_EPS_LOAD.search(text)
            if m:
                identity.mtx_eps_version = m.group(1)

        if not identity.seccode_version:
            m = self._RE_SECCODE_VER.search(text) or self._RE_SECCODE_LOAD.search(text)
            if m:
                identity.seccode_version = m.group(1)

        if not identity.pos_version:
            m = self._RE_POS_VER.search(text)
            if m:
                identity.pos_version = m.group(1)

        # Pinpad hardware
        if not identity.pinpad_model:
            m = self._RE_LSS_MODEL.search(text) or self._RE_PINPAD_MODEL.search(text)
            if m:
                val = m.group(1).strip()
                if val and "No PIN Pad" not in val:
                    identity.pinpad_model = val

        if not identity.pinpad_serial:
            m = self._RE_LSS_SERIAL.search(text) or self._RE_PINPAD_SERIAL.search(text)
            if m:
                val = m.group(1).strip()
                if val:
                    identity.pinpad_serial = val

        if not identity.pinpad_firmware:
            m = self._RE_FIRMWARE_VER.search(text)
            if m:
                identity.pinpad_firmware = m.group(1)

        if not identity.pinpad_os:
            m = self._RE_LSS_OS_VER.search(text) or self._RE_OS_RELEASE.search(text)
            if m:
                identity.pinpad_os = m.group(1).strip()

        if not identity.pinpad_kernel:
            m = self._RE_KERNEL_VER.search(text)
            if m:
                identity.pinpad_kernel = m.group(1).strip()

        # POS version from LaneServiceStatusUpload XML
        if not identity.pos_version:
            m = self._RE_LSS_POS_VER.search(text)
            if m:
                identity.pos_version = m.group(1)

        # Configuration (collect all found)
        for name, pattern in [
            ("EndOrderIntervalMsg", self._RE_END_ORDER),
            ("LeaveTerminalActive", self._RE_LEAVE_TERMINAL),
            ("MakeMXfaster", self._RE_MAKE_FASTER),
            ("C30Delay", self._RE_C30_DELAY),
        ]:
            if name not in identity.config:
                m = pattern.search(text)
                if m:
                    identity.config[name] = m.group(1)

        m = self._RE_UISOIO.search(text)
        if m and "UIsoio" not in identity.config:
            identity.config["UIsoio"] = m.group(1).strip()

        m = self._RE_TIMEOUT.search(text)
        if m and "timeout" not in identity.config:
            identity.config["timeout"] = m.group(1)

        # Network
        if not identity.server_eps_hosts:
            m = self._RE_SERVER_URL.search(text)
            if m:
                url = m.group(1)
                if url not in identity.server_eps_hosts:
                    identity.server_eps_hosts.append(url)
        elif len(identity.server_eps_hosts) < 4:
            m = self._RE_SERVER_URL.search(text)
            if m:
                url = m.group(1)
                if url not in identity.server_eps_hosts:
                    identity.server_eps_hosts.append(url)

        if not identity.ip_address:
            m = self._RE_LSS_IP.search(text) or self._RE_IP_ADDR.search(text)
            if m:
                identity.ip_address = m.group(1)
