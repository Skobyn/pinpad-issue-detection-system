"""Transaction segmenter: detects BeginOrder..EndOrder boundaries."""

from __future__ import annotations

import re
from datetime import datetime
from enum import Enum
from typing import Iterator, Optional

from pinpad_analyzer.ingestion.models import LogEntry
from pinpad_analyzer.segmentation.models import TransactionData


class TransactionState(Enum):
    IDLE = "idle"
    IN_PROGRESS = "in_progress"
    ONLINE_PENDING = "online_pending"
    COMPLETING = "completing"


# Key regex patterns for transaction boundary detection
PATTERNS = {
    "begin_order": re.compile(r"MTX_POS_BeginOrder"),
    "tac_b": re.compile(r"\*{4}NOTE: Processing TAC >B<"),
    "tender_type_set": re.compile(
        r"MTX_POS_SET_TenderTypePOS = [0-9A-F]"
    ),
    "tender_ok": re.compile(r"TenderTypeStatus = 1 <OK>"),
    "tender_type": re.compile(r"TenderTypeMTXi = \S+ <(\w+[\s\w]*)>"),
    "card_entry": re.compile(r"CardEntryType = (\w)"),
    "track2": re.compile(r"Track2Data = "),
    "pan": re.compile(r"PersonalAccountNumber = (\S+)"),
    "se_send": re.compile(
        r"SE_SEND\(TimeOutSecs (\d+)\).*?URL\[([^\]]+)\].*?Ae(\d+)"
    ),
    "se_recv": re.compile(
        r"SE_RECV\(([0-9.]+) secs\).*?Ae(\d+)"
    ),
    "response_code": re.compile(r"ResponseCode = (\w+)"),
    "host_response": re.compile(r"HostResponseCode = (\d+)"),
    "auth_number": re.compile(r"AuthorizationNumber = (\w+)"),
    "end_order": re.compile(r"MTX_POS_EndOrder"),
    "reset_clear_end": re.compile(r"Reset_Clear END"),
    "abort": re.compile(r"DoAbortAnyTransaction"),
    "aid": re.compile(r"AppID >(\w+)<"),
    "app_label": re.compile(r"AppLabel >([^<]+)<"),
    "cvm_result": re.compile(r"CVMR >(\w+)<"),
    "tvr": re.compile(r"tvr=(\w+),"),
    "amount": re.compile(r"Da(\d+)"),
    "cashback": re.compile(r"CashBackAmount = \$([.\d]+)"),
    "seq_number": re.compile(r"Seq #: (\d+)"),
    "cmd_sequence": re.compile(r"\*{4}COMMAND SEQUENCE for .+ >([^<]+)<"),
    "quickchip": re.compile(r"IsQuickChip=(\w)"),
    "host_url": re.compile(r"URL\[(https?://[^\]]+)\]"),
    "scat_state": re.compile(r">>>>>>SCATState = (\w+)"),
    "send_ack_fail": re.compile(
        r"\*{4}ERROR: SendMsgWaitAck3Tries failed"
    ),
}

# Patterns to extract fields from SE_SEND message content
SE_SEND_FIELD_PATTERNS = {
    "company": re.compile(r"Aa(\d+)"),
    "store": re.compile(r"Ab(\d+)"),
    "processing_code": re.compile(r"Ac(\d+)"),
    "seq_number": re.compile(r"Ae(\d+)"),
    "amount": re.compile(r"Da(\d+)"),
    "cashback": re.compile(r"Dc(\d+)"),
    "card_type_code": re.compile(r"Bn(\w{2})"),
    "pan_last4": re.compile(r"Bp(\d+)"),
    "entry_method": re.compile(r"BfE?C?(\w)"),
    "aid_field": re.compile(r"84-(\w+)"),
    "app_label_field": re.compile(r"50-([^|]+)"),
    "tvr_field": re.compile(r"95-(\w+)"),
    "cvm_field": re.compile(r"9F34-(\w+)"),
    "tac_seq": re.compile(r"Gh([^[]+)"),
}

# SE_RECV field patterns
SE_RECV_FIELD_PATTERNS = {
    "response_code_host": re.compile(r"Af(\w{2})"),
    "auth_number": re.compile(r"Ag(\w+)"),
    "approved_flag": re.compile(r"Au([YN])"),
    "response_text": re.compile(r"Ao([^[]+)"),
    "host_response": re.compile(r"Mb(\d{3})"),
}

# Card type code to name mapping
CARD_TYPE_MAP = {
    "DB": "Debit",
    "VS": "Credit",
    "MC": "Credit",
    "AX": "Credit",
    "DS": "Credit",
    "EF": "EBT Food",
    "EC": "EBT Cash",
}

# Entry method mapping
ENTRY_METHOD_MAP = {
    "E": "Chip",
    "S": "Swipe",
    "C": "Contactless",
    "M": "Manual",
    "K": "Keyed",
}


class TransactionSegmenter:
    """Detects and extracts transaction data from log entries."""

    def __init__(self) -> None:
        self._state = TransactionState.IDLE
        self._current: Optional[TransactionData] = None
        self._entries_buffer: list[LogEntry] = []

    def process_entries(
        self, entries: Iterator[LogEntry]
    ) -> Iterator[TransactionData]:
        """Process a stream of entries, yielding complete transactions."""
        for entry in entries:
            result = self._process_entry(entry)
            if result is not None:
                yield result

        # Flush any incomplete transaction at end
        if self._current is not None and self._current.start_time is not None:
            self._finalize_current(self._entries_buffer[-1] if self._entries_buffer else None)
            result = self._current
            self._current = None
            self._state = TransactionState.IDLE
            if result.start_time is not None:
                yield result

    def _process_entry(self, entry: LogEntry) -> Optional[TransactionData]:
        msg = entry.message

        # Start of transaction: BeginOrder
        m = PATTERNS["begin_order"].search(msg)
        if m:
            # If there's an existing transaction, finalize it
            completed = None
            if self._current is not None and self._current.start_time is not None:
                self._finalize_current(entry)
                completed = self._current
            self._start_new(entry)
            return completed

        # If idle, also start on TenderTypePOS set
        if self._state == TransactionState.IDLE:
            m = PATTERNS["tender_type_set"].search(msg)
            if m and entry.category == "DLL-EX":
                self._start_new(entry)

        if self._current is None:
            return None

        # Track entries
        self._entries_buffer.append(entry)
        self._current.entry_count += 1

        # Extract transaction fields
        self._extract_fields(entry)

        # SE_SEND - online authorization sent
        m = PATTERNS["se_send"].search(msg)
        if m and entry.category == "SVREPS":
            self._state = TransactionState.ONLINE_PENDING
            self._current.se_send_time = entry.timestamp
            self._current.host_url = m.group(2) if m.group(2) else ""
            self._current.sequence_number = m.group(3) if m.group(3) else ""
            # Extract fields from SE_SEND message
            self._extract_se_send_fields(msg)

        # SE_RECV - response received
        m = PATTERNS["se_recv"].search(msg)
        if m and entry.category == "SVREPS":
            self._state = TransactionState.COMPLETING
            self._current.se_recv_time = entry.timestamp
            latency = float(m.group(1))
            self._current.host_latency_ms = latency * 1000
            # Extract fields from SE_RECV message
            self._extract_se_recv_fields(msg)

        # Response code from TCP/IP channel
        m = PATTERNS["response_code"].search(msg)
        if m and entry.category == "TCP/IP" and not self._current.response_code:
            self._current.response_code = m.group(1)

        # Host response code
        m = PATTERNS["host_response"].search(msg)
        if m and entry.category == "TCP/IP":
            self._current.host_response_code = m.group(1)

        # End of transaction: EndOrder
        m = PATTERNS["end_order"].search(msg)
        if m:
            self._finalize_current(entry)
            completed = self._current
            self._current = None
            self._state = TransactionState.IDLE
            self._entries_buffer = []
            return completed

        # Reset_Clear END can also end a transaction
        m = PATTERNS["reset_clear_end"].search(msg)
        if m and self._current.response_code:
            self._finalize_current(entry)
            completed = self._current
            self._current = None
            self._state = TransactionState.IDLE
            self._entries_buffer = []
            return completed

        # Serial error during transaction
        if PATTERNS["send_ack_fail"].search(msg):
            self._current.serial_error_count += 1

        # SCAT state transition
        m = PATTERNS["scat_state"].search(msg)
        if m:
            self._current.state_sequence.append(m.group(1).strip())

        return None

    def _start_new(self, entry: LogEntry) -> None:
        self._current = TransactionData(
            start_line=entry.line_number,
            start_time=entry.timestamp,
        )
        self._state = TransactionState.IN_PROGRESS
        self._entries_buffer = [entry]

    def _finalize_current(self, entry: Optional[LogEntry]) -> None:
        if self._current is None:
            return
        if entry is not None:
            self._current.end_line = entry.line_number
            self._current.end_time = entry.timestamp

    def _extract_fields(self, entry: LogEntry) -> None:
        """Extract transaction fields from individual log entries."""
        msg = entry.message
        txn = self._current
        if txn is None:
            return

        # Card entry method
        m = PATTERNS["card_entry"].search(msg)
        if m and entry.category in ("DLL-EX", "TCP/IP"):
            txn.entry_method = m.group(1)

        # AID
        m = PATTERNS["aid"].search(msg)
        if m:
            txn.aid = m.group(1)

        # App label
        m = PATTERNS["app_label"].search(msg)
        if m:
            txn.app_label = m.group(1).strip()

        # CVM result
        m = PATTERNS["cvm_result"].search(msg)
        if m and m.group(1) != "3F0000":  # Skip pre-PIN phase value
            txn.cvm_result = m.group(1)

        # TVR
        m = PATTERNS["tvr"].search(msg)
        if m:
            txn.tvr = m.group(1)

        # Command sequence (TAC)
        m = PATTERNS["cmd_sequence"].search(msg)
        if m:
            txn.tac_sequence = m.group(1).strip()

        # QuickChip
        m = PATTERNS["quickchip"].search(msg)
        if m:
            txn.is_quickchip = m.group(1) == "Y"

        # Auth number
        m = PATTERNS["auth_number"].search(msg)
        if m and entry.category == "TCP/IP":
            txn.authorization_number = m.group(1)

        # TenderType name
        m = PATTERNS["tender_type"].search(msg)
        if m:
            tender_name = m.group(1).strip()
            if "Debit" in tender_name:
                txn.card_type = "Debit"
            elif "Credit" in tender_name:
                txn.card_type = "Credit"
            elif "EBT Food" in tender_name or "Food Stamp" in tender_name:
                txn.card_type = "EBT Food"
            elif "EBT Cash" in tender_name:
                txn.card_type = "EBT Cash"

    def _extract_se_send_fields(self, msg: str) -> None:
        """Extract fields from SE_SEND message content."""
        txn = self._current
        if txn is None:
            return

        for field_name, pattern in SE_SEND_FIELD_PATTERNS.items():
            m = pattern.search(msg)
            if not m:
                continue
            val = m.group(1)
            if field_name == "amount" and not txn.amount_cents:
                txn.amount_cents = int(val)
            elif field_name == "cashback":
                txn.cashback_cents = int(val)
            elif field_name == "card_type_code" and not txn.card_type:
                txn.card_type = CARD_TYPE_MAP.get(val, val)
            elif field_name == "pan_last4":
                txn.pan_last4 = val
            elif field_name == "entry_method" and not txn.entry_method:
                txn.entry_method = val
            elif field_name == "aid_field" and not txn.aid:
                txn.aid = val
            elif field_name == "app_label_field" and not txn.app_label:
                txn.app_label = val.strip()
            elif field_name == "tvr_field" and not txn.tvr:
                txn.tvr = val
            elif field_name == "cvm_field" and val != "3F0000":
                txn.cvm_result = val
            elif field_name == "tac_seq" and not txn.tac_sequence:
                txn.tac_sequence = val.strip()

    def _extract_se_recv_fields(self, msg: str) -> None:
        """Extract fields from SE_RECV message content."""
        txn = self._current
        if txn is None:
            return

        for field_name, pattern in SE_RECV_FIELD_PATTERNS.items():
            m = pattern.search(msg)
            if not m:
                continue
            val = m.group(1)
            if field_name == "response_code_host":
                if val == "00":
                    txn.response_code = "AA"
                elif val in ("05", "14", "51"):
                    txn.response_code = "DD"
            elif field_name == "auth_number":
                txn.authorization_number = val.strip()
            elif field_name == "host_response":
                txn.host_response_code = val
