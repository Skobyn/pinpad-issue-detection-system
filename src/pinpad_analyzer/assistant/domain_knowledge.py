"""Domain knowledge base for EMV/POS troubleshooting."""

from __future__ import annotations

# Error pattern -> meaning -> common causes -> resolution
DOMAIN_PATTERNS = {
    "scat_dead": {
        "meaning": "SCAT (Self-Contained Automated Terminal) is unresponsive",
        "common_causes": [
            "P2P encryption mismatch (DLL requires P2P but terminal not capable)",
            "Serial cable disconnection or loose connection",
            "Pinpad power loss or hardware failure",
            "Firmware crash or lock-up",
            "COM port conflict with other software",
        ],
        "correlation_checks": [
            "Check if P2P mismatch detected (IsP2PDLL=Y, IsTermP2PCapable=N)",
            "Check for preceding serial comm failures",
            "Check if overnight (power-saving mode issue)",
            "Check for firmware version mismatches",
        ],
    },
    "serial_comm_failure": {
        "meaning": "USB/serial communication between POS and pinpad failed",
        "common_causes": [
            "Loose USB cable connection",
            "USB port power management turning off port",
            "Cable damage or intermittent connection",
            "COM port driver issue",
            "Pinpad in bad state not responding to ACK",
        ],
        "correlation_checks": [
            "Check if SCAT went dead after serial failures",
            "Check if failures are clustered (bad cable) vs sporadic (driver issue)",
            "Check retry count pattern (increasing = degrading connection)",
        ],
    },
    "servereps_500": {
        "meaning": "ServerEPS backend returning HTTP 500 Internal Server Error",
        "common_causes": [
            "ServerEPS service outage or maintenance",
            "Database connectivity issue on server side",
            "Server overload during peak hours",
            "Configuration error after server update",
        ],
        "correlation_checks": [
            "Check if both svc1 and svc2 affected (dual DC outage)",
            "Check if ExchangeInfo or MonitoringStatus specific",
            "Check time of day (maintenance window?)",
            "Check if auto-failover to secondary DC occurred",
        ],
    },
    "servereps_socket_error": {
        "meaning": "Network socket error connecting to ServerEPS",
        "common_causes": [
            "10054: Remote server forcibly closed connection",
            "10060: Connection timed out (network latency/routing)",
            "10061: Connection refused (server not listening)",
            "Store internet connectivity issue",
            "Firewall blocking outbound HTTPS",
        ],
        "correlation_checks": [
            "Differentiate error codes for root cause",
            "Check if DNS resolution working",
            "Check if other network services affected",
        ],
    },
    "host_timeout": {
        "meaning": "Transaction host authorization taking too long",
        "common_causes": [
            "Network congestion between store and processor",
            "Payment processor overload",
            "DNS resolution delay",
            "Firewall inspection delay",
        ],
        "correlation_checks": [
            "Check average vs max latency (sporadic vs systemic)",
            "Check which host URL affected (trn1 vs trn2)",
            "Check time-of-day pattern (peak hours)",
        ],
    },
    "p2p_encryption_mismatch": {
        "meaning": "P2P encryption DLL requires encryption but terminal reports not capable",
        "common_causes": [
            "Terminal firmware doesn't support P2P/SRED",
            "Terminal config version mismatch after update",
            "VSD module not enabled on pinpad",
            "Wrong terminal model connected",
        ],
        "correlation_checks": [
            "This is almost always the root cause of SCAT dead",
            "Check terminal model and firmware version",
            "Check if recently changed pinpad hardware",
        ],
    },
    "chip_read_failure": {
        "meaning": "EMV chip card reader unable to read card chip",
        "common_causes": [
            "Dirty chip reader contacts",
            "Damaged customer card chip",
            "Worn chip reader hardware",
            "EMV kernel version incompatibility",
        ],
        "correlation_checks": [
            "Check if same card retried (card issue) vs multiple cards (reader issue)",
            "Check InBadChipReadMode counter",
            "Check if fallback to swipe succeeds",
        ],
    },
    "repeated_decline": {
        "meaning": "Multiple consecutive transaction declines",
        "common_causes": [
            "Insufficient funds (normal issuer behavior)",
            "Card fraud hold by issuer",
            "Expired card or wrong PIN",
            "EBT balance depleted",
        ],
        "correlation_checks": [
            "Check if same PAN (customer issue) vs different PANs (system issue)",
            "Check response codes (DD=decline, DN=decline)",
            "If all cards declining: check host connectivity",
        ],
    },
    "card_read_intermittent": {
        "meaning": "Pinpad card reader intermittently failing to read cards (chip/swipe/tap)",
        "common_causes": [
            "Worn or dirty chip card reader contacts",
            "Damaged mag stripe reader head",
            "Loose NFC/contactless antenna connection",
            "USB power management suspending port (causes 7-8 AM startup failures)",
            "Thermal issue - reader fails when cold at startup",
            "Intermittent cable connection causing reader resets",
            "Hardware degradation (reader reaching end of life)",
        ],
        "correlation_checks": [
            "Check no-read rate by hour (7-8 AM spike = power/thermal startup issue)",
            "Check burst pattern (consecutive no-reads = active failure vs random = degradation)",
            "Check if rate is increasing over days/weeks (degradation trend)",
            "Compare to baseline rate across other lanes/stores",
            "Check if approved txns during same period are abnormally slow",
            "Check customer wait time on failed attempts",
        ],
        "cashier_cancel_note": (
            "Cashier pressing credit button then clearing accounts for <3% of no-reads. "
            "These are filtered by duration (<15s). The vast majority (97%+) show "
            "customers waiting 30-100+ seconds, peaking at the POS-configured 45s timeout. "
            "DoAbortAnyTransaction in logs is programmatic cleanup by Reset_Clear, "
            "not a manual cashier action."
        ),
    },
}

# SCAT state descriptions
SCAT_STATES = {
    "StateNone": "No state / initial",
    "StateReset": "Resetting pinpad",
    "StateGetAppVersions": "Retrieving app versions",
    "StateCheckConfig": "Checking configuration",
    "StateStartSession": "Starting session",
    "StateIdle": "Ready for transactions",
    "StateProcessRequest": "Processing transaction request",
    "StateSendRecv": "Sending/receiving with host",
}

# EMV response code meanings
RESPONSE_CODES = {
    "AP": "Approved",
    "DD": "Declined by host",
    "DN": "Declined by terminal",
    "CT": "Call center (referral)",
    "TO": "Timeout",
    "ER": "Error",
}

# Entry method codes
ENTRY_METHODS = {
    "E": "EMV chip insert",
    "S": "Magnetic stripe swipe",
    "C": "Contactless tap",
    "M": "Manual key entry",
    "K": "Keyed (fallback)",
    "EC": "EMV contactless",
}
