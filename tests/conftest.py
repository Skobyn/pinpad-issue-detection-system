"""Shared test fixtures and sample log data."""

from __future__ import annotations

import pytest
from pathlib import Path

from pinpad_analyzer.storage.database import Database


SAMPLE_LOG_PATH = Path(__file__).parent.parent / "jrnl0002-20251130.txt"


@pytest.fixture
def tmp_db(tmp_path):
    """Create a temporary DuckDB database for testing."""
    db_path = str(tmp_path / "test.duckdb")
    with Database(db_path) as db:
        yield db


@pytest.fixture
def sample_log_path():
    """Path to the sample log file."""
    if SAMPLE_LOG_PATH.exists():
        return str(SAMPLE_LOG_PATH)
    pytest.skip("Sample log file not found")


# Real log line samples for unit testing
SAMPLE_LINES = {
    "standard_serial": '11/30/25 00:00:00.005 SERIAL Serial In on "Data Recv:" BytesToRecv 2',
    "standard_tcp": '11/30/25 00:02:32.997 TCP/IP SendMsgWaitAck3Tries: iMsg 6000, retry 0',
    "standard_dll_ex": '11/30/25 08:06:19.279 DLL-EX MTX_POS_BeginOrder',
    "standard_svreps": '11/30/25 08:07:31.399 SVREPS SE_SEND(TimeOutSecs 30) [60 bytes] URL[https://trn2.servereps.com/sCAT2] Ae9218',
    "standard_metric": '11/30/25 00:00:00.005 METRIC VirtualAvailMB: 3795, HeapTotalFree: 1007180',
    "repeat_single": "                  (Above Line Repeated 609 Times)",
    "repeat_multi": "                  (Above 2 Lines Repeated 1 Times)",
    "error_line": '11/30/25 00:02:36.150 SERIAL ****ERROR: SendMsgWaitAck3Tries failed, rtn 0',
    "scat_dead": '11/30/25 00:02:42.006 DLL-EX SCATAliveInt = 0 (ReportScatDead)',
    "scat_alive": '11/30/25 07:56:55.671 DLL-EX SCATAliveInt = 3 (ReportScatAlive)',
    "p2p_mismatch": '11/30/25 00:00:30.850 DLL-EX IsP2PDLL=Y, IsTermP2PCapable=N',
    "end_order": '11/30/25 08:08:40.447 DLL-EX MTX_POS_EndOrder',
    "se_recv": '11/30/25 08:07:33.149 SVREPS SE_RECV(1.743 secs) [250 bytes] Ae9218 Af00 Ag123456',
    "blank": "",
    "continuation": "  Some continuation data with no timestamp",
}
