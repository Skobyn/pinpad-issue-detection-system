"""Per-transaction feature vector extraction."""

from __future__ import annotations

import numpy as np

from pinpad_analyzer.storage.database import Database

# Card type encoding
CARD_TYPE_ENCODING = {"Debit": 0, "Credit": 1, "EBT Food": 2, "EBT Cash": 3}
# Entry method encoding
ENTRY_METHOD_ENCODING = {"E": 0, "S": 1, "C": 2, "M": 3, "K": 4, "EC": 2}

FEATURE_NAMES = [
    "host_latency_ms",
    "duration_ms",
    "amount_cents",
    "cashback_cents",
    "serial_error_count",
    "card_type_encoded",
    "entry_method_encoded",
    "is_approved",
    "is_quickchip",
    "is_fallback",
    "hour_of_day",
    "has_tvr",
    "tvr_byte1",
    "tvr_byte3",
    "cvm_online_pin",
]


class TransactionFeatureExtractor:
    """Extracts numerical feature vectors from transaction data in DB."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def extract_for_file(self, file_id: str) -> tuple[np.ndarray, list[str]]:
        """Extract feature matrix for all transactions in a file.

        Returns (feature_matrix, event_ids).
        """
        rows = self._db.conn.execute("""
            SELECT
                e.event_id,
                e.start_time,
                e.duration_ms,
                t.host_latency_ms,
                t.amount_cents,
                t.cashback_cents,
                t.serial_error_count,
                t.card_type,
                t.entry_method,
                t.is_approved,
                t.is_quickchip,
                t.is_fallback,
                t.tvr,
                t.cvm_result,
                t.response_code
            FROM events e
            JOIN transactions t ON e.event_id = t.event_id
            WHERE e.file_id = ?
            ORDER BY e.start_time
        """, [file_id]).fetchall()

        if not rows:
            return np.array([]).reshape(0, len(FEATURE_NAMES)), []

        features = []
        event_ids = []
        for row in rows:
            (event_id, start_time, duration_ms, host_latency_ms,
             amount_cents, cashback_cents, serial_error_count,
             card_type, entry_method, is_approved, is_quickchip,
             is_fallback, tvr, cvm_result, response_code) = row

            # TVR parsing
            tvr = tvr or ""
            has_tvr = 1 if len(tvr) >= 10 else 0
            tvr_byte1 = int(tvr[0:2], 16) if len(tvr) >= 2 else 0
            tvr_byte3 = int(tvr[4:6], 16) if len(tvr) >= 6 else 0

            # CVM: online PIN check
            cvm_online_pin = 1 if cvm_result and cvm_result.startswith("42") else 0

            # Hour of day
            hour = start_time.hour if start_time else 0

            vec = [
                float(host_latency_ms or 0),
                float(duration_ms or 0),
                float(amount_cents or 0),
                float(cashback_cents or 0),
                float(serial_error_count or 0),
                float(CARD_TYPE_ENCODING.get(card_type or "", -1)),
                float(ENTRY_METHOD_ENCODING.get(entry_method or "", -1)),
                float(1 if is_approved else 0),
                float(1 if is_quickchip else 0),
                float(1 if is_fallback else 0),
                float(hour),
                float(has_tvr),
                float(tvr_byte1),
                float(tvr_byte3),
                float(cvm_online_pin),
            ]
            features.append(vec)
            event_ids.append(event_id)

        return np.array(features, dtype=np.float64), event_ids
