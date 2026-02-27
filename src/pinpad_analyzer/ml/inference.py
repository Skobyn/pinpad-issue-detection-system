"""Inference pipeline: load model -> score -> combine with rules -> rank."""

from __future__ import annotations

from typing import Optional

import numpy as np

from pinpad_analyzer.features.transaction_features import TransactionFeatureExtractor
from pinpad_analyzer.ml.anomaly import AnomalyDetector
from pinpad_analyzer.ml.registry import ModelRegistry
from pinpad_analyzer.ml.rules import RuleEngine
from pinpad_analyzer.storage.database import Database


class InferencePipeline:
    """Combines rule-based detection with ML anomaly scoring."""

    def __init__(self, db: Database) -> None:
        self._db = db
        self._registry = ModelRegistry(db)
        self._rule_engine = RuleEngine(db)
        self._detector: Optional[AnomalyDetector] = None

    def _load_detector(self) -> Optional[AnomalyDetector]:
        """Load the active anomaly detector from registry."""
        if self._detector is not None:
            return self._detector

        active = self._registry.get_active("anomaly_detector")
        if not active:
            return None

        detector = AnomalyDetector()
        try:
            detector.load(active["file_path"])
            self._detector = detector
            return detector
        except Exception:
            return None

    def analyze_file(self, file_id: str) -> list[dict]:
        """Run full analysis: rules + ML scoring on a file.

        Returns ranked list of issues, each with:
        - issue_type, severity, confidence, evidence, resolution_steps
        - anomaly_scores (if ML model available)
        """
        # Tier 1: Rule-based detection
        rule_issues = self._rule_engine.analyze_file(file_id)

        # Tier 2: ML anomaly detection
        detector = self._load_detector()
        anomaly_issues = []
        if detector:
            anomaly_issues = self._score_anomalies(file_id, detector)

        # Merge and deduplicate
        all_issues = self._merge_issues(rule_issues, anomaly_issues)

        # Rank by severity then confidence
        all_issues.sort(key=lambda x: (x["severity_rank"], -x["confidence"]))

        return all_issues

    def _score_anomalies(
        self, file_id: str, detector: AnomalyDetector
    ) -> list[dict]:
        """Score transactions with anomaly detector."""
        extractor = TransactionFeatureExtractor(self._db)
        features, event_ids = extractor.extract_for_file(file_id)

        if features.shape[0] == 0:
            return []

        scores = detector.score(features)
        predictions = detector.predict(features)

        issues = []
        anomaly_mask = predictions == -1
        if not anomaly_mask.any():
            return []

        # Group anomalous transactions
        anomaly_indices = np.where(anomaly_mask)[0]
        anomaly_event_ids = [event_ids[i] for i in anomaly_indices]
        anomaly_scores_list = [float(scores[i]) for i in anomaly_indices]

        # Get transaction details for anomalies
        for idx, event_id in zip(anomaly_indices, anomaly_event_ids):
            row = self._db.conn.execute(
                """SELECT e.start_time, t.card_type, t.response_code,
                          t.host_latency_ms, t.amount_cents
                   FROM events e
                   JOIN transactions t ON e.event_id = t.event_id
                   WHERE e.event_id = ?""",
                [event_id],
            ).fetchone()

            if not row:
                continue

            score_val = float(scores[idx])
            confidence = min(0.85, max(0.3, -score_val * 2))

            issues.append({
                "issue_type": "ml_anomaly",
                "severity": "medium",
                "severity_rank": 3,
                "confidence": confidence,
                "description": "ML-detected anomalous transaction",
                "time_range": str(row[0]),
                "evidence": (
                    f"Anomaly score: {score_val:.3f}, "
                    f"card={row[1]}, response={row[2]}, "
                    f"latency={row[3]}ms, amount={row[4]}c"
                ),
                "resolution_steps": [
                    "Review transaction details for unusual patterns",
                    "Compare with normal transaction baseline",
                    "Check if correlated with other detected issues",
                ],
                "event_id": event_id,
                "anomaly_score": score_val,
            })

        return issues

    @staticmethod
    def _merge_issues(
        rule_issues: list[dict], anomaly_issues: list[dict]
    ) -> list[dict]:
        """Merge rule-based and ML issues, boosting confidence for overlaps."""
        merged = list(rule_issues)

        # Check if ML anomalies overlap with rule detections (same time window)
        rule_times = set()
        for issue in rule_issues:
            tr = issue.get("time_range", "")
            if tr:
                rule_times.add(tr.split(" - ")[0][:16])  # Match on minute

        for anomaly in anomaly_issues:
            tr = anomaly.get("time_range", "")
            time_key = tr[:16] if tr else ""
            if time_key in rule_times:
                # Boost confidence of matching rule issue
                for issue in merged:
                    issue_time = issue.get("time_range", "").split(" - ")[0][:16]
                    if issue_time == time_key:
                        issue["confidence"] = min(0.99, issue["confidence"] + 0.1)
                        issue["evidence"] += " [ML-confirmed]"
                        break
            else:
                merged.append(anomaly)

        return merged
