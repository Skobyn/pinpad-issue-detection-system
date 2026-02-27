"""Tests for ML pipeline: rules, anomaly detection, training."""

from __future__ import annotations

from datetime import datetime

import numpy as np
import pytest

from pinpad_analyzer.ml.anomaly import AnomalyDetector
from pinpad_analyzer.config.issue_types import ISSUE_TYPES


class TestAnomalyDetector:
    """Test Isolation Forest anomaly detector."""

    def test_fit_and_predict(self):
        """Test basic fit-predict cycle."""
        detector = AnomalyDetector(contamination=0.1)

        # Generate normal data
        rng = np.random.RandomState(42)
        normal_data = rng.randn(100, 5)

        detector.fit(normal_data)
        assert detector.is_fitted

        predictions = detector.predict(normal_data)
        assert predictions.shape == (100,)
        assert set(np.unique(predictions)).issubset({-1, 1})

        # Most should be normal
        normal_count = (predictions == 1).sum()
        assert normal_count > 80

    def test_score(self):
        """Test anomaly scoring."""
        detector = AnomalyDetector()

        rng = np.random.RandomState(42)
        data = rng.randn(50, 3)
        detector.fit(data)

        scores = detector.score(data)
        assert scores.shape == (50,)
        # Scores should be centered around a negative value
        assert scores.mean() < 0

    def test_unfitted_returns_defaults(self):
        """Unfitted detector should return safe defaults."""
        detector = AnomalyDetector()
        assert not detector.is_fitted

        features = np.random.randn(5, 3)
        scores = detector.score(features)
        assert np.all(scores == 0)

        predictions = detector.predict(features)
        assert np.all(predictions == 1)

    def test_too_few_samples(self):
        """Fewer than 10 samples should not fit."""
        detector = AnomalyDetector()
        data = np.random.randn(5, 3)
        detector.fit(data)
        assert not detector.is_fitted

    def test_save_and_load(self, tmp_path):
        """Test model serialization."""
        detector = AnomalyDetector()
        data = np.random.randn(50, 3)
        detector.fit(data)

        path = str(tmp_path / "model.joblib")
        detector.save(path)

        loaded = AnomalyDetector()
        loaded.load(path)
        assert loaded.is_fitted

        # Predictions should match
        orig_pred = detector.predict(data)
        loaded_pred = loaded.predict(data)
        np.testing.assert_array_equal(orig_pred, loaded_pred)

    def test_empty_features(self):
        """Empty feature array should return empty results."""
        detector = AnomalyDetector()
        data = np.random.randn(50, 3)
        detector.fit(data)

        empty = np.array([]).reshape(0, 3)
        assert detector.score(empty).shape == (0,)
        assert detector.predict(empty).shape == (0,)


class TestIssueTypes:
    """Test issue type definitions."""

    def test_all_issue_types_have_required_fields(self):
        for name, issue in ISSUE_TYPES.items():
            assert issue.id > 0
            assert issue.name == name
            assert issue.severity in ("critical", "high", "medium", "low")
            assert issue.severity_rank in (1, 2, 3, 4)
            assert len(issue.description) > 10
            assert len(issue.indicators) > 0
            assert len(issue.resolution_steps) > 0

    def test_severity_ranks_consistent(self):
        expected_ranks = {
            "critical": 1,
            "high": 2,
            "medium": 3,
            "low": 4,
        }
        for name, issue in ISSUE_TYPES.items():
            assert issue.severity_rank == expected_ranks[issue.severity], (
                f"{name}: severity={issue.severity} but rank={issue.severity_rank}"
            )

    def test_known_issue_types_exist(self):
        expected = [
            "serial_comm_failure", "scat_dead", "servereps_500",
            "servereps_socket_error", "host_timeout", "chip_read_failure",
            "repeated_decline", "p2p_encryption_mismatch",
        ]
        for name in expected:
            assert name in ISSUE_TYPES, f"Missing issue type: {name}"
