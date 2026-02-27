"""Anomaly detection using Isolation Forest."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import joblib


class AnomalyDetector:
    """Isolation Forest anomaly detector for transaction features."""

    def __init__(self, contamination: float = 0.05) -> None:
        self.contamination = contamination
        self.model = IsolationForest(
            n_estimators=200,
            contamination=contamination,
            random_state=42,
            n_jobs=-1,
        )
        self.scaler = StandardScaler()
        self._is_fitted = False

    def fit(self, features: np.ndarray) -> None:
        """Train on 'normal' transaction features."""
        if features.shape[0] < 10:
            return
        scaled = self.scaler.fit_transform(features)
        self.model.fit(scaled)
        self._is_fitted = True

    def score(self, features: np.ndarray) -> np.ndarray:
        """Score transactions. More negative = more anomalous."""
        if not self._is_fitted or features.shape[0] == 0:
            return np.zeros(features.shape[0])
        scaled = self.scaler.transform(features)
        return self.model.score_samples(scaled)

    def predict(self, features: np.ndarray) -> np.ndarray:
        """Predict anomaly labels. -1 = anomaly, 1 = normal."""
        if not self._is_fitted or features.shape[0] == 0:
            return np.ones(features.shape[0])
        scaled = self.scaler.transform(features)
        return self.model.predict(scaled)

    def save(self, path: str) -> None:
        """Save model and scaler to disk."""
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump({"model": self.model, "scaler": self.scaler}, path)

    def load(self, path: str) -> None:
        """Load model and scaler from disk."""
        data = joblib.load(path)
        self.model = data["model"]
        self.scaler = data["scaler"]
        self._is_fitted = True

    @property
    def is_fitted(self) -> bool:
        return self._is_fitted
