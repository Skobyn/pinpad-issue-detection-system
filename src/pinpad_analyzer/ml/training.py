"""Training pipeline: extract features -> train models -> evaluate -> save."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import numpy as np

from pinpad_analyzer.features.transaction_features import TransactionFeatureExtractor
from pinpad_analyzer.ml.anomaly import AnomalyDetector
from pinpad_analyzer.ml.registry import ModelRegistry
from pinpad_analyzer.storage.database import Database


class TrainingPipeline:
    """Orchestrates model training from ingested data."""

    def __init__(self, db: Database) -> None:
        self._db = db
        self._registry = ModelRegistry(db)

    def train_anomaly_detector(
        self,
        contamination: float = 0.05,
    ) -> dict:
        """Train an Isolation Forest anomaly detector on all ingested transactions.

        Returns training metrics dict.
        """
        extractor = TransactionFeatureExtractor(self._db)

        # Gather features from all files
        file_ids = self._db.conn.execute(
            "SELECT file_id FROM log_files"
        ).fetchall()

        all_features = []
        all_event_ids = []
        for (file_id,) in file_ids:
            features, event_ids = extractor.extract_for_file(file_id)
            if features.shape[0] > 0:
                all_features.append(features)
                all_event_ids.extend(event_ids)

        if not all_features:
            return {"status": "error", "message": "No transaction data to train on"}

        feature_matrix = np.vstack(all_features)
        n_samples = feature_matrix.shape[0]

        if n_samples < 10:
            return {"status": "error", "message": f"Too few samples ({n_samples}), need >= 10"}

        # Train
        detector = AnomalyDetector(contamination=contamination)
        detector.fit(feature_matrix)

        # Evaluate: score training data to get baseline stats
        scores = detector.score(feature_matrix)
        predictions = detector.predict(feature_matrix)
        n_anomalies = int((predictions == -1).sum())

        metrics = {
            "n_samples": n_samples,
            "n_features": feature_matrix.shape[1],
            "n_anomalies": n_anomalies,
            "anomaly_rate": n_anomalies / n_samples,
            "score_mean": float(scores.mean()),
            "score_std": float(scores.std()),
            "score_min": float(scores.min()),
            "contamination": contamination,
        }

        # Save model artifact
        version = datetime.now().strftime("%Y%m%d_%H%M%S")
        model_id = f"anomaly_detector_{version}"
        model_path = self._registry.model_path(model_id)
        detector.save(str(model_path))

        # Register in DB
        self._registry.register(
            model_id=model_id,
            model_type="anomaly_detector",
            version=version,
            file_path=str(model_path),
            training_samples=n_samples,
            metrics=metrics,
            activate=True,
        )

        metrics["model_id"] = model_id
        metrics["status"] = "success"
        return metrics
