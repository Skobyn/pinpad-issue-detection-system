"""Model registry: save/load/version ML artifacts."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from pinpad_analyzer.storage.database import Database

DEFAULT_MODEL_DIR = "./data/models"


class ModelRegistry:
    """Manages versioned ML model artifacts and metadata."""

    def __init__(self, db: Database, model_dir: str = DEFAULT_MODEL_DIR) -> None:
        self._db = db
        self._model_dir = Path(model_dir)
        self._model_dir.mkdir(parents=True, exist_ok=True)

    def register(
        self,
        model_id: str,
        model_type: str,
        version: str,
        file_path: str,
        training_samples: int = 0,
        metrics: Optional[dict] = None,
        activate: bool = True,
    ) -> None:
        """Register a trained model in the registry."""
        if activate:
            # Deactivate previous active model of same type
            self._db.conn.execute(
                "UPDATE models SET is_active = FALSE WHERE model_type = ? AND is_active = TRUE",
                [model_type],
            )

        self._db.conn.execute(
            """INSERT INTO models
               (model_id, model_type, version, file_path,
                training_date, training_samples, metrics, is_active)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT (model_id) DO UPDATE SET
                 version = EXCLUDED.version,
                 file_path = EXCLUDED.file_path,
                 training_date = EXCLUDED.training_date,
                 training_samples = EXCLUDED.training_samples,
                 metrics = EXCLUDED.metrics,
                 is_active = EXCLUDED.is_active""",
            [
                model_id,
                model_type,
                version,
                file_path,
                datetime.now(),
                training_samples,
                json.dumps(metrics or {}),
                activate,
            ],
        )

    def get_active(self, model_type: str) -> Optional[dict]:
        """Get the currently active model for a given type."""
        row = self._db.conn.execute(
            """SELECT model_id, model_type, version, file_path,
                      training_date, training_samples, metrics, is_active
               FROM models
               WHERE model_type = ? AND is_active = TRUE
               LIMIT 1""",
            [model_type],
        ).fetchone()

        if not row:
            return None

        return {
            "model_id": row[0],
            "model_type": row[1],
            "version": row[2],
            "file_path": row[3],
            "training_date": row[4],
            "training_samples": row[5],
            "metrics": json.loads(row[6]) if row[6] else {},
            "is_active": row[7],
        }

    def list_models(self, model_type: Optional[str] = None) -> list[dict]:
        """List all registered models, optionally filtered by type."""
        query = "SELECT model_id, model_type, version, file_path, training_date, training_samples, is_active FROM models"
        params = []
        if model_type:
            query += " WHERE model_type = ?"
            params.append(model_type)
        query += " ORDER BY training_date DESC"

        rows = self._db.conn.execute(query, params).fetchall()
        return [
            {
                "model_id": r[0],
                "model_type": r[1],
                "version": r[2],
                "file_path": r[3],
                "training_date": r[4],
                "training_samples": r[5],
                "is_active": r[6],
            }
            for r in rows
        ]

    def model_path(self, model_id: str) -> Path:
        """Return the file path for storing a model artifact."""
        return self._model_dir / f"{model_id}.joblib"
