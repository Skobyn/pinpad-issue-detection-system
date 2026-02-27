"""GCS uploader with resumable uploads and retry logic."""

from __future__ import annotations

import hashlib
import logging
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def compute_sha256(file_path: str) -> str:
    """Compute SHA-256 hash of a file."""
    sha = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()


class GCSUploader:
    """Uploads journal log files to Google Cloud Storage."""

    def __init__(
        self,
        bucket_name: str,
        company_id: str,
        store_id: str,
        credentials_path: str = "",
        agent_version: str = "1.0.0",
    ) -> None:
        self._bucket_name = bucket_name
        self._company_id = company_id
        self._store_id = store_id
        self._agent_version = agent_version
        self._client = None
        self._bucket = None
        self._credentials_path = credentials_path

    def _get_bucket(self):
        """Lazy-init GCS client and bucket."""
        if self._bucket is None:
            from google.cloud import storage as gcs

            if self._credentials_path:
                self._client = gcs.Client.from_service_account_json(
                    self._credentials_path
                )
            else:
                self._client = gcs.Client()
            self._bucket = self._client.bucket(self._bucket_name)
        return self._bucket

    def build_gcs_path(
        self, file_name: str, lane: int, log_date: str
    ) -> str:
        """Build the GCS object path.

        Format: {company_id}/{store_id}/lane{NN}/{YYYY-MM-DD}/{filename}
        """
        lane_str = f"lane{lane:02d}" if lane > 0 else "lane00"
        return f"{self._company_id}/{self._store_id}/{lane_str}/{log_date}/{file_name}"

    def upload(
        self,
        file_path: str,
        lane: int,
        log_date: str,
        sha256: str = "",
        max_retries: int = 3,
    ) -> Optional[str]:
        """Upload a file to GCS with retries.

        Returns the GCS URI on success, None on failure.
        """
        p = Path(file_path)
        if not sha256:
            sha256 = compute_sha256(file_path)

        gcs_path = self.build_gcs_path(p.name, lane, log_date)
        bucket = self._get_bucket()

        metadata = {
            "company_id": self._company_id,
            "store_id": self._store_id,
            "lane": str(lane),
            "log_date": log_date,
            "sha256": sha256,
            "agent_version": self._agent_version,
        }

        for attempt in range(1, max_retries + 1):
            try:
                blob = bucket.blob(gcs_path)
                blob.metadata = metadata

                # Use resumable upload for files > 5MB
                if p.stat().st_size > 5 * 1024 * 1024:
                    blob.upload_from_filename(
                        str(p), content_type="text/plain", timeout=300
                    )
                else:
                    blob.upload_from_filename(
                        str(p), content_type="text/plain", timeout=120
                    )

                uri = f"gs://{self._bucket_name}/{gcs_path}"
                logger.info("Uploaded %s -> %s", p.name, uri)
                return uri

            except Exception as e:
                wait = 2**attempt
                logger.warning(
                    "Upload attempt %d/%d failed for %s: %s. Retrying in %ds...",
                    attempt,
                    max_retries,
                    p.name,
                    e,
                    wait,
                )
                if attempt < max_retries:
                    time.sleep(wait)

        logger.error("Failed to upload %s after %d attempts", p.name, max_retries)
        return None

    def exists(self, file_name: str, lane: int, log_date: str) -> bool:
        """Check if an object already exists in GCS."""
        gcs_path = self.build_gcs_path(file_name, lane, log_date)
        bucket = self._get_bucket()
        blob = bucket.blob(gcs_path)
        return blob.exists()
