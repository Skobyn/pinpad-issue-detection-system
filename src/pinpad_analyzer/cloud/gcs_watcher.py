"""GCS bucket watcher - detects new journal log uploads."""

from __future__ import annotations

import logging
import time
from typing import Callable, Iterator

logger = logging.getLogger(__name__)


class GCSWatcher:
    """Watches a GCS bucket for new journal log files.

    Supports two modes:
    - Polling: periodically lists bucket contents (simple, for dev/small scale)
    - Pub/Sub: subscribes to GCS notifications (production, for scale)
    """

    def __init__(
        self,
        bucket_name: str,
        prefix: str = "",
        credentials_path: str = "",
        poll_interval: int = 60,
    ) -> None:
        self._bucket_name = bucket_name
        self._prefix = prefix
        self._credentials_path = credentials_path
        self._poll_interval = poll_interval
        self._client = None
        self._bucket = None
        self._seen: set[str] = set()
        self._running = False

    def _get_bucket(self):
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

    def list_new_objects(self) -> list[dict]:
        """List objects not yet seen, excluding processed/ prefix."""
        bucket = self._get_bucket()
        new_objects = []

        blobs = bucket.list_blobs(prefix=self._prefix)
        for blob in blobs:
            # Skip processed files
            if "/processed/" in blob.name:
                continue
            # Skip non-text files
            if not blob.name.endswith(".txt"):
                continue
            # Skip already seen
            if blob.name in self._seen:
                continue

            self._seen.add(blob.name)
            new_objects.append({
                "name": blob.name,
                "size": blob.size,
                "updated": blob.updated,
                "metadata": blob.metadata or {},
            })

        return new_objects

    def poll(
        self,
        on_new_file: Callable[[dict], None],
    ) -> None:
        """Run polling loop, calling on_new_file for each new object."""
        self._running = True
        logger.info(
            "Polling gs://%s/%s every %ds for new files",
            self._bucket_name,
            self._prefix,
            self._poll_interval,
        )

        # Initial scan to populate seen set without processing
        self._initialize_seen()

        while self._running:
            try:
                new_files = self.list_new_objects()
                for obj_info in new_files:
                    logger.info("New file detected: %s (%d bytes)", obj_info["name"], obj_info["size"])
                    on_new_file(obj_info)
            except Exception as e:
                logger.error("Error during poll: %s", e)

            time.sleep(self._poll_interval)

    def _initialize_seen(self) -> None:
        """Pre-populate seen set with existing objects."""
        bucket = self._get_bucket()
        blobs = bucket.list_blobs(prefix=self._prefix)
        for blob in blobs:
            if "/processed/" not in blob.name and blob.name.endswith(".txt"):
                self._seen.add(blob.name)
        logger.info("Initialized with %d existing objects", len(self._seen))

    def stop(self) -> None:
        self._running = False
