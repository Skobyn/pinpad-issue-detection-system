"""Cloud Processing Worker entry point.

Usage: python -m pinpad_analyzer.cloud

Watches a GCS bucket for new journal log uploads and processes them
into MotherDuck using the existing ingestion pipeline.

Configure via environment variables:
    PINPAD_DB              - Database path (e.g., md:pinpad_analyzer)
    MOTHERDUCK_TOKEN       - MotherDuck authentication token
    PINPAD_GCS_BUCKET      - GCS bucket to watch
    PINPAD_GCS_PREFIX      - Optional prefix filter (e.g., "145714/")
    PINPAD_POLL_INTERVAL   - Seconds between bucket polls (default: 60)
    GOOGLE_APPLICATION_CREDENTIALS - GCS service account key path
"""

from __future__ import annotations

import logging
import os
import signal
import sys

from pinpad_analyzer.cloud.gcs_watcher import GCSWatcher
from pinpad_analyzer.cloud.processor import CloudProcessor
from pinpad_analyzer.storage.database import Database, resolve_db_path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("pinpad_cloud")


def main() -> None:
    db_path = resolve_db_path()
    bucket_name = os.environ.get("PINPAD_GCS_BUCKET", "")
    prefix = os.environ.get("PINPAD_GCS_PREFIX", "")
    poll_interval = int(os.environ.get("PINPAD_POLL_INTERVAL", "60"))
    credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")

    if not bucket_name:
        logger.error("PINPAD_GCS_BUCKET environment variable is required")
        sys.exit(1)

    if not db_path or db_path == "./data/pinpad_analyzer.duckdb":
        logger.warning(
            "Using local database. Set PINPAD_DB=md:pinpad_analyzer for MotherDuck."
        )

    logger.info("Cloud processor starting")
    logger.info("  Database: %s", db_path)
    logger.info("  Bucket: gs://%s/%s", bucket_name, prefix)
    logger.info("  Poll interval: %ds", poll_interval)

    db = Database(db_path)
    db.initialize()

    processor = CloudProcessor(db, credentials_path=credentials_path)
    watcher = GCSWatcher(
        bucket_name=bucket_name,
        prefix=prefix,
        credentials_path=credentials_path,
        poll_interval=poll_interval,
    )

    def handle_new_file(obj_info: dict) -> None:
        """Process a newly detected GCS object."""
        try:
            processor.process_gcs_object(
                bucket_name=bucket_name,
                object_name=obj_info["name"],
                object_metadata=obj_info.get("metadata", {}),
            )
        except Exception as e:
            logger.error("Failed to process %s: %s", obj_info["name"], e)

    # Graceful shutdown
    def shutdown(sig, frame):
        logger.info("Shutting down...")
        watcher.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Start polling
    watcher.poll(on_new_file=handle_new_file)

    db.close()
    logger.info("Cloud processor stopped.")


if __name__ == "__main__":
    main()
