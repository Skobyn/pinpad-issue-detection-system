"""POS Upload Agent entry point.

Usage: python -m pinpad_analyzer.agent

Watches a directory for journal log files and uploads them to GCS.
Configure via environment variables:
    PINPAD_WATCH_DIR       - Directory to watch for jrnl*.txt files
    PINPAD_GCS_BUCKET      - GCS bucket name
    PINPAD_COMPANY_ID      - Company identifier
    PINPAD_STORE_ID        - Store identifier
    PINPAD_POLL_INTERVAL   - Seconds between directory scans (default: 30)
    PINPAD_SETTLE_SECONDS  - Seconds of no size change before upload (default: 60)
    GOOGLE_APPLICATION_CREDENTIALS - Path to GCS service account key
"""

from __future__ import annotations

import logging
import signal
import sys

from pinpad_analyzer.agent.config import AgentConfig
from pinpad_analyzer.agent.uploader import GCSUploader
from pinpad_analyzer.agent.watcher import LogWatcher, StateDB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("pinpad_agent")


def main() -> None:
    config = AgentConfig.from_env()
    errors = config.validate()
    if errors:
        for err in errors:
            logger.error("Config error: %s", err)
        sys.exit(1)

    logger.info(
        "Starting POS Upload Agent v%s for company=%s store=%s",
        config.agent_version,
        config.company_id,
        config.store_id,
    )
    logger.info("Watching: %s", config.watch_dir)
    logger.info("Uploading to: gs://%s/", config.gcs_bucket)

    state_db = StateDB(config.state_db_path)
    uploader = GCSUploader(
        bucket_name=config.gcs_bucket,
        company_id=config.company_id,
        store_id=config.store_id,
        credentials_path=config.gcs_credentials,
        agent_version=config.agent_version,
    )
    watcher = LogWatcher(
        watch_dir=config.watch_dir,
        state_db=state_db,
        settle_seconds=config.settle_seconds,
        poll_interval=config.poll_interval,
    )

    def handle_file(file_info: dict) -> None:
        """Upload a ready file to GCS."""
        fp = file_info["file_path"]
        lane = file_info["lane"]
        log_date = file_info["log_date"]
        sha256 = file_info["sha256"]
        file_size = file_info["file_size"]

        logger.info("Uploading %s (lane=%d, date=%s, %d bytes)", fp, lane, log_date, file_size)

        gcs_uri = uploader.upload(fp, lane, log_date, sha256=sha256)
        if gcs_uri:
            state_db.mark_uploaded(fp, sha256, gcs_uri, file_size)
            state_db.dequeue(fp)
            logger.info("Uploaded: %s", gcs_uri)
        else:
            # Queue for retry when network recovers
            state_db.enqueue(fp, sha256, lane, log_date)
            logger.warning("Queued for later upload: %s", fp)

    def drain_queue() -> None:
        """Retry any queued files from previous failures."""
        queued = state_db.get_queued()
        if queued:
            logger.info("Retrying %d queued files...", len(queued))
        for item in queued:
            handle_file({
                "file_path": item["file_path"],
                "lane": item["lane"],
                "log_date": item["log_date"],
                "sha256": item["sha256"],
                "file_size": 0,
            })

    # Handle graceful shutdown
    def shutdown(sig, frame):
        logger.info("Shutting down...")
        watcher.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Drain any pending queue first
    drain_queue()

    # Start the main watch loop
    watcher.run(on_file_ready=handle_file)

    state_db.close()
    logger.info("Agent stopped.")


if __name__ == "__main__":
    main()
