"""Cloud processor - downloads from GCS and runs ingestion pipeline into MotherDuck."""

from __future__ import annotations

import logging
import os
import re
import tempfile
from pathlib import Path

from pinpad_analyzer.ingestion.file_reader import FileReader
from pinpad_analyzer.ingestion.metadata_extractor import MetadataExtractor
from pinpad_analyzer.ingestion.models import LogEntry
from pinpad_analyzer.segmentation.error_cascade import ErrorCascadeDetector
from pinpad_analyzer.segmentation.health_segmenter import HealthSegmenter
from pinpad_analyzer.segmentation.state_machine import SCATStateMachine
from pinpad_analyzer.segmentation.transaction_segmenter import TransactionSegmenter
from pinpad_analyzer.storage.database import Database
from pinpad_analyzer.storage.repositories import (
    EventRepo,
    LogEntryRepo,
    LogFileRepo,
    SCATTimelineRepo,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = 10_000


class CloudProcessor:
    """Processes journal log files from GCS into MotherDuck.

    Reuses the entire existing ingestion pipeline - parser, segmenter,
    metadata extractor, and storage repositories.
    """

    def __init__(self, db: Database, credentials_path: str = "") -> None:
        self._db = db
        self._credentials_path = credentials_path
        self._client = None

    def _get_gcs_client(self):
        if self._client is None:
            from google.cloud import storage as gcs

            if self._credentials_path:
                self._client = gcs.Client.from_service_account_json(
                    self._credentials_path
                )
            else:
                self._client = gcs.Client()
        return self._client

    def process_gcs_object(
        self,
        bucket_name: str,
        object_name: str,
        object_metadata: dict | None = None,
    ) -> bool:
        """Download a GCS object and run the full ingestion pipeline.

        Returns True if processed successfully, False if skipped or failed.
        """
        object_metadata = object_metadata or {}

        # Check for duplicate by SHA-256 if available in GCS metadata
        sha256 = object_metadata.get("sha256", "")
        if sha256:
            dup = self._db.conn.execute(
                "SELECT 1 FROM log_files WHERE sha256_hash = ?", [sha256]
            ).fetchone()
            if dup:
                logger.info("Skipping duplicate (sha256=%s...): %s", sha256[:12], object_name)
                return False

        # Download to temp file
        with tempfile.TemporaryDirectory() as tmpdir:
            file_name = Path(object_name).name
            local_path = os.path.join(tmpdir, file_name)

            client = self._get_gcs_client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            blob.download_to_filename(local_path)
            logger.info("Downloaded %s (%d bytes)", file_name, Path(local_path).stat().st_size)

            # Run the ingestion pipeline
            success = self._ingest_file(
                local_path,
                store_id=object_metadata.get("store_id", ""),
                upload_source="gcs",
            )

            if success:
                # Move to processed prefix in GCS
                self._mark_processed(bucket, object_name)

            return success

    def _ingest_file(
        self,
        file_path: str,
        store_id: str = "",
        upload_source: str = "gcs",
    ) -> bool:
        """Run the full ingestion pipeline on a local file.

        This reuses the same logic as cli/commands/ingest.py::_ingest_file
        but without Rich console output.
        """
        try:
            reader = FileReader(file_path)
            metadata = reader.metadata

            if not metadata.log_date:
                logger.warning("Skipping %s (no date)", metadata.file_name)
                return False

            file_repo = LogFileRepo(self._db)
            file_id = file_repo.file_id_for(metadata)

            # Extract identity metadata
            extractor = MetadataExtractor()
            identity = extractor.extract_from_file(file_path)
            identity.upload_source = upload_source
            effective_store = store_id if store_id else identity.store_id

            # Check for duplicate by SHA-256 (only skip if fully ingested)
            if identity.sha256_hash:
                dup = self._db.conn.execute(
                    "SELECT line_count FROM log_files WHERE sha256_hash = ?",
                    [identity.sha256_hash],
                ).fetchone()
                if dup and dup[0] > 0:
                    logger.info("Skipping duplicate file: %s", metadata.file_name)
                    return False

            # If already fully ingested, skip. Allow retry of partial ingestions.
            if file_repo.exists(file_id):
                row = self._db.conn.execute(
                    "SELECT line_count FROM log_files WHERE file_id = ?", [file_id]
                ).fetchone()
                if row and row[0] > 0:
                    logger.info("Skipping already-ingested file: %s", metadata.file_name)
                    return False
                # Partial ingestion - clean up and retry
                logger.info("Retrying partial ingestion for: %s", metadata.file_name)
                self._db.conn.execute("DELETE FROM log_entries WHERE file_id = ?", [file_id])
                self._db.conn.execute("DELETE FROM events WHERE file_id = ?", [file_id])
                self._db.conn.execute("DELETE FROM scat_timeline WHERE file_id = ?", [file_id])
                self._db.conn.execute("DELETE FROM log_files WHERE file_id = ?", [file_id])

            logger.info(
                "Ingesting %s (lane=%d, date=%s, company=%s, store=%s)",
                metadata.file_name,
                metadata.lane,
                metadata.log_date,
                identity.company_id,
                effective_store,
            )

            # Insert file record
            file_repo.insert(metadata, file_id, effective_store, 0, identity=identity)

            # Parse entries
            entry_repo = LogEntryRepo(self._db)
            event_repo = EventRepo(self._db)
            scat_repo = SCATTimelineRepo(self._db)

            all_entries: list[LogEntry] = []
            batch: list[LogEntry] = []
            total_stored = 0

            for entry in reader.read_entries(expand_repeats=True):
                all_entries.append(entry)
                batch.append(entry)
                if len(batch) >= BATCH_SIZE:
                    total_stored += entry_repo.insert_batch(file_id, batch)
                    batch = []

            if batch:
                total_stored += entry_repo.insert_batch(file_id, batch)

            # Update file record
            self._db.conn.execute(
                "UPDATE log_files SET line_count = ? WHERE file_id = ?",
                [total_stored, file_id],
            )

            # Second-pass metadata extraction from parsed entries
            extractor2 = MetadataExtractor()
            identity2 = extractor2.extract(iter(all_entries))
            for field_name in (
                "company_id", "store_id", "mid", "mtx_pos_version",
                "mtx_eps_version", "seccode_version", "pos_version",
                "pinpad_model", "pinpad_serial", "pinpad_firmware",
            ):
                if not getattr(identity, field_name) and getattr(identity2, field_name):
                    setattr(identity, field_name, getattr(identity2, field_name))
            for k, v in identity2.config.items():
                if k not in identity.config:
                    identity.config[k] = v
            file_repo.update_identity(file_id, identity)

            # Segment transactions
            segmenter = TransactionSegmenter()
            txn_count = 0
            for txn in segmenter.process_entries(iter(all_entries)):
                if txn.start_time is None or txn.end_time is None:
                    continue
                event_id = event_repo.insert_event(
                    event_type="transaction",
                    file_id=file_id,
                    lane=metadata.lane,
                    log_date=metadata.log_date,
                    start_time=txn.start_time,
                    end_time=txn.end_time,
                    start_line=txn.start_line,
                    end_line=txn.end_line,
                    line_count=txn.entry_count,
                )
                event_repo.insert_transaction(
                    event_id,
                    sequence_number=txn.sequence_number,
                    card_type=txn.card_type,
                    entry_method=txn.entry_method,
                    pan_last4=txn.pan_last4,
                    aid=txn.aid,
                    app_label=txn.app_label,
                    tac_sequence=txn.tac_sequence,
                    cvm_result=txn.cvm_result,
                    response_code=txn.response_code,
                    host_response_code=txn.host_response_code,
                    authorization_number=txn.authorization_number,
                    amount_cents=txn.amount_cents,
                    cashback_cents=txn.cashback_cents,
                    host_url=txn.host_url,
                    host_latency_ms=txn.host_latency_ms,
                    tvr=txn.tvr,
                    is_approved=txn.is_approved,
                    is_quickchip=txn.is_quickchip,
                    is_fallback=txn.is_fallback,
                    serial_error_count=txn.serial_error_count,
                )
                txn_count += 1

            # SCAT state tracking
            scat = SCATStateMachine()
            for entry in all_entries:
                scat.process_entry(entry)
            if scat.alive_history:
                scat_repo.insert_batch(
                    file_id,
                    [(ts, status) for ts, status, _name in scat.alive_history],
                )

            # Error cascades
            cascade_detector = ErrorCascadeDetector()
            cascade_count = 0
            for cascade in cascade_detector.process_entries(iter(all_entries)):
                if cascade.start_time is None or cascade.end_time is None:
                    continue
                event_id = event_repo.insert_event(
                    event_type="error_cascade",
                    file_id=file_id,
                    lane=metadata.lane,
                    log_date=metadata.log_date,
                    start_time=cascade.start_time,
                    end_time=cascade.end_time,
                    start_line=cascade.start_line,
                    end_line=cascade.end_line,
                    line_count=cascade.error_count,
                )
                event_repo.insert_error_cascade(
                    event_id,
                    error_pattern=cascade.error_pattern,
                    error_count=cascade.error_count,
                    recovery_achieved=cascade.recovery_achieved,
                    recovery_time_ms=cascade.recovery_time_ms,
                )
                cascade_count += 1

            # Health checks
            health_seg = HealthSegmenter()
            health_count = 0
            for hc in health_seg.process_entries(iter(all_entries)):
                if hc.start_time is None:
                    continue
                event_id = event_repo.insert_event(
                    event_type="health_check",
                    file_id=file_id,
                    lane=metadata.lane,
                    log_date=metadata.log_date,
                    start_time=hc.start_time,
                    end_time=hc.end_time or hc.start_time,
                    start_line=hc.start_line,
                    end_line=hc.end_line or hc.start_line,
                    line_count=1,
                )
                event_repo.insert_health_check(
                    event_id,
                    check_type=hc.check_type,
                    target_host=hc.target_host or "",
                    success=hc.success,
                    error_code=hc.error_code or "",
                    http_status=hc.http_status or "",
                    latency_ms=hc.latency_ms,
                )
                health_count += 1

            logger.info(
                "Ingested %s: %d entries, %d txns, %d cascades, %d health checks",
                metadata.file_name,
                total_stored,
                txn_count,
                cascade_count,
                health_count,
            )
            return True

        except Exception as e:
            logger.error("Failed to ingest %s: %s", file_path, e, exc_info=True)
            return False

    def _mark_processed(self, bucket, object_name: str) -> None:
        """Move a processed object to the processed/ prefix."""
        try:
            # Build processed path: insert 'processed/' after store_id segment
            parts = object_name.split("/")
            if len(parts) >= 3:
                # company/store/laneNN/date/file -> company/store/processed/laneNN/date/file
                processed_name = f"{parts[0]}/{parts[1]}/processed/{'/'.join(parts[2:])}"
            else:
                processed_name = f"processed/{object_name}"

            source_blob = bucket.blob(object_name)
            bucket.copy_blob(source_blob, bucket, processed_name)
            source_blob.delete()
            logger.info("Moved to processed: %s", processed_name)
        except Exception as e:
            logger.warning("Failed to move %s to processed: %s", object_name, e)
