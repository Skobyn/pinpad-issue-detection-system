"""FastAPI app for Pub/Sub push-based GCS event processing.

Receives OBJECT_FINALIZE notifications from GCS via Pub/Sub push subscription
and runs the existing CloudProcessor ingestion pipeline.
"""

from __future__ import annotations

import base64
import json
import logging
import os

from fastapi import FastAPI, Request, Response

from pinpad_analyzer.cloud.processor import CloudProcessor
from pinpad_analyzer.storage.database import Database, resolve_db_path

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

app = FastAPI(title="Pinpad Log Processor", version="0.1.0")

_processor: CloudProcessor | None = None


def _get_processor() -> CloudProcessor:
    global _processor
    if _processor is None:
        db_path = resolve_db_path()
        db = Database(db_path)
        db.initialize()
        _processor = CloudProcessor(db)
    return _processor


@app.get("/health")
async def health():
    """Health check endpoint for Cloud Run."""
    return {"status": "healthy"}


@app.post("/process")
async def process_pubsub(request: Request):
    """Receive a Pub/Sub push message triggered by GCS OBJECT_FINALIZE.

    Expected envelope format:
    {
        "message": {
            "data": "<base64-encoded JSON>",
            "messageId": "...",
            "publishTime": "..."
        },
        "subscription": "projects/.../subscriptions/..."
    }

    The decoded data contains GCS event attributes:
    {"bucket": "...", "name": "path/to/file.txt", ...}

    Returns 200 on success or skip (Pub/Sub acks).
    Returns 500 on failure (Pub/Sub retries with backoff).
    """
    envelope = await request.json()
    message = envelope.get("message", {})

    if not message.get("data"):
        logger.warning("Received message with no data")
        return {"status": "skipped", "reason": "no data"}

    try:
        data = json.loads(base64.b64decode(message["data"]))
    except (json.JSONDecodeError, KeyError, ValueError) as exc:
        logger.error("Failed to decode message data: %s", exc)
        return {"status": "skipped", "reason": "invalid data"}

    bucket = data.get("bucket", "")
    name = data.get("name", "")

    if not bucket or not name:
        logger.warning("Missing bucket or name in event data")
        return {"status": "skipped", "reason": "missing fields"}

    # Skip non-journal text files
    if not name.endswith(".txt"):
        logger.info("Skipping non-txt file: %s", name)
        return {"status": "skipped", "reason": "not txt"}

    # Skip already-processed files
    if "/processed/" in name:
        logger.info("Skipping already-processed file: %s", name)
        return {"status": "skipped", "reason": "already processed"}

    logger.info("Processing gs://%s/%s", bucket, name)
    processor = _get_processor()

    try:
        success = processor.process_gcs_object(bucket, name)
    except Exception:
        logger.exception("Failed to process gs://%s/%s", bucket, name)
        return Response(status_code=500, content="Processing failed")

    if success:
        logger.info("Successfully processed gs://%s/%s", bucket, name)
        return {"status": "processed"}
    else:
        logger.info("Skipped gs://%s/%s (duplicate or no date)", bucket, name)
        return {"status": "skipped", "reason": "duplicate or invalid"}
