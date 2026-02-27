"""Agent configuration from environment variables or config file."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class AgentConfig:
    """Configuration for the POS upload agent."""

    # Directory to watch for journal log files
    watch_dir: str = ""
    # GCS bucket name
    gcs_bucket: str = ""
    # Company and store identifiers
    company_id: str = ""
    store_id: str = ""
    # Local state DB for tracking uploads
    state_db_path: str = ""
    # GCS credentials path (or use Application Default Credentials)
    gcs_credentials: str = ""
    # Polling interval in seconds (used when watchdog is not available)
    poll_interval: int = 30
    # Seconds of no file size change before considering upload-ready
    settle_seconds: int = 60
    # Agent version for metadata tagging
    agent_version: str = "1.0.0"

    @classmethod
    def from_env(cls) -> AgentConfig:
        """Load configuration from environment variables."""
        home = Path.home()
        default_state = str(home / ".pinpad_agent" / "state.db")

        return cls(
            watch_dir=os.environ.get("PINPAD_WATCH_DIR", ""),
            gcs_bucket=os.environ.get("PINPAD_GCS_BUCKET", ""),
            company_id=os.environ.get("PINPAD_COMPANY_ID", ""),
            store_id=os.environ.get("PINPAD_STORE_ID", ""),
            state_db_path=os.environ.get("PINPAD_STATE_DB", default_state),
            gcs_credentials=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
            poll_interval=int(os.environ.get("PINPAD_POLL_INTERVAL", "30")),
            settle_seconds=int(os.environ.get("PINPAD_SETTLE_SECONDS", "60")),
        )

    def validate(self) -> list[str]:
        """Return list of validation errors, empty if config is valid."""
        errors = []
        if not self.watch_dir:
            errors.append("PINPAD_WATCH_DIR is required")
        elif not Path(self.watch_dir).is_dir():
            errors.append(f"PINPAD_WATCH_DIR does not exist: {self.watch_dir}")
        if not self.gcs_bucket:
            errors.append("PINPAD_GCS_BUCKET is required")
        if not self.company_id:
            errors.append("PINPAD_COMPANY_ID is required")
        if not self.store_id:
            errors.append("PINPAD_STORE_ID is required")
        return errors
