"""DuckDB table definitions."""

SCHEMA_DDL = """
-- Core log storage
CREATE TABLE IF NOT EXISTS log_files (
    file_id       TEXT PRIMARY KEY,
    file_path     TEXT NOT NULL,
    file_name     TEXT NOT NULL,
    lane          INTEGER NOT NULL,
    log_date      DATE NOT NULL,
    store_id      TEXT,
    line_count    INTEGER,
    byte_size     INTEGER,
    ingested_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    parse_duration_ms REAL,
    company_id    TEXT,
    mtx_pos_version TEXT,
    mtx_eps_version TEXT,
    seccode_version TEXT,
    pos_version   TEXT,
    pinpad_model  TEXT,
    pinpad_serial TEXT,
    pinpad_firmware TEXT,
    config_json   TEXT,
    upload_source TEXT DEFAULT 'local',
    sha256_hash   TEXT
);

CREATE TABLE IF NOT EXISTS log_entries (
    entry_id      INTEGER PRIMARY KEY,
    file_id       TEXT NOT NULL REFERENCES log_files(file_id),
    line_number   INTEGER NOT NULL,
    timestamp     TIMESTAMP NOT NULL,
    category      TEXT NOT NULL,
    message       TEXT NOT NULL,
    is_expanded   BOOLEAN DEFAULT FALSE,
    expansion_count INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS message_templates (
    template_id   INTEGER PRIMARY KEY,
    template_text TEXT NOT NULL UNIQUE,
    category      TEXT NOT NULL,
    occurrence_count INTEGER DEFAULT 0,
    first_seen    TIMESTAMP,
    last_seen     TIMESTAMP
);

-- Segmented events
CREATE TABLE IF NOT EXISTS events (
    event_id      TEXT PRIMARY KEY,
    event_type    TEXT NOT NULL,
    file_id       TEXT REFERENCES log_files(file_id),
    lane          INTEGER NOT NULL,
    log_date      DATE NOT NULL,
    start_time    TIMESTAMP NOT NULL,
    end_time      TIMESTAMP NOT NULL,
    start_line    INTEGER NOT NULL,
    end_line      INTEGER NOT NULL,
    line_count    INTEGER NOT NULL,
    duration_ms   REAL NOT NULL,
    parent_event_id TEXT REFERENCES events(event_id)
);

-- Transaction-specific fields
CREATE TABLE IF NOT EXISTS transactions (
    event_id          TEXT PRIMARY KEY REFERENCES events(event_id),
    sequence_number   TEXT,
    card_type         TEXT,
    entry_method      TEXT,
    pan_last4         TEXT,
    aid               TEXT,
    app_label         TEXT,
    tac_sequence      TEXT,
    cvm_result        TEXT,
    response_code     TEXT,
    host_response_code TEXT,
    authorization_number TEXT,
    amount_cents      INTEGER,
    cashback_cents    INTEGER,
    host_url          TEXT,
    host_latency_ms   REAL,
    tvr               TEXT,
    is_approved       BOOLEAN,
    is_quickchip      BOOLEAN,
    is_fallback       BOOLEAN,
    serial_error_count INTEGER
);

-- Health check specific fields
CREATE TABLE IF NOT EXISTS health_checks (
    event_id      TEXT PRIMARY KEY REFERENCES events(event_id),
    check_type    TEXT NOT NULL,
    target_host   TEXT,
    success       BOOLEAN,
    error_code    TEXT,
    http_status   TEXT,
    latency_ms    REAL
);

-- Error cascade details
CREATE TABLE IF NOT EXISTS error_cascades (
    event_id          TEXT PRIMARY KEY REFERENCES events(event_id),
    error_pattern     TEXT NOT NULL,
    error_count       INTEGER,
    recovery_achieved BOOLEAN,
    recovery_time_ms  REAL
);

-- SCAT alive status timeline
CREATE TABLE IF NOT EXISTS scat_timeline (
    file_id       TEXT REFERENCES log_files(file_id),
    timestamp     TIMESTAMP NOT NULL,
    alive_status  INTEGER NOT NULL,
    PRIMARY KEY (file_id, timestamp)
);

-- ML results
CREATE TABLE IF NOT EXISTS predictions (
    prediction_id     INTEGER PRIMARY KEY,
    event_id          TEXT REFERENCES events(event_id),
    model_id          TEXT NOT NULL,
    model_version     TEXT NOT NULL,
    prediction_type   TEXT NOT NULL,
    label             TEXT,
    confidence        REAL,
    anomaly_score     REAL,
    details           TEXT,
    predicted_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Model registry
CREATE TABLE IF NOT EXISTS models (
    model_id      TEXT PRIMARY KEY,
    model_type    TEXT NOT NULL,
    version       TEXT NOT NULL,
    file_path     TEXT NOT NULL,
    training_date TIMESTAMP,
    training_samples INTEGER,
    metrics       TEXT,
    is_active     BOOLEAN DEFAULT FALSE
);

-- Technician cases
CREATE TABLE IF NOT EXISTS cases (
    case_id TEXT PRIMARY KEY,
    company_id TEXT,
    store_id TEXT,
    lane_number INTEGER,
    incident_time TIMESTAMP,
    symptom_description TEXT NOT NULL,
    root_cause TEXT,
    root_cause_confidence REAL,
    evidence_summary TEXT,
    evidence_log_lines TEXT,
    resolution_steps TEXT,
    resolution_status TEXT DEFAULT 'open',
    tech_verified BOOLEAN DEFAULT FALSE,
    ml_labels TEXT,
    tags TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS case_patterns (
    pattern_id TEXT PRIMARY KEY,
    case_id TEXT REFERENCES cases(case_id),
    pattern_type TEXT,
    pattern_text TEXT,
    frequency INTEGER DEFAULT 1,
    stores TEXT,
    confidence REAL
);
"""

# Columns added in cloud-scale update. Safe to re-run (ALTER TABLE ADD IF NOT EXISTS not
# supported by DuckDB, so we catch errors per column).
MIGRATION_COLUMNS = [
    ("log_files", "company_id", "TEXT"),
    ("log_files", "mtx_pos_version", "TEXT"),
    ("log_files", "mtx_eps_version", "TEXT"),
    ("log_files", "seccode_version", "TEXT"),
    ("log_files", "pos_version", "TEXT"),
    ("log_files", "pinpad_model", "TEXT"),
    ("log_files", "pinpad_serial", "TEXT"),
    ("log_files", "pinpad_firmware", "TEXT"),
    ("log_files", "config_json", "TEXT"),
    ("log_files", "upload_source", "TEXT DEFAULT 'local'"),
    ("log_files", "sha256_hash", "TEXT"),
]
