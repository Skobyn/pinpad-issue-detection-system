# Multi-stage build for Cloud Run pinpad log processor
FROM python:3.11-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml .
COPY src/ src/

RUN pip install --no-cache-dir ".[cloud-run]"

# Pre-cache MotherDuck extension (no token needed at build time)
RUN python -c "import duckdb; duckdb.connect(':memory:').execute('INSTALL motherduck')"

FROM python:3.11-slim

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin/uvicorn /usr/local/bin/uvicorn
COPY --from=builder /root/.duckdb /root/.duckdb
COPY src/ src/

ENV PORT=8080
ENV PYTHONUNBUFFERED=1

CMD uvicorn pinpad_analyzer.cloud.pubsub_handler:app --host 0.0.0.0 --port ${PORT}
