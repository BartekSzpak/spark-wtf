# Spark + Delta + S3 (MinIO) demo

This project demonstrates how to use PySpark 4.x to write a Delta table to an S3-compatible store and read it back.

## Prerequisites
- Docker / Docker Compose
- Python 3.10+ (recommended)

## Quick start
1) Start MinIO:

```bash
docker compose up -d
```

If ports `9000/9001` are already in use, you can override them:

```bash
MINIO_PORT=19000 MINIO_CONSOLE_PORT=19001 docker compose up -d
```

2) Create a virtualenv and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3) Run the test:

```bash
pytest -q
```

## Configuration
The defaults work with the provided `docker-compose.yml`, but you can override via env vars:

- `S3_ENDPOINT` (default: `http://localhost:9000`)
- `S3_ACCESS_KEY` (default: `minioadmin`)
- `S3_SECRET_KEY` (default: `minioadmin`)
- `S3_REGION` (default: `us-east-1`)
- `S3_BUCKET` (default: `spark-delta-demo`)
- `S3_SECURE` (default: auto based on endpoint scheme)
- `S3_PATH_STYLE` (default: `true`)
- `DELTA_SPARK_PACKAGE` (default: `io.delta:delta-spark_2.13:4.0.0`, used if `delta-spark` is not installed)
- `SPARK_EXTRA_PACKAGES` (comma-separated Maven coordinates to override default AWS/Hadoop packages)
- `MINIO_PORT` / `MINIO_CONSOLE_PORT` (docker compose overrides, update `S3_ENDPOINT` accordingly)

## Notes
- Spark is configured entirely from Python in `spark_session.py`.
- The test writes a Delta table to MinIO via `s3a://...` and reads it back.
