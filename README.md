# Spark Delta on MinIO

This repository demonstrates how to use **PySpark 4.1.0** with **Delta Lake** to read and write Delta tables on an S3-compatible store provided by **MinIO**. Everything is configured in Python—no extra Spark configuration files are required.

## Prerequisites

- Docker and Docker Compose
- Python 3.10+ and `pip`

## Getting started

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Start the MinIO stack:

   ```bash
   docker compose up -d minio minio-setup
   ```

   This launches MinIO at `http://127.0.0.1:9000` and creates the `delta` bucket.

3. Run the integration test, which writes a Delta table to S3 and reads it back:

   ```bash
   pytest -q
   ```

4. Shut down the stack when finished:

   ```bash
   docker compose down -v
   ```

## Project layout

- `docker-compose.yml` – Runs MinIO and provisions the `delta` bucket via an `mc` helper container.
- `spark_delta_demo/session.py` – Builds a Spark session with Delta and S3 support entirely in Python.
- `spark_delta_demo/io.py` – Helpers to write and read Delta data on S3.
- `tests/test_delta_s3.py` – Integration test that proves round-trip Delta I/O against MinIO.

## Notes

- Spark is configured for Delta Lake via Python (`configure_spark_with_delta_pip`), and S3 access is set through Spark Hadoop configs—no separate XML files are used.
- The test uses the `s3a` scheme and path-style access for MinIO compatibility.
