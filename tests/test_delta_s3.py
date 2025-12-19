import subprocess
import time
from pathlib import Path

import boto3
import pytest
from botocore.exceptions import ClientError

from spark_delta_demo.io import make_sample_df, names_sorted, read_delta, write_delta
from spark_delta_demo.session import MinioConfig, build_spark


COMPOSE_FILE = Path(__file__).resolve().parents[1] / "docker-compose.yml"
COMPOSE_BASE_CMD = ["docker", "compose", "-f", str(COMPOSE_FILE)]


def _wait_for_bucket(config: MinioConfig, timeout: int = 90) -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=config.endpoint,
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        region_name="us-east-1",
    )
    deadline = time.time() + timeout
    last_error: ClientError | None = None
    while time.time() < deadline:
        try:
            s3.head_bucket(Bucket=config.bucket)
            return
        except ClientError as exc:
            last_error = exc
            time.sleep(2)
    raise RuntimeError(f"Bucket {config.bucket} was not ready: {last_error}")


def _clear_bucket(config: MinioConfig) -> None:
    s3 = boto3.resource(
        "s3",
        endpoint_url=config.endpoint,
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        region_name="us-east-1",
    )
    bucket = s3.Bucket(config.bucket)
    bucket.objects.all().delete()


@pytest.fixture(scope="session")
def minio_stack() -> MinioConfig:
    subprocess.run(COMPOSE_BASE_CMD + ["down", "-v"], check=False)
    subprocess.run(COMPOSE_BASE_CMD + ["up", "-d", "minio", "minio-setup"], check=True)
    config = MinioConfig()
    _wait_for_bucket(config)
    yield config
    subprocess.run(COMPOSE_BASE_CMD + ["down", "-v"], check=False)


def test_delta_round_trip(minio_stack: MinioConfig) -> None:
    config = minio_stack
    _clear_bucket(config)

    spark = build_spark(config)
    table_path = f"s3a://{config.bucket}/people"

    try:
        df = make_sample_df(spark, [(1, "Alice"), (2, "Bob"), (3, "Charlie")])
        write_delta(df, table_path)

        loaded = read_delta(spark, table_path)
        assert loaded.count() == 3
        assert names_sorted(loaded) == ["Alice", "Bob", "Charlie"]
    finally:
        spark.stop()
