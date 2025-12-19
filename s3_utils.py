import os

import boto3
from botocore.exceptions import ClientError


def _s3_config() -> dict[str, str]:
    return {
        "endpoint": os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        "access_key": os.getenv("S3_ACCESS_KEY", "minioadmin"),
        "secret_key": os.getenv("S3_SECRET_KEY", "minioadmin"),
        "region": os.getenv("S3_REGION", "us-east-1"),
    }


def s3_client():
    config = _s3_config()
    return boto3.client(
        "s3",
        endpoint_url=config["endpoint"],
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
        region_name=config["region"],
    )


def ensure_bucket(bucket: str) -> None:
    client = s3_client()
    try:
        client.head_bucket(Bucket=bucket)
        return
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code not in {"404", "NoSuchBucket", "NotFound"}:
            raise

    try:
        client.create_bucket(Bucket=bucket)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
            raise
