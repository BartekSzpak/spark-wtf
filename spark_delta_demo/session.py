"""Spark session factory configured for Delta Lake on an S3-compatible endpoint."""

from __future__ import annotations

import os
from dataclasses import dataclass

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


DEFAULT_ENDPOINT = "http://127.0.0.1:9000"
DEFAULT_ACCESS_KEY = "minio"
DEFAULT_SECRET_KEY = "minio123"
DEFAULT_BUCKET = "delta"


@dataclass
class MinioConfig:
    """Connection details for the MinIO gateway."""

    endpoint: str = DEFAULT_ENDPOINT
    access_key: str = DEFAULT_ACCESS_KEY
    secret_key: str = DEFAULT_SECRET_KEY
    bucket: str = DEFAULT_BUCKET

    @classmethod
    def from_env(cls) -> "MinioConfig":
        return cls(
            endpoint=os.getenv("MINIO_ENDPOINT", DEFAULT_ENDPOINT),
            access_key=os.getenv("MINIO_ACCESS_KEY", DEFAULT_ACCESS_KEY),
            secret_key=os.getenv("MINIO_SECRET_KEY", DEFAULT_SECRET_KEY),
            bucket=os.getenv("MINIO_BUCKET", DEFAULT_BUCKET),
        )


def build_spark(minio: MinioConfig | None = None) -> SparkSession:
    """Create a SparkSession configured for Delta Lake and S3A access to MinIO.

    All configuration is expressed in Python—no additional XML or properties
    files are required.
    """

    minio = minio or MinioConfig.from_env()
    packages = [
        # Delta Lake runtime for Spark 4.x on Scala 2.13
        "io.delta:delta-spark_2.13:3.2.0",
        # Hadoop connector for S3A
        "org.apache.hadoop:hadoop-aws:3.4.0",
    ]

    builder = (
        SparkSession.builder.appName("DeltaLakeS3Demo")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", minio.endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
