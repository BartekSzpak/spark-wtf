import os

from pyspark.sql import SparkSession


DEFAULT_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
DEFAULT_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
DEFAULT_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
DEFAULT_REGION = os.getenv("S3_REGION", "us-east-1")
DEFAULT_DELTA_PACKAGE = os.getenv("DELTA_SPARK_PACKAGE", "io.delta:delta-spark_2.13:4.0.0")


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _packages_env(name: str, default: list[str]) -> list[str]:
    value = os.getenv(name)
    if not value:
        return default
    return [item.strip() for item in value.split(",") if item.strip()]


def _with_delta(builder: SparkSession.Builder, extra_packages: list[str]) -> SparkSession.Builder:
    try:
        from delta import configure_spark_with_delta_pip
    except ModuleNotFoundError:
        packages = [DEFAULT_DELTA_PACKAGE, *extra_packages]
        return builder.config("spark.jars.packages", ",".join(packages))

    return configure_spark_with_delta_pip(builder, extra_packages=extra_packages)


def build_spark_session(app_name: str = "delta-minio") -> SparkSession:
    endpoint = DEFAULT_ENDPOINT
    secure = _bool_env("S3_SECURE", endpoint.startswith("https://"))
    path_style = _bool_env("S3_PATH_STYLE", True)

    extra_packages = _packages_env(
        "SPARK_EXTRA_PACKAGES",
        [
            "org.apache.hadoop:hadoop-aws:3.4.0",
            "com.amazonaws:aws-java-sdk-bundle:1.12.698",
        ],
    )

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", DEFAULT_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", DEFAULT_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", str(path_style).lower())
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(secure).lower())
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.endpoint.region", DEFAULT_REGION)
    )

    return _with_delta(builder, extra_packages).getOrCreate()
