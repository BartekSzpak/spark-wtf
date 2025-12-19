"""IO helpers for writing and reading Delta tables on S3."""

from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def sample_schema() -> StructType:
    return StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
        ]
    )


def make_sample_df(spark: SparkSession, rows: Iterable[tuple[int, str]]) -> DataFrame:
    """Create a small DataFrame with a stable schema for tests."""

    return spark.createDataFrame(rows, schema=sample_schema())


def write_delta(df: DataFrame, path: str) -> None:
    """Write a DataFrame as a Delta table."""

    df.write.format("delta").mode("overwrite").save(path)


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    """Load a Delta table from the given path."""

    return spark.read.format("delta").load(path)


def names_sorted(df: DataFrame) -> list[str]:
    """Return names sorted by id for deterministic assertions."""

    return [row.name for row in df.select(col("name")).orderBy(col("id")).collect()]
