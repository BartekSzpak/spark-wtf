import os
from uuid import uuid4

from spark_session import build_spark_session
from s3_utils import ensure_bucket


def test_delta_roundtrip_to_s3():
    bucket = os.getenv("S3_BUCKET", "spark-delta-demo")
    ensure_bucket(bucket)

    spark = build_spark_session("delta-s3-test")
    spark.sparkContext.setLogLevel("ERROR")

    try:
        data = [("alpha", 1), ("beta", 2), ("gamma", 3)]
        df = spark.createDataFrame(data, ["word", "value"])

        path = f"s3a://{bucket}/delta/{uuid4()}"
        df.write.format("delta").mode("overwrite").save(path)

        read_df = spark.read.format("delta").load(path)
        result = {row["word"]: row["value"] for row in read_df.collect()}
        assert result == {"alpha": 1, "beta": 2, "gamma": 3}
    finally:
        spark.stop()
