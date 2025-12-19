"""
PySpark Delta Lake S3 Demo
Demonstrates writing and reading Delta tables to/from S3-compatible storage (MinIO)
"""

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def create_spark_session(app_name="DeltaLakeS3Demo"):
    """
    Create and configure a Spark session with Delta Lake and S3 support.
    All configuration is done in Python without external configuration files.
    """
    import os
    
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    jars_dir = os.path.join(script_dir, "jars")
    
    # Build jar paths
    hadoop_aws_jar = os.path.join(jars_dir, "hadoop-aws-3.4.0.jar")
    aws_sdk_v1_jar = os.path.join(jars_dir, "aws-java-sdk-bundle-1.12.262.jar")
    aws_sdk_v2_jar = os.path.join(jars_dir, "bundle-2.28.21.jar")
    jars_list = f"{hadoop_aws_jar},{aws_sdk_v1_jar},{aws_sdk_v2_jar}"
    
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.jars", jars_list)
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def write_delta_table(spark, data, s3_path):
    """
    Write data to S3 as a Delta table.
    
    Args:
        spark: SparkSession instance
        data: List of tuples or dictionary to write
        s3_path: S3 path where to write the Delta table
    """
    df = spark.createDataFrame(data)
    df.write.format("delta").mode("overwrite").save(s3_path)
    print(f"✓ Data written to {s3_path}")
    return df


def read_delta_table(spark, s3_path):
    """
    Read Delta table from S3.
    
    Args:
        spark: SparkSession instance
        s3_path: S3 path to read the Delta table from
        
    Returns:
        DataFrame containing the Delta table data
    """
    df = spark.read.format("delta").load(s3_path)
    print(f"✓ Data read from {s3_path}")
    return df


def main():
    """Main demonstration function"""
    print("=== PySpark Delta Lake S3 Demo ===\n")
    
    # Create Spark session with all configuration in Python
    print("Creating Spark session...")
    spark = create_spark_session()
    print(f"✓ Spark version: {spark.version}\n")
    
    # Sample data to write
    data = [
        (1, "Alice", 25, "Engineer"),
        (2, "Bob", 30, "Data Scientist"),
        (3, "Charlie", 35, "Manager"),
        (4, "Diana", 28, "Analyst"),
        (5, "Eve", 32, "Developer")
    ]
    
    columns = ["id", "name", "age", "role"]
    df_with_columns = spark.createDataFrame(data, columns)
    
    # S3 path (using MinIO)
    s3_path = "s3a://spark-demo/delta-table"
    
    # Write to Delta table
    print(f"Writing data to {s3_path}...")
    df_with_columns.write.format("delta").mode("overwrite").save(s3_path)
    print("✓ Data written successfully\n")
    
    # Read back from Delta table
    print(f"Reading data from {s3_path}...")
    df_read = spark.read.format("delta").load(s3_path)
    print("✓ Data read successfully\n")
    
    # Show the data
    print("Data retrieved from Delta table:")
    df_read.show()
    
    # Verify data integrity
    print(f"Record count: {df_read.count()}")
    print(f"Schema: {df_read.schema}\n")
    
    spark.stop()
    print("=== Demo completed successfully ===")


if __name__ == "__main__":
    main()
