"""
Test script for PySpark Delta Lake S3 functionality
Tests writing and reading Delta tables to/from S3-compatible storage
"""

import sys
from spark_delta_s3 import create_spark_session, write_delta_table, read_delta_table


def test_write_and_read_delta_table():
    """
    Test that demonstrates:
    1. Writing data to S3 using Delta format
    2. Reading the data back from S3
    3. Verifying data integrity
    """
    print("=" * 60)
    print("TEST: Write and Read Delta Table from S3")
    print("=" * 60)
    
    # Create Spark session
    print("\n[1/4] Creating Spark session...")
    spark = create_spark_session(app_name="DeltaS3Test")
    print(f"✓ Spark session created (version: {spark.version})")
    
    # Prepare test data
    print("\n[2/4] Preparing test data...")
    test_data = [
        (101, "Test User 1", "test1@example.com"),
        (102, "Test User 2", "test2@example.com"),
        (103, "Test User 3", "test3@example.com"),
    ]
    columns = ["user_id", "username", "email"]
    df_original = spark.createDataFrame(test_data, columns)
    
    print(f"✓ Created test dataset with {df_original.count()} records")
    print("\nOriginal data:")
    df_original.show()
    
    # Write to S3 as Delta table
    s3_test_path = "s3a://spark-demo/test-delta-table"
    print(f"\n[3/4] Writing data to S3 (Delta format): {s3_test_path}")
    df_original.write.format("delta").mode("overwrite").save(s3_test_path)
    print("✓ Data written successfully")
    
    # Read back from S3
    print(f"\n[4/4] Reading data back from S3: {s3_test_path}")
    df_read = spark.read.format("delta").load(s3_test_path)
    print("✓ Data read successfully")
    
    print("\nData read from S3:")
    df_read.show()
    
    # Verify data integrity
    print("\n" + "=" * 60)
    print("DATA VERIFICATION")
    print("=" * 60)
    
    original_count = df_original.count()
    read_count = df_read.count()
    
    print(f"Original record count: {original_count}")
    print(f"Read record count:     {read_count}")
    
    if original_count == read_count:
        print("✓ Record counts match")
    else:
        print(f"✗ Record count mismatch!")
        spark.stop()
        return False
    
    # Verify schema
    if df_original.schema == df_read.schema:
        print("✓ Schemas match")
    else:
        print("✗ Schema mismatch!")
        spark.stop()
        return False
    
    # Verify actual data by collecting and comparing
    original_data = sorted(df_original.collect())
    read_data = sorted(df_read.collect())
    
    if original_data == read_data:
        print("✓ Data content matches")
    else:
        print("✗ Data content mismatch!")
        spark.stop()
        return False
    
    # Clean up
    spark.stop()
    
    print("\n" + "=" * 60)
    print("TEST PASSED: All verifications successful! ✓")
    print("=" * 60)
    return True


if __name__ == "__main__":
    try:
        success = test_write_and_read_delta_table()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n✗ TEST FAILED with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
