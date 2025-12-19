# PySpark Delta Lake S3 Demo

A demonstration project showing how to use PySpark to create and manage Delta tables on S3-compatible storage (MinIO).

## Features

- ✅ PySpark 4.0.0 with Delta Lake support
- ✅ S3-compatible storage using MinIO (via Docker Compose)
- ✅ Complete Spark configuration from Python code (no external config files)
- ✅ Write Delta tables to S3
- ✅ Read Delta tables from S3
- ✅ Automated tests for data integrity verification

## Prerequisites

- Docker and Docker Compose
- Python 3.8 or higher
- Java 11 or higher (for PySpark)

## Project Structure

```
.
├── docker-compose.yml      # MinIO S3-compatible service configuration
├── requirements.txt        # Python dependencies
├── spark_delta_s3.py      # Main PySpark application
├── test_delta_s3.py       # Test script
└── README.md              # This file
```

## Setup

### 1. Start MinIO S3 Service

Start the MinIO service using Docker Compose:

```bash
docker-compose up -d
```

This will start MinIO on:
- API: http://localhost:9000
- Console: http://localhost:9001

MinIO credentials:
- Access Key: `minioadmin`
- Secret Key: `minioadmin`

### 2. Create S3 Bucket

You can create the required bucket using the MinIO console at http://localhost:9001, or using the AWS CLI:

```bash
# Install AWS CLI (if not already installed)
pip install awscli

# Configure AWS CLI for MinIO
aws configure set aws_access_key_id minioadmin
aws configure set aws_secret_access_key minioadmin

# Create bucket
aws --endpoint-url http://localhost:9000 s3 mb s3://spark-demo
```

Alternatively, the bucket will be created automatically when you first run the Spark application.

### 3. Install Python Dependencies

```bash
pip install -r requirements.txt
```

Note: This will download PySpark 4.0.0 and its dependencies, which may take a few minutes.

### 4. Download Required JAR Files

Download the necessary Hadoop and AWS SDK JAR files for S3 connectivity:

```bash
./setup_jars.sh
```

This script will download:
- `hadoop-aws-3.4.0.jar` - Hadoop S3A filesystem implementation
- `aws-java-sdk-bundle-1.12.262.jar` - AWS SDK v1
- `bundle-2.28.21.jar` - AWS SDK v2

The JARs will be placed in the `jars/` directory (~830 MB total).

## Quick Start

After completing the setup steps above, you can run:

```bash
# Start MinIO
docker-compose up -d

# Create bucket (optional - will be created automatically on first use)
python setup_minio.py

# Run the demo
python spark_delta_s3.py

# Or run the test
python test_delta_s3.py
```

## Usage

### Running the Main Demo

The main demonstration script writes sample data to S3 as a Delta table and reads it back:

```bash
python spark_delta_s3.py
```

Expected output:
```
=== PySpark Delta Lake S3 Demo ===

Creating Spark session...
✓ Spark version: 4.0.0

Writing data to s3a://spark-demo/delta-table...
✓ Data written successfully

Reading data from s3a://spark-demo/delta-table...
✓ Data read successfully

Data retrieved from Delta table:
+---+-------+---+--------------+
| id|   name|age|          role|
+---+-------+---+--------------+
|  1|  Alice| 25|      Engineer|
|  2|    Bob| 30|Data Scientist|
|  3|Charlie| 35|       Manager|
|  4|  Diana| 28|       Analyst|
|  5|    Eve| 32|     Developer|
+---+-------+---+--------------+

Record count: 5
...
```

### Running the Test

The test script validates that data can be written to and read from S3 correctly:

```bash
python test_delta_s3.py
```

The test will:
1. Create a Spark session with S3 configuration
2. Write test data to S3 in Delta format
3. Read the data back from S3
4. Verify data integrity (count, schema, and content)

## Configuration Details

All Spark configuration is done programmatically in Python without external configuration files:

```python
builder = SparkSession.builder \
    .appName(app_name) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    ...
```

Key configurations:
- **Delta Lake**: Enabled via SQL extensions and catalog configuration
- **S3 Endpoint**: Points to MinIO at `http://localhost:9000`
- **Credentials**: Uses MinIO default credentials
- **Path Style Access**: Enabled for MinIO compatibility
- **SSL**: Disabled for local development

## Troubleshooting

### Issue: Connection refused to MinIO

**Solution**: Ensure MinIO is running:
```bash
docker-compose ps
```

If not running, start it:
```bash
docker-compose up -d
```

### Issue: Java heap space error

**Solution**: Increase Spark driver memory:
```python
.config("spark.driver.memory", "4g")
```

### Issue: Package download failures

**Solution**: Ensure you have internet connectivity for Maven package downloads on first run.

## Clean Up

To stop and remove the MinIO service:

```bash
docker-compose down
```

To also remove the data volume:

```bash
docker-compose down -v
```

## Dependencies

- **pyspark**: 4.0.0 - Apache Spark Python API
- **delta-spark**: 3.2.1 - Delta Lake library for PySpark
- **boto3**: 1.35.76 - AWS SDK for Python (for S3 compatibility)

## License

This is a demonstration project for educational purposes.
