Create a project demonstrating how to use pyspark to create a delta table on s3 storage.
You must use pyspark==4.0.0 or 4.1.0 and create a service providing s3 compatible api using docker compose. Use minio, rustfs or any other service for that.
Project must provide a test illustrating that the data can be written to s3 with spark using delta format and read back.
If possible configure spark from python file, without creating extra configuration files.