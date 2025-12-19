"""
Helper script to set up MinIO bucket for the demo
Creates the required S3 bucket if it doesn't exist
"""

import boto3
from botocore.client import Config
import sys


def setup_minio_bucket(bucket_name="spark-demo"):
    """
    Create MinIO bucket if it doesn't exist
    
    Args:
        bucket_name: Name of the bucket to create
    """
    print(f"Setting up MinIO bucket: {bucket_name}")
    
    try:
        # Configure S3 client for MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        
        # Check if bucket exists
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"✓ Bucket '{bucket_name}' already exists")
            return True
        except:
            # Bucket doesn't exist, create it
            print(f"Creating bucket '{bucket_name}'...")
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"✓ Bucket '{bucket_name}' created successfully")
            return True
            
    except Exception as e:
        print(f"✗ Error setting up MinIO bucket: {e}")
        print("\nMake sure MinIO is running:")
        print("  docker-compose up -d")
        return False


if __name__ == "__main__":
    success = setup_minio_bucket()
    sys.exit(0 if success else 1)
