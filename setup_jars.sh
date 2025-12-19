#!/bin/bash
# Setup script to download required Hadoop and AWS SDK JAR files

set -e

JARS_DIR="jars"
MAVEN_REPO="https://repo1.maven.org/maven2"

# Create jars directory if it doesn't exist
mkdir -p "$JARS_DIR"

echo "Downloading required JAR files..."
echo "=================================="

# Hadoop AWS
echo "1. Downloading hadoop-aws-3.4.0.jar..."
curl -L -o "$JARS_DIR/hadoop-aws-3.4.0.jar" \
    "$MAVEN_REPO/org/apache/hadoop/hadoop-aws/3.4.0/hadoop-aws-3.4.0.jar"
echo "✓ Downloaded hadoop-aws-3.4.0.jar"

# AWS SDK v1 Bundle
echo "2. Downloading aws-java-sdk-bundle-1.12.262.jar..."
curl -L -o "$JARS_DIR/aws-java-sdk-bundle-1.12.262.jar" \
    "$MAVEN_REPO/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
echo "✓ Downloaded aws-java-sdk-bundle-1.12.262.jar"

# AWS SDK v2 Bundle
echo "3. Downloading bundle-2.28.21.jar (AWS SDK v2)..."
curl -L -o "$JARS_DIR/bundle-2.28.21.jar" \
    "$MAVEN_REPO/software/amazon/awssdk/bundle/2.28.21/bundle-2.28.21.jar"
echo "✓ Downloaded bundle-2.28.21.jar"

echo ""
echo "=================================="
echo "✓ All JAR files downloaded successfully!"
echo ""
echo "JAR files location: $JARS_DIR/"
ls -lh "$JARS_DIR/"
