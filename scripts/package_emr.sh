#!/bin/bash
# Package Spark job + common module and upload to S3.
#
# Uploads spark_job.py and common.zip to the input bucket under
# the scripts/ prefix, where EMR Serverless can read them.
#
# Usage:
#   chmod +x scripts/package_emr.sh
#   ./scripts/package_emr.sh <s3-bucket-name>
#
# Example:
#   ./scripts/package_emr.sh ranjith-search-attribution-input
#
# After uploading, submit the job with:
#   ./scripts/submit_emr_job.sh <app-id> <role-arn> <input-bucket> <output-bucket>

set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 <s3-bucket-name>"
    echo "Example: $0 ranjith-search-attribution-input"
    exit 1
fi

BUCKET="$1"
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="$PROJECT_ROOT/build/emr"

echo "=== Cleaning previous EMR build ==="
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

echo "=== Zipping common/ package ==="
cd "$PROJECT_ROOT/src"
zip -r "$BUILD_DIR/common.zip" common/ -q

echo "=== Copying spark_job.py ==="
cp "$PROJECT_ROOT/src/emr/spark_job.py" "$BUILD_DIR/"

echo "=== Uploading to S3 ==="
aws s3 cp "$BUILD_DIR/spark_job.py" "s3://$BUCKET/scripts/spark_job.py"
aws s3 cp "$BUILD_DIR/common.zip" "s3://$BUCKET/scripts/common.zip"

echo "=== Done ==="
echo "Uploaded to s3://$BUCKET/scripts/"
echo "  - spark_job.py"
echo "  - common.zip"
