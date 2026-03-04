#!/bin/bash
# Submit a Spark job to EMR Serverless.
#
# Runs spark_job.py (previously uploaded by package_emr.sh) against
# an input TSV file in S3 and writes results to the output bucket.
#
# Usage:
#   chmod +x scripts/submit_emr_job.sh
#   ./scripts/submit_emr_job.sh <app-id> <role-arn> <input-bucket> <output-bucket> [input-key]
#
# Example:
#   ./scripts/submit_emr_job.sh \
#     00abcdef12345678 \
#     arn:aws:iam::123456789012:role/ranjith-search-attribution-spark-role \
#     ranjith-search-attribution-input \
#     ranjith-search-attribution-output \
#     data.tsv
#
# Get <app-id> and <role-arn> from: terraform output

set -euo pipefail

if [ $# -lt 4 ]; then
    echo "Usage: $0 <app-id> <role-arn> <input-bucket> <output-bucket> [input-key]"
    echo ""
    echo "Get app-id and role-arn from: cd terraform && terraform output"
    exit 1
fi

APP_ID="$1"
ROLE_ARN="$2"
INPUT_BUCKET="$3"
OUTPUT_BUCKET="$4"
INPUT_KEY="${5:-data.tsv}"

echo "Submitting Spark job to EMR Serverless..."
echo "  Application:  $APP_ID"
echo "  Input:        s3://$INPUT_BUCKET/$INPUT_KEY"
echo "  Output:       s3://$OUTPUT_BUCKET/emr-output/"

JOB_RUN=$(aws emr-serverless start-job-run \
  --application-id "$APP_ID" \
  --execution-role-arn "$ROLE_ARN" \
  --job-driver "{
    \"sparkSubmit\": {
      \"entryPoint\": \"s3://$INPUT_BUCKET/scripts/spark_job.py\",
      \"entryPointArguments\": [
        \"s3://$INPUT_BUCKET/$INPUT_KEY\",
        \"s3://$OUTPUT_BUCKET/emr-output/\"
      ],
      \"sparkSubmitParameters\": \"--py-files s3://$INPUT_BUCKET/scripts/common.zip --conf spark.executor.cores=1 --conf spark.executor.memory=2g --conf spark.executor.instances=1 --conf spark.dynamicAllocation.enabled=false\"
    }
  }" \
  --output text --query 'jobRunId')

echo ""
echo "Job submitted: $JOB_RUN"
echo ""
echo "Check status:"
echo "  aws emr-serverless get-job-run --application-id $APP_ID --job-run-id $JOB_RUN"
echo ""
echo "View output:"
echo "  aws s3 ls s3://$OUTPUT_BUCKET/emr-output/"
