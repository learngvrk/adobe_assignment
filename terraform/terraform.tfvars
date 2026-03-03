# -----------------------------------------------------------------
# Fill in these values before running: terraform apply
# -----------------------------------------------------------------

# AWS region to deploy into
aws_region = "us-east-1"

# Prefix for all resource names — must be globally unique for S3 buckets
# Change this to something unique to you (e.g., "yourname-search-attribution")
project_prefix = "ranjith-search-attribution"

# Path to the Lambda zip (built by scripts/package_lambda.sh)
deployment_package_path = "../build/lambda.zip"

# Polars Lambda layer ARN (leave empty if polars is bundled in the zip)
# If the zip exceeds 50MB, you'll need a layer — see README for instructions.
lambda_layer_arns = []
