"""
Lambda handler — entry point for search keyword attribution.

Accepts a single argument: the S3 file to process.

Invocation:
    aws lambda invoke --function-name <name> \\
        --payload '{"file": "s3://bucket/path/to/data.tsv"}' \\
        --cli-binary-format raw-in-base64-out response.json

Flow:
    Payload with S3 file path
    → reads TSV from S3
    → runs SessionAwareAnalyzer (session-aware first-touch attribution)
    → writes tab-delimited output to output bucket

Environment variables:
    OUTPUT_BUCKET : S3 bucket for result files (falls back to input file's bucket)
    OUTPUT_PREFIX : key prefix for output files (default: "output/")
    LOG_LEVEL     : logging level (default: "INFO")
"""

import logging
import os

import boto3
from botocore.exceptions import ClientError

from common.config import load_config
from common.analyzer import SessionAwareAnalyzer

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

s3 = boto3.client("s3")

# Initialized once at cold start, reused across warm invocations
config = load_config()
analyzer = SessionAwareAnalyzer(config)

OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET", "")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "output/")


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    """Parse 's3://bucket/key' into (bucket, key)."""
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got: {uri}")
    path = uri[5:]  # strip 's3://'
    bucket, _, key = path.partition("/")
    if not bucket or not key:
        raise ValueError(f"Invalid S3 URI (need bucket and key): {uri}")
    return bucket, key


def handler(event, context):
    """
    AWS Lambda entry point — accepts a single file argument.

    Args:
        event:   Dict with "file" key containing the S3 URI to process.
                 Example: {"file": "s3://my-bucket/data.tsv"}
        context: Lambda runtime context.

    Returns:
        Dict with statusCode and output file location.
    """
    # Validate: the application accepts a single argument — the file
    file_uri = event.get("file")
    if not file_uri:
        logger.error("Missing 'file' in event payload")
        return {"statusCode": 400, "error": "Missing 'file' — expected {\"file\": \"s3://bucket/key.tsv\"}"}

    try:
        input_bucket, input_key = _parse_s3_uri(file_uri)
    except ValueError as exc:
        logger.error("Invalid file argument: %s", exc)
        return {"statusCode": 400, "error": str(exc)}

    logger.info("Processing s3://%s/%s", input_bucket, input_key)

    # Read input TSV from S3
    try:
        response = s3.get_object(Bucket=input_bucket, Key=input_key)
        tsv_content = response["Body"].read().decode("utf-8")
    except ClientError as exc:
        logger.error("Failed to read s3://%s/%s: %s", input_bucket, input_key, exc)
        raise
    except UnicodeDecodeError as exc:
        logger.error("Encoding error reading s3://%s/%s: %s", input_bucket, input_key, exc)
        raise

    # Run session-aware attribution
    results = analyzer.process(tsv_content)
    logger.info("Attribution complete: %d keyword groups found", len(results))

    # Generate tab-delimited output
    filename, tab_content = analyzer.to_tab_delimited(results)
    output_key = f"{OUTPUT_PREFIX}{filename}"

    # Write output to S3
    output_bucket = OUTPUT_BUCKET or input_bucket
    try:
        s3.put_object(
            Bucket=output_bucket,
            Key=output_key,
            Body=tab_content.encode("utf-8"),
            ContentType="text/tab-separated-values",
        )
    except ClientError as exc:
        logger.error("Failed to write s3://%s/%s: %s", output_bucket, output_key, exc)
        raise

    logger.info("Output written to s3://%s/%s", output_bucket, output_key)

    return {
        "statusCode": 200,
        "output": f"s3://{output_bucket}/{output_key}",
        "keyword_groups": len(results),
    }
