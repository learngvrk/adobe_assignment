"""
Lambda handler — S3-triggered entry point for search keyword attribution.

Flow:
    S3 ObjectCreated event
    → reads TSV from input bucket
    → runs SessionAwareAnalyzer (session-aware first-touch attribution)
    → writes tab-delimited output to output bucket

Environment variables:
    OUTPUT_BUCKET : S3 bucket for result files (required)
    OUTPUT_PREFIX : key prefix for output files (default: "output/")
    LOG_LEVEL     : logging level (default: "INFO")
"""

import logging
import os
import urllib.parse

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


def handler(event, context):
    """
    AWS Lambda entry point — triggered by S3 ObjectCreated.

    Args:
        event:   S3 event payload (contains bucket name and object key).
        context: Lambda runtime context.

    Returns:
        Dict with statusCode and output file location.
    """
    # Validate event structure
    records = event.get("Records")
    if not records:
        logger.error("Invalid event: missing or empty 'Records'")
        return {"statusCode": 400, "error": "Invalid S3 event: no Records"}

    record = records[0]
    try:
        input_bucket = record["s3"]["bucket"]["name"]
        input_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
    except (KeyError, TypeError) as exc:
        logger.error("Malformed S3 event record: %s", exc)
        return {"statusCode": 400, "error": f"Malformed S3 event: {exc}"}

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
