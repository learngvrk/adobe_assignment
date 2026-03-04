# EMR Serverless module — Spark application and IAM role.
#
# Creates an EMR Serverless application (zero idle cost — just a config
# resource) and an execution role with least-privilege S3 + CloudWatch access.
#
# Jobs are submitted manually via: aws emr-serverless start-job-run
# See scripts/submit_emr_job.sh for the full command.

# ------------------------------------------------------------------
# IAM Role — what the Spark job is allowed to do
# ------------------------------------------------------------------

# Trust policy: allows EMR Serverless to assume this role
data "aws_iam_policy_document" "assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr" {
  name               = "${var.application_name}-role"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
}

# Permissions: S3 read (input data + scripts), S3 write (output), CloudWatch logs
data "aws_iam_policy_document" "emr_permissions" {
  # Read input TSV files and Spark scripts from input bucket
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${var.input_bucket_arn}/*"]
  }

  # List input bucket (required for Spark to read directories)
  statement {
    actions   = ["s3:ListBucket"]
    resources = [var.input_bucket_arn]
  }

  # List output bucket (Spark checks if output directory exists before writing)
  statement {
    actions   = ["s3:ListBucket"]
    resources = [var.output_bucket_arn]
  }

  # Write output results
  statement {
    actions   = ["s3:PutObject", "s3:DeleteObject"]
    resources = ["${var.output_bucket_arn}/*"]
  }

  # CloudWatch logs for Spark driver and executor
  statement {
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:aws:logs:*:*:*"]
  }
}

resource "aws_iam_role_policy" "emr" {
  name   = "${var.application_name}-policy"
  role   = aws_iam_role.emr.id
  policy = data.aws_iam_policy_document.emr_permissions.json
}

# ------------------------------------------------------------------
# EMR Serverless Application
# ------------------------------------------------------------------

resource "aws_emrserverless_application" "this" {
  name          = var.application_name
  release_label = "emr-7.0.0"
  type          = "SPARK"

  # No initial_capacity — resources are allocated on-demand per job.
  # Maximum capacity — enough for 1 driver + 1 small executor.
  # You only pay per-second for resources actually used.
  maximum_capacity {
    cpu    = "8 vCPU"
    memory = "32 GB"
  }
}
