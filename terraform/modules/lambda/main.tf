# Lambda module — function, IAM role, and S3 trigger.
#
# IAM follows least-privilege:
#   - Read from input bucket only
#   - Write to output bucket only
#   - Push logs to CloudWatch

# ------------------------------------------------------------------
# IAM Role — what the Lambda is allowed to do
# ------------------------------------------------------------------

# Trust policy: allows Lambda service to assume this role
data "aws_iam_policy_document" "assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${var.function_name}-role"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
}

# Permissions policy: S3 read/write + CloudWatch logs
data "aws_iam_policy_document" "lambda_permissions" {
  # Read from input bucket
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${var.input_bucket_arn}/*"]
  }

  # Write to output bucket
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${var.output_bucket_arn}/*"]
  }

  # CloudWatch logs
  statement {
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:aws:logs:*:*:*"]
  }
}

resource "aws_iam_role_policy" "lambda" {
  name   = "${var.function_name}-policy"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda_permissions.json
}

# ------------------------------------------------------------------
# Lambda Function
# ------------------------------------------------------------------

resource "aws_lambda_function" "this" {
  function_name = var.function_name
  role          = aws_iam_role.lambda.arn
  handler       = "lambda.handler.handler"
  runtime       = "python3.12"
  timeout       = var.timeout
  memory_size   = var.memory_size

  # Deployment package — zip built by the packaging script
  filename         = var.deployment_package_path
  source_code_hash = filebase64sha256(var.deployment_package_path)

  # Polars as a Lambda layer (built separately)
  layers = var.lambda_layer_arns

  environment {
    variables = {
      OUTPUT_BUCKET = var.output_bucket_name
      OUTPUT_PREFIX = var.output_prefix
    }
  }
}

# ------------------------------------------------------------------
# Invocation — Lambda is invoked directly via AWS CLI or SDK:
#   aws lambda invoke --function-name <name> \
#       --payload '{"file": "s3://bucket/data.tsv"}' response.json
# ------------------------------------------------------------------
