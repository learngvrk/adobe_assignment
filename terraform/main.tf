# Root Terraform configuration — wires S3, Lambda, and EMR modules together.
#
# Usage:
#   cd terraform/
#   terraform init
#   terraform plan -var="project_prefix=my-search-attribution"
#   terraform apply -var="project_prefix=my-search-attribution"

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ------------------------------------------------------------------
# S3 module — input and output buckets
# ------------------------------------------------------------------

module "s3" {
  source = "./modules/s3"

  input_bucket_name  = "${var.project_prefix}-input"
  output_bucket_name = "${var.project_prefix}-output"
}

# ------------------------------------------------------------------
# Lambda module — function, IAM, S3 trigger
# ------------------------------------------------------------------

module "lambda" {
  source = "./modules/lambda"

  function_name           = "${var.project_prefix}-analyzer"
  deployment_package_path = var.deployment_package_path
  lambda_layer_arns       = var.lambda_layer_arns

  input_bucket_name  = module.s3.input_bucket_name
  input_bucket_arn   = module.s3.input_bucket_arn
  output_bucket_name = module.s3.output_bucket_name
  output_bucket_arn  = module.s3.output_bucket_arn
}

# ------------------------------------------------------------------
# EMR Serverless module — Spark application for large files (> 3 GB)
# ------------------------------------------------------------------

module "emr" {
  source = "./modules/emr"

  application_name  = "${var.project_prefix}-spark"
  input_bucket_arn  = module.s3.input_bucket_arn
  output_bucket_arn = module.s3.output_bucket_arn
}
