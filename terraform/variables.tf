variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "project_prefix" {
  description = "Prefix for all resource names (e.g., 'search-keyword-attribution')"
  type        = string
}

variable "deployment_package_path" {
  description = "Local path to the Lambda deployment zip file"
  type        = string
  default     = "../build/lambda.zip"
}

variable "lambda_layer_arns" {
  description = "List of Lambda layer ARNs (e.g., polars layer)"
  type        = list(string)
  default     = []
}
