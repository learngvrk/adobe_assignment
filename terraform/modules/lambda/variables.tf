variable "function_name" {
  description = "Name of the Lambda function"
  type        = string
}

variable "deployment_package_path" {
  description = "Local path to the Lambda deployment zip file"
  type        = string
}

variable "lambda_layer_arns" {
  description = "List of Lambda layer ARNs (e.g., polars layer)"
  type        = list(string)
  default     = []
}

variable "input_bucket_arn" {
  description = "ARN of the S3 input bucket"
  type        = string
}

variable "output_bucket_name" {
  description = "Name of the S3 output bucket"
  type        = string
}

variable "output_bucket_arn" {
  description = "ARN of the S3 output bucket"
  type        = string
}

variable "output_prefix" {
  description = "Key prefix for output files in the output bucket"
  type        = string
  default     = "output/"
}

variable "timeout" {
  description = "Lambda timeout in seconds"
  type        = number
  default     = 300
}

variable "memory_size" {
  description = "Lambda memory in MB"
  type        = number
  default     = 512
}
