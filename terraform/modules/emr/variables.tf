variable "application_name" {
  description = "Name for the EMR Serverless application"
  type        = string
}

variable "input_bucket_arn" {
  description = "ARN of the S3 input bucket (also hosts Spark scripts under scripts/ prefix)"
  type        = string
}

variable "output_bucket_arn" {
  description = "ARN of the S3 output bucket for Spark results"
  type        = string
}
